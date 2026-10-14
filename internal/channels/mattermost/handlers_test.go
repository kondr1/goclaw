package mattermost

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

const shortWait = 200 * time.Millisecond

func TestHandlePosted_DMPublishesInbound(t *testing.T) {
	fs := newFakeServer(t)
	ch, msgBus := newTestChannel(t, fs, nil)

	ch.handlePostedEvent(context.Background(), postedEvent(t, mmPost{
		ID: "p1", UserID: "u1", ChannelID: "dm1", Message: "hello",
	}, "D", nil))

	msg, ok := consumeInbound(t, msgBus, time.Second)
	if !ok {
		t.Fatal("expected inbound message")
	}
	if msg.Content != "hello" || msg.PeerKind != "direct" || msg.ChatID != "dm1" || msg.SenderID != "u1" {
		t.Fatalf("unexpected inbound: %+v", msg)
	}
	if msg.Metadata["local_key"] != "dm1" || msg.Metadata["message_thread_id"] != "" {
		t.Fatalf("DM must stay flat, got metadata %v", msg.Metadata)
	}
	if msg.Metadata["display_name"] != "Alice Smith" || msg.Metadata["username"] != "alice" {
		t.Fatalf("unexpected sender names: %v", msg.Metadata)
	}

	placeholders := fs.callsTo("POST", "/api/v4/posts")
	if len(placeholders) != 1 || placeholders[0].Body["message"] != placeholderText {
		t.Fatalf("expected one placeholder post, got %+v", placeholders)
	}
	if pID, ok := ch.placeholders.Load("dm1"); !ok || pID == "" {
		t.Fatal("placeholder not stored under local key")
	}
}

func TestHandlePosted_GroupRecordsHistoryUntilMentioned(t *testing.T) {
	fs := newFakeServer(t)
	ch, msgBus := newTestChannel(t, fs, nil)

	ch.handlePostedEvent(context.Background(), postedEvent(t, mmPost{
		ID: "p1", UserID: "u1", ChannelID: "c1", Message: "deploy failed again",
	}, "O", nil))
	if _, ok := consumeInbound(t, msgBus, shortWait); ok {
		t.Fatal("un-mentioned channel post must not reach the agent")
	}

	ch.handlePostedEvent(context.Background(), postedEvent(t, mmPost{
		ID: "p2", UserID: "u1", ChannelID: "c1", Message: "@goclawbot why?",
	}, "O", nil))

	msg, ok := consumeInbound(t, msgBus, time.Second)
	if !ok {
		t.Fatal("expected inbound message after mention")
	}
	if msg.PeerKind != "group" {
		t.Fatalf("PeerKind = %q, want group", msg.PeerKind)
	}
	for _, want := range []string{"deploy failed again", "[From: Alice Smith]", "why?"} {
		if !strings.Contains(msg.Content, want) {
			t.Fatalf("content missing %q:\n%s", want, msg.Content)
		}
	}
	if strings.Contains(msg.Content, "@goclawbot") {
		t.Fatalf("bot mention must be stripped:\n%s", msg.Content)
	}
	if msg.Metadata["local_key"] != "c1:thread:p2" || msg.Metadata["message_thread_id"] != "p2" {
		t.Fatalf("channel reply must open a thread on the trigger post, got %v", msg.Metadata)
	}
	if msg.Metadata[tools.MetaChatTitle] != "Town Square" {
		t.Fatalf("chat title not propagated: %v", msg.Metadata)
	}

	placeholders := fs.callsTo("POST", "/api/v4/posts")
	if len(placeholders) != 1 || placeholders[0].Body["root_id"] != "p2" {
		t.Fatalf("placeholder must be posted in the thread, got %+v", placeholders)
	}
}

func TestHandlePosted_MentionFromServerList(t *testing.T) {
	fs := newFakeServer(t)
	ch, msgBus := newTestChannel(t, fs, nil)

	ch.handlePostedEvent(context.Background(), postedEvent(t, mmPost{
		ID: "p1", UserID: "u1", ChannelID: "c1", Message: "@here status?",
	}, "P", []string{testBotID}))

	if _, ok := consumeInbound(t, msgBus, time.Second); !ok {
		t.Fatal("mention listed by the server must trigger the bot")
	}
}

func TestHandlePosted_SkipsOwnSystemBotAndDuplicatePosts(t *testing.T) {
	fs := newFakeServer(t)
	ch, msgBus := newTestChannel(t, fs, nil)
	ctx := context.Background()

	ch.handlePostedEvent(ctx, postedEvent(t, mmPost{ID: "p1", UserID: testBotID, ChannelID: "dm1", Message: "own"}, "D", nil))
	ch.handlePostedEvent(ctx, postedEvent(t, mmPost{ID: "p2", UserID: "u1", ChannelID: "dm1", Message: "joined", Type: "system_join_channel"}, "D", nil))
	ch.handlePostedEvent(ctx, postedEvent(t, mmPost{ID: "p3", UserID: "u2", ChannelID: "dm1", Message: "beep", Props: map[string]any{"from_bot": "true"}}, "D", nil))
	if _, ok := consumeInbound(t, msgBus, shortWait); ok {
		t.Fatal("own, system and bot posts must be ignored")
	}

	dup := postedEvent(t, mmPost{ID: "p4", UserID: "u1", ChannelID: "dm1", Message: "once"}, "D", nil)
	ch.handlePostedEvent(ctx, dup)
	ch.handlePostedEvent(ctx, dup)
	if _, ok := consumeInbound(t, msgBus, time.Second); !ok {
		t.Fatal("expected first copy to be delivered")
	}
	if _, ok := consumeInbound(t, msgBus, shortWait); ok {
		t.Fatal("replayed post must be deduplicated")
	}
}

func TestHandlePosted_ThreadParticipationAndRootContext(t *testing.T) {
	fs := newFakeServer(t)
	ch, msgBus := newTestChannel(t, fs, nil)
	ctx := context.Background()
	fs.addPost(&mmPost{ID: "r1", UserID: "u1", ChannelID: "c1", Message: "@goclawbot summarize the incident"})

	ch.handlePostedEvent(ctx, postedEvent(t, mmPost{ID: "r1", UserID: "u1", ChannelID: "c1", Message: "@goclawbot summarize the incident"}, "O", nil))
	if _, ok := consumeInbound(t, msgBus, time.Second); !ok {
		t.Fatal("expected mention to be delivered")
	}

	// Follow-up in the bot's thread without a mention.
	ch.handlePostedEvent(ctx, postedEvent(t, mmPost{ID: "p2", UserID: "u1", ChannelID: "c1", RootID: "r1", Message: "and the root cause?"}, "O", nil))
	msg, ok := consumeInbound(t, msgBus, time.Second)
	if !ok {
		t.Fatal("reply in a participated thread must not require a mention")
	}
	if msg.Metadata["local_key"] != "c1:thread:r1" {
		t.Fatalf("thread reply must share the thread session, got %v", msg.Metadata)
	}
	if !strings.Contains(msg.Content, "[Replying to Alice Smith]") || !strings.Contains(msg.Content, "summarize the incident") {
		t.Fatalf("thread root context missing:\n%s", msg.Content)
	}
}

func TestHandlePosted_ThreadTTLDisabledRequiresMention(t *testing.T) {
	fs := newFakeServer(t)
	zero := 0
	ch, msgBus := newTestChannel(t, fs, func(c *Config) { c.ThreadTTL = &zero })
	ctx := context.Background()

	ch.handlePostedEvent(ctx, postedEvent(t, mmPost{ID: "r1", UserID: "u1", ChannelID: "c1", Message: "@goclawbot hi"}, "O", nil))
	if _, ok := consumeInbound(t, msgBus, time.Second); !ok {
		t.Fatal("expected mention to be delivered")
	}
	ch.handlePostedEvent(ctx, postedEvent(t, mmPost{ID: "p2", UserID: "u1", ChannelID: "c1", RootID: "r1", Message: "more"}, "O", nil))
	if _, ok := consumeInbound(t, msgBus, shortWait); ok {
		t.Fatal("thread_ttl=0 must always require a mention")
	}
}

func TestHandlePosted_AllowlistAcceptsChannelID(t *testing.T) {
	fs := newFakeServer(t)
	ch, msgBus := newTestChannel(t, fs, func(c *Config) {
		c.GroupPolicy = "allowlist"
		c.AllowFrom = []string{"c-allowed"}
	})
	ctx := context.Background()

	ch.handlePostedEvent(ctx, postedEvent(t, mmPost{ID: "p1", UserID: "u1", ChannelID: "c-other", Message: "@goclawbot hi"}, "O", nil))
	if _, ok := consumeInbound(t, msgBus, shortWait); ok {
		t.Fatal("channel outside the allowlist must be rejected")
	}
	ch.handlePostedEvent(ctx, postedEvent(t, mmPost{ID: "p2", UserID: "u1", ChannelID: "c-allowed", Message: "@goclawbot hi"}, "O", nil))
	if _, ok := consumeInbound(t, msgBus, time.Second); !ok {
		t.Fatal("allowlisted channel must be accepted")
	}
}

func TestHandlePosted_GroupPairingUsesChannelTemplate(t *testing.T) {
	fs := newFakeServer(t)
	ch, msgBus := newTestChannel(t, fs, func(c *Config) { c.GroupPolicy = "pairing" })
	ps := &fakePairingStore{}
	ch.SetPairingService(ps)

	ch.handlePostedEvent(context.Background(), postedEvent(t, mmPost{ID: "p1", UserID: "u1", ChannelID: "c1", Message: "@goclawbot hi"}, "O", nil))
	if _, ok := consumeInbound(t, msgBus, shortWait); ok {
		t.Fatal("unpaired channel must not reach the agent")
	}
	if len(ps.requests) != 1 || ps.requests[0] != "group:c1" {
		t.Fatalf("expected group pairing request, got %v", ps.requests)
	}
	posts := fs.callsTo("POST", "/api/v4/posts")
	if len(posts) != 1 {
		t.Fatalf("expected one pairing reply, got %d", len(posts))
	}
	if !strings.Contains(posts[0].Body["message"].(string), "This channel is not authorized") {
		t.Fatalf("channel must get the group approval template, got %q", posts[0].Body["message"])
	}
}

func TestHandlePosted_DownloadsAttachments(t *testing.T) {
	fs := newFakeServer(t)
	ch, msgBus := newTestChannel(t, fs, nil)
	fs.addFile("f1", []byte("quarterly numbers"))

	ch.handlePostedEvent(context.Background(), postedEvent(t, mmPost{
		ID: "p1", UserID: "u1", ChannelID: "dm1", Message: "see attached",
		Metadata: &mmPostMetadata{Files: []mmFileInfo{{ID: "f1", Name: "notes.txt", Size: 17, MimeType: "text/plain"}}},
	}, "D", nil))

	msg, ok := consumeInbound(t, msgBus, time.Second)
	if !ok {
		t.Fatal("expected inbound message")
	}
	if len(msg.Media) != 1 || msg.Media[0].MimeType != "text/plain" || msg.Media[0].Filename != "notes.txt" {
		t.Fatalf("unexpected media: %+v", msg.Media)
	}
	for _, want := range []string{`<media:document name="notes.txt">`, "quarterly numbers", "see attached"} {
		if !strings.Contains(msg.Content, want) {
			t.Fatalf("content missing %q:\n%s", want, msg.Content)
		}
	}
}

func TestMentionMatching(t *testing.T) {
	ch := &Channel{}
	ch.setBotIdentity(testBotID, testBotUsername)

	cases := []struct {
		text string
		want bool
	}{
		{"@goclawbot hi", true},
		{"hi @GoClawBot.", true},
		{"(@goclawbot)", true},
		{"@goclawbot2 hi", false},
		{"@goclawbot_x hi", false},
		{"mail a@goclawbot.com", false},
		{"no mention", false},
	}
	for _, tc := range cases {
		if got := ch.isBotMentioned(tc.text, nil); got != tc.want {
			t.Errorf("isBotMentioned(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}

	if got := ch.stripBotMention("hey @goclawbot, check this"); got != "hey , check this" {
		t.Errorf("stripBotMention = %q", got)
	}
}
