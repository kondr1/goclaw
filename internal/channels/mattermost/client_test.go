package mattermost

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestNormalizeServerURL(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "https://chat.example.com/", want: "https://chat.example.com"},
		{in: " http://10.0.0.5:8065 ", want: "http://10.0.0.5:8065"},
		{in: "https://example.com/mattermost/", want: "https://example.com/mattermost"},
		{in: "", wantErr: true},
		{in: "chat.example.com", wantErr: true},
		{in: "ftp://chat.example.com", wantErr: true},
		{in: "https://chat.example.com/?x=1", wantErr: true},
	}
	for _, tc := range cases {
		got, err := normalizeServerURL(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("normalizeServerURL(%q) = %q, want error", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("normalizeServerURL(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
}

func TestWebsocketURL(t *testing.T) {
	cases := map[string]string{
		"https://chat.example.com":       "wss://chat.example.com/api/v4/websocket",
		"http://localhost:8065":          "ws://localhost:8065/api/v4/websocket",
		"https://example.com/mattermost": "wss://example.com/mattermost/api/v4/websocket",
	}
	for in, want := range cases {
		if got := newAPIClient(in, "t").websocketURL(); got != want {
			t.Errorf("websocketURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAPIClient_AuthErrorIsClassified(t *testing.T) {
	fs := newFakeServer(t)

	_, err := newAPIClient(fs.URL, "wrong").getMe(context.Background())
	if err == nil || !isAuthError(err) {
		t.Fatalf("expected auth error, got %v", err)
	}

	me, err := newAPIClient(fs.URL, testToken).getMe(context.Background())
	if err != nil || me.ID != testBotID || me.Username != testBotUsername {
		t.Fatalf("getMe = %+v, %v", me, err)
	}
}

func TestAPIClient_DownloadRejectsOversizedFile(t *testing.T) {
	fs := newFakeServer(t)
	fs.addFile("f1", bytes.Repeat([]byte("x"), 64))
	api := newAPIClient(fs.URL, testToken)

	var buf bytes.Buffer
	if err := api.downloadFile(context.Background(), "f1", &buf, 32); err == nil {
		t.Fatal("expected size limit error")
	}

	buf.Reset()
	if err := api.downloadFile(context.Background(), "f1", &buf, 64); err != nil || buf.Len() != 64 {
		t.Fatalf("download at the limit = %d bytes, %v", buf.Len(), err)
	}
}

func TestParsePostedEvent(t *testing.T) {
	post := mmPost{ID: "p1", UserID: "u1", ChannelID: "c1", RootID: "r1", Message: "hi"}
	ev := postedEvent(t, post, "O", []string{"bot1", "u9"})

	got, err := parsePostedEvent(ev)
	if err != nil {
		t.Fatalf("parsePostedEvent: %v", err)
	}
	if got.ID != "p1" || got.RootID != "r1" || got.Message != "hi" {
		t.Fatalf("unexpected post: %+v", got)
	}
	if ids := eventMentions(ev); len(ids) != 2 || ids[0] != "bot1" {
		t.Fatalf("eventMentions = %v", ids)
	}

	if _, err := parsePostedEvent(wsEvent{Event: "posted"}); err == nil {
		t.Fatal("expected error for event without post")
	}
}

func TestPostFromBot(t *testing.T) {
	var p mmPost
	if err := json.Unmarshal([]byte(`{"props":{"from_bot":"true"}}`), &p); err != nil {
		t.Fatal(err)
	}
	if !p.fromBot() {
		t.Fatal("string from_bot must be detected")
	}
	if (&mmPost{Props: map[string]any{"from_bot": true}}).fromBot() != true {
		t.Fatal("bool from_bot must be detected")
	}
	if (&mmPost{}).fromBot() {
		t.Fatal("post without props is not from a bot")
	}
}
