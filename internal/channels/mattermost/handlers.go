package mattermost

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/channels/media"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/systemmessages"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

const placeholderText = "Thinking..."

// threadKey builds the composite local key for a thread. The ":thread:"
// suffix is what the gateway consumer uses for per-thread session isolation.
func threadKey(channelID, rootID string) string {
	return channelID + ":thread:" + rootID
}

func participKey(channelID, rootID string) string {
	return channelID + ":particip:" + rootID
}

func (c *Channel) handlePostedEvent(ctx context.Context, ev wsEvent) {
	post, err := parsePostedEvent(ev)
	if err != nil {
		slog.Debug("mattermost: skip malformed posted event", "error", err)
		return
	}

	// Own posts, system posts (joins, header changes, …) and other bots.
	if post.UserID == "" || post.UserID == c.botUserID || post.Type != "" || post.fromBot() {
		return
	}

	// Websocket reconnects may replay recent events.
	if _, loaded := c.dedup.LoadOrStore(post.ID, time.Now()); loaded {
		return
	}

	ctx = store.WithTenantID(ctx, c.TenantID())
	channelID := post.ChannelID
	senderID := post.UserID

	isDM := ev.dataString("channel_type") == "D"
	peerKind := "group"
	if isDM {
		peerKind = "direct"
	}

	displayName, username := c.resolveUser(ctx, senderID)

	if isDM {
		if !c.checkDMPolicy(ctx, senderID, channelID) {
			return
		}
	} else if !c.checkGroupPolicy(ctx, senderID, channelID) {
		return
	}

	content := post.Message

	var items []media.MediaInfo
	if fileItems, docContent := c.resolveMedia(ctx, post); len(fileItems) > 0 || docContent != "" {
		items = append(items, fileItems...)
		content = joinNonEmpty(content, docContent)
	}

	// Thread replies carry the root post as context, like a quoted reply.
	if post.RootID != "" {
		rootCtx, rootItems := c.fetchThreadRootContext(ctx, post.RootID)
		content = joinNonEmpty(rootCtx, content)
		items = append(rootItems, items...)
	}

	var mediaPaths []string
	var mediaFiles []bus.MediaFile
	if len(items) > 0 {
		content = joinNonEmpty(media.BuildMediaTags(items), content)
		for _, it := range items {
			if it.FilePath == "" {
				continue
			}
			mediaPaths = append(mediaPaths, it.FilePath)
			mediaFiles = append(mediaFiles, bus.MediaFile{Path: it.FilePath, MimeType: it.ContentType, Filename: it.FileName})
		}
	}

	if strings.TrimSpace(content) == "" {
		return
	}

	// historyKey groups un-mentioned messages: the channel itself for
	// top-level posts, the thread for replies.
	historyKey := channelID
	parentHistoryKey := ""
	if post.RootID != "" {
		historyKey = threadKey(channelID, post.RootID)
		parentHistoryKey = channelID
	}

	// In channels the bot always answers in a thread, so the whole exchange
	// lives in one per-thread session. DMs stay flat unless the user threads.
	replyRootID := post.RootID
	if !isDM && replyRootID == "" {
		replyRootID = post.ID
	}
	localKey := channelID
	if replyRootID != "" {
		localKey = threadKey(channelID, replyRootID)
	}

	if !isDM && c.RequireMention() {
		mentioned := c.isBotMentioned(post.Message, eventMentions(ev))

		if !mentioned && post.RootID != "" && c.threadTTL > 0 {
			key := participKey(channelID, post.RootID)
			if last, ok := c.threadParticip.Load(key); ok {
				if time.Since(last.(time.Time)) < c.threadTTL {
					mentioned = true
				} else {
					c.threadParticip.Delete(key)
				}
			}
		}

		if !mentioned {
			c.GroupHistory().Record(historyKey, channels.HistoryEntry{
				Sender:           displayName,
				SenderID:         senderID,
				Body:             content,
				ParentHistoryKey: parentHistoryKey,
				Media:            mediaPaths,
				Timestamp:        time.Now(),
				MessageID:        post.ID,
			}, c.HistoryLimit())

			if cc := c.ContactCollector(); cc != nil {
				cc.EnsureContact(ctx, c.Type(), c.Name(), senderID, senderID, displayName, username, "group", "user", "", "")
			}
			return
		}
	}

	content = strings.TrimSpace(c.stripBotMention(content))
	if content == "" {
		return
	}

	slog.Debug("mattermost message received",
		"sender_id", senderID, "channel_id", channelID,
		"is_dm", isDM, "preview", channels.Truncate(content, 50))

	if placeholder, err := c.api.createPost(ctx, &mmPost{
		ChannelID: channelID,
		RootID:    replyRootID,
		Message:   placeholderText,
	}); err == nil {
		c.placeholders.Store(localKey, placeholder.ID)
	} else {
		slog.Debug("mattermost: placeholder post failed", "channel_id", channelID, "error", err)
	}

	finalContent := content
	if peerKind == "group" {
		annotated := fmt.Sprintf("[From: %s]\n%s", displayName, content)
		finalContent = annotated
		if c.HistoryLimit() > 0 {
			for _, p := range c.GroupHistory().CollectMedia(historyKey) {
				mediaFiles = append(mediaFiles, bus.MediaFile{Path: p, MimeType: media.DetectMIMEType(p)})
			}
			finalContent = c.GroupHistory().BuildContext(historyKey, annotated, c.HistoryLimit())
		}
	}

	metadata := map[string]string{
		"message_id":      post.ID,
		"user_id":         senderID,
		"username":        username,
		"display_name":    channels.SanitizeDisplayName(displayName),
		"channel_id":      channelID,
		"is_dm":           fmt.Sprintf("%t", isDM),
		"local_key":       localKey,
		"placeholder_key": localKey,
	}
	if replyRootID != "" {
		metadata["message_thread_id"] = replyRootID
	}
	if title := ev.dataString("channel_display_name"); title != "" && !isDM {
		metadata[tools.MetaChatTitle] = title
	}

	c.HandleAuthorizedMessageMedia(senderID, channelID, finalContent, mediaFiles, metadata, peerKind)

	if peerKind == "group" {
		c.threadParticip.Store(participKey(channelID, replyRootID), time.Now())
		c.GroupHistory().Clear(historyKey)
	}
}

// fetchThreadRootContext returns the thread root post as reply context plus
// any media attached to it.
func (c *Channel) fetchThreadRootContext(ctx context.Context, rootID string) (string, []media.MediaInfo) {
	root, err := c.api.getPost(ctx, rootID)
	if err != nil {
		slog.Debug("mattermost: failed to fetch thread root", "root_id", rootID, "error", err)
		return "", nil
	}

	var replyCtx string
	if root.Message != "" {
		name := "bot"
		if root.UserID != c.botUserID {
			name, _ = c.resolveUser(ctx, root.UserID)
		}
		replyCtx = fmt.Sprintf("[Replying to %s]\n%s\n[/Replying]", name, channels.Truncate(root.Message, 500))
	}

	items, docContent := c.resolveMedia(ctx, root)
	for i := range items {
		items[i].FromReply = true
	}
	return joinNonEmpty(replyCtx, docContent), items
}

// isBotMentioned reports whether the post mentions the bot, either through the
// server-computed mention list or a literal @username in the text.
func (c *Channel) isBotMentioned(text string, mentionIDs []string) bool {
	if slices.Contains(mentionIDs, c.botUserID) {
		return true
	}
	return c.mentionRe != nil && c.mentionRe.MatchString(text)
}

// stripBotMention removes @botusername from the text.
func (c *Channel) stripBotMention(text string) string {
	if c.mentionRe == nil {
		return text
	}
	return c.mentionRe.ReplaceAllString(text, "${1}${2}")
}

// setBotIdentity records the bot account and compiles the @username matcher.
// Mattermost usernames may contain letters, digits, '.', '-' and '_'; a
// trailing '.' is treated as punctuation.
func (c *Channel) setBotIdentity(userID, username string) {
	c.botUserID = userID
	c.botUsername = username
	c.mentionRe = nil
	if username != "" {
		c.mentionRe = regexp.MustCompile(`(?i)(^|[^a-z0-9._-])@` + regexp.QuoteMeta(username) + `($|[^a-z0-9_-])`)
	}
}

func joinNonEmpty(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	default:
		return a + "\n\n" + b
	}
}

// --- Policy checks ---

func (c *Channel) checkDMPolicy(ctx context.Context, senderID, channelID string) bool {
	switch c.CheckDMPolicy(ctx, senderID, c.cfg.DMPolicy) {
	case channels.PolicyAllow:
		return true
	case channels.PolicyNeedsPairing:
		c.sendPairingReply(ctx, senderID, channelID)
		return false
	default:
		return false
	}
}

func (c *Channel) checkGroupPolicy(ctx context.Context, senderID, channelID string) bool {
	groupPolicy := c.cfg.GroupPolicy
	if groupPolicy == "" {
		groupPolicy = "open"
	}

	// Allowlist accepts either the sender or the whole Mattermost channel.
	if groupPolicy == "allowlist" {
		if !c.HasAllowList() {
			return false
		}
		return c.IsAllowed(senderID) || c.IsAllowed(channelID)
	}

	switch c.CheckGroupPolicy(ctx, senderID, channelID, groupPolicy) {
	case channels.PolicyAllow:
		return true
	case channels.PolicyNeedsPairing:
		c.sendPairingReply(ctx, "group:"+channelID, channelID)
		return false
	default:
		return false
	}
}

func (c *Channel) sendPairingReply(ctx context.Context, senderID, channelID string) {
	ps := c.PairingService()
	if ps == nil {
		return
	}
	if !c.CanSendPairingNotif(senderID, pairingDebounceTime) {
		return
	}

	code, err := ps.RequestPairing(ctx, senderID, c.Name(), channelID, "default", nil)
	if err != nil {
		slog.Warn("mattermost: failed to request pairing code", "error", err)
		return
	}

	// Channels get the admin-approval template instead of the DM pairing prompt.
	var msg string
	if strings.HasPrefix(senderID, "group:") {
		msg = c.SystemMessage("", systemmessages.KeyPairingGroupPrivateRequired, systemmessages.Vars{
			"platform": "Mattermost",
			"code":     code,
		})
	} else {
		msg = c.SystemMessage("", systemmessages.KeyPairingAccountRequired, systemmessages.Vars{
			"platform":  "Mattermost",
			"sender_id": senderID,
			"code":      code,
		})
	}
	if _, err := c.api.createPost(ctx, &mmPost{ChannelID: channelID, Message: msg}); err != nil {
		slog.Warn("mattermost: failed to send pairing reply", "channel_id", channelID, "error", err)
		return
	}
	c.MarkPairingNotifSent(senderID)
}
