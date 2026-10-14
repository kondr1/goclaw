package mattermost

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
)

// maxFilesPerPost is the server limit on file_ids per post.
const maxFilesPerPost = 10

// Send delivers an outbound message. The first text chunk replaces the
// "Thinking..." placeholder; attachments follow as a separate post.
func (c *Channel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	if !c.IsRunning() {
		return fmt.Errorf("mattermost bot not running")
	}

	channelID := msg.ChatID
	if channelID == "" {
		return fmt.Errorf("empty chat ID for mattermost send")
	}

	placeholderKey := channelID
	if pk := msg.Metadata["placeholder_key"]; pk != "" {
		placeholderKey = pk
	}
	rootID := msg.Metadata["message_thread_id"]

	// Placeholder update (LLM retry notification) keeps the placeholder.
	if msg.Metadata["placeholder_update"] == "true" {
		if pID, ok := c.placeholders.Load(placeholderKey); ok {
			if err := c.api.patchPost(ctx, pID.(string), msg.Content); err != nil {
				slog.Debug("mattermost: placeholder update failed", "error", err)
			}
		}
		return nil
	}
	defer c.stopTyping(placeholderKey)

	fileIDs, failed := c.uploadMedia(ctx, channelID, msg.Media)
	chunks := channels.ChunkMarkdown(msg.Content, maxMessageLen)
	for _, name := range failed {
		chunks = append(chunks, fmt.Sprintf("[File upload failed: %s]", name))
	}

	if pID, ok := c.placeholders.LoadAndDelete(placeholderKey); ok {
		placeholderID := pID.(string)
		switch {
		case len(chunks) == 0:
			// NO_REPLY or attachments only: the placeholder has nothing to show.
			if err := c.api.deletePost(ctx, placeholderID); err != nil {
				slog.Debug("mattermost: placeholder delete failed", "error", err)
			}
		default:
			if err := c.api.patchPost(ctx, placeholderID, chunks[0]); err == nil {
				chunks = chunks[1:]
			} else {
				slog.Warn("mattermost: placeholder edit failed, sending new post",
					"channel_id", channelID, "error", err)
			}
		}
	}

	for _, chunk := range chunks {
		if _, err := c.api.createPost(ctx, &mmPost{ChannelID: channelID, RootID: rootID, Message: chunk}); err != nil {
			return fmt.Errorf("send mattermost post: %w", err)
		}
	}

	if len(fileIDs) > 0 {
		caption := mediaCaption(msg.Media)
		for start := 0; start < len(fileIDs); start += maxFilesPerPost {
			batch := fileIDs[start:min(start+maxFilesPerPost, len(fileIDs))]
			post := &mmPost{ChannelID: channelID, RootID: rootID, Message: caption, FileIDs: batch}
			if _, err := c.api.createPost(ctx, post); err != nil {
				return fmt.Errorf("send mattermost attachments: %w", err)
			}
			caption = ""
		}
	}
	return nil
}

// uploadMedia uploads local attachments and returns their file IDs plus the
// names of files that could not be uploaded.
func (c *Channel) uploadMedia(ctx context.Context, channelID string, attachments []bus.MediaAttachment) (fileIDs, failed []string) {
	for _, m := range attachments {
		name := filepath.Base(m.URL)
		id, err := c.uploadLocalFile(ctx, channelID, m.URL)
		if err != nil {
			slog.Warn("mattermost: file upload failed", "file", m.URL, "error", err)
			failed = append(failed, name)
			continue
		}
		fileIDs = append(fileIDs, id)
	}
	return fileIDs, failed
}

func (c *Channel) uploadLocalFile(ctx context.Context, channelID, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return c.api.uploadFile(ctx, channelID, filepath.Base(path), f)
}

func mediaCaption(attachments []bus.MediaAttachment) string {
	var captions []string
	for _, m := range attachments {
		if s := strings.TrimSpace(m.Caption); s != "" {
			captions = append(captions, s)
		}
	}
	return strings.Join(captions, "\n")
}
