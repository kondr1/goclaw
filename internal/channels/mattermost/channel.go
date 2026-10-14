// Package mattermost connects GoClaw agents to a Mattermost server through a
// bot account: inbound posts arrive over the API v4 websocket, replies go out
// through the REST API.
package mattermost

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/safego"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

const (
	pairingDebounceTime   = 60 * time.Second
	maxMessageLen         = 16000 // bytes; server limit is 16383 runes
	userCacheTTL          = 1 * time.Hour
	dedupTTL              = 5 * time.Minute
	mediaMaxBytes         = int64(20 * 1024 * 1024)
	defaultHistoryLimit   = 50
	defaultThreadTTLHours = 24
	eventQueueSize        = 256
)

// Config is the resolved per-instance configuration.
type Config struct {
	ServerURL      string
	BotToken       string
	AllowFrom      []string
	DMPolicy       string
	GroupPolicy    string
	RequireMention *bool
	HistoryLimit   int
	DMStream       *bool
	GroupStream    *bool
	ReactionLevel  string
	BlockReply     *bool
	ChatBehavior   *config.ChatBehaviorConfig
	ThreadTTL      *int
}

// Channel is a Mattermost bot connection.
type Channel struct {
	*channels.BaseChannel
	cfg         Config
	api         *apiClient
	botUserID   string // populated on Start() via /users/me
	botUsername string
	mentionRe   *regexp.Regexp // matches @botUsername; set with the bot identity

	events chan wsEvent

	placeholders   sync.Map // localKey -> placeholder post ID
	dedup          sync.Map // post ID -> time.Time
	threadParticip sync.Map // channelID:particip:rootID -> time.Time (auto-reply without @mention)
	reactions      sync.Map // chatID:postID -> *reactionState

	userCacheMu sync.RWMutex
	userCache   map[string]cachedUser

	threadTTL   time.Duration // 0 = always require @mention in threads
	wg          sync.WaitGroup
	lifecycleMu sync.Mutex
	cancelFn    context.CancelFunc // set by Start, taken by Stop
}

type cachedUser struct {
	displayName string
	username    string
	fetchedAt   time.Time
}

// Compile-time interface assertions.
var _ channels.Channel = (*Channel)(nil)
var _ channels.StreamingChannel = (*Channel)(nil)
var _ channels.ReactionChannel = (*Channel)(nil)
var _ channels.BlockReplyChannel = (*Channel)(nil)
var _ channels.ChatBehaviorChannel = (*Channel)(nil)
var _ channels.PendingCompactable = (*Channel)(nil)

// New creates a Mattermost channel from a resolved config.
func New(cfg Config, msgBus *bus.MessageBus, pairingSvc store.PairingStore, pendingStore store.PendingMessageStore) (*Channel, error) {
	serverURL, err := normalizeServerURL(cfg.ServerURL)
	if err != nil {
		return nil, err
	}
	if cfg.BotToken == "" {
		return nil, fmt.Errorf("mattermost bot_token is required")
	}
	cfg.ServerURL = serverURL

	base := channels.NewBaseChannel(channels.TypeMattermost, msgBus, cfg.AllowFrom)
	base.ValidatePolicy(cfg.DMPolicy, cfg.GroupPolicy)

	requireMention := true
	if cfg.RequireMention != nil {
		requireMention = *cfg.RequireMention
	}

	historyLimit := cfg.HistoryLimit
	if historyLimit == 0 {
		historyLimit = defaultHistoryLimit
	}

	threadTTL := defaultThreadTTLHours * time.Hour
	if cfg.ThreadTTL != nil {
		threadTTL = max(time.Duration(*cfg.ThreadTTL)*time.Hour, 0)
	}

	ch := &Channel{
		BaseChannel: base,
		cfg:         cfg,
		api:         newAPIClient(serverURL, cfg.BotToken),
		events:      make(chan wsEvent, eventQueueSize),
		userCache:   make(map[string]cachedUser),
		threadTTL:   threadTTL,
	}
	ch.SetRequireMention(requireMention)
	ch.SetPairingService(pairingSvc)
	ch.SetGroupHistory(channels.MakeHistory(channels.TypeMattermost, pendingStore, base.TenantID()))
	ch.SetHistoryLimit(historyLimit)
	return ch, nil
}

// Start verifies the token and opens the websocket.
func (c *Channel) Start(ctx context.Context) error {
	c.MarkStarting("Connecting to Mattermost")

	me, err := c.api.getMe(ctx)
	if err != nil {
		kind := channels.ChannelFailureKindNetwork
		if isAuthError(err) {
			kind = channels.ChannelFailureKindAuth
		}
		c.MarkFailed("Cannot reach Mattermost", err.Error(), kind, kind != channels.ChannelFailureKindAuth)
		return fmt.Errorf("mattermost users/me failed: %w", err)
	}
	c.setBotIdentity(me.ID, me.Username)

	c.GroupHistory().StartFlusher()

	runCtx, cancel := context.WithCancel(ctx)
	c.lifecycleMu.Lock()
	c.cancelFn = cancel
	c.lifecycleMu.Unlock()

	c.wg.Add(3) // websocket loop + event worker + periodic sweep

	go func() {
		defer c.wg.Done()
		defer safego.Recover(nil, "component", "mattermost_websocket")
		c.runWebSocket(runCtx)
	}()

	go func() {
		defer c.wg.Done()
		defer safego.Recover(nil, "component", "mattermost_event_worker")
		c.eventWorker(runCtx)
	}()

	go func() {
		defer c.wg.Done()
		defer safego.Recover(nil, "component", "mattermost_sweep")
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				c.sweepMaps()
			}
		}
	}()

	c.SetRunning(true)
	slog.Info("mattermost bot started", "channel", c.Name(), "user_id", c.botUserID, "username", c.botUsername)
	return nil
}

// sweepMaps evicts expired entries from TTL-controlled maps.
func (c *Channel) sweepMaps() {
	now := time.Now()

	c.dedup.Range(func(k, v any) bool {
		if t, ok := v.(time.Time); ok && now.Sub(t) > dedupTTL {
			c.dedup.Delete(k)
		}
		return true
	})

	if c.threadTTL > 0 {
		c.threadParticip.Range(func(k, v any) bool {
			if t, ok := v.(time.Time); ok && now.Sub(t) > c.threadTTL {
				c.threadParticip.Delete(k)
			}
			return true
		})
	}

	c.userCacheMu.Lock()
	for k, v := range c.userCache {
		if now.Sub(v.fetchedAt) > userCacheTTL {
			delete(c.userCache, k)
		}
	}
	c.userCacheMu.Unlock()
}

// Stop closes the websocket and waits for background goroutines.
func (c *Channel) Stop(_ context.Context) error {
	slog.Info("stopping mattermost bot", "channel", c.Name())
	c.SetRunning(false)

	// cancelFn is set only once Start got past /users/me and started the
	// flusher; StopFlusher without a running flusher would block forever.
	// Taking it out under the lock keeps a repeated Stop a no-op.
	c.lifecycleMu.Lock()
	cancel := c.cancelFn
	c.cancelFn = nil
	c.lifecycleMu.Unlock()
	if cancel != nil {
		c.GroupHistory().StopFlusher()
		cancel()
	}

	doneCh := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		slog.Warn("mattermost bot stop timed out after 10s", "channel", c.Name())
	}
	return nil
}

// SetPendingCompaction configures LLM-based auto-compaction for pending messages.
func (c *Channel) SetPendingCompaction(cfg *channels.CompactionConfig) {
	if gh := c.GroupHistory(); gh != nil {
		gh.SetCompactionConfig(cfg)
	}
}

// SetPendingHistoryTenantID propagates tenant_id to the pending history for DB operations.
func (c *Channel) SetPendingHistoryTenantID(id uuid.UUID) {
	if gh := c.GroupHistory(); gh != nil {
		gh.SetTenantID(id)
	}
}

// BlockReplyEnabled returns the per-channel block_reply override.
func (c *Channel) BlockReplyEnabled() *bool { return c.cfg.BlockReply }

// ChatBehaviorConfig returns the per-channel chat_behavior override.
func (c *Channel) ChatBehaviorConfig() *config.ChatBehaviorConfig { return c.cfg.ChatBehavior }

// resolveUser fetches and caches the display name and username for a user ID.
// Falls back to the raw ID when the lookup fails.
func (c *Channel) resolveUser(ctx context.Context, userID string) (displayName, username string) {
	c.userCacheMu.RLock()
	cu, found := c.userCache[userID]
	c.userCacheMu.RUnlock()
	if found && time.Since(cu.fetchedAt) < userCacheTTL {
		return cu.displayName, cu.username
	}

	u, err := c.api.getUser(ctx, userID)
	if err != nil {
		slog.Debug("mattermost: failed to resolve user", "user_id", userID, "error", err)
		return userID, ""
	}

	cu = cachedUser{displayName: u.displayName(), username: u.Username, fetchedAt: time.Now()}
	c.userCacheMu.Lock()
	c.userCache[userID] = cu
	c.userCacheMu.Unlock()
	return cu.displayName, cu.username
}
