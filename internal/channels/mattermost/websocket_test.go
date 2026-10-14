package mattermost

import (
	"context"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// serveAuth reads the authentication challenge and answers with status.
func serveAuth(t *testing.T, conn *websocket.Conn, status string) bool {
	t.Helper()
	var challenge struct {
		Seq    int64             `json:"seq"`
		Action string            `json:"action"`
		Data   map[string]string `json:"data"`
	}
	if err := conn.ReadJSON(&challenge); err != nil {
		return false
	}
	if challenge.Action != "authentication_challenge" || challenge.Seq != wsAuthSeq {
		t.Errorf("unexpected first frame: %+v", challenge)
		return false
	}
	if challenge.Data["token"] != testToken {
		status = "FAIL"
	}
	_ = conn.WriteJSON(map[string]any{"status": status, "seq_reply": challenge.Seq})
	return status == "OK"
}

func TestConnectAndServe_DeliversPostedEvents(t *testing.T) {
	fs := newFakeServer(t)
	ch, _ := newTestChannel(t, fs, nil)

	done := make(chan struct{})
	fs.wsHandler = func(conn *websocket.Conn) {
		if !serveAuth(t, conn, "OK") {
			return
		}
		_ = conn.WriteJSON(map[string]any{"event": "hello", "data": map[string]any{}})
		_ = conn.WriteJSON(postedEvent(t, mmPost{ID: "p1", UserID: "u1", ChannelID: "dm1", Message: "hi"}, "D", nil))
		<-done
	}
	defer close(done)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan bool, 1)
	go func() {
		authenticated, _ := ch.connectAndServe(ctx)
		result <- authenticated
	}()

	select {
	case ev := <-ch.events:
		post, err := parsePostedEvent(ev)
		if err != nil || post.ID != "p1" {
			t.Fatalf("unexpected event: %+v, %v", ev, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("posted event was not delivered")
	}

	cancel()
	select {
	case authenticated := <-result:
		if !authenticated {
			t.Fatal("session must report successful authentication")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connectAndServe did not return after cancel")
	}
}

func TestConnectAndServe_RejectedTokenIsAuthError(t *testing.T) {
	fs := newFakeServer(t)
	ch, _ := newTestChannel(t, fs, func(c *Config) { c.BotToken = "wrong" })

	fs.wsHandler = func(conn *websocket.Conn) {
		serveAuth(t, conn, "OK")
		// Keep the connection open until the client hangs up.
		_, _, _ = conn.ReadMessage()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	authenticated, err := ch.connectAndServe(ctx)
	if authenticated || !isAuthError(err) {
		t.Fatalf("authenticated=%v err=%v, want auth error", authenticated, err)
	}
}
