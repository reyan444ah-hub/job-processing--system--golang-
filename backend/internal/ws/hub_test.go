package ws_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"distributed-job-system/internal/config"
	"distributed-job-system/internal/models"
	rdb "distributed-job-system/internal/redis"
	"distributed-job-system/internal/service/auth"
	"distributed-job-system/internal/ws"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocketHub_ConnectionAndAuth(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient, err := rdb.Connect(mr.Addr(), "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	cfg := &config.Config{
		JWTSecret:       "websocket-hub-test-secret-key-12345",
		JWTAccessExpiry: 15 * time.Minute,
	}
	authSvc := auth.NewAuthService(nil, cfg)

	hub := ws.NewHub(redisClient.Client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Run(ctx)

	// Create test HTTP server serving WebSocket endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWsWithOrigins(hub, authSvc, []string{"https://trusted.example"}, w, r)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// 1. Connection without token -> must be rejected 401
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	assert.Error(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// 2. Connection with valid JWT token -> accepted & receive initial connected event
	orgID := uuid.New()
	userID := uuid.New()
	memberToken, err := authSvc.GenerateAccessToken(&models.User{
		ID:             userID,
		OrganizationID: orgID,
		Email:          "member@example.com",
		Role:           "member",
	})
	require.NoError(t, err)
	memberConn, resp, err := websocket.DefaultDialer.Dial(wsURL+"?token="+memberToken, nil)
	require.NoError(t, err)
	defer memberConn.Close()
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	_, msg, err := memberConn.ReadMessage()
	require.NoError(t, err)
	assert.Contains(t, string(msg), "connected")

	user := &models.User{
		ID:             userID,
		OrganizationID: orgID,
		Email:          "ws_user@example.com",
		Role:           "admin",
	}

	// Sign valid token using authService
	validToken, err := authSvc.GenerateAccessToken(user)
	require.NoError(t, err)

	evilOrigin := http.Header{"Origin": []string{"https://attacker.example"}}
	_, resp, err = websocket.DefaultDialer.Dial(wsURL+"?token="+validToken, evilOrigin)
	assert.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL+"?token="+validToken, nil)
	require.NoError(t, err)
	defer wsConn.Close()

	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	// Read initial connected acknowledgement
	_, msg, err = wsConn.ReadMessage()
	assert.NoError(t, err)
	assert.Contains(t, string(msg), "connected")
	assert.Contains(t, string(msg), orgID.String())

	otherUser := &models.User{
		ID:             uuid.New(),
		OrganizationID: orgID,
		Email:          "other-member@example.com",
		Role:           "member",
	}
	otherToken, err := authSvc.GenerateAccessToken(otherUser)
	require.NoError(t, err)
	otherConn, _, err := websocket.DefaultDialer.Dial(wsURL+"?token="+otherToken, nil)
	require.NoError(t, err)
	defer otherConn.Close()
	_, _, err = otherConn.ReadMessage()
	require.NoError(t, err)

	jobID := uuid.New()
	completion, err := json.Marshal(map[string]interface{}{
		"event_type":        "job.completed",
		"aggregate_id":      jobID.String(),
		"recipient_user_id": userID,
		"payload":           map[string]interface{}{"job_type": "email.send"},
	})
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, redisClient.Client.Publish(context.Background(), "events:org:"+orgID.String(), completion).Err())

	_ = memberConn.SetReadDeadline(time.Now().Add(time.Second))
	_, msg, err = memberConn.ReadMessage()
	require.NoError(t, err)
	assert.Contains(t, string(msg), "job.completed")
	assert.Contains(t, string(msg), "email.send")

	_ = wsConn.SetReadDeadline(time.Now().Add(time.Second))
	_, msg, err = wsConn.ReadMessage()
	require.NoError(t, err)
	assert.Contains(t, string(msg), "job.completed")

	_ = otherConn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	_, _, err = otherConn.ReadMessage()
	require.Error(t, err, "a different member must not receive this user's completion notification")
}
