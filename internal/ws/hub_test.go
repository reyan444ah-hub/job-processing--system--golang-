package ws_test

import (
	"context"
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
		ws.ServeWs(hub, authSvc, w, r)
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
	user := &models.User{
		ID:             userID,
		OrganizationID: orgID,
		Email:          "ws_user@example.com",
		Role:           "member",
	}

	// Sign valid token using authService
	validToken, err := authSvc.GenerateAccessToken(user)
	require.NoError(t, err)

	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL+"?token="+validToken, nil)
	require.NoError(t, err)
	defer wsConn.Close()

	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	// Read initial connected acknowledgement
	_, msg, err := wsConn.ReadMessage()
	assert.NoError(t, err)
	assert.Contains(t, string(msg), "connected")
	assert.Contains(t, string(msg), orgID.String())
}
