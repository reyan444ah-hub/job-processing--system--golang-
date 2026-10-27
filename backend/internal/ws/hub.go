package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"distributed-job-system/internal/service/auth"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return sameOrigin(r.Header.Get("Origin"), r.Host)
	},
}

type Client struct {
	hub            *Hub
	conn           *websocket.Conn
	send           chan []byte
	organizationID uuid.UUID
	userID         uuid.UUID
	role           string
}

type Hub struct {
	// Registered clients mapped by OrganizationID -> Client map
	orgClients  map[uuid.UUID]map[*Client]bool
	register    chan *Client
	unregister  chan *Client
	broadcast   chan *BroadcastMessage
	redisClient *redis.Client
	mu          sync.RWMutex
}

type BroadcastMessage struct {
	OrganizationID uuid.UUID
	RecipientUserID *uuid.UUID
	Message        []byte
}

func NewHub(redisClient *redis.Client) *Hub {
	return &Hub{
		orgClients:  make(map[uuid.UUID]map[*Client]bool),
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		broadcast:   make(chan *BroadcastMessage, 256),
		redisClient: redisClient,
	}
}

func (h *Hub) Run(ctx context.Context) {
	// Start Redis Pub/Sub subscription pattern for cross-instance broadcasts
	go h.listenRedisPubSub(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case client := <-h.register:
			h.mu.Lock()
			if _, ok := h.orgClients[client.organizationID]; !ok {
				h.orgClients[client.organizationID] = make(map[*Client]bool)
			}
			h.orgClients[client.organizationID][client] = true
			h.mu.Unlock()
			log.Printf("[WebSocket Hub] Client registered for org %s. Total in org: %d\n", client.organizationID, len(h.orgClients[client.organizationID]))

		case client := <-h.unregister:
			h.mu.Lock()
			if clients, ok := h.orgClients[client.organizationID]; ok {
				if _, exists := clients[client]; exists {
					delete(clients, client)
					close(client.send)
					if len(clients) == 0 {
						delete(h.orgClients, client.organizationID)
					}
				}
			}
			h.mu.Unlock()
			log.Printf("[WebSocket Hub] Client disconnected from org %s\n", client.organizationID)

		case msg := <-h.broadcast:
			h.mu.RLock()
			clients, ok := h.orgClients[msg.OrganizationID]
			if ok {
				for client := range clients {
					if client.role != "admin" && (msg.RecipientUserID == nil || client.userID != *msg.RecipientUserID) {
						continue
					}
					select {
					case client.send <- msg.Message:
					default:
						// Slow client: drop buffer to prevent blocking worker or queue processing
						close(client.send)
						delete(clients, client)
					}
				}
			}
			h.mu.RUnlock()
		}
	}
}

// listenRedisPubSub listens to Redis pub/sub channel pattern `events:org:*`
func (h *Hub) listenRedisPubSub(ctx context.Context) {
	pubsub := h.redisClient.PSubscribe(ctx, "events:org:*")
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}

			// Channel format: events:org:<org_uuid>
			var orgUUIDStr string
			_, err := fmt.Sscanf(msg.Channel, "events:org:%s", &orgUUIDStr)
			if err != nil {
				continue
			}

			orgID, err := uuid.Parse(orgUUIDStr)
			if err != nil {
				continue
			}

			var envelope struct {
				EventType       string     `json:"event_type"`
				RecipientUserID *uuid.UUID `json:"recipient_user_id"`
			}
			if err := json.Unmarshal([]byte(msg.Payload), &envelope); err != nil {
				log.Printf("[WebSocket Hub] Invalid broadcast message: %v", err)
				continue
			}
			h.broadcast <- &BroadcastMessage{
				OrganizationID: orgID,
				RecipientUserID: envelope.RecipientUserID,
				Message:        []byte(msg.Payload),
			}
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, _ = w.Write(message)

			// Add queued chat messages to the current websocket message.
			n := len(c.send)
			for i := 0; i < n; i++ {
				_, _ = w.Write([]byte{'\n'})
				_, _ = w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ServeWs handles websocket upgrade, authentication, and client registration
func ServeWs(hub *Hub, authService *auth.AuthService, w http.ResponseWriter, r *http.Request) {
	ServeWsWithOrigins(hub, authService, nil, w, r)
}

func ServeWsWithOrigins(hub *Hub, authService *auth.AuthService, allowedOrigins []string, w http.ResponseWriter, r *http.Request) {
	// Authentication: token passed via query param "token" or "Sec-WebSocket-Protocol" / "Authorization"
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		tokenStr = r.Header.Get("Authorization")
		if len(tokenStr) > 7 && tokenStr[:7] == "Bearer " {
			tokenStr = tokenStr[7:]
		}
	}

	if tokenStr == "" {
		http.Error(w, "Unauthorized: missing token", http.StatusUnauthorized)
		return
	}

	claims, err := authService.ValidateAccessToken(tokenStr)
	if err != nil {
		http.Error(w, "Unauthorized: invalid token", http.StatusUnauthorized)
		return
	}
	if claims.Role != "admin" && claims.Role != "member" {
		http.Error(w, "Forbidden: unsupported account role", http.StatusForbidden)
		return
	}

	wsUpgrader := upgrader
	wsUpgrader.CheckOrigin = func(req *http.Request) bool {
		origin := req.Header.Get("Origin")
		if origin == "" || sameOrigin(origin, req.Host) {
			return true
		}
		for _, allowed := range allowedOrigins {
			if origin == allowed {
				return true
			}
		}
		return false
	}
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WebSocket] Upgrade error: %v\n", err)
		return
	}

	client := &Client{
		hub:            hub,
		conn:           conn,
		send:           make(chan []byte, 256),
		organizationID: claims.OrganizationID,
		userID:         claims.UserID,
		role:           claims.Role,
	}

	client.hub.register <- client

	// Send initial connection ack
	initAck, _ := json.Marshal(map[string]interface{}{
		"event":           "connected",
		"organization_id": claims.OrganizationID,
		"user_id":         claims.UserID,
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
	})
	client.send <- initAck

	go client.writePump()
	go client.readPump()
}

func sameOrigin(origin, host string) bool {
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	return err == nil && parsed.Host != "" && parsed.Path == "" && parsed.RawQuery == "" &&
		parsed.Fragment == "" && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host == host
}
