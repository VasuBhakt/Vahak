package api

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // tighten in production
	},
}

// Hub manages all active WebSocket connections per endpoint
type Hub struct {
	mu      sync.RWMutex
	clients map[uuid.UUID]map[*websocket.Conn]struct{}
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[uuid.UUID]map[*websocket.Conn]struct{}),
	}
}

func (h *Hub) register(endpointID uuid.UUID, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[endpointID] == nil {
		h.clients[endpointID] = make(map[*websocket.Conn]struct{})
	}
	h.clients[endpointID][conn] = struct{}{}
}

func (h *Hub) unregister(endpointID uuid.UUID, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if clients, ok := h.clients[endpointID]; ok {
		delete(clients, conn)
	}
	if len(h.clients[endpointID]) == 0 {
		delete(h.clients, endpointID)
	}
}

// Broadcast sends a request to all clients watching this endpoint
func (h *Hub) Broadcast(endpointID uuid.UUID, req *models.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	conns, ok := h.clients[endpointID]
	if !ok || len(conns) == 0 {
		return
	}

	data, err := json.Marshal(req)
	if err != nil {
		return
	}

	for conn := range conns {
		// non-blocking write per client
		go func(c *websocket.Conn) {
			if err := c.WriteMessage(websocket.TextMessage, data); err != nil {
				// client disconnected, will be cleaned up by ServeWS
				c.Close()
			}
		}(conn)
	}
}

// GET /ws/{id} - protected, dashboard connects here
func (h *Handler) ServeWS(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid endpoint id")
		return
	}

	// verify endpoint exists
	_, err = h.store.GetEndpoint(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "endpoint not found")
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed", zap.Error(err))
		return
	}

	h.hub.register(id, conn)
	h.logger.Info("ws client connected",
		zap.String("endpoint_id", id.String()),
	)

	// keep connection alive, clean up on disconnect
	defer func() {
		h.hub.unregister(id, conn)
		conn.Close()
		h.logger.Info("ws client disconnected", zap.String("endpoint_id", id.String()))
	}()

	// read loop - needed to detect client disconnect and handle pings
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}
