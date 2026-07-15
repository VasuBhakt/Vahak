package api

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/VasuBhakt/vahak/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Handler struct {
	store  *store.Store
	logger *zap.Logger
	hub    *Hub
}

func New(store *store.Store, logger *zap.Logger, hub *Hub) *Handler {
	return &Handler{
		store:  store,
		logger: logger,
		hub:    hub,
	}
}

// helpers

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// POST /endpoints
func (h *Handler) CreateEndpoint(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name              string `json:"name"`
		TargetURL         string `json:"target_url"`
		TransformerScript string `json:"transformer_script"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.TargetURL == "" || body.Name == "" {
		writeError(w, http.StatusBadRequest, "target_url is required")
		return
	}

	endpoint, err := h.store.CreateEndpoint(r.Context(), body.Name, body.TargetURL, body.TransformerScript)
	if err != nil {
		h.logger.Error("CreateEndpoint failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to create endpoint")
		return
	}

	writeJSON(w, http.StatusCreated, endpoint)
}

// GET /endpoints
func (h *Handler) ListEndpoints(w http.ResponseWriter, r *http.Request) {
	endpoints, err := h.store.ListEndpoints(r.Context())
	if err != nil {
		h.logger.Error("ListEndpoints failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to list endpoints")
		return
	}
	if endpoints == nil {
		endpoints = []models.Endpoint{}
	}
	writeJSON(w, http.StatusOK, endpoints)
}

// GET /endpoints/{id}
func (h *Handler) GetEndpoint(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid endpoint id")
		return
	}
	endpoint, err := h.store.GetEndpoint(r.Context(), id)
	if err != nil {
		h.logger.Error("GetEndpoint failed", zap.Error(err))
		writeError(w, http.StatusBadGateway, "failed to get endpoint")
		return
	}
	writeJSON(w, http.StatusOK, endpoint)
}

// DELETE /endpoints/{id}
func (h *Handler) DeleteEndpoint(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid endpoint id")
		return
	}
	if err := h.store.DeleteEndpoint(r.Context(), id); err != nil {
		h.logger.Error("DeleteEndpoint failed", zap.Error(err))
		writeError(w, http.StatusBadGateway, "failed to delete endpoint")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /endpoints/{id}/requests
func (h *Handler) GetRequests(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid endpoint id")
		return
	}

	requests, err := h.store.GetRequestsByEndpoint(r.Context(), id)
	if err != nil {
		h.logger.Error("GetRequests failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to get requests")
		return
	}

	if requests == nil {
		requests = []models.Request{}
	}

	writeJSON(w, http.StatusOK, requests)
}

// POST /hooks/{id} - public, captures incoming webhook
func (h *Handler) CaptureWebhook(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid endpoint id")
		return
	}

	// verify endpoint exists
	endpoint, err := h.store.GetEndpoint(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "endpoint not found")
		return
	}

	// extract headers
	headers := r.Header.Clone()

	// limit body size to 1MB
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	// read body
	var bodyStr string
	if r.Body != nil {
		defer r.Body.Close()
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			h.logger.Error("failed to read body", zap.Error(err))
			writeError(w, http.StatusInternalServerError, "failed to read request body")
			return
		}
		bodyStr = string(bodyBytes)
	}

	// save request
	req := &models.Request{
		ID:         uuid.New(),
		EndpointID: id,
		Method:     r.Method,
		Headers:    headers,
		Body:       bodyStr,
		SourceIP:   r.RemoteAddr,
		ReceivedAt: time.Now(),
	}

	if err := h.store.SaveRequest(r.Context(), req); err != nil {
		h.logger.Error("SaveRequest failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to save request")
		return
	}

	// broadcast to live dashboard clients
	h.hub.Broadcast(id, req)

	// create delivery job
	if _, err := h.store.CreateDeliveryJob(r.Context(), req.ID, endpoint.TargetURL); err != nil {
		h.logger.Error("CreateDeliveryJob failed", zap.Error(err))
		// non-critical, webhook is captured regardless
	}

	h.logger.Info("webhook captured",
		zap.String("endpoint_id", id.String()),
		zap.String("method", r.Method),
	)

	writeJSON(w, http.StatusOK, map[string]string{"status": "captured"})
}

// POST /endpoints/{id}/replay/{request_id}
func (h *Handler) ReplayRequest(w http.ResponseWriter, r *http.Request) {
	requestID, err := uuid.Parse(chi.URLParam(r, "request_id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request_id")
		return
	}
	req, err := h.store.GetRequest(r.Context(), requestID)
	if err != nil {
		h.logger.Error("GetRequest failed", zap.Error(err))
		writeError(w, http.StatusNotFound, "endpoint not found")
		return
	}
	endpoint, err := h.store.GetEndpoint(r.Context(), req.EndpointID)
	if err != nil {
		h.logger.Error("GetEndpoint failed", zap.Error(err))
		writeError(w, http.StatusBadGateway, "failed to get endpoint")
		return
	}
	// create a new delivery job for replay
	if _, err := h.store.CreateDeliveryJob(r.Context(), req.ID, endpoint.TargetURL); err != nil {
		h.logger.Error("CreateDeliveryJob failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to create delivery job")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "queued for replay"})
}
