package api

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/VasuBhakt/vahak/internal/queue"
	"github.com/VasuBhakt/vahak/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Handler struct {
	store      *store.Store
	logger     *zap.Logger
	hub        *Hub
	queue      *queue.JobQueue
	epCache    sync.Map
	ingestCh   chan *store.IngestItem
	resultPool sync.Pool // reuse chan error to reduce GC pressure
}

func New(store *store.Store, logger *zap.Logger, hub *Hub, jq *queue.JobQueue, ingestCh chan *store.IngestItem) *Handler {
	h := &Handler{
		store:    store,
		logger:   logger,
		hub:      hub,
		queue:    jq,
		ingestCh: ingestCh,
	}
	h.resultPool = sync.Pool{
		New: func() any {
			ch := make(chan error, 1)
			return ch
		},
	}
	return h
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

// PUT /endpoints/{id}
func (h *Handler) UpdateEndpoint(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid endpoint id")
		return
	}

	// Fetch existing endpoint first
	existing, err := h.store.GetEndpoint(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "endpoint not found")
		return
	}

	// Use pointers so we can distinguish between omitted fields (nil) and empty strings ("")
	var body struct {
		Name              *string `json:"name"`
		TargetURL         *string `json:"target_url"`
		TransformerScript *string `json:"transformer_script"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Merge fields if they were provided in the JSON payload
	if body.Name != nil {
		existing.Name = *body.Name
	}
	if body.TargetURL != nil {
		existing.TargetURL = *body.TargetURL
	}
	if body.TransformerScript != nil {
		existing.TransformerScript = *body.TransformerScript
	}

	if existing.TargetURL == "" || existing.Name == "" {
		writeError(w, http.StatusBadRequest, "target_url and name cannot be empty")
		return
	}

	endpoint, err := h.store.UpdateEndpoint(r.Context(), id, existing.Name, existing.TargetURL, existing.TransformerScript)
	if err != nil {
		h.logger.Error("UpdateEndpoint failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to update endpoint")
		return
	}

	h.epCache.Store(id, endpoint)

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
	h.epCache.Delete(id)
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
	var endpoint *models.Endpoint
	if cached, ok := h.epCache.Load(id); ok {
		endpoint = cached.(*models.Endpoint)
	} else {
		ep, err := h.store.GetEndpoint(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "endpoint not found")
			return
		}
		h.epCache.Store(id, ep)
		endpoint = ep
	}

	// extract only the headers that matter — skip noisy client headers
	headers := make(http.Header)
	for _, key := range []string{"Content-Type", "X-Signature", "X-Hub-Signature", "X-Hub-Signature-256", "User-Agent"} {
		if val := r.Header.Get(key); val != "" {
			headers.Set(key, val)
		}
	}

	// limit body size to 100KB to prevent Memory Exhaustion (OOM) DoS
	r.Body = http.MaxBytesReader(w, r.Body, 100*1024)

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

	// prepare delivery job - carry request data in-memory to skip DB reads in forwarder
	job := &models.DeliveryJob{
		ID:                    uuid.New(),
		RequestID:             req.ID,
		TargetURL:             endpoint.TargetURL,
		Status:                "pending",
		Attempts:              0,
		NextAttempt:           time.Now(),
		CreatedAt:             time.Now(),
		InMemMethod:           req.Method,
		InMemHeaders:          headers,
		InMemBody:             bodyStr,
		InMemTransformerScript: endpoint.TransformerScript,
	}

	// acquire a result channel from the pool
	resultCh := h.resultPool.Get().(chan error)
	item := &store.IngestItem{
		Request: req,
		Job:     job,
		Result:  resultCh,
	}

	// push to batcher memory queue with a timeout.
	// If the queue is full (system overloaded or postgres down), we return 503
	// immediately rather than blocking the HTTP connection indefinitely.
	select {
	case h.ingestCh <- item:
	case <-time.After(5 * time.Second):
		h.resultPool.Put(resultCh)
		writeError(w, http.StatusServiceUnavailable, "system overloaded, try again later")
		return
	}

	// block ONLY until the background worker flushes the batch to disk
	if err := <-resultCh; err != nil {
		h.resultPool.Put(resultCh)
		h.logger.Error("Batch ingest failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to save request")
		return
	}
	h.resultPool.Put(resultCh)

	// now push to fast-path in-memory queue for delivery
	h.queue.Push(*job)

	// broadcast to live dashboard clients
	h.hub.Broadcast(id, req)

	// h.logger.Info("webhook captured",
	// 	zap.String("endpoint_id", id.String()),
	// 	zap.String("method", r.Method),
	// )

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
