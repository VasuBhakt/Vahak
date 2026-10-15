package api_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/VasuBhakt/vahak/internal/api"
	"github.com/VasuBhakt/vahak/internal/queue"
	"github.com/VasuBhakt/vahak/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestHandler_OverloadReturns503(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := store.NewSQLite(filepath.Join(tmpDir, "overload_test.db"))
	require.NoError(t, err)
	defer st.Close()

	ctx := context.Background()
	ep, err := st.CreateEndpoint(ctx, "Test EP", "http://example.com", "")
	require.NoError(t, err)

	// Buffer capacity of 1, and we intentionally fill it so subsequent requests block
	ingestCh := make(chan *store.IngestItem, 1)
	ingestCh <- &store.IngestItem{} // pre-fill the queue

	logger := zap.NewNop()
	hub := api.NewHub()
	jq := queue.NewJobQueue(10)

	// Durable mode
	h := api.New(st, logger, hub, jq, ingestCh, "durable")

	r := chi.NewRouter()
	r.Post("/hooks/{id}", h.CaptureWebhook)

	// Start request with context timeout or wait for backpressure timeout
	// (Note: handler uses a 2s timer on durable mode for overload fallback)
	req := httptest.NewRequest("POST", "/hooks/"+ep.ID.String(), bytes.NewBufferString(`{"event":"overload"}`))
	req.Header.Set("Content-Type", "application/json")

	// Set request context timeout to 50ms so client gets cancelled if queue does not accept
	reqCtx, reqCancel := context.WithTimeout(req.Context(), 50*time.Millisecond)
	defer reqCancel()
	req = req.WithContext(reqCtx)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// When client times out while blocked on full queue, request never returns 200 OK
	assert.NotEqual(t, http.StatusOK, w.Code, "never return 200 OK when queue is full")
}

func TestHandler_CommitFailureReturns500(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := store.NewSQLite(filepath.Join(tmpDir, "commit_fail_test.db"))
	require.NoError(t, err)
	defer st.Close()

	ctx := context.Background()
	ep, err := st.CreateEndpoint(ctx, "Test EP", "http://example.com", "")
	require.NoError(t, err)

	ingestCh := make(chan *store.IngestItem, 10)
	logger := zap.NewNop()
	hub := api.NewHub()
	jq := queue.NewJobQueue(10)

	// In durable mode: handler sends item to ingestCh and waits on item.Result
	h := api.New(st, logger, hub, jq, ingestCh, "durable")

	// Mock batch worker simulator that fails commits
	go func() {
		for item := range ingestCh {
			if item.Result != nil {
				item.Result <- errors.New("simulated disk write failure")
			}
		}
	}()

	r := chi.NewRouter()
	r.Post("/hooks/{id}", h.CaptureWebhook)

	req := httptest.NewRequest("POST", "/hooks/"+ep.ID.String(), bytes.NewBufferString(`{"event":"fail"}`))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code, "must return 500 when batch commit fails")
	assert.Contains(t, w.Body.String(), "failed to durably record webhook")
}
