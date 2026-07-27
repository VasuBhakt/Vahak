package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/VasuBhakt/vahak/internal/queue"
	"github.com/VasuBhakt/vahak/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func setupTestStore(t *testing.T) *store.Store {
	dbUrl := os.Getenv("DB_URL")
	if dbUrl == "" {
		// fallback to local docker-compose defaults
		dbUrl = "postgres://vahak:vahak_password@127.0.0.1:5432/vahak?sslmode=disable"
	}
	
	pool, err := store.NewPool(dbUrl)
	if err != nil {
		t.Skipf("Skipping integration test: failed to connect to database: %v", err)
	}

	st := store.New(pool)
	
	// Create tables if they don't exist
	_, err = pool.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS endpoints (
			id UUID PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			target_url TEXT NOT NULL,
			transformer_script TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS delivery_jobs (
			id UUID PRIMARY KEY,
			request_id UUID,
			target_url TEXT,
			status VARCHAR(50),
			attempts INT,
			last_attempt TIMESTAMP WITH TIME ZONE,
			next_attempt TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE
		);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}
	
	return st
}

func setupTestHandler(t *testing.T) (*Handler, *store.Store, chan *store.IngestItem) {
	st := setupTestStore(t)
	logger := zap.NewNop()
	hub := NewHub()
	jq := queue.NewJobQueue(100)
	ingestCh := make(chan *store.IngestItem, 100)

	h := New(st, logger, hub, jq, ingestCh)
	return h, st, ingestCh
}

func TestAPI_CreateEndpoint(t *testing.T) {
	h, _, _ := setupTestHandler(t)

	r := chi.NewRouter()
	r.Post("/api/endpoints", h.CreateEndpoint)

	payload := `{"name": "test-endpoint", "target_url": "http://example.com"}`
	req := httptest.NewRequest("POST", "/api/endpoints", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", w.Code, w.Body.String())
	}

	var ep models.Endpoint
	if err := json.NewDecoder(w.Body).Decode(&ep); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if ep.Name != "test-endpoint" || ep.TargetURL != "http://example.com" {
		t.Errorf("unexpected endpoint values: %+v", ep)
	}
}

func TestAPI_CaptureWebhook(t *testing.T) {
	h, st, ingestCh := setupTestHandler(t)

	// Create an endpoint in the DB
	ctx := context.Background()
	epID := uuid.New()
	_, err := st.UpdateEndpoint(ctx, epID, "test-webhook", "http://example.com", "")
	if err != nil {
		t.Fatalf("failed to seed endpoint: %v", err)
	}

	r := chi.NewRouter()
	r.Post("/hooks/{id}", h.CaptureWebhook)

	payload := `{"msg": "hello"}`
	req := httptest.NewRequest("POST", "/hooks/"+epID.String(), bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	// Verify the webhook was pushed to the ingest channel
	select {
	case item := <-ingestCh:
		if item.Job.TargetURL != "http://example.com" {
			t.Errorf("expected target http://example.com, got %s", item.Job.TargetURL)
		}
		if item.Job.InMemBody != payload {
			t.Errorf("expected body %s, got %s", payload, item.Job.InMemBody)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for ingest channel")
	}
}

func TestAPI_CaptureWebhook_NotFound(t *testing.T) {
	h, _, _ := setupTestHandler(t)

	r := chi.NewRouter()
	r.Post("/hooks/{id}", h.CaptureWebhook)

	req := httptest.NewRequest("POST", "/hooks/"+uuid.New().String(), bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d", w.Code)
	}
}
