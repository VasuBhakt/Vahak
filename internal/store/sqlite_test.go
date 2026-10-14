package store

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/google/uuid"
)

func setupTestSQLite(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test_vahak.db")
	st, err := NewSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	t.Cleanup(func() {
		st.Close()
	})
	return st
}

func TestSQLiteEndpointsCRUD(t *testing.T) {
	st := setupTestSQLite(t)
	ctx := context.Background()

	// 1. Create Endpoint
	ep, err := st.CreateEndpoint(ctx, "Test Hook", "http://example.com/webhook", "return payload;")
	if err != nil {
		t.Fatalf("CreateEndpoint failed: %v", err)
	}
	if ep.Name != "Test Hook" || ep.TargetURL != "http://example.com/webhook" {
		t.Errorf("unexpected endpoint: %+v", ep)
	}

	// 2. Get Endpoint
	fetched, err := st.GetEndpoint(ctx, ep.ID)
	if err != nil {
		t.Fatalf("GetEndpoint failed: %v", err)
	}
	if fetched.ID != ep.ID || fetched.TransformerScript != "return payload;" {
		t.Errorf("fetched mismatch: %+v", fetched)
	}

	// 3. Update Endpoint
	updated, err := st.UpdateEndpoint(ctx, ep.ID, "Updated Hook", "http://example.com/new", "return payload.data;")
	if err != nil {
		t.Fatalf("UpdateEndpoint failed: %v", err)
	}
	if updated.Name != "Updated Hook" || updated.TargetURL != "http://example.com/new" {
		t.Errorf("unexpected update: %+v", updated)
	}

	// 4. List Endpoints
	list, err := st.ListEndpoints(ctx)
	if err != nil {
		t.Fatalf("ListEndpoints failed: %v", err)
	}
	if len(list) != 1 || list[0].ID != ep.ID {
		t.Errorf("unexpected list: %+v", list)
	}

	// 5. Delete Endpoint
	if err := st.DeleteEndpoint(ctx, ep.ID); err != nil {
		t.Fatalf("DeleteEndpoint failed: %v", err)
	}
	_, err = st.GetEndpoint(ctx, ep.ID)
	if err == nil {
		t.Fatal("expected error getting deleted endpoint, got nil")
	}
}

func TestSQLiteRequestsAndJobs(t *testing.T) {
	st := setupTestSQLite(t)
	ctx := context.Background()

	ep, err := st.CreateEndpoint(ctx, "Job Hook", "http://sink:9090", "")
	if err != nil {
		t.Fatalf("CreateEndpoint failed: %v", err)
	}

	// Save request
	reqID := uuid.New()
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	r := &models.Request{
		ID:         reqID,
		EndpointID: ep.ID,
		Method:     "POST",
		Headers:    headers,
		Body:       `{"event":"test"}`,
		SourceIP:   "127.0.0.1",
		ReceivedAt: time.Now().UTC(),
	}
	if err := st.SaveRequest(ctx, r); err != nil {
		t.Fatalf("SaveRequest failed: %v", err)
	}

	// Fetch request
	fetchedReq, err := st.GetRequest(ctx, reqID)
	if err != nil {
		t.Fatalf("GetRequest failed: %v", err)
	}
	if fetchedReq.Body != r.Body || fetchedReq.Headers.Get("Content-Type") != "application/json" {
		t.Errorf("fetched request mismatch: %+v", fetchedReq)
	}

	// Create Delivery Job
	job, err := st.CreateDeliveryJob(ctx, reqID, ep.TargetURL)
	if err != nil {
		t.Fatalf("CreateDeliveryJob failed: %v", err)
	}
	if job.Status != "pending" {
		t.Errorf("expected pending status, got %s", job.Status)
	}

	// Get Pending Jobs
	pending, err := st.GetPendingJobs(ctx)
	if err != nil {
		t.Fatalf("GetPendingJobs failed: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != job.ID {
		t.Errorf("expected 1 pending job, got %v", pending)
	}

	// Batch Mark Delivered
	if err := st.BatchMarkDelivered(ctx, []uuid.UUID{job.ID}); err != nil {
		t.Fatalf("BatchMarkDelivered failed: %v", err)
	}

	// Pending jobs should now be 0 (thanks to partial index and status filter)
	pendingAfter, err := st.GetPendingJobs(ctx)
	if err != nil {
		t.Fatalf("GetPendingJobs after delivery failed: %v", err)
	}
	if len(pendingAfter) != 0 {
		t.Errorf("expected 0 pending jobs, got %d", len(pendingAfter))
	}
}

func TestSQLiteBatchFlush(t *testing.T) {
	st := setupTestSQLite(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ep, err := st.CreateEndpoint(ctx, "Batch Hook", "http://sink:9090", "")
	if err != nil {
		t.Fatalf("CreateEndpoint failed: %v", err)
	}

	ingestCh := make(chan *IngestItem, 100)
	st.StartBatchWorker(ctx, ingestCh)

	const count = 5
	results := make([]chan error, count)
	for i := 0; i < count; i++ {
		res := make(chan error, 1)
		results[i] = res
		req := &models.Request{
			ID:         uuid.New(),
			EndpointID: ep.ID,
			Method:     "POST",
			Headers:    make(http.Header),
			Body:       `{"idx": 1}`,
			SourceIP:   "127.0.0.1",
			ReceivedAt: time.Now().UTC(),
		}
		job := &models.DeliveryJob{
			ID:          uuid.New(),
			RequestID:   req.ID,
			TargetURL:   ep.TargetURL,
			Status:      "pending",
			Attempts:    0,
			NextAttempt: time.Now().UTC(),
			CreatedAt:   time.Now().UTC(),
		}
		ingestCh <- &IngestItem{
			Request: req,
			Job:     job,
			Result:  res,
		}
	}

	// Wait for results
	for i, res := range results {
		select {
		case err := <-res:
			if err != nil {
				t.Fatalf("item %d flush failed: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for batch flush item %d", i)
		}
	}

	// Verify requests saved
	reqs, err := st.GetRequestsByEndpoint(ctx, ep.ID)
	if err != nil {
		t.Fatalf("GetRequestsByEndpoint failed: %v", err)
	}
	if len(reqs) != count {
		t.Errorf("expected %d requests, got %d", count, len(reqs))
	}
}

func TestSQLiteSaveDeliveryJobAndPrune(t *testing.T) {
	st := setupTestSQLite(t)
	ctx := context.Background()

	ep, err := st.CreateEndpoint(ctx, "Prune Hook", "http://sink:9090", "")
	if err != nil {
		t.Fatalf("CreateEndpoint failed: %v", err)
	}

	reqID := uuid.New()
	r := &models.Request{
		ID:         reqID,
		EndpointID: ep.ID,
		Method:     "POST",
		Headers:    make(http.Header),
		Body:       `{"expired": true}`,
		SourceIP:   "127.0.0.1",
		ReceivedAt: time.Now().UTC().AddDate(0, 0, -10), // 10 days ago
	}
	if err := st.SaveRequest(ctx, r); err != nil {
		t.Fatalf("SaveRequest failed: %v", err)
	}

	job := &models.DeliveryJob{
		ID:          uuid.New(),
		RequestID:   reqID,
		TargetURL:   ep.TargetURL,
		Status:      "failed",
		Attempts:    6,
		NextAttempt: time.Now().UTC(),
		CreatedAt:   time.Now().UTC().AddDate(0, 0, -10),
	}
	if err := st.SaveDeliveryJob(ctx, job); err != nil {
		t.Fatalf("SaveDeliveryJob failed: %v", err)
	}

	// Prune older than 7 days
	if err := st.PruneExpired(ctx, 7); err != nil {
		t.Fatalf("PruneExpired failed: %v", err)
	}

	// Request should be pruned
	_, err = st.GetRequest(ctx, reqID)
	if err == nil {
		t.Fatal("expected request to be pruned, but found it")
	}
}
