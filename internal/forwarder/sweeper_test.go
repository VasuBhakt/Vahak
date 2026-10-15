package forwarder_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VasuBhakt/vahak/internal/forwarder"
	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/VasuBhakt/vahak/internal/queue"
	"github.com/VasuBhakt/vahak/internal/store"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSweeper_DrainsAllPendingInOneSweep(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "sweeper_drain_test.db")

	st, err := store.NewSQLite(dbPath)
	require.NoError(t, err)
	defer st.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Track received requests at mock sink
	var receivedCount int32
	var receivedEventIDs sync.Map
	var receivedDeliveryIDs sync.Map

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		eventID := r.Header.Get("X-Vahak-Event-ID")
		deliveryID := r.Header.Get("X-Vahak-Delivery-ID")

		if eventID != "" {
			receivedEventIDs.Store(eventID, true)
		}
		if deliveryID != "" {
			receivedDeliveryIDs.Store(deliveryID, true)
		}

		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		atomic.AddInt32(&receivedCount, 1)
	}))
	defer server.Close()

	// 1. Create endpoint
	ep, err := st.CreateEndpoint(ctx, "Drain EP", server.URL, "")
	require.NoError(t, err)

	// 2. Insert 250 pending jobs with expired next_attempt
	const numJobs = 250
	now := time.Now().UTC()
	expiredTime := now.Add(-1 * time.Minute)

	for i := 0; i < numJobs; i++ {
		req := &models.Request{
			ID:         uuid.New(),
			EndpointID: ep.ID,
			Method:     "POST",
			Headers:    http.Header{"Content-Type": []string{"application/json"}},
			Body:       `{"idx":` + string(rune(i)) + `}`,
			SourceIP:   "127.0.0.1",
			ReceivedAt: expiredTime,
		}
		require.NoError(t, st.SaveRequest(ctx, req))

		job := &models.DeliveryJob{
			ID:          uuid.New(),
			RequestID:   req.ID,
			TargetURL:   ep.TargetURL,
			Status:      models.JobStatusPending,
			Attempts:    0,
			NextAttempt: expiredTime,
			CreatedAt:   expiredTime,
		}
		require.NoError(t, st.SaveDeliveryJob(ctx, job))
	}

	// 3. Initialize Forwarder with AllowLocalTargets=true
	logger := zap.NewNop()
	jq := queue.NewJobQueue(500)
	fwd := forwarder.New(st, logger, jq, true)
	fwd.Start(ctx)

	// Wait up to 10 seconds for all 250 jobs to be swept and delivered in a single sweep
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&receivedCount) == int32(numJobs)
	}, 10*time.Second, 100*time.Millisecond, "all 250 pending jobs must be swept and delivered")

	// Verify headers were populated
	var eventIDCount int
	receivedEventIDs.Range(func(_, _ any) bool {
		eventIDCount++
		return true
	})
	assert.Equal(t, numJobs, eventIDCount, "every job must have a distinct X-Vahak-Event-ID")

	var deliveryIDCount int
	receivedDeliveryIDs.Range(func(_, _ any) bool {
		deliveryIDCount++
		return true
	})
	assert.Equal(t, numJobs, deliveryIDCount, "every delivery attempt must have a distinct X-Vahak-Delivery-ID")
}
