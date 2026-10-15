package store_test

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/VasuBhakt/vahak/internal/store"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPruneExpired_PendingJobsSurvive(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "prune_test.db")

	st, err := store.NewSQLite(dbPath)
	require.NoError(t, err)
	defer st.Close()

	ctx := context.Background()

	// 1. Create endpoint
	ep, err := st.CreateEndpoint(ctx, "Test EP", "http://example.com", "")
	require.NoError(t, err)

	now := time.Now().UTC()
	oldTime := now.AddDate(0, 0, -10) // 10 days ago (older than 7 days retention)

	// Case A: Old request with a PENDING job (must SURVIVE)
	reqPending := &models.Request{
		ID:         uuid.New(),
		EndpointID: ep.ID,
		Method:     "POST",
		Headers:    http.Header{"Content-Type": []string{"application/json"}},
		Body:       `{"event":"pending"}`,
		SourceIP:   "127.0.0.1",
		ReceivedAt: oldTime,
	}
	require.NoError(t, st.SaveRequest(ctx, reqPending))

	jobPending := &models.DeliveryJob{
		ID:          uuid.New(),
		RequestID:   reqPending.ID,
		TargetURL:   ep.TargetURL,
		Status:      models.JobStatusPending,
		Attempts:    0,
		NextAttempt: now,
		CreatedAt:   oldTime,
	}
	require.NoError(t, st.SaveDeliveryJob(ctx, jobPending))

	// Case B: Old request with a DELIVERED job (must be PRUNED)
	reqDelivered := &models.Request{
		ID:         uuid.New(),
		EndpointID: ep.ID,
		Method:     "POST",
		Headers:    http.Header{"Content-Type": []string{"application/json"}},
		Body:       `{"event":"delivered"}`,
		SourceIP:   "127.0.0.1",
		ReceivedAt: oldTime,
	}
	require.NoError(t, st.SaveRequest(ctx, reqDelivered))

	jobDelivered := &models.DeliveryJob{
		ID:          uuid.New(),
		RequestID:   reqDelivered.ID,
		TargetURL:   ep.TargetURL,
		Status:      models.JobStatusDelivered,
		Attempts:    1,
		NextAttempt: oldTime,
		CreatedAt:   oldTime,
	}
	require.NoError(t, st.SaveDeliveryJob(ctx, jobDelivered))

	// Case C: Old fast-mode request with NO job row (must be PRUNED)
	reqFastMode := &models.Request{
		ID:         uuid.New(),
		EndpointID: ep.ID,
		Method:     "POST",
		Headers:    http.Header{"Content-Type": []string{"application/json"}},
		Body:       `{"event":"fast"}`,
		SourceIP:   "127.0.0.1",
		ReceivedAt: oldTime,
	}
	require.NoError(t, st.SaveRequest(ctx, reqFastMode))

	// Run retention pruning with 7 days retention
	err = st.PruneExpired(ctx, 7)
	require.NoError(t, err)

	// Verify Case A: reqPending must STILL EXIST
	savedReqPending, err := st.GetRequest(ctx, reqPending.ID)
	require.NoError(t, err, "request with pending job must survive retention pruning")
	assert.Equal(t, reqPending.ID, savedReqPending.ID)

	// Verify Case B: reqDelivered must be GONE
	_, err = st.GetRequest(ctx, reqDelivered.ID)
	assert.Error(t, err, "request with delivered job must be pruned")

	// Verify Case C: reqFastMode must be GONE
	_, err = st.GetRequest(ctx, reqFastMode.ID)
	assert.Error(t, err, "expired fast mode request must be pruned")
}
