package forwarder

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/VasuBhakt/vahak/internal/queue"
	"github.com/VasuBhakt/vahak/internal/store"
	"github.com/VasuBhakt/vahak/internal/transformer"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	MaxAttempts    = 6
	InitialBackoff = 4
)

type Forwarder struct {
	store      *store.Store
	logger     *zap.Logger
	client     *http.Client
	queue      *queue.JobQueue
	processing sync.Map
}

func New(store *store.Store, logger *zap.Logger, jq *queue.JobQueue) *Forwarder {
	return &Forwarder{
		store:  store,
		logger: logger,
		queue:  jq,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Start - runs the forwarder loop in the background
func (f *Forwarder) Start(ctx context.Context) {
	// 1. Fast-Path Memory Queue Consumer
	go func() {
		f.logger.Info("fast-path queue consumer started")
		for {
			select {
			case <-ctx.Done():
				return
			case job := <-f.queue.Jobs:
				go f.processJob(ctx, job)
			}
		}
	}()

	// 2. DB Sweeper Loop (Reliability / Retries)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		f.logger.Info("database sweeper started")

		for {
			select {
			case <-ctx.Done():
				f.logger.Info("forwarder stopped")
				return
			case <-ticker.C:
				f.processPendingJobs(ctx)
			}
		}
	}()
}

func (f *Forwarder) processPendingJobs(ctx context.Context) {
	jobs, err := f.store.GetPendingJobs(ctx)
	if err != nil {
		f.logger.Error("failed to get pending jobs", zap.Error(err))
		return
	}

	for _, job := range jobs {
		go f.processJob(ctx, job)
	}
}

func (f *Forwarder) processJob(ctx context.Context, job models.DeliveryJob) {
	// Prevent duplicate concurrent processing of the same job
	if _, loaded := f.processing.LoadOrStore(job.ID, true); loaded {
		return
	}
	defer f.processing.Delete(job.ID)

	// get the original request
	req, err := f.store.GetRequest(ctx, job.RequestID)
	if err != nil {
		f.logger.Error("failed to get request for job",
			zap.String("job_id", job.ID.String()),
			zap.Error(err),
		)
		return
	}

	endpoint, err := f.store.GetEndpoint(ctx, req.EndpointID)
	if err != nil {
		f.logger.Error("failed to get endpoint for job",
			zap.String("job_id", job.ID.String()),
			zap.Error(err),
		)
		return
	}

	finalBody := req.Body
	if endpoint.TransformerScript != "" {
		transformed, err := transformer.Transform(endpoint.TransformerScript, req.Body)
		if err != nil {
			f.logger.Error("transformation failed", zap.Error(err))
			return
		} else {
			finalBody = transformed
		}
	}
	req.Body = finalBody
	// attempt delivery
	err = f.deliver(job.TargetURL, req)
	attempts := job.Attempts + 1

	if err == nil {
		// success
		f.logger.Info("webhook delivered",
			zap.String("job_id", job.ID.String()),
			zap.String("target", job.TargetURL),
			zap.Int("attempts", attempts),
		)
		f.store.UpdateJobStatus(ctx, job.ID, "delivered", attempts, time.Now())
		return
	}

	// failed attempt
	f.logger.Warn("delivery attempt failed",
		zap.String("job_id", job.ID.String()),
		zap.String("target", job.TargetURL),
		zap.Int("attempts", attempts),
		zap.Error(err),
	)

	if attempts >= MaxAttempts {
		// give up
		f.logger.Error("delivery failed after maximum attempts, marking job as failed",
			zap.String("job_id", job.ID.String()),
		)
		f.store.UpdateJobStatus(ctx, job.ID, "failed", attempts, time.Now())
		return
	}

	// schedule next retry with exponential backoff
	nextAttempt := CalculateNextAttempt(attempts)
	f.store.UpdateJobStatus(ctx, job.ID, "pending", attempts, nextAttempt)
	f.logger.Info("retry scheduled",
		zap.String("job_id", job.ID.String()),
		zap.Time("next_attempt", nextAttempt),
	)
}

func (f *Forwarder) deliver(targetURL string, req *models.Request) error {
	// build the outgoing request
	httpReq, err := http.NewRequest(req.Method, targetURL, bytes.NewBufferString(req.Body))
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}

	// forward original headers
	for key, values := range req.Headers {
		// skip content-length because the transformed body might have a different size
		if key == "Content-Length" {
			continue
		}
		for _, v := range values {
			httpReq.Header.Add(key, v)
		}
	}

	// add vahak metadata header
	httpReq.Header.Set("X-Vahak-Delivery", uuid.New().String())
	httpReq.Header.Set("X-Vahak-Timestamp", time.Now().UTC().Format(time.RFC3339))

	// send
	resp, err := f.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("reuqet failed: %w", err)
	}
	defer resp.Body.Close()

	// treat non-2xx as failure
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("target returned status %d", resp.StatusCode)
	}

	return nil
}
