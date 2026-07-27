package forwarder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
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
	store       *store.Store
	logger      *zap.Logger
	client      *http.Client
	queue       *queue.JobQueue
	processing  sync.Map
	deliveredCh chan uuid.UUID      // batches successful delivery IDs to reduce DB lock contention
	circuits    *CircuitManager     // per-endpoint circuit breaker to avoid hammering dead targets
}

func New(store *store.Store, logger *zap.Logger, jq *queue.JobQueue, allowLocal bool) *Forwarder {
	// SSRF Protection: custom dialer that blocks private and loopback IPs
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			for _, ip := range ips {
				// Block loopback, private networks, and unspecified IPs
				if !allowLocal && (ip.IP.IsPrivate() || ip.IP.IsLoopback() || ip.IP.IsUnspecified()) {
					return nil, errors.New("SSRF blocked: private/loopback IP address not allowed")
				}
			}
			// Safe to dial
			return dialer.DialContext(ctx, network, addr)
		},
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}

	return &Forwarder{
		store:       store,
		logger:      logger,
		queue:       jq,
		deliveredCh: make(chan uuid.UUID, 10000),
		circuits:    NewCircuitManager(),
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: transport,
		},
	}
}

// Start - runs the forwarder loop in the background
func (f *Forwarder) Start(ctx context.Context) {
	// 1. Fast-Path Memory Queue Consumer (bounded worker pool)
	f.logger.Info("starting fast-path queue workers", zap.Int("workers", 100))
	for i := 0; i < 100; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					return
				case job := <-f.queue.Jobs:
					f.processJob(ctx, job)
				}
			}
		}(i)
	}

	// 2. Batch Status Flusher - drains the deliveredCh and bulk-updates DB
	// This prevents 100 individual UPDATE statements from fighting the ingester's
	// CopyFrom for table locks on delivery_jobs.
	go func() {
		var ids []uuid.UUID
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		flush := func() {
			if len(ids) == 0 {
				return
			}
			if err := f.store.BatchMarkDelivered(ctx, ids); err != nil {
				f.logger.Error("batch mark delivered failed", zap.Error(err))
			}
			ids = nil
		}

		for {
			select {
			case <-ctx.Done():
				flush() // flush whatever is left on shutdown
				return
			case id := <-f.deliveredCh:
				ids = append(ids, id)
				if len(ids) >= 200 {
					flush()
				}
			case <-ticker.C:
				flush()
			}
		}
	}()

	// 3. DB Sweeper Loop (Reliability / Retries)
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

	// Fast path: request data was carried in-memory from the ingester.
	// This skips two SELECT queries per delivery (GetRequest + GetEndpoint).
	// Slow path (DB sweeper retries): InMemBody is empty, so we load from DB.
	var req *models.Request
	if job.InMemBody != "" || job.InMemHeaders != nil {
		req = &models.Request{
			ID:      job.RequestID,
			Method:  job.InMemMethod,
			Headers: job.InMemHeaders,
			Body:    job.InMemBody,
		}
	} else {
		var err error
		req, err = f.store.GetRequest(ctx, job.RequestID)
		if err != nil {
			f.logger.Error("failed to get request for job",
				zap.String("job_id", job.ID.String()),
				zap.Error(err),
			)
			return
		}
	}

	// TransformerScript is also carried in-memory; load from DB only for retries.
	transformerScript := job.InMemTransformerScript
	if transformerScript == "" && job.Attempts > 0 {
		ep, err := f.store.GetEndpoint(ctx, job.RequestID)
		if err == nil {
			transformerScript = ep.TransformerScript
		}
	}

	finalBody := req.Body
	if transformerScript != "" {
		transformed, err := transformer.Transform(transformerScript, req.Body)
		if err != nil {
			f.logger.Error("transformation failed", zap.Error(err))
			return
		}
		finalBody = transformed
	}
	req.Body = finalBody

	// circuit breaker: skip delivery if endpoint is consistently failing
	if !f.circuits.Allow(job.TargetURL) {
		f.logger.Debug("circuit open, skipping delivery",
			zap.String("target", job.TargetURL),
			zap.String("job_id", job.ID.String()),
		)
		// reschedule for after cooldown
		nextAttempt := time.Now().Add(30 * time.Second)
		f.store.UpdateJobStatus(ctx, job.ID, "pending", job.Attempts, nextAttempt)
		return
	}

	// attempt delivery
	err := f.deliver(job.TargetURL, req)
	attempts := job.Attempts + 1

	if err == nil {
		f.circuits.RecordSuccess(job.TargetURL)
		// success — push to batch flusher instead of individual UPDATE
		select {
		case f.deliveredCh <- job.ID:
		default:
			// channel full, fall back to direct update
			f.store.UpdateJobStatus(ctx, job.ID, "delivered", attempts, time.Now())
		}
		return
	}

	// failed attempt
	f.circuits.RecordFailure(job.TargetURL)
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
