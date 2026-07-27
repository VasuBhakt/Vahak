package store

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{db: pool}
}

// Endpoints

func (s *Store) CreateEndpoint(ctx context.Context, name, targetUrl string, transfomer string) (*models.Endpoint, error) {
	e := &models.Endpoint{
		ID:                uuid.New(),
		Name:              name,
		TargetURL:         targetUrl,
		CreatedAt:         time.Now(),
		TransformerScript: transfomer,
	}

	_, err := s.db.Exec(ctx,
		`INSERT INTO endpoints (id, name, target_url, transformer_script, created_at)
		VALUES ($1, $2, $3, $4, $5)`,
		e.ID, e.Name, e.TargetURL, e.TransformerScript, e.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("CreateEndpoint: %w", err)
	}
	return e, nil
}

func (s *Store) GetEndpoint(ctx context.Context, id uuid.UUID) (*models.Endpoint, error) {
	e := &models.Endpoint{}
	err := s.db.QueryRow(ctx,
		`SELECT id, name, target_url, transformer_script, created_at FROM endpoints WHERE id = $1`,
		id,
	).Scan(&e.ID, &e.Name, &e.TargetURL, &e.TransformerScript, &e.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("GetEndpoint: %w", err)
	}
	return e, nil
}

func (s *Store) UpdateEndpoint(ctx context.Context, id uuid.UUID, name, targetUrl, transformer string) (*models.Endpoint, error) {
	_, err := s.db.Exec(ctx,
		`UPDATE endpoints SET name = $1, target_url = $2, transformer_script = $3 WHERE id = $4`,
		name, targetUrl, transformer, id,
	)
	if err != nil {
		return nil, fmt.Errorf("UpdateEndpoint: %w", err)
	}
	return s.GetEndpoint(ctx, id)
}

func (s *Store) ListEndpoints(ctx context.Context) ([]models.Endpoint, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, name, target_url, transformer_script, created_at FROM endpoints ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("ListEndpoints: %w", err)
	}
	defer rows.Close()

	var endpoints []models.Endpoint
	for rows.Next() {
		var e models.Endpoint
		if err := rows.Scan(&e.ID, &e.Name, &e.TargetURL, &e.TransformerScript, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("ListEndpoints scan: %w", err)
		}
		endpoints = append(endpoints, e)
	}
	return endpoints, nil
}

func (s *Store) DeleteEndpoint(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Exec(ctx, `DELETE FROM endpoints WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("DeleteEndpoint: %w", err)
	}
	return nil
}

// Requests

func (s *Store) SaveRequest(ctx context.Context, r *models.Request) error {
	headersJSON, err := json.Marshal(r.Headers)
	if err != nil {
		return fmt.Errorf("SaveRequest marshal: %w", err)
	}

	_, err = s.db.Exec(ctx,
		`INSERT INTO requests (id, endpoint_id, method, headers, body, source_ip, received_at)
		 VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)`,
		r.ID, r.EndpointID, r.Method, string(headersJSON), r.Body, r.SourceIP, r.ReceivedAt,
	)
	if err != nil {
		return fmt.Errorf("SaveRequest: %w", err)
	}
	return nil
}

func (s *Store) GetRequestsByEndpoint(ctx context.Context, endpointID uuid.UUID) ([]models.Request, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, endpoint_id, method, headers, body, source_ip, received_at
		 FROM requests WHERE endpoint_id = $1 ORDER BY received_at DESC`, endpointID,
	)
	if err != nil {
		return nil, fmt.Errorf("GetRequestsByEndpoint: %w", err)
	}
	defer rows.Close()

	var requests []models.Request
	for rows.Next() {
		var r models.Request
		var headersJSON []byte
		var headers http.Header
		if err := rows.Scan(&r.ID, &r.EndpointID, &r.Method, &headersJSON, &r.Body, &r.SourceIP, &r.ReceivedAt); err != nil {
			return nil, fmt.Errorf("GetRequestsByEndpoint scan: %w", err)
		}
		if err := json.Unmarshal(headersJSON, &headers); err != nil {
			return nil, fmt.Errorf("GetRequestsByEndpoint unmarshal: %w", err)
		}
		r.Headers = headers
		requests = append(requests, r)
	}
	return requests, nil
}

func (s *Store) GetRequest(ctx context.Context, id uuid.UUID) (*models.Request, error) {
	var r models.Request
	var headersJSON []byte
	var headers http.Header
	err := s.db.QueryRow(ctx,
		`SELECT id, endpoint_id, method, headers, body, source_ip, received_at
		 FROM requests WHERE id = $1`, id,
	).Scan(&r.ID, &r.EndpointID, &r.Method, &headersJSON, &r.Body, &r.SourceIP, &r.ReceivedAt)
	if err != nil {
		return nil, fmt.Errorf("GetRequest: %w", err)
	}
	if err := json.Unmarshal(headersJSON, &headers); err != nil {
		return nil, fmt.Errorf("GetRequest unmarshal: %w", err)
	}
	r.Headers = headers
	return &r, nil
}

// Delivery Jobs

func (s *Store) CreateDeliveryJob(ctx context.Context, requestID uuid.UUID, targetURL string) (*models.DeliveryJob, error) {
	job := &models.DeliveryJob{
		ID:          uuid.New(),
		RequestID:   requestID,
		TargetURL:   targetURL,
		Status:      "pending",
		Attempts:    0,
		NextAttempt: time.Now(),
		CreatedAt:   time.Now(),
	}
	_, err := s.db.Exec(ctx,
		`INSERT INTO delivery_jobs (id, request_id, target_url, status, attempts, next_attempt, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		job.ID, job.RequestID, job.TargetURL, job.Status, job.Attempts, job.NextAttempt, job.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("CreateDeliveryJob: %w", err)
	}
	return job, nil
}

func (s *Store) GetPendingJobs(ctx context.Context) ([]models.DeliveryJob, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, request_id, target_url, status, attempts, last_attempt, next_attempt, created_at
		 FROM delivery_jobs
		 WHERE status = 'pending' AND next_attempt <= NOW()
		 ORDER BY next_attempt ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("GetPendingJobs: %w", err)
	}
	defer rows.Close()

	var jobs []models.DeliveryJob
	for rows.Next() {
		var j models.DeliveryJob
		if err := rows.Scan(&j.ID, &j.RequestID, &j.TargetURL, &j.Status, &j.Attempts, &j.LastAttempt, &j.NextAttempt, &j.CreatedAt); err != nil {
			return nil, fmt.Errorf("GetPendingJobs scan: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func (s *Store) UpdateJobStatus(ctx context.Context, id uuid.UUID, status string, attempts int, nextAttempt time.Time) error {
	now := time.Now()
	_, err := s.db.Exec(ctx,
		`UPDATE delivery_jobs
		 SET status = $1, attempts = $2, last_attempt = $3, next_attempt = $4
		 WHERE id = $5`,
		status, attempts, now, nextAttempt, id,
	)
	if err != nil {
		return fmt.Errorf("UpdateJobStatus: %w", err)
	}
	return nil
}

// BatchMarkDelivered bulk-updates a list of jobs to status='delivered' in a single query.
// This replaces N individual UPDATE statements with one, eliminating table lock contention
// against the ingester's CopyFrom operations on the delivery_jobs table.
func (s *Store) BatchMarkDelivered(ctx context.Context, ids []uuid.UUID) error {
	now := time.Now()
	_, err := s.db.Exec(ctx,
		`UPDATE delivery_jobs
		 SET status = 'delivered', last_attempt = $1, next_attempt = $1
		 WHERE id = ANY($2)`,
		now, ids,
	)
	if err != nil {
		return fmt.Errorf("BatchMarkDelivered: %w", err)
	}
	return nil
}


type IngestItem struct {
	Request *models.Request
	Job     *models.DeliveryJob
	Result  chan error
}

func (s *Store) StartBatchIngester(ctx context.Context) chan *IngestItem {
	// Buffered channel to hold up to 10k incoming webhooks in memory
	ingestCh := make(chan *IngestItem, 10000)
	s.StartBatchWorker(ctx, ingestCh)
	return ingestCh
}

// StartBatchWorker starts the background batch flush goroutine against an existing channel.
// Use this when you need to create the channel before the context is available.
func (s *Store) StartBatchWorker(ctx context.Context, ingestCh chan *IngestItem) {
	go s.batchWorker(ctx, ingestCh)
}

func (s *Store) batchWorker(ctx context.Context, ingestCh <-chan *IngestItem) {
	var buffer []*IngestItem
	// flush every 50ms even if we don't reach 500 items
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown: flush whatever is left in the buffer
			// so in-flight webhooks are not silently dropped.
			if len(buffer) > 0 {
				s.flushBatch(context.Background(), buffer)
			}
			return
		case item := <-ingestCh:
			buffer = append(buffer, item)
			if len(buffer) >= 500 {
				s.flushBatch(ctx, buffer)
				buffer = nil
			}
		case <-ticker.C:
			if len(buffer) > 0 {
				s.flushBatch(ctx, buffer)
				buffer = nil
			}
		}
	}
}

func (s *Store) flushBatch(ctx context.Context, items []*IngestItem) {
	// Wrap both CopyFrom calls in a single transaction.
	// This guarantees atomicity: either BOTH tables get written or NEITHER does.
	// Without this, a crash between the two CopyFroms would leave orphaned
	// requests with no delivery job, silently losing webhooks forever.
	tx, err := s.db.Begin(ctx)
	if err != nil {
		for _, item := range items {
			item.Result <- fmt.Errorf("begin tx: %w", err)
		}
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	// Bulk insert all requests
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"requests"},
		[]string{"id", "endpoint_id", "method", "headers", "body", "source_ip", "received_at"},
		pgx.CopyFromSlice(len(items), func(i int) ([]any, error) {
			headersJSON, _ := json.Marshal(items[i].Request.Headers)
			return []any{
				items[i].Request.ID,
				items[i].Request.EndpointID,
				items[i].Request.Method,
				string(headersJSON),
				items[i].Request.Body,
				items[i].Request.SourceIP,
				items[i].Request.ReceivedAt,
			}, nil
		}),
	)
	if err != nil {
		for _, item := range items {
			item.Result <- fmt.Errorf("copy requests: %w", err)
		}
		return
	}

	// Bulk insert all delivery jobs
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"delivery_jobs"},
		[]string{"id", "request_id", "target_url", "status", "attempts", "last_attempt", "next_attempt", "created_at"},
		pgx.CopyFromSlice(len(items), func(i int) ([]any, error) {
			return []any{
				items[i].Job.ID,
				items[i].Job.RequestID,
				items[i].Job.TargetURL,
				items[i].Job.Status,
				items[i].Job.Attempts,
				items[i].Job.LastAttempt,
				items[i].Job.NextAttempt,
				items[i].Job.CreatedAt,
			}, nil
		}),
	)
	if err != nil {
		for _, item := range items {
			item.Result <- fmt.Errorf("copy delivery_jobs: %w", err)
		}
		return
	}

	err = tx.Commit(ctx)
	// Notify all blocked HTTP handlers with the commit result
	for _, item := range items {
		item.Result <- err
	}
}
