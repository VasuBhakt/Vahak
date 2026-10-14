package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLite(dbPath string) (*SQLiteStore, error) {
	if dbPath == "" {
		dbPath = "vahak.db"
	}

	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)&_pragma=wal_autocheckpoint(10000)&_pragma=foreign_keys(ON)&_pragma=cache_size(-64000)&_pragma=temp_store(MEMORY)&_pragma=mmap_size(268435456)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Optimize connection pool for concurrent readers and a single WAL writer
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	s := &SQLiteStore{db: db}
	if err := s.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to init sqlite schema: %w", err)
	}

	return s, nil
}

func (s *SQLiteStore) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS endpoints (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		target_url TEXT NOT NULL,
		transformer_script TEXT,
		created_at TIMESTAMP NOT NULL
	);

	CREATE TABLE IF NOT EXISTS requests (
		id TEXT PRIMARY KEY,
		endpoint_id TEXT NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
		method TEXT NOT NULL,
		headers TEXT NOT NULL,
		body TEXT,
		source_ip TEXT,
		received_at TIMESTAMP NOT NULL
	);

	CREATE TABLE IF NOT EXISTS delivery_jobs (
		id TEXT PRIMARY KEY,
		request_id TEXT NOT NULL REFERENCES requests(id) ON DELETE CASCADE,
		target_url TEXT NOT NULL,
		status TEXT DEFAULT 'pending',
		attempts INTEGER DEFAULT 0,
		last_attempt TIMESTAMP,
		next_attempt TIMESTAMP NOT NULL,
		created_at TIMESTAMP NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_delivery_jobs_pending 
	ON delivery_jobs(next_attempt ASC) 
	WHERE status = 'pending';

	CREATE INDEX IF NOT EXISTS idx_requests_endpoint_received 
	ON requests(endpoint_id, received_at DESC);
	`

	_, err := s.db.Exec(schema)
	return err
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// Endpoints

func (s *SQLiteStore) CreateEndpoint(ctx context.Context, name, targetUrl, transformer string) (*models.Endpoint, error) {
	e := &models.Endpoint{
		ID:                uuid.New(),
		Name:              name,
		TargetURL:         targetUrl,
		TransformerScript: transformer,
		CreatedAt:         time.Now().UTC(),
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO endpoints (id, name, target_url, transformer_script, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		e.ID.String(), e.Name, e.TargetURL, e.TransformerScript, e.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("CreateEndpoint: %w", err)
	}
	return e, nil
}

func (s *SQLiteStore) GetEndpoint(ctx context.Context, id uuid.UUID) (*models.Endpoint, error) {
	e := &models.Endpoint{}
	var idStr string
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, target_url, COALESCE(transformer_script, ''), created_at 
		 FROM endpoints WHERE id = ?`,
		id.String(),
	).Scan(&idStr, &e.Name, &e.TargetURL, &e.TransformerScript, &e.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("GetEndpoint: %w", err)
	}
	parsedID, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("parse uuid: %w", err)
	}
	e.ID = parsedID
	return e, nil
}

func (s *SQLiteStore) UpdateEndpoint(ctx context.Context, id uuid.UUID, name, targetUrl, transformer string) (*models.Endpoint, error) {
	_, err := s.db.ExecContext(ctx,
		`UPDATE endpoints SET name = ?, target_url = ?, transformer_script = ? WHERE id = ?`,
		name, targetUrl, transformer, id.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("UpdateEndpoint: %w", err)
	}
	return s.GetEndpoint(ctx, id)
}

func (s *SQLiteStore) ListEndpoints(ctx context.Context) ([]models.Endpoint, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, target_url, COALESCE(transformer_script, ''), created_at 
		 FROM endpoints ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("ListEndpoints: %w", err)
	}
	defer rows.Close()

	var endpoints []models.Endpoint
	for rows.Next() {
		var e models.Endpoint
		var idStr string
		if err := rows.Scan(&idStr, &e.Name, &e.TargetURL, &e.TransformerScript, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("ListEndpoints scan: %w", err)
		}
		e.ID, _ = uuid.Parse(idStr)
		endpoints = append(endpoints, e)
	}
	return endpoints, nil
}

func (s *SQLiteStore) DeleteEndpoint(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM endpoints WHERE id = ?`, id.String())
	if err != nil {
		return fmt.Errorf("DeleteEndpoint: %w", err)
	}
	return nil
}

// Requests

func (s *SQLiteStore) SaveRequest(ctx context.Context, r *models.Request) error {
	headersJSON, err := json.Marshal(r.Headers)
	if err != nil {
		return fmt.Errorf("SaveRequest marshal: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO requests (id, endpoint_id, method, headers, body, source_ip, received_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.ID.String(), r.EndpointID.String(), r.Method, string(headersJSON), r.Body, r.SourceIP, r.ReceivedAt,
	)
	if err != nil {
		return fmt.Errorf("SaveRequest: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetRequestsByEndpoint(ctx context.Context, endpointID uuid.UUID) ([]models.Request, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, endpoint_id, method, headers, body, source_ip, received_at
		 FROM requests WHERE endpoint_id = ? ORDER BY received_at DESC`,
		endpointID.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("GetRequestsByEndpoint: %w", err)
	}
	defer rows.Close()

	var requests []models.Request
	for rows.Next() {
		var r models.Request
		var idStr, epIDStr, headersJSON string
		if err := rows.Scan(&idStr, &epIDStr, &r.Method, &headersJSON, &r.Body, &r.SourceIP, &r.ReceivedAt); err != nil {
			return nil, fmt.Errorf("GetRequestsByEndpoint scan: %w", err)
		}
		r.ID, _ = uuid.Parse(idStr)
		r.EndpointID, _ = uuid.Parse(epIDStr)
		var headers http.Header
		if err := json.Unmarshal([]byte(headersJSON), &headers); err != nil {
			return nil, fmt.Errorf("GetRequestsByEndpoint unmarshal: %w", err)
		}
		r.Headers = headers
		requests = append(requests, r)
	}
	return requests, nil
}

func (s *SQLiteStore) GetRequest(ctx context.Context, id uuid.UUID) (*models.Request, error) {
	var r models.Request
	var idStr, epIDStr, headersJSON string
	err := s.db.QueryRowContext(ctx,
		`SELECT id, endpoint_id, method, headers, body, source_ip, received_at
		 FROM requests WHERE id = ?`,
		id.String(),
	).Scan(&idStr, &epIDStr, &r.Method, &headersJSON, &r.Body, &r.SourceIP, &r.ReceivedAt)
	if err != nil {
		return nil, fmt.Errorf("GetRequest: %w", err)
	}
	r.ID, _ = uuid.Parse(idStr)
	r.EndpointID, _ = uuid.Parse(epIDStr)
	var headers http.Header
	if err := json.Unmarshal([]byte(headersJSON), &headers); err != nil {
		return nil, fmt.Errorf("GetRequest unmarshal: %w", err)
	}
	r.Headers = headers
	return &r, nil
}

// Delivery Jobs

func (s *SQLiteStore) CreateDeliveryJob(ctx context.Context, requestID uuid.UUID, targetURL string) (*models.DeliveryJob, error) {
	job := &models.DeliveryJob{
		ID:          uuid.New(),
		RequestID:   requestID,
		TargetURL:   targetURL,
		Status:      "pending",
		Attempts:    0,
		NextAttempt: time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO delivery_jobs (id, request_id, target_url, status, attempts, next_attempt, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		job.ID.String(), job.RequestID.String(), job.TargetURL, job.Status, job.Attempts, job.NextAttempt, job.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("CreateDeliveryJob: %w", err)
	}
	return job, nil
}

func (s *SQLiteStore) GetPendingJobs(ctx context.Context) ([]models.DeliveryJob, error) {
	now := time.Now().UTC()
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, request_id, target_url, status, attempts, last_attempt, next_attempt, created_at
		 FROM delivery_jobs
		 WHERE status = 'pending' AND next_attempt <= ?
		 ORDER BY next_attempt ASC
		 LIMIT 100`,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("GetPendingJobs: %w", err)
	}
	defer rows.Close()

	var jobs []models.DeliveryJob
	for rows.Next() {
		var j models.DeliveryJob
		var idStr, reqIDStr string
		var lastAttempt sql.NullTime
		if err := rows.Scan(&idStr, &reqIDStr, &j.TargetURL, &j.Status, &j.Attempts, &lastAttempt, &j.NextAttempt, &j.CreatedAt); err != nil {
			return nil, fmt.Errorf("GetPendingJobs scan: %w", err)
		}
		j.ID, _ = uuid.Parse(idStr)
		j.RequestID, _ = uuid.Parse(reqIDStr)
		if lastAttempt.Valid {
			j.LastAttempt = &lastAttempt.Time
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func (s *SQLiteStore) UpdateJobStatus(ctx context.Context, id uuid.UUID, status string, attempts int, nextAttempt time.Time) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx,
		`UPDATE delivery_jobs
		 SET status = ?, attempts = ?, last_attempt = ?, next_attempt = ?
		 WHERE id = ?`,
		status, attempts, now, nextAttempt, id.String(),
	)
	if err != nil {
		return fmt.Errorf("UpdateJobStatus: %w", err)
	}
	return nil
}

func (s *SQLiteStore) BatchMarkDelivered(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))

	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id.String()
	}

	// In SQLite, deleting delivered jobs keeps delivery_jobs near zero rows
	// and eliminates lock contention against flushBatch!
	query := fmt.Sprintf(
		`DELETE FROM delivery_jobs WHERE id IN (%s)`,
		strings.Join(placeholders, ","),
	)

	_, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("BatchMarkDelivered: %w", err)
	}
	return nil
}

func (s *SQLiteStore) StartBatchWorker(ctx context.Context, ingestCh chan *IngestItem) {
	go s.batchWorker(ctx, ingestCh)
}

func (s *SQLiteStore) batchWorker(ctx context.Context, ingestCh <-chan *IngestItem) {
	var buffer []*IngestItem
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
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

func (s *SQLiteStore) SaveDeliveryJob(ctx context.Context, job *models.DeliveryJob) error {
	var lastAttempt any = nil
	if job.LastAttempt != nil {
		lastAttempt = *job.LastAttempt
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO delivery_jobs (id, request_id, target_url, status, attempts, last_attempt, next_attempt, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   status = excluded.status,
		   attempts = excluded.attempts,
		   last_attempt = excluded.last_attempt,
		   next_attempt = excluded.next_attempt`,
		job.ID.String(), job.RequestID.String(), job.TargetURL, job.Status, job.Attempts, lastAttempt, job.NextAttempt, job.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("SaveDeliveryJob: %w", err)
	}
	return nil
}

func (s *SQLiteStore) PruneExpired(ctx context.Context, retentionDays int) error {
	if retentionDays <= 0 {
		return nil
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)
	_, err := s.db.ExecContext(ctx, "DELETE FROM requests WHERE received_at < ?", cutoff)
	if err != nil {
		return fmt.Errorf("PruneExpired requests: %w", err)
	}
	_, err = s.db.ExecContext(ctx, "DELETE FROM delivery_jobs WHERE status = 'failed' AND created_at < ?", cutoff)
	if err != nil {
		return fmt.Errorf("PruneExpired delivery_jobs: %w", err)
	}
	return nil
}

func (s *SQLiteStore) flushBatch(ctx context.Context, items []*IngestItem) {
	if len(items) == 0 {
		return
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		for _, item := range items {
			if item.Result != nil {
				item.Result <- fmt.Errorf("begin tx: %w", err)
			}
		}
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	const chunkSize = 50

	// Insert requests in multi-row chunks (requests-only on fast path)
	for i := 0; i < len(items); i += chunkSize {
		end := i + chunkSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		reqPlaceholders := make([]string, 0, len(chunk))
		reqArgs := make([]any, 0, len(chunk)*7)

		for _, item := range chunk {
			reqPlaceholders = append(reqPlaceholders, "(?, ?, ?, ?, ?, ?, ?)")
			headersJSON, _ := json.Marshal(item.Request.Headers)
			reqArgs = append(reqArgs,
				item.Request.ID.String(),
				item.Request.EndpointID.String(),
				item.Request.Method,
				string(headersJSON),
				item.Request.Body,
				item.Request.SourceIP,
				item.Request.ReceivedAt,
			)
		}

		reqQuery := fmt.Sprintf(
			"INSERT INTO requests (id, endpoint_id, method, headers, body, source_ip, received_at) VALUES %s",
			strings.Join(reqPlaceholders, ","),
		)

		if _, err = tx.ExecContext(ctx, reqQuery, reqArgs...); err != nil {
			for _, it := range items {
				if it.Result != nil {
					it.Result <- fmt.Errorf("exec chunk requests: %w", err)
				}
			}
			return
		}
	}

	err = tx.Commit()
	for _, item := range items {
		if item.Result != nil {
			item.Result <- err
		}
	}
}
