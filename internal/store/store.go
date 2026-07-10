package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{db: pool}
}

// Endpoints

func (s *Store) CreateEndpoint(ctx context.Context, name, targetUrl string) (*models.Endpoint, error) {
	e := &models.Endpoint{
		ID:        uuid.New().String(),
		Name:      name,
		TargetUrl: targetUrl,
		CreatedAt: time.Now(),
	}

	_, err := s.db.Exec(ctx,
		`INSERT INTO endpoints (id, name, target_url, created_at)
		VALUES ($1, $2, $3, $4)`,
		e.ID, e.Name, e.TargetUrl, e.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("CreateEndpoint: %w", err)
	}
	return e, nil
}

func (s *Store) GetEndpoint(ctx context.Context, id uuid.UUID) (*models.Endpoint, error) {
	e := &models.Endpoint{}
	err := s.db.QueryRow(ctx,
		`SELECT id, name, target_url, created_at FROM endpoints WHERE id = $1`,
		id,
	).Scan(&e.ID, &e.Name, &e.TargetUrl, &e.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("GetEndpoint: %w", err)
	}
	return e, nil
}

func (s *Store) ListEndpoints(ctx context.Context) ([]models.Endpoint, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, name, target_url, created_at FROM endpoints ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("ListEndpoints: %w", err)
	}
	defer rows.Close()

	var endpoints []models.Endpoint
	for rows.Next() {
		var e models.Endpoint
		if err := rows.Scan(&e.ID, &e.Name, &e.TargetUrl, &e.CreatedAt); err != nil {
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
		`INSERT INTO requests (id, endpoint_id, method, headers, body, source_ip, recieved_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		r.ID, r.EndpointID, r.Method, headersJSON, r.Body, r.SourceIP, r.ReceivedAt,
	)
	if err != nil {
		return fmt.Errorf("SaveRequest marshal: %w", err)
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
		if err := rows.Scan(&r.ID, &r.EndpointID, &r.Method, &headersJSON, &r.Body, &r.SourceIP, &r.ReceivedAt); err != nil {
			return nil, fmt.Errorf("GetRequestsByEndpoint scan: %w", err)
		}
		if err := json.Unmarshal(headersJSON, &r.Headers); err != nil {
			return nil, fmt.Errorf("GetRequestsByEndpoint unmarshal: %w", err)
		}
		requests = append(requests, r)
	}
	return requests, nil
}

func (s *Store) GetRequest(ctx context.Context, id uuid.UUID) (*models.Request, error) {
	var r models.Request
	var headersJSON []byte
	err := s.db.QueryRow(ctx,
		`SELECT id, endpoint_id, method, headers, body, source_ip, received_at
		 FROM requests WHERE id = $1`, id,
	).Scan(&r.ID, &r.EndpointID, &r.Method, &headersJSON, &r.Body, &r.SourceIP, &r.ReceivedAt)
	if err != nil {
		return nil, fmt.Errorf("GetRequest: %w", err)
	}
	if err := json.Unmarshal(headersJSON, &r.Headers); err != nil {
		return nil, fmt.Errorf("GetRequest unmarshal: %w", err)
	}
	return &r, nil
}

// Delivery Jobs

func (s *Store) CreateDeliveryJob(ctx context.Context, requestID uuid.UUID, targetURL string) (*models.DeliveryJob, error) {
	job := &models.DeliveryJob{
		ID:          uuid.New().String(),
		RequestID:   requestID.String(),
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
