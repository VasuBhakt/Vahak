package store

import (
	"context"
	"time"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/google/uuid"
)

type Storage interface {
	// Endpoints
	CreateEndpoint(ctx context.Context, name, targetUrl, transformer string) (*models.Endpoint, error)
	GetEndpoint(ctx context.Context, id uuid.UUID) (*models.Endpoint, error)
	UpdateEndpoint(ctx context.Context, id uuid.UUID, name, targetUrl, transformer string) (*models.Endpoint, error)
	ListEndpoints(ctx context.Context) ([]models.Endpoint, error)
	DeleteEndpoint(ctx context.Context, id uuid.UUID) error

	// Requests
	SaveRequest(ctx context.Context, r *models.Request) error
	GetRequestsByEndpoint(ctx context.Context, endpointID uuid.UUID) ([]models.Request, error)
	GetRequest(ctx context.Context, id uuid.UUID) (*models.Request, error)

	// Delivery Jobs
	CreateDeliveryJob(ctx context.Context, requestID uuid.UUID, targetURL string) (*models.DeliveryJob, error)
	SaveDeliveryJob(ctx context.Context, job *models.DeliveryJob) error
	GetPendingJobs(ctx context.Context) ([]models.DeliveryJob, error)
	UpdateJobStatus(ctx context.Context, id uuid.UUID, status string, attempts int, nextAttempt time.Time) error
	BatchMarkDelivered(ctx context.Context, ids []uuid.UUID) error

	// Retention
	PruneExpired(ctx context.Context, retentionDays int) error

	// Batch worker
	StartBatchWorker(ctx context.Context, ingestCh chan *IngestItem)
	Close() error
}
