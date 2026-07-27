package models

import (
	"net/http"
	"time"

	"github.com/google/uuid"
)

type Endpoint struct {
	ID                uuid.UUID `json:"id"`
	Name              string    `json:"name"`
	TargetURL         string    `json:"target_url"`
	TransformerScript string    `json:"transformer_script,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}

type Request struct {
	ID         uuid.UUID   `json:"id"`
	EndpointID uuid.UUID   `json:"endpoint_id"`
	Method     string      `json:"method"`
	Headers    http.Header `json:"headers"`
	Body       string      `json:"body"`
	SourceIP   string      `json:"source_ip"`
	ReceivedAt time.Time   `json:"received_at"`
}

type DeliveryJob struct {
	ID          uuid.UUID  `json:"id"`
	RequestID   uuid.UUID  `json:"request_id"`
	TargetURL   string     `json:"target_url"`
	Status      string     `json:"status"`
	Attempts    int        `json:"attempts"`
	LastAttempt *time.Time `json:"last_attempt"`
	NextAttempt time.Time  `json:"next_attempt"`
	CreatedAt   time.Time  `json:"created_at"`

	// In-memory only: carried through the fast-path queue to avoid redundant DB reads.
	// Tagged json:"-" so they are never serialised or persisted.
	InMemMethod            string      `json:"-"`
	InMemHeaders           http.Header `json:"-"`
	InMemBody              string      `json:"-"`
	InMemTransformerScript string      `json:"-"`
}
