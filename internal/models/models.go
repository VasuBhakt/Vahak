package models

import (
	"net/http"
	"time"
)

type Endpoint struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	TargetUrl string    `json:"target_url"`
	CreatedAt time.Time `json:"created_at"`
}

type Request struct {
	ID         string      `json:"id"`
	EndpointID string      `json:"endpoint_id"`
	Method     string      `json:"method"`
	Headers    http.Header `json:"headers"`
	Body       string      `json:"body"`
	SourceIP   string      `json:"source_ip"`
	ReceivedAt time.Time   `json:"received_at"`
}

type DeliveryJob struct {
	ID          string     `json:"id"`
	RequestID   string     `json:"request_id"`
	TargetURL   string     `json:"target_url"`
	Status      string     `json:"status"`
	Attempts    int        `json:"attempts"`
	LastAttempt *time.Time `json:"last_attempt"`
	NextAttempt time.Time  `json:"next_attempt"`
	CreatedAt   time.Time  `json:"created_at"`
}
