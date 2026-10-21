package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Organization struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type User struct {
	ID             uuid.UUID `json:"id"`
	OrganizationID uuid.UUID `json:"organization_id"`
	Email          string    `json:"email"`
	PasswordHash   string    `json:"-"`
	Role           string    `json:"role"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type RefreshToken struct {
	ID        uuid.UUID  `json:"id"`
	UserID    uuid.UUID  `json:"user_id"`
	TokenHash string     `json:"-"`
	ExpiresAt time.Time  `json:"expires_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

type JobStatus string

const (
	StatusQueued     JobStatus = "queued"
	StatusProcessing JobStatus = "processing"
	StatusRetrying   JobStatus = "retrying"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusDeadLetter JobStatus = "dead_letter"
	StatusCancelled  JobStatus = "cancelled"
)

type Job struct {
	ID             uuid.UUID       `json:"id"`
	OrganizationID uuid.UUID       `json:"organization_id"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         JobStatus       `json:"status"`
	Priority       int             `json:"priority"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty"`
	AttemptCount   int             `json:"attempt_count"`
	MaxAttempts    int             `json:"max_attempts"`
	LockedBy       *string         `json:"locked_by,omitempty"`
	LockedAt       *time.Time      `json:"locked_at,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	Version        int             `json:"version"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type JobEvent struct {
	ID             int64           `json:"id"`
	JobID          uuid.UUID       `json:"job_id"`
	OrganizationID uuid.UUID       `json:"organization_id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	CreatedAt      time.Time       `json:"created_at"`
}

type OutboxEvent struct {
	ID             int64           `json:"id"`
	OrganizationID uuid.UUID       `json:"organization_id"`
	EventType      string          `json:"event_type"`
	AggregateType  string          `json:"aggregate_type"`
	AggregateID    string          `json:"aggregate_id"`
	Payload        json.RawMessage `json:"payload"`
	Dispatched     bool            `json:"dispatched"`
	DispatchedAt   *time.Time      `json:"dispatched_at,omitempty"`
	RetryCount     int             `json:"retry_count"`
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

type IdempotencyKey struct {
	ID             uuid.UUID       `json:"id"`
	OrganizationID uuid.UUID       `json:"organization_id"`
	Key            string          `json:"key"`
	RequestHash    string          `json:"request_hash"`
	Status         string          `json:"status"` // pending, processed
	ResponseCode   *int            `json:"response_code,omitempty"`
	ResponseBody   json.RawMessage `json:"response_body,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}
