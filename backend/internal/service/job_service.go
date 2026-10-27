package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"distributed-job-system/internal/cache"
	"distributed-job-system/internal/models"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/repository"

	"github.com/google/uuid"
)

var (
	ErrIdempotencyConflict   = errors.New("idempotency key was previously used with a different request payload")
	ErrIdempotencyKeyPending = errors.New("concurrent request with the same idempotency key is currently being processed")
	ErrNotFound              = errors.New("resource not found or access denied")
)

type SubmitJobRequest struct {
	Type        string          `json:"type" binding:"required"`
	Payload     json.RawMessage `json:"payload" binding:"required"`
	Priority    int             `json:"priority"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	MaxAttempts int             `json:"max_attempts"`
}

type JobService struct {
	jobRepo    *repository.JobRepository
	cache      *cache.JobCache
	redisQueue *queue.RedisQueue
}

func NewJobService(
	jobRepo *repository.JobRepository,
	cache *cache.JobCache,
	redisQueue *queue.RedisQueue,
) *JobService {
	return &JobService{
		jobRepo:    jobRepo,
		cache:      cache,
		redisQueue: redisQueue,
	}
}

// SubmitJob accepts a job, checks idempotency key if provided, persists job & outbox atomically, and returns immediately
func (s *JobService) SubmitJob(ctx context.Context, orgID uuid.UUID, idempotencyKey string, req *SubmitJobRequest) (*models.Job, bool, error) {
	return s.submitJob(ctx, orgID, nil, idempotencyKey, req)
}

func (s *JobService) SubmitJobForUser(ctx context.Context, orgID, userID uuid.UUID, idempotencyKey string, req *SubmitJobRequest) (*models.Job, bool, error) {
	return s.submitJob(ctx, orgID, &userID, idempotencyKey, req)
}

func (s *JobService) submitJob(ctx context.Context, orgID uuid.UUID, userID *uuid.UUID, idempotencyKey string, req *SubmitJobRequest) (*models.Job, bool, error) {
	maxAttempts := req.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	// 1. Idempotency handling
	var reqHash string
	if idempotencyKey != "" {
		var canonicalPayload bytes.Buffer
		if err := json.Compact(&canonicalPayload, req.Payload); err != nil {
			return nil, false, fmt.Errorf("invalid job payload: %w", err)
		}
		payloadDecoder := json.NewDecoder(bytes.NewReader(canonicalPayload.Bytes()))
		payloadDecoder.UseNumber()
		var payloadValue interface{}
		if err := payloadDecoder.Decode(&payloadValue); err != nil {
			return nil, false, fmt.Errorf("invalid job payload: %w", err)
		}
		canonicalPayloadBytes, err := json.Marshal(payloadValue)
		if err != nil {
			return nil, false, fmt.Errorf("failed to normalize job payload: %w", err)
		}
		scheduledAt := req.ScheduledAt
		if scheduledAt != nil {
			normalized := scheduledAt.UTC()
			scheduledAt = &normalized
		}
		requestFingerprint, err := json.Marshal(struct {
			Type            string          `json:"type"`
			CreatedByUserID *uuid.UUID      `json:"created_by_user_id,omitempty"`
			Payload         json.RawMessage `json:"payload"`
			Priority        int             `json:"priority"`
			ScheduledAt     *time.Time      `json:"scheduled_at,omitempty"`
			MaxAttempts     int             `json:"max_attempts"`
		}{
			Type: req.Type, CreatedByUserID: userID, Payload: canonicalPayloadBytes, Priority: req.Priority,
			ScheduledAt: scheduledAt, MaxAttempts: maxAttempts,
		})
		if err != nil {
			return nil, false, fmt.Errorf("failed to fingerprint job request: %w", err)
		}
		hash := sha256.Sum256(requestFingerprint)
		reqHash = hex.EncodeToString(hash[:])

	}

	job := &models.Job{
		OrganizationID:  orgID,
		CreatedByUserID: userID,
		Type:            req.Type,
		Payload:         req.Payload,
		Priority:        req.Priority,
		ScheduledAt:     req.ScheduledAt,
		MaxAttempts:     maxAttempts,
	}
	if idempotencyKey != "" {
		job.IdempotencyKey = &idempotencyKey
	}

	// 2. Transactional persistence: DB writes Job + Outbox atomically
	var createdJob *models.Job
	var deduplicated bool
	var err error
	if idempotencyKey != "" {
		createdJob, deduplicated, err = s.jobRepo.CreateJobWithOutboxAndIdempotency(ctx, job, orgID, idempotencyKey, reqHash)
	} else {
		createdJob, err = s.jobRepo.CreateJobWithOutbox(ctx, job)
	}
	if err != nil {
		if errors.Is(err, repository.ErrIdempotencyConflict) {
			return nil, false, ErrIdempotencyConflict
		}
		if errors.Is(err, repository.ErrIdempotencyKeyPending) {
			return nil, false, ErrIdempotencyKeyPending
		}
		return nil, false, fmt.Errorf("failed to create job: %w", err)
	}

	// 4. Populate cache
	s.cache.SetJob(ctx, createdJob)

	return createdJob, deduplicated, nil
}

// GetJob retrieves job by ID ensuring multi-tenant isolation, checking cache first
func (s *JobService) GetJob(ctx context.Context, orgID, jobID uuid.UUID) (*models.Job, error) {
	// Check Redis cache first
	if cachedJob, hit := s.cache.GetJob(ctx, orgID, jobID); hit {
		return cachedJob, nil
	}

	// Fallback to PostgreSQL
	job, err := s.jobRepo.GetJobByID(ctx, orgID, jobID)
	if err != nil {
		return nil, err
	}

	// Update cache
	s.cache.SetJob(ctx, job)
	return job, nil
}

// ListJobs retrieves paginated jobs for the tenant
func (s *JobService) ListJobs(ctx context.Context, orgID uuid.UUID, status string, limit, offset int) ([]*models.Job, int, error) {
	return s.jobRepo.ListJobs(ctx, orgID, status, limit, offset)
}

func (s *JobService) ListUserJobs(ctx context.Context, orgID, userID uuid.UUID, status string, limit, offset int) ([]*models.Job, int, error) {
	return s.jobRepo.ListJobsByUser(ctx, orgID, userID, status, limit, offset)
}

// CancelJob cancels an eligible job and invalidates cache
func (s *JobService) CancelJob(ctx context.Context, orgID, jobID uuid.UUID) (*models.Job, error) {
	job, err := s.jobRepo.CancelJob(ctx, orgID, jobID)
	if err != nil {
		return nil, err
	}

	s.cache.Invalidate(ctx, orgID, jobID)
	return job, nil
}

// RetryJob resets a dead_letter job to queued and invalidates cache
func (s *JobService) RetryJob(ctx context.Context, orgID, jobID uuid.UUID) (*models.Job, error) {
	job, err := s.jobRepo.RetryDeadLetterJob(ctx, orgID, jobID)
	if err != nil {
		return nil, err
	}

	s.cache.Invalidate(ctx, orgID, jobID)
	return job, nil
}

// GetJobEvents retrieves event history for reconnect replay and audit
func (s *JobService) GetJobEvents(ctx context.Context, orgID, jobID uuid.UUID) ([]*models.JobEvent, error) {
	return s.jobRepo.GetJobEvents(ctx, orgID, jobID)
}

// GetMetrics returns queue, stream, worker, and Redis statistics
func (s *JobService) GetMetrics(ctx context.Context) (map[string]interface{}, error) {
	return s.redisQueue.GetMetrics(ctx)
}
