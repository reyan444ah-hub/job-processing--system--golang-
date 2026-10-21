package service

import (
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
	jobRepo        *repository.JobRepository
	idempotencyRepo *repository.IdempotencyRepository
	cache          *cache.JobCache
	redisQueue     *queue.RedisQueue
}

func NewJobService(
	jobRepo *repository.JobRepository,
	idempotencyRepo *repository.IdempotencyRepository,
	cache *cache.JobCache,
	redisQueue *queue.RedisQueue,
) *JobService {
	return &JobService{
		jobRepo:        jobRepo,
		idempotencyRepo: idempotencyRepo,
		cache:          cache,
		redisQueue:     redisQueue,
	}
}

// SubmitJob accepts a job, checks idempotency key if provided, persists job & outbox atomically, and returns immediately
func (s *JobService) SubmitJob(ctx context.Context, orgID uuid.UUID, idempotencyKey string, req *SubmitJobRequest) (*models.Job, bool, error) {
	// 1. Idempotency handling
	var reqHash string
	if idempotencyKey != "" {
		hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%s", req.Type, string(req.Payload))))
		reqHash = hex.EncodeToString(hash[:])

		// Try to record pending idempotency key
		_, err := s.idempotencyRepo.CreatePending(ctx, orgID, idempotencyKey, reqHash)
		if errors.Is(err, repository.ErrAlreadyExists) {
			// Existing key found: fetch previous record
			existingRecord, err := s.idempotencyRepo.GetRecord(ctx, orgID, idempotencyKey)
			if err == nil && existingRecord != nil {
				// Reusing the same key with a different payload must be rejected
				if existingRecord.RequestHash != reqHash {
					return nil, false, ErrIdempotencyConflict
				}

				if existingRecord.Status == "processed" && len(existingRecord.ResponseBody) > 0 {
					var existingJob models.Job
					if err := json.Unmarshal(existingRecord.ResponseBody, &existingJob); err == nil {
						// Return existing job with deduplicated flag = true
						return &existingJob, true, nil
					}
				}
				// If status is still pending, another concurrent request is currently creating it
				return nil, true, ErrIdempotencyKeyPending
			}
		} else if err != nil {
			return nil, false, fmt.Errorf("failed to process idempotency key: %w", err)
		}
	}

	maxAttempts := req.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	job := &models.Job{
		OrganizationID: orgID,
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       req.Priority,
		ScheduledAt:    req.ScheduledAt,
		MaxAttempts:    maxAttempts,
	}
	if idempotencyKey != "" {
		job.IdempotencyKey = &idempotencyKey
	}

	// 2. Transactional persistence: DB writes Job + Outbox atomically
	createdJob, err := s.jobRepo.CreateJobWithOutbox(ctx, job)
	if err != nil {
		if idempotencyKey != "" {
			_ = s.idempotencyRepo.DeleteRecord(ctx, orgID, idempotencyKey)
		}
		return nil, false, fmt.Errorf("failed to create job: %w", err)
	}

	// 3. Save idempotency response if key provided
	if idempotencyKey != "" {
		respBytes, _ := json.Marshal(createdJob)
		_ = s.idempotencyRepo.SaveResponse(ctx, orgID, idempotencyKey, 202, respBytes)
	}

	// 4. Populate cache
	s.cache.SetJob(ctx, createdJob)

	return createdJob, false, nil
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
