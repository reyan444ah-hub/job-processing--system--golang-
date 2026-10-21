package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"distributed-job-system/internal/database"
	"distributed-job-system/internal/models"

	"github.com/google/uuid"
)

type JobRepository struct {
	db *database.DB
}

func NewJobRepository(db *database.DB) *JobRepository {
	return &JobRepository{db: db}
}

// CreateJobWithOutbox performs an ACID transaction: creates job, writes initial job_event,
// and writes outbox_event for reliable async publishing.
func (r *JobRepository) CreateJobWithOutbox(ctx context.Context, job *models.Job) (*models.Job, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	now := time.Now().UTC()
	job.CreatedAt = now
	job.UpdatedAt = now
	job.Status = models.StatusQueued
	job.Version = 1

	// 1. Insert Job
	insertJobQuery := `
		INSERT INTO jobs (
			id, organization_id, idempotency_key, type, payload, status,
			priority, scheduled_at, attempt_count, max_attempts, version,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11,
			$12, $13
		) RETURNING id, organization_id, idempotency_key, type, payload, status, priority, scheduled_at, attempt_count, max_attempts, version, created_at, updated_at`

	err = tx.QueryRowContext(ctx, insertJobQuery,
		job.ID, job.OrganizationID, job.IdempotencyKey, job.Type, job.Payload, job.Status,
		job.Priority, job.ScheduledAt, job.AttemptCount, job.MaxAttempts, job.Version,
		job.CreatedAt, job.UpdatedAt,
	).Scan(
		&job.ID, &job.OrganizationID, &job.IdempotencyKey, &job.Type, &job.Payload, &job.Status,
		&job.Priority, &job.ScheduledAt, &job.AttemptCount, &job.MaxAttempts, &job.Version,
		&job.CreatedAt, &job.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert job: %w", err)
	}

	// 2. Insert Job Event
	eventPayload, _ := json.Marshal(map[string]interface{}{
		"job_id":          job.ID,
		"organization_id": job.OrganizationID,
		"status":          job.Status,
		"type":            job.Type,
		"priority":        job.Priority,
		"scheduled_at":    job.ScheduledAt,
	})

	insertEventQuery := `
		INSERT INTO job_events (job_id, organization_id, event_type, payload, created_at)
		VALUES ($1, $2, $3, $4, $5)`
	_, err = tx.ExecContext(ctx, insertEventQuery, job.ID, job.OrganizationID, "job.queued", eventPayload, now)
	if err != nil {
		return nil, fmt.Errorf("failed to insert job event: %w", err)
	}

	// 3. Insert Outbox Event
	insertOutboxQuery := `
		INSERT INTO outbox_events (organization_id, event_type, aggregate_type, aggregate_id, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`
	_, err = tx.ExecContext(ctx, insertOutboxQuery, job.OrganizationID, "job.queued", "job", job.ID.String(), eventPayload, now)
	if err != nil {
		return nil, fmt.Errorf("failed to insert outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit job creation transaction: %w", err)
	}

	return job, nil
}

func (r *JobRepository) GetJobByID(ctx context.Context, orgID, jobID uuid.UUID) (*models.Job, error) {
	job := &models.Job{}
	query := `
		SELECT id, organization_id, idempotency_key, type, payload, status,
		       priority, scheduled_at, attempt_count, max_attempts,
		       locked_by, locked_at, started_at, completed_at, result, last_error,
		       version, created_at, updated_at
		FROM jobs
		WHERE id = $1 AND organization_id = $2`

	var rawPayload, rawResult []byte
	err := r.db.QueryRowContext(ctx, query, jobID, orgID).Scan(
		&job.ID, &job.OrganizationID, &job.IdempotencyKey, &job.Type, &rawPayload, &job.Status,
		&job.Priority, &job.ScheduledAt, &job.AttemptCount, &job.MaxAttempts,
		&job.LockedBy, &job.LockedAt, &job.StartedAt, &job.CompletedAt, &rawResult, &job.LastError,
		&job.Version, &job.CreatedAt, &job.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	job.Payload = rawPayload
	job.Result = rawResult
	return job, nil
}

// GetJobByIDUnrestricted retrieves a job without org check (for internal worker processing)
func (r *JobRepository) GetJobByIDUnrestricted(ctx context.Context, jobID uuid.UUID) (*models.Job, error) {
	job := &models.Job{}
	query := `
		SELECT id, organization_id, idempotency_key, type, payload, status,
		       priority, scheduled_at, attempt_count, max_attempts,
		       locked_by, locked_at, started_at, completed_at, result, last_error,
		       version, created_at, updated_at
		FROM jobs
		WHERE id = $1`

	var rawPayload, rawResult []byte
	err := r.db.QueryRowContext(ctx, query, jobID).Scan(
		&job.ID, &job.OrganizationID, &job.IdempotencyKey, &job.Type, &rawPayload, &job.Status,
		&job.Priority, &job.ScheduledAt, &job.AttemptCount, &job.MaxAttempts,
		&job.LockedBy, &job.LockedAt, &job.StartedAt, &job.CompletedAt, &rawResult, &job.LastError,
		&job.Version, &job.CreatedAt, &job.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	job.Payload = rawPayload
	job.Result = rawResult
	return job, nil
}

func (r *JobRepository) ListJobs(ctx context.Context, orgID uuid.UUID, status string, limit, offset int) ([]*models.Job, int, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	whereClause := "WHERE organization_id = $1"
	args := []interface{}{orgID}
	paramCount := 1

	if status != "" {
		paramCount++
		whereClause += fmt.Sprintf(" AND status = $%d", paramCount)
		args = append(args, status)
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM jobs %s", whereClause)
	var total int
	err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	paramCount++
	limitParam := paramCount
	paramCount++
	offsetParam := paramCount
	args = append(args, limit, offset)

	selectQuery := fmt.Sprintf(`
		SELECT id, organization_id, idempotency_key, type, payload, status,
		       priority, scheduled_at, attempt_count, max_attempts,
		       locked_by, locked_at, started_at, completed_at, result, last_error,
		       version, created_at, updated_at
		FROM jobs
		%s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d`, whereClause, limitParam, offsetParam)

	rows, err := r.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		job := &models.Job{}
		var rawPayload, rawResult []byte
		err := rows.Scan(
			&job.ID, &job.OrganizationID, &job.IdempotencyKey, &job.Type, &rawPayload, &job.Status,
			&job.Priority, &job.ScheduledAt, &job.AttemptCount, &job.MaxAttempts,
			&job.LockedBy, &job.LockedAt, &job.StartedAt, &job.CompletedAt, &rawResult, &job.LastError,
			&job.Version, &job.CreatedAt, &job.UpdatedAt,
		)
		if err != nil {
			return nil, 0, err
		}
		job.Payload = rawPayload
		job.Result = rawResult
		jobs = append(jobs, job)
	}

	return jobs, total, nil
}

// ClaimJobAtomic atomically marks a job as processing if it is queued or retrying,
// protecting against concurrent workers or race conditions using optimistic concurrency and row locking.
func (r *JobRepository) ClaimJobAtomic(ctx context.Context, jobID uuid.UUID, workerName string) (*models.Job, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// SELECT ... FOR UPDATE ensures strict serialization on this job row
	query := `
		SELECT id, organization_id, idempotency_key, type, payload, status,
		       priority, scheduled_at, attempt_count, max_attempts,
		       locked_by, locked_at, started_at, completed_at, result, last_error,
		       version, created_at, updated_at
		FROM jobs
		WHERE id = $1
		FOR UPDATE`

	job := &models.Job{}
	var rawPayload, rawResult []byte
	err = tx.QueryRowContext(ctx, query, jobID).Scan(
		&job.ID, &job.OrganizationID, &job.IdempotencyKey, &job.Type, &rawPayload, &job.Status,
		&job.Priority, &job.ScheduledAt, &job.AttemptCount, &job.MaxAttempts,
		&job.LockedBy, &job.LockedAt, &job.StartedAt, &job.CompletedAt, &rawResult, &job.LastError,
		&job.Version, &job.CreatedAt, &job.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Check if already claimed or cancelled/completed
	if job.Status != models.StatusQueued && job.Status != models.StatusRetrying {
		return nil, fmt.Errorf("job %s is not in claimable state (current: %s)", jobID, job.Status)
	}

	now := time.Now().UTC()
	job.Status = models.StatusProcessing
	job.LockedBy = &workerName
	job.LockedAt = &now
	job.StartedAt = &now
	job.AttemptCount++
	job.Version++
	job.UpdatedAt = now

	updateQuery := `
		UPDATE jobs
		SET status = $1, locked_by = $2, locked_at = $3, started_at = $4,
		    attempt_count = $5, version = $6, updated_at = $7
		WHERE id = $8 AND version = $9`

	res, err := tx.ExecContext(ctx, updateQuery,
		job.Status, job.LockedBy, job.LockedAt, job.StartedAt,
		job.AttemptCount, job.Version, job.UpdatedAt, job.ID, job.Version-1,
	)
	if err != nil {
		return nil, err
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return nil, errors.New("optimistic lock failed while claiming job")
	}

	// Write event and outbox inside transaction
	eventPayload, _ := json.Marshal(map[string]interface{}{
		"job_id":        job.ID,
		"worker":        workerName,
		"attempt_count": job.AttemptCount,
		"status":        job.Status,
	})

	_, err = tx.ExecContext(ctx,
		`INSERT INTO job_events (job_id, organization_id, event_type, payload, created_at) VALUES ($1, $2, $3, $4, $5)`,
		job.ID, job.OrganizationID, "job.started", eventPayload, now)
	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox_events (organization_id, event_type, aggregate_type, aggregate_id, payload, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		job.OrganizationID, "job.started", "job", job.ID.String(), eventPayload, now)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	job.Payload = rawPayload
	job.Result = rawResult
	return job, nil
}

// CompleteJob updates job to completed with result
func (r *JobRepository) CompleteJob(ctx context.Context, jobID uuid.UUID, expectedVersion int, result json.RawMessage) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var orgID uuid.UUID
	err = tx.QueryRowContext(ctx, `SELECT organization_id FROM jobs WHERE id = $1 FOR UPDATE`, jobID).Scan(&orgID)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	updateQuery := `
		UPDATE jobs
		SET status = 'completed', result = $1, completed_at = $2, locked_by = NULL, locked_at = NULL,
		    version = version + 1, updated_at = $2
		WHERE id = $3 AND version = $4 AND status = 'processing'`

	res, err := tx.ExecContext(ctx, updateQuery, result, now, jobID, expectedVersion)
	if err != nil {
		return err
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("cannot complete job: version mismatch or job not in processing status")
	}

	eventPayload, _ := json.Marshal(map[string]interface{}{
		"job_id": jobID,
		"status": models.StatusCompleted,
		"result": result,
	})

	_, _ = tx.ExecContext(ctx,
		`INSERT INTO job_events (job_id, organization_id, event_type, payload, created_at) VALUES ($1, $2, $3, $4, $5)`,
		jobID, orgID, "job.completed", eventPayload, now)

	_, _ = tx.ExecContext(ctx,
		`INSERT INTO outbox_events (organization_id, event_type, aggregate_type, aggregate_id, payload, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		orgID, "job.completed", "job", jobID.String(), eventPayload, now)

	return tx.Commit()
}

// FailOrRetryJob transitions job to retrying or dead_letter based on attempt_count and max_attempts
func (r *JobRepository) FailOrRetryJob(ctx context.Context, jobID uuid.UUID, expectedVersion int, errMsg string, scheduledRetry *time.Time) (models.JobStatus, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var orgID uuid.UUID
	var attemptCount, maxAttempts int
	err = tx.QueryRowContext(ctx, `SELECT organization_id, attempt_count, max_attempts FROM jobs WHERE id = $1 FOR UPDATE`, jobID).
		Scan(&orgID, &attemptCount, &maxAttempts)
	if err != nil {
		return "", err
	}

	newStatus := models.StatusRetrying
	eventType := "job.retrying"
	if attemptCount >= maxAttempts {
		newStatus = models.StatusDeadLetter
		eventType = "job.dead_letter"
	}

	now := time.Now().UTC()
	var updateQuery string
	var res sql.Result

	if newStatus == models.StatusRetrying {
		updateQuery = `
			UPDATE jobs
			SET status = $1, last_error = $2, scheduled_at = $3, locked_by = NULL, locked_at = NULL,
			    version = version + 1, updated_at = $4
			WHERE id = $5 AND version = $6 AND status = 'processing'`
		res, err = tx.ExecContext(ctx, updateQuery, newStatus, errMsg, scheduledRetry, now, jobID, expectedVersion)
	} else {
		updateQuery = `
			UPDATE jobs
			SET status = $1, last_error = $2, completed_at = $3, locked_by = NULL, locked_at = NULL,
			    version = version + 1, updated_at = $3
			WHERE id = $4 AND version = $5 AND status = 'processing'`
		res, err = tx.ExecContext(ctx, updateQuery, newStatus, errMsg, now, jobID, expectedVersion)
	}

	if err != nil {
		return "", err
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return "", errors.New("cannot update job status: version mismatch or status not processing")
	}

	eventPayload, _ := json.Marshal(map[string]interface{}{
		"job_id":        jobID,
		"status":        newStatus,
		"attempt_count": attemptCount,
		"max_attempts":  maxAttempts,
		"error":         errMsg,
		"retry_at":      scheduledRetry,
	})

	_, _ = tx.ExecContext(ctx,
		`INSERT INTO job_events (job_id, organization_id, event_type, payload, created_at) VALUES ($1, $2, $3, $4, $5)`,
		jobID, orgID, eventType, eventPayload, now)

	_, _ = tx.ExecContext(ctx,
		`INSERT INTO outbox_events (organization_id, event_type, aggregate_type, aggregate_id, payload, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		orgID, eventType, "job", jobID.String(), eventPayload, now)

	if err := tx.Commit(); err != nil {
		return "", err
	}

	return newStatus, nil
}

// CancelJob cancels an eligible job (queued or retrying)
func (r *JobRepository) CancelJob(ctx context.Context, orgID, jobID uuid.UUID) (*models.Job, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	job := &models.Job{}
	query := `SELECT id, organization_id, status, version FROM jobs WHERE id = $1 AND organization_id = $2 FOR UPDATE`
	err = tx.QueryRowContext(ctx, query, jobID, orgID).Scan(&job.ID, &job.OrganizationID, &job.Status, &job.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if job.Status != models.StatusQueued && job.Status != models.StatusRetrying {
		return nil, fmt.Errorf("job is not eligible for cancellation (status: %s)", job.Status)
	}

	now := time.Now().UTC()
	updateQuery := `
		UPDATE jobs
		SET status = 'cancelled', completed_at = $1, version = version + 1, updated_at = $1
		WHERE id = $2 AND version = $3`

	res, err := tx.ExecContext(ctx, updateQuery, now, jobID, job.Version)
	if err != nil {
		return nil, err
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return nil, errors.New("concurrent modification detected during cancellation")
	}

	job.Status = models.StatusCancelled

	eventPayload, _ := json.Marshal(map[string]interface{}{
		"job_id": jobID,
		"status": models.StatusCancelled,
	})

	_, _ = tx.ExecContext(ctx,
		`INSERT INTO job_events (job_id, organization_id, event_type, payload, created_at) VALUES ($1, $2, $3, $4, $5)`,
		jobID, orgID, "job.cancelled", eventPayload, now)

	_, _ = tx.ExecContext(ctx,
		`INSERT INTO outbox_events (organization_id, event_type, aggregate_type, aggregate_id, payload, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		orgID, "job.cancelled", "job", jobID.String(), eventPayload, now)

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return job, nil
}

// RetryDeadLetterJob resets a dead_letter or failed job back to queued
func (r *JobRepository) RetryDeadLetterJob(ctx context.Context, orgID, jobID uuid.UUID) (*models.Job, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	job := &models.Job{}
	query := `SELECT id, organization_id, status, attempt_count, max_attempts, version FROM jobs WHERE id = $1 AND organization_id = $2 FOR UPDATE`
	err = tx.QueryRowContext(ctx, query, jobID, orgID).Scan(&job.ID, &job.OrganizationID, &job.Status, &job.AttemptCount, &job.MaxAttempts, &job.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if job.Status != models.StatusDeadLetter && job.Status != models.StatusFailed {
		return nil, fmt.Errorf("only dead_letter or failed jobs can be manually retried (current: %s)", job.Status)
	}

	now := time.Now().UTC()
	job.Status = models.StatusQueued
	job.AttemptCount = 0 // reset counter
	job.ScheduledAt = nil
	job.LastError = nil

	updateQuery := `
		UPDATE jobs
		SET status = 'queued', attempt_count = 0, scheduled_at = NULL, last_error = NULL, completed_at = NULL,
		    version = version + 1, updated_at = $1
		WHERE id = $2 AND version = $3`

	res, err := tx.ExecContext(ctx, updateQuery, now, jobID, job.Version)
	if err != nil {
		return nil, err
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return nil, errors.New("concurrent modification detected during retry")
	}

	eventPayload, _ := json.Marshal(map[string]interface{}{
		"job_id": jobID,
		"status": models.StatusQueued,
		"action": "manual_retry",
	})

	_, _ = tx.ExecContext(ctx,
		`INSERT INTO job_events (job_id, organization_id, event_type, payload, created_at) VALUES ($1, $2, $3, $4, $5)`,
		jobID, orgID, "job.queued", eventPayload, now)

	_, _ = tx.ExecContext(ctx,
		`INSERT INTO outbox_events (organization_id, event_type, aggregate_type, aggregate_id, payload, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		orgID, "job.queued", "job", jobID.String(), eventPayload, now)

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return job, nil
}

// GetJobEvents retrieves all events for a given job and organization
func (r *JobRepository) GetJobEvents(ctx context.Context, orgID, jobID uuid.UUID) ([]*models.JobEvent, error) {
	query := `
		SELECT id, job_id, organization_id, event_type, payload, created_at
		FROM job_events
		WHERE job_id = $1 AND organization_id = $2
		ORDER BY id ASC`

	rows, err := r.db.QueryContext(ctx, query, jobID, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*models.JobEvent
	for rows.Next() {
		ev := &models.JobEvent{}
		var rawPayload []byte
		err := rows.Scan(&ev.ID, &ev.JobID, &ev.OrganizationID, &ev.EventType, &rawPayload, &ev.CreatedAt)
		if err != nil {
			return nil, err
		}
		ev.Payload = rawPayload
		events = append(events, ev)
	}

	return events, nil
}

// GetStuckProcessingJobs finds jobs stuck in processing beyond a threshold (worker crash recovery fallback)
func (r *JobRepository) GetStuckProcessingJobs(ctx context.Context, idleDuration time.Duration) ([]*models.Job, error) {
	cutoff := time.Now().UTC().Add(-idleDuration)
	query := `
		SELECT id, organization_id, type, payload, status, attempt_count, max_attempts, version
		FROM jobs
		WHERE status = 'processing' AND locked_at < $1
		ORDER BY locked_at ASC
		LIMIT 50`

	rows, err := r.db.QueryContext(ctx, query, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		job := &models.Job{}
		var rawPayload []byte
		err := rows.Scan(&job.ID, &job.OrganizationID, &job.Type, &rawPayload, &job.Status, &job.AttemptCount, &job.MaxAttempts, &job.Version)
		if err != nil {
			return nil, err
		}
		job.Payload = rawPayload
		jobs = append(jobs, job)
	}
	return jobs, nil
}
