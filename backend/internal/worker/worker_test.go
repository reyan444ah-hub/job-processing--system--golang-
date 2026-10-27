package worker_test

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"distributed-job-system/internal/cache"
	"distributed-job-system/internal/database"
	"distributed-job-system/internal/models"
	"distributed-job-system/internal/queue"
	rdb "distributed-job-system/internal/redis"
	"distributed-job-system/internal/repository"
	"distributed-job-system/internal/worker"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type completedNotificationPayload struct {
	userID uuid.UUID
}

func (p completedNotificationPayload) Match(value driver.Value) bool {
	raw, ok := value.([]byte)
	if !ok {
		return false
	}
	var payload struct {
		UserID  uuid.UUID `json:"user_id"`
		JobType string    `json:"job_type"`
		Status  string    `json:"status"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false
	}
	return payload.UserID == p.userID && payload.JobType == "email.send" && payload.Status == "completed"
}

// Export or test worker execution behavior
func TestWorker_ExecutionLifecycle(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient, err := rdb.Connect(mr.Addr(), "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	dbMock, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbMock.Close()

	db := &database.DB{DB: dbMock}
	jobRepo := repository.NewJobRepository(db)
	jobCache := cache.NewJobCache(redisClient, 1*time.Minute)
	redisQueue := queue.NewRedisQueue(redisClient, "test:stream", "test:group", "test:dlq")

	w := worker.NewWorker("worker-unit-test", 1, redisQueue, jobRepo, jobCache, 10*time.Second)
	require.NotNil(t, w)

	ctx := context.Background()
	orgID := uuid.New()
	jobID := uuid.New()

	// 1. Test Atomic Claim
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, organization_id, idempotency_key, type, payload, status, priority, scheduled_at, attempt_count, max_attempts, locked_by, locked_at, started_at, completed_at, result, last_error, version, created_at, updated_at FROM jobs WHERE id = \$1 FOR UPDATE`).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "idempotency_key", "type", "payload", "status", "priority", "scheduled_at", "attempt_count", "max_attempts", "locked_by", "locked_at", "started_at", "completed_at", "result", "last_error", "version", "created_at", "updated_at"}).
			AddRow(jobID, orgID, nil, "email.send", []byte(`{"to":"test@example.com"}`), models.StatusQueued, 1, nil, 0, 3, nil, nil, nil, nil, nil, nil, 1, time.Now(), time.Now()))

	mock.ExpectExec(`UPDATE jobs SET status = \$1, locked_by = \$2, locked_at = \$3, started_at = \$4, attempt_count = \$5, version = \$6, updated_at = \$7 WHERE id = \$8 AND version = \$9`).
		WithArgs(models.StatusProcessing, "worker-unit-test", sqlmock.AnyArg(), sqlmock.AnyArg(), 1, 2, sqlmock.AnyArg(), jobID, 1).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(`INSERT INTO job_events`).WithArgs(jobID, orgID, "job.started", sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO outbox_events`).WithArgs(orgID, "job.started", "job", jobID.String(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	claimedJob, err := jobRepo.ClaimJobAtomic(ctx, jobID, "worker-unit-test", false, 10*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, models.StatusProcessing, claimedJob.Status)
	assert.Equal(t, 2, claimedJob.Version)

	// 2. Test Safe Completion with optimistic locking
	mock.ExpectBegin()
	creatorID := uuid.New()
	mock.ExpectQuery(`SELECT organization_id, created_by_user_id, type FROM jobs WHERE id = \$1 FOR UPDATE`).WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"organization_id", "created_by_user_id", "type"}).AddRow(orgID, creatorID, "email.send"))
	mock.ExpectExec(`UPDATE jobs SET status = 'completed', result = \$1, completed_at = \$2, locked_by = NULL, locked_at = NULL, version = version \+ 1, updated_at = \$2 WHERE id = \$3 AND version = \$4 AND status = 'processing'`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), jobID, 2).
		WillReturnResult(sqlmock.NewResult(1, 1))
	notificationPayload := completedNotificationPayload{userID: creatorID}
	mock.ExpectExec(`INSERT INTO job_events`).WithArgs(jobID, orgID, "job.completed", notificationPayload, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO outbox_events`).WithArgs(orgID, "job.completed", "job", jobID.String(), notificationPayload, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	resultData, _ := json.Marshal(map[string]interface{}{"status": "delivered"})
	err = jobRepo.CompleteJob(ctx, jobID, 2, resultData)
	assert.NoError(t, err)

	// 3. Test Stale Worker Prevention: if version does not match, update returns 0 affected rows and errors
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT organization_id, created_by_user_id, type FROM jobs WHERE id = \$1 FOR UPDATE`).WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"organization_id", "created_by_user_id", "type"}).AddRow(orgID, creatorID, "email.send"))
	mock.ExpectExec(`UPDATE jobs SET status = 'completed', result = \$1, completed_at = \$2, locked_by = NULL, locked_at = NULL, version = version \+ 1, updated_at = \$2 WHERE id = \$3 AND version = \$4 AND status = 'processing'`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), jobID, 1). // old version 1
		WillReturnResult(sqlmock.NewResult(1, 0))               // 0 rows affected
	mock.ExpectRollback()

	err = jobRepo.CompleteJob(ctx, jobID, 1, resultData)
	assert.Error(t, err, "Stale worker attempt with mismatched version must be rejected")
}

func TestClaimJobAtomic_ReclaimsOnlyStaleProcessingJobs(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbMock.Close()
	jobRepo := repository.NewJobRepository(&database.DB{DB: dbMock})
	ctx := context.Background()
	orgID, staleJobID, activeJobID := uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC()
	columns := []string{"id", "organization_id", "idempotency_key", "type", "payload", "status", "priority", "scheduled_at", "attempt_count", "max_attempts", "locked_by", "locked_at", "started_at", "completed_at", "result", "last_error", "version", "created_at", "updated_at"}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, organization_id, idempotency_key, type, payload, status, priority, scheduled_at, attempt_count, max_attempts, locked_by, locked_at, started_at, completed_at, result, last_error, version, created_at, updated_at FROM jobs WHERE id = \$1 FOR UPDATE`).
		WithArgs(staleJobID).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(staleJobID, orgID, nil, "test", []byte(`{}`), models.StatusProcessing, 0, nil, 1, 3, "old-worker", now.Add(-2*time.Minute), now.Add(-2*time.Minute), nil, nil, nil, 4, now, now))
	mock.ExpectExec(`UPDATE jobs SET status = \$1, locked_by = \$2, locked_at = \$3, started_at = \$4, attempt_count = \$5, version = \$6, updated_at = \$7 WHERE id = \$8 AND version = \$9`).
		WithArgs(models.StatusProcessing, "recovery-worker", sqlmock.AnyArg(), sqlmock.AnyArg(), 2, 5, sqlmock.AnyArg(), staleJobID, 4).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO job_events`).WithArgs(staleJobID, orgID, "job.started", sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO outbox_events`).WithArgs(orgID, "job.started", "job", staleJobID.String(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	reclaimed, err := jobRepo.ClaimJobAtomic(ctx, staleJobID, "recovery-worker", true, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 2, reclaimed.AttemptCount)
	assert.Equal(t, 5, reclaimed.Version)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, organization_id, idempotency_key, type, payload, status, priority, scheduled_at, attempt_count, max_attempts, locked_by, locked_at, started_at, completed_at, result, last_error, version, created_at, updated_at FROM jobs WHERE id = \$1 FOR UPDATE`).
		WithArgs(activeJobID).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(activeJobID, orgID, nil, "test", []byte(`{}`), models.StatusProcessing, 0, nil, 1, 3, "active-worker", now, now, nil, nil, nil, 4, now, now))
	mock.ExpectRollback()

	_, err = jobRepo.ClaimJobAtomic(ctx, activeJobID, "recovery-worker", true, time.Minute)
	assert.True(t, errors.Is(err, repository.ErrJobNotClaimable), "fresh processing jobs must not be stolen")
	require.NoError(t, mock.ExpectationsWereMet())
}
