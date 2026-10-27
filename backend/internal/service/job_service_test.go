package service_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"distributed-job-system/internal/cache"
	"distributed-job-system/internal/database"
	"distributed-job-system/internal/models"
	"distributed-job-system/internal/queue"
	rdb "distributed-job-system/internal/redis"
	"distributed-job-system/internal/repository"
	"distributed-job-system/internal/service"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobService_SubmitAndIdempotency(t *testing.T) {
	// Setup Miniredis
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient, err := rdb.Connect(mr.Addr(), "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	// Setup SQL mock
	dbMock, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbMock.Close()

	db := &database.DB{DB: dbMock}
	jobRepo := repository.NewJobRepository(db)
	jobCache := cache.NewJobCache(redisClient, 1*time.Minute)
	redisQueue := queue.NewRedisQueue(redisClient, "test:stream", "test:group", "test:dlq")

	svc := service.NewJobService(jobRepo, jobCache, redisQueue)
	ctx := context.Background()

	orgID := uuid.New()
	userID := uuid.New()
	jobID := uuid.New()
	idempKey := "order-12345"

	// 1. Submit Job with Idempotency Key (First submission)
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO idempotency_keys`).
		WithArgs(sqlmock.AnyArg(), orgID, idempKey, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(uuid.New()))

	// CreateJobWithOutbox (ACID Transaction)
	mock.ExpectQuery(`INSERT INTO jobs`).
		WithArgs(sqlmock.AnyArg(), orgID, &userID, &idempKey, "email.send", sqlmock.AnyArg(), models.StatusQueued, 1, nil, 0, 3, 1, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "idempotency_key", "type", "payload", "status", "priority", "scheduled_at", "attempt_count", "max_attempts", "version", "created_at", "updated_at"}).
			AddRow(jobID, orgID, idempKey, "email.send", []byte(`{"to":"test@example.com"}`), models.StatusQueued, 1, nil, 0, 3, 1, time.Now(), time.Now()))
	mock.ExpectExec(`INSERT INTO job_events`).
		WithArgs(jobID, orgID, "job.queued", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO outbox_events`).
		WithArgs(orgID, "job.queued", "job", jobID.String(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE idempotency_keys SET status = 'processed', response_code = 202, response_body = \$1, updated_at = NOW\(\) WHERE organization_id = \$2 AND key = \$3 AND request_hash = \$4`).
		WithArgs(sqlmock.AnyArg(), orgID, idempKey, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	job, dedup, err := svc.SubmitJobForUser(ctx, orgID, userID, idempKey, &service.SubmitJobRequest{
		Type:        "email.send",
		Payload:     json.RawMessage(`{"to":"test@example.com"}`),
		Priority:    1,
		MaxAttempts: 3,
	})
	assert.NoError(t, err)
	assert.False(t, dedup, "First call is not deduplicated")
	assert.Equal(t, jobID, job.ID)

	// 2. Submit Duplicate with same Key & Payload -> Return cached response (Deduplication)
	cachedBody, _ := json.Marshal(job)
	expectedHash := requestHashForTest(t, &userID, service.SubmitJobRequest{
		Type: "email.send", Payload: json.RawMessage(`{"to":"test@example.com"}`), Priority: 1, MaxAttempts: 3,
	})
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO idempotency_keys`).
		WithArgs(sqlmock.AnyArg(), orgID, idempKey, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(sql.ErrNoRows) // ON CONFLICT DO NOTHING returns 0 rows
	mock.ExpectQuery(`SELECT request_hash, status, response_body FROM idempotency_keys`).
		WithArgs(orgID, idempKey).
		WillReturnRows(sqlmock.NewRows([]string{"request_hash", "status", "response_body"}).
			AddRow(expectedHash, "processed", cachedBody))
	mock.ExpectRollback()

	dupJob, dedup2, err := svc.SubmitJobForUser(ctx, orgID, userID, idempKey, &service.SubmitJobRequest{
		Type:        "email.send",
		Payload:     json.RawMessage(`{"to": "test@example.com"}`),
		Priority:    1,
		MaxAttempts: 3,
	})
	assert.NoError(t, err)
	assert.True(t, dedup2, "Duplicate call must return deduplicated = true")
	assert.Equal(t, job.ID, dupJob.ID)

	// 3. Changing an execution option while reusing the key must be rejected.
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO idempotency_keys`).
		WithArgs(sqlmock.AnyArg(), orgID, idempKey, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(sql.ErrNoRows)

	mock.ExpectQuery(`SELECT request_hash, status, response_body FROM idempotency_keys`).
		WithArgs(orgID, idempKey).
		WillReturnRows(sqlmock.NewRows([]string{"request_hash", "status", "response_body"}).
			AddRow(expectedHash, "processed", cachedBody))
	mock.ExpectRollback()

	_, _, err = svc.SubmitJobForUser(ctx, orgID, userID, idempKey, &service.SubmitJobRequest{
		Type:        "email.send",
		Payload:     json.RawMessage(`{"to":"test@example.com"}`),
		Priority:    2,
		MaxAttempts: 3,
	})
	assert.ErrorIs(t, err, service.ErrIdempotencyConflict, "Changed priority on same key must return ErrIdempotencyConflict")
	require.NoError(t, mock.ExpectationsWereMet())
}

func requestHashForTest(t *testing.T, createdByUserID *uuid.UUID, req service.SubmitJobRequest) string {
	t.Helper()
	var compactPayload bytes.Buffer
	require.NoError(t, json.Compact(&compactPayload, req.Payload))
	var payload interface{}
	require.NoError(t, json.Unmarshal(compactPayload.Bytes(), &payload))
	canonicalPayload, err := json.Marshal(payload)
	require.NoError(t, err)
	fingerprint, err := json.Marshal(struct {
		Type            string          `json:"type"`
		CreatedByUserID *uuid.UUID      `json:"created_by_user_id,omitempty"`
		Payload         json.RawMessage `json:"payload"`
		Priority        int             `json:"priority"`
		ScheduledAt     *time.Time      `json:"scheduled_at,omitempty"`
		MaxAttempts     int             `json:"max_attempts"`
	}{
		Type:            req.Type,
		CreatedByUserID: createdByUserID,
		Payload:         canonicalPayload, Priority: req.Priority,
		ScheduledAt: req.ScheduledAt, MaxAttempts: req.MaxAttempts,
	})
	require.NoError(t, err)
	hash := sha256.Sum256(fingerprint)
	return hex.EncodeToString(hash[:])
}

func TestJobService_CacheHitAndInvalidation(t *testing.T) {
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

	svc := service.NewJobService(jobRepo, jobCache, redisQueue)
	ctx := context.Background()

	orgID := uuid.New()
	jobID := uuid.New()

	// Prime Cache
	primeJob := &models.Job{
		ID:             jobID,
		OrganizationID: orgID,
		Type:           "report.generate",
		Status:         models.StatusQueued,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	jobCache.SetJob(ctx, primeJob)

	// GET Job: should be served directly from Redis Cache without hitting PostgreSQL DB
	cachedJob, err := svc.GetJob(ctx, orgID, jobID)
	assert.NoError(t, err)
	assert.Equal(t, jobID, cachedJob.ID)

	// Invalidate Cache
	jobCache.Invalidate(ctx, orgID, jobID)

	// GET Job after invalidation: should query PostgreSQL
	mock.ExpectQuery(`SELECT id, organization_id, idempotency_key, type, payload, status`).
		WithArgs(jobID, orgID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "idempotency_key", "type", "payload", "status", "priority", "scheduled_at", "attempt_count", "max_attempts", "locked_by", "locked_at", "started_at", "completed_at", "result", "last_error", "version", "created_at", "updated_at"}).
			AddRow(jobID, orgID, nil, "report.generate", []byte(`{}`), models.StatusProcessing, 1, nil, 1, 3, nil, nil, nil, nil, nil, nil, 2, time.Now(), time.Now()))

	dbJob, err := svc.GetJob(ctx, orgID, jobID)
	assert.NoError(t, err)
	assert.Equal(t, models.StatusProcessing, dbJob.Status)
}
