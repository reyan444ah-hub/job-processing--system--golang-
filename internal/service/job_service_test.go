package service_test

import (
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
	idempRepo := repository.NewIdempotencyRepository(db)
	jobCache := cache.NewJobCache(redisClient, 1*time.Minute)
	redisQueue := queue.NewRedisQueue(redisClient, "test:stream", "test:group", "test:dlq")

	svc := service.NewJobService(jobRepo, idempRepo, jobCache, redisQueue)
	ctx := context.Background()

	orgID := uuid.New()
	jobID := uuid.New()
	idempKey := "order-12345"

	// 1. Submit Job with Idempotency Key (First submission)
	// CreatePending -> success
	mock.ExpectQuery(`INSERT INTO idempotency_keys`).
		WithArgs(sqlmock.AnyArg(), orgID, idempKey, sqlmock.AnyArg(), "pending", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "key", "request_hash", "status", "created_at", "updated_at"}).
			AddRow(uuid.New(), orgID, idempKey, "hash1", "pending", time.Now(), time.Now()))

	// CreateJobWithOutbox (ACID Transaction)
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO jobs`).
		WithArgs(sqlmock.AnyArg(), orgID, &idempKey, "email.send", sqlmock.AnyArg(), models.StatusQueued, 1, nil, 0, 3, 1, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "idempotency_key", "type", "payload", "status", "priority", "scheduled_at", "attempt_count", "max_attempts", "version", "created_at", "updated_at"}).
			AddRow(jobID, orgID, idempKey, "email.send", []byte(`{"to":"test@example.com"}`), models.StatusQueued, 1, nil, 0, 3, 1, time.Now(), time.Now()))
	mock.ExpectExec(`INSERT INTO job_events`).
		WithArgs(jobID, orgID, "job.queued", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO outbox_events`).
		WithArgs(orgID, "job.queued", "job", jobID.String(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// SaveResponse on Idempotency record
	mock.ExpectExec(`UPDATE idempotency_keys SET status = 'processed', response_code = \$1, response_body = \$2, updated_at = NOW\(\) WHERE organization_id = \$3 AND key = \$4`).
		WithArgs(202, sqlmock.AnyArg(), orgID, idempKey).
		WillReturnResult(sqlmock.NewResult(1, 1))

	job, dedup, err := svc.SubmitJob(ctx, orgID, idempKey, &service.SubmitJobRequest{
		Type:        "email.send",
		Payload:     json.RawMessage(`{"to":"test@example.com"}`),
		Priority:    1,
		MaxAttempts: 3,
	})
	assert.NoError(t, err)
	assert.False(t, dedup, "First call is not deduplicated")
	assert.Equal(t, jobID, job.ID)

	// 2. Submit Duplicate with same Key & Payload -> Return cached response (Deduplication)
	mock.ExpectQuery(`INSERT INTO idempotency_keys`).
		WithArgs(sqlmock.AnyArg(), orgID, idempKey, sqlmock.AnyArg(), "pending", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(sql.ErrNoRows) // ON CONFLICT DO NOTHING returns 0 rows

	cachedBody, _ := json.Marshal(job)
	expectedHash := "d78d100067644be716df070267e2ac18bf349c89423b0a702ab92276531beeb5" // sha256("email.send:{\"to\":\"test@example.com\"}")
	// Let's compute it explicitly:
	h := sha256.Sum256([]byte("email.send:{\"to\":\"test@example.com\"}"))
	expectedHash = hex.EncodeToString(h[:])

	mock.ExpectQuery(`SELECT id, organization_id, key, request_hash, status, response_code, response_body, created_at, updated_at FROM idempotency_keys`).
		WithArgs(orgID, idempKey).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "key", "request_hash", "status", "response_code", "response_body", "created_at", "updated_at"}).
			AddRow(uuid.New(), orgID, idempKey, expectedHash, "processed", 202, cachedBody, time.Now(), time.Now()))

	dupJob, dedup2, err := svc.SubmitJob(ctx, orgID, idempKey, &service.SubmitJobRequest{
		Type:        "email.send",
		Payload:     json.RawMessage(`{"to":"test@example.com"}`),
		Priority:    1,
		MaxAttempts: 3,
	})
	assert.NoError(t, err)
	assert.True(t, dedup2, "Duplicate call must return deduplicated = true")
	assert.Equal(t, job.ID, dupJob.ID)

	// 3. Submit with same key but DIFFERENT payload -> Must reject with ErrIdempotencyConflict
	mock.ExpectQuery(`INSERT INTO idempotency_keys`).
		WithArgs(sqlmock.AnyArg(), orgID, idempKey, sqlmock.AnyArg(), "pending", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(sql.ErrNoRows)

	mock.ExpectQuery(`SELECT id, organization_id, key, request_hash, status, response_code, response_body, created_at, updated_at FROM idempotency_keys`).
		WithArgs(orgID, idempKey).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "key", "request_hash", "status", "response_code", "response_body", "created_at", "updated_at"}).
			AddRow(uuid.New(), orgID, idempKey, "different_hash_value", "processed", 202, cachedBody, time.Now(), time.Now()))

	_, _, err = svc.SubmitJob(ctx, orgID, idempKey, &service.SubmitJobRequest{
		Type:        "email.send",
		Payload:     json.RawMessage(`{"to":"different_person@example.com"}`),
		Priority:    1,
		MaxAttempts: 3,
	})
	assert.ErrorIs(t, err, service.ErrIdempotencyConflict, "Payload mismatch on same key must return ErrIdempotencyConflict")
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
	idempRepo := repository.NewIdempotencyRepository(db)
	jobCache := cache.NewJobCache(redisClient, 1*time.Minute)
	redisQueue := queue.NewRedisQueue(redisClient, "test:stream", "test:group", "test:dlq")

	svc := service.NewJobService(jobRepo, idempRepo, jobCache, redisQueue)
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
