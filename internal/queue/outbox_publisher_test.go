package queue_test

import (
	"context"
	"testing"
	"time"

	"distributed-job-system/internal/database"
	"distributed-job-system/internal/models"
	"distributed-job-system/internal/queue"
	rdb "distributed-job-system/internal/redis"
	"distributed-job-system/internal/repository"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboxPublisher_DispatchAndRetry(t *testing.T) {
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
	outboxRepo := repository.NewOutboxRepository(db)
	redisQueue := queue.NewRedisQueue(redisClient, "jobs:stream", "job-workers-group", "jobs:dlq")

	publisher := queue.NewOutboxPublisher(outboxRepo, redisQueue, redisClient.Client, 100*time.Millisecond)
	ctx := context.Background()

	orgID := uuid.New()
	jobID := uuid.New()

	ev := &models.OutboxEvent{
		ID:             1,
		OrganizationID: orgID,
		EventType:      "job.queued",
		AggregateType:  "job",
		AggregateID:    jobID.String(),
		Payload:        []byte(`{"type":"email.send","priority":2}`),
		CreatedAt:      time.Now(),
	}

	// 1. FetchPendingEvents
	mock.ExpectQuery(`SELECT id, organization_id, event_type, aggregate_type, aggregate_id, payload, dispatched, dispatched_at, retry_count, last_error, created_at FROM outbox_events WHERE dispatched = FALSE`).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "event_type", "aggregate_type", "aggregate_id", "payload", "dispatched", "dispatched_at", "retry_count", "last_error", "created_at"}).
			AddRow(ev.ID, ev.OrganizationID, ev.EventType, ev.AggregateType, ev.AggregateID, ev.Payload, false, nil, 0, nil, ev.CreatedAt))

	// 2. MarkDispatched
	mock.ExpectExec(`UPDATE outbox_events SET dispatched = TRUE, dispatched_at = \$1 WHERE id = \$2`).
		WithArgs(sqlmock.AnyArg(), ev.ID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = publisher.ProcessBatch(ctx)
	assert.NoError(t, err)

	// Verify that the event was actually written to the Redis Stream
	metrics, err := redisQueue.GetMetrics(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), metrics["stream_length"])
}

func TestRedisQueue_ScheduledSweepRecovery(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient, err := rdb.Connect(mr.Addr(), "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	redisQueue := queue.NewRedisQueue(redisClient, "jobs:stream", "job-workers-group", "jobs:dlq")
	ctx := context.Background()

	orgID := uuid.New()
	jobID := uuid.New()

	msg := &queue.JobMessage{
		JobID:          jobID,
		OrganizationID: orgID,
		Type:           "scheduled.backup",
		Priority:       1,
	}

	// Schedule for right now (ready to run)
	err = redisQueue.ScheduleJob(ctx, msg, time.Now().Add(-1*time.Second))
	assert.NoError(t, err)

	// Sweep into stream
	count, err := redisQueue.SweepScheduledJobs(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify it reached stream
	metrics, err := redisQueue.GetMetrics(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), metrics["stream_length"])
}
