package queue_test

import (
	"context"
	"encoding/json"
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
	mock.ExpectQuery(`WITH pending AS`).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "event_type", "aggregate_type", "aggregate_id", "payload", "dispatched", "dispatched_at", "retry_count", "last_error", "created_at"}).
			AddRow(ev.ID, ev.OrganizationID, ev.EventType, ev.AggregateType, ev.AggregateID, ev.Payload, false, nil, 0, nil, ev.CreatedAt))

	// 2. MarkDispatched
	mock.ExpectExec(`UPDATE outbox_events SET dispatched = TRUE, dispatched_at = \$1, claimed_until = NULL WHERE id = \$2`).
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
	metrics, err := redisQueue.GetMetrics(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), metrics["stream_length"])
}

func TestOutboxPublisher_RetryAndDeadLetterDispatch(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient, err := rdb.Connect(mr.Addr(), "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	q := queue.NewRedisQueue(redisClient, "jobs:stream", "job-workers-group", "jobs:dlq")
	publisher := queue.NewOutboxPublisher(nil, q, redisClient.Client, time.Second)
	ctx := context.Background()
	orgID, jobID := uuid.New(), uuid.New()
	retryAt := time.Now().UTC().Add(time.Minute)

	retryEvent := &models.OutboxEvent{
		ID: 1, OrganizationID: orgID, EventType: "job.retrying", AggregateID: jobID.String(),
		Payload:   []byte(`{"retry_at":"` + retryAt.Format(time.RFC3339Nano) + `"}`),
		CreatedAt: time.Now().UTC(),
	}
	err = publisher.DispatchEvent(ctx, retryEvent)
	require.NoError(t, err)
	require.NoError(t, publisher.DispatchEvent(ctx, retryEvent), "replaying the same scheduled outbox event is idempotent")
	metrics, err := q.GetMetrics(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, metrics["scheduled_jobs"], "retry is persisted to the scheduled queue")

	err = publisher.DispatchEvent(ctx, &models.OutboxEvent{
		ID: 2, OrganizationID: orgID, EventType: "job.dead_letter", AggregateID: jobID.String(),
		Payload:   []byte(`{"attempt_count":3,"max_attempts":3,"error":"permanent failure"}`),
		CreatedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	dlqLength, err := redisClient.Client.XLen(ctx, "jobs:dlq").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, dlqLength, "dead-letter state is relayed to Redis")
}

func TestOutboxPublisher_TargetsCompletionNotificationToJobOwner(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient, err := rdb.Connect(mr.Addr(), "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	orgID, userID, jobID := uuid.New(), uuid.New(), uuid.New()
	channel := queue.EventBroadcastChannelPrefix + orgID.String()
	pubsub := redisClient.Client.Subscribe(context.Background(), channel)
	defer pubsub.Close()
	_, err = pubsub.Receive(context.Background())
	require.NoError(t, err)

	publisher := queue.NewOutboxPublisher(nil, nil, redisClient.Client, time.Second)
	event := &models.OutboxEvent{
		ID: 1, OrganizationID: orgID, EventType: "job.completed", AggregateType: "job",
		AggregateID: jobID.String(), Payload: []byte(`{"job_id":"` + jobID.String() + `","user_id":"` + userID.String() + `","job_type":"email.send","status":"completed"}`),
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, publisher.DispatchEvent(context.Background(), event))

	message, err := pubsub.ReceiveMessage(context.Background())
	require.NoError(t, err)
	var broadcast struct {
		EventType       string    `json:"event_type"`
		RecipientUserID uuid.UUID `json:"recipient_user_id"`
		AggregateID     string    `json:"aggregate_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(message.Payload), &broadcast))
	assert.Equal(t, "job.completed", broadcast.EventType)
	assert.Equal(t, userID, broadcast.RecipientUserID)
	assert.Equal(t, jobID.String(), broadcast.AggregateID)
}

func TestOutboxPublisher_RecordsDispatchFailureForRetry(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient, err := rdb.Connect(mr.Addr(), "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	dbMock, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbMock.Close()
	outboxRepo := repository.NewOutboxRepository(&database.DB{DB: dbMock})
	publisher := queue.NewOutboxPublisher(outboxRepo,
		queue.NewRedisQueue(redisClient, "jobs:stream", "job-workers-group", "jobs:dlq"),
		redisClient.Client, time.Second)
	event := &models.OutboxEvent{
		ID: 7, OrganizationID: uuid.New(), EventType: "job.queued",
		AggregateID: "invalid-uuid", Payload: []byte(`{}`), CreatedAt: time.Now().UTC(),
	}

	mock.ExpectQuery(`WITH pending AS`).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "event_type", "aggregate_type", "aggregate_id", "payload", "dispatched", "dispatched_at", "retry_count", "last_error", "created_at"}).
			AddRow(event.ID, event.OrganizationID, event.EventType, "job", event.AggregateID, event.Payload, false, nil, 0, nil, event.CreatedAt))
	mock.ExpectExec(`UPDATE outbox_events SET retry_count = retry_count \+ 1, last_error = \$1, claimed_until = NULL WHERE id = \$2`).
		WithArgs(sqlmock.AnyArg(), event.ID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	require.Error(t, publisher.ProcessBatch(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}
