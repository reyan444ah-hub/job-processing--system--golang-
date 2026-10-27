package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	rdb "distributed-job-system/internal/redis"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	DefaultStreamName    = "jobs:stream"
	DefaultConsumerGroup = "job-workers-group"
	DefaultDLQStream     = "jobs:dlq"
	ScheduledZSetKey     = "jobs:scheduled"
	MetricsProcessedKey  = "metrics:jobs:processed"
	MetricsFailedKey     = "metrics:jobs:failed"
	WorkerHeartbeatKey   = "workers:heartbeats"
)

type JobMessage struct {
	JobID          uuid.UUID `json:"job_id"`
	OrganizationID uuid.UUID `json:"organization_id"`
	Type           string    `json:"type"`
	Priority       int       `json:"priority"`
	AttemptCount   int       `json:"attempt_count"`
	MaxAttempts    int       `json:"max_attempts"`
	EnqueuedAt     time.Time `json:"enqueued_at"`
}

type RedisQueue struct {
	client        *rdb.Client
	streamName    string
	consumerGroup string
	dlqStreamName string
}

func NewRedisQueue(client *rdb.Client, streamName, consumerGroup, dlqStreamName string) *RedisQueue {
	if streamName == "" {
		streamName = DefaultStreamName
	}
	if consumerGroup == "" {
		consumerGroup = DefaultConsumerGroup
	}
	if dlqStreamName == "" {
		dlqStreamName = DefaultDLQStream
	}
	return &RedisQueue{
		client:        client,
		streamName:    streamName,
		consumerGroup: consumerGroup,
		dlqStreamName: dlqStreamName,
	}
}

// EnsureConsumerGroup creates the stream consumer group if it doesn't already exist
func (q *RedisQueue) EnsureConsumerGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.streamName, q.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// Publish enqueues a job message into Redis Stream
func (q *RedisQueue) Publish(ctx context.Context, msg *JobMessage) (string, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	res, err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.streamName,
		Values: map[string]interface{}{
			"payload": string(data),
			"job_id":  msg.JobID.String(),
		},
	}).Result()

	return res, err
}

// ScheduleJob places a job into a Sorted Set with timestamp score for future execution
func (q *RedisQueue) ScheduleJob(ctx context.Context, msg *JobMessage, runAt time.Time) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	score := float64(runAt.UnixMilli())
	return q.client.ZAdd(ctx, ScheduledZSetKey, redis.Z{
		Score:  score,
		Member: string(data),
	}).Err()
}

// SweepScheduledJobs moves ready jobs from the scheduled Sorted Set to the main Stream
func (q *RedisQueue) SweepScheduledJobs(ctx context.Context) (int, error) {
	nowScore := fmt.Sprintf("%d", time.Now().UnixMilli())

	// ZPOPMIN or ZRANGEBYSCORE with lock
	members, err := q.client.ZRangeByScore(ctx, ScheduledZSetKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    nowScore,
		Offset: 0,
		Count:  50,
	}).Result()
	if err != nil || len(members) == 0 {
		return 0, err
	}

	count := 0
	for _, raw := range members {
		// Atomic removal check to avoid duplicate sweep across multiple workers
		removed, err := q.client.ZRem(ctx, ScheduledZSetKey, raw).Result()
		if err != nil || removed == 0 {
			continue
		}

		var msg JobMessage
		if err := json.Unmarshal([]byte(raw), &msg); err == nil {
			if _, err := q.Publish(ctx, &msg); err != nil {
				// Prevent job loss: re-add to scheduled set with current score
				_ = q.client.ZAdd(ctx, ScheduledZSetKey, redis.Z{
					Score:  float64(time.Now().UnixMilli()),
					Member: raw,
				}).Err()
				continue
			}
			count++
		}
	}
	return count, nil
}

// ConsumeNext reads jobs from the consumer group. If count=0, uses default 1.
func (q *RedisQueue) ConsumeNext(ctx context.Context, workerName string, count int64, block time.Duration) ([]redis.XMessage, error) {
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.consumerGroup,
		Consumer: workerName,
		Streams:  []string{q.streamName, ">"},
		Count:    count,
		Block:    block,
	}).Result()

	if err != nil {
		if errorsIsNil(err) {
			return nil, nil
		}
		return nil, err
	}

	if len(streams) > 0 {
		return streams[0].Messages, nil
	}
	return nil, nil
}

// Ack acknowledges message completion in Redis Stream
func (q *RedisQueue) Ack(ctx context.Context, messageID string) error {
	return q.client.XAck(ctx, q.streamName, q.consumerGroup, messageID).Err()
}

// ClaimPendingMessages uses XAUTOCLAIM to reclaim messages pending in crashed workers (PEL recovery)
func (q *RedisQueue) ClaimPendingMessages(ctx context.Context, workerName string, minIdle time.Duration, count int64) ([]redis.XMessage, error) {
	messages, _, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   q.streamName,
		Group:    q.consumerGroup,
		Consumer: workerName,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    count,
	}).Result()

	if err != nil {
		if errorsIsNil(err) {
			return nil, nil
		}
		return nil, err
	}
	return messages, nil
}

// SendToDLQ routes an unrecoverable job to the Dead-Letter Queue stream
func (q *RedisQueue) SendToDLQ(ctx context.Context, msg *JobMessage, reason string) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to serialize dead-letter message: %w", err)
	}
	err = q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.dlqStreamName,
		Values: map[string]interface{}{
			"payload":   string(data),
			"job_id":    msg.JobID.String(),
			"reason":    reason,
			"failed_at": time.Now().UTC().Format(time.RFC3339),
		},
	}).Err()

	return err
}

// CalculateBackoff calculates exponential backoff with jitter
// Formula: min(maxBackoff, base * 2^(attempt-1)) + jitter
func CalculateBackoff(attempt int, base time.Duration, maxBackoff time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	multiplier := math.Pow(2, float64(attempt-1))
	backoffSec := base.Seconds() * multiplier

	maxSec := maxBackoff.Seconds()
	if backoffSec > maxSec {
		backoffSec = maxSec
	}

	// Full jitter: random between 0.5 and 1.5 of backoffSec
	jitter := (rand.Float64()*1.0 - 0.5) * (backoffSec * 0.3)
	totalSec := backoffSec + jitter
	if totalSec < base.Seconds() {
		totalSec = base.Seconds()
	}

	return time.Duration(totalSec * float64(time.Second))
}

// Heartbeat records worker liveness in a Redis hash
func (q *RedisQueue) Heartbeat(ctx context.Context, workerName string) error {
	return q.client.HSet(ctx, WorkerHeartbeatKey, workerName, time.Now().Unix()).Err()
}

// GetMetrics returns active stream length, pending count, dlq count, and worker heartbeats
func (q *RedisQueue) GetMetrics(ctx context.Context) (map[string]interface{}, error) {
	streamLen, _ := q.client.XLen(ctx, q.streamName).Result()
	dlqLen, _ := q.client.XLen(ctx, q.dlqStreamName).Result()
	scheduledCount, _ := q.client.ZCard(ctx, ScheduledZSetKey).Result()

	// Pending entries in group
	var pendingCount int64 = 0
	groupInfo, err := q.client.XInfoGroups(ctx, q.streamName).Result()
	if err == nil {
		for _, g := range groupInfo {
			if g.Name == q.consumerGroup {
				pendingCount = g.Pending
				break
			}
		}
	}

	// Workers
	workers, _ := q.client.HGetAll(ctx, WorkerHeartbeatKey).Result()
	activeWorkers := make([]string, 0)
	now := time.Now().Unix()
	for w, tsStr := range workers {
		var ts int64
		_, _ = fmt.Sscanf(tsStr, "%d", &ts)
		if now-ts < 60 { // Active within 60s
			activeWorkers = append(activeWorkers, w)
		}
	}

	return map[string]interface{}{
		"stream_length":    streamLen,
		"pending_messages": pendingCount,
		"dlq_length":       dlqLen,
		"scheduled_jobs":   scheduledCount,
		"active_workers":   activeWorkers,
		"worker_count":     len(activeWorkers),
		"consumer_group":   q.consumerGroup,
	}, nil
}

func errorsIsNil(err error) bool {
	return err == redis.Nil
}
