package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"distributed-job-system/internal/models"
	"distributed-job-system/internal/repository"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	EventBroadcastChannelPrefix = "events:org:"
)

type OutboxPublisher struct {
	outboxRepo   *repository.OutboxRepository
	queue        *RedisQueue
	redisClient  *redis.Client
	pollInterval time.Duration
	stopChan     chan struct{}
}

func NewOutboxPublisher(outboxRepo *repository.OutboxRepository, q *RedisQueue, rdb *redis.Client, pollInterval time.Duration) *OutboxPublisher {
	if pollInterval <= 0 {
		pollInterval = 200 * time.Millisecond
	}
	return &OutboxPublisher{
		outboxRepo:   outboxRepo,
		queue:        q,
		redisClient:  rdb,
		pollInterval: pollInterval,
		stopChan:     make(chan struct{}),
	}
}

func (p *OutboxPublisher) Start(ctx context.Context) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	log.Println("[OutboxPublisher] Started polling transactional outbox...")

	for {
		select {
		case <-ctx.Done():
			log.Println("[OutboxPublisher] Shutting down...")
			return
		case <-p.stopChan:
			log.Println("[OutboxPublisher] Stopped.")
			return
		case <-ticker.C:
			if err := p.ProcessBatch(ctx); err != nil {
				log.Printf("[OutboxPublisher] Error processing batch: %v\n", err)
			}
		}
	}
}

func (p *OutboxPublisher) Stop() {
	close(p.stopChan)
}

func (p *OutboxPublisher) ProcessBatch(ctx context.Context) error {
	events, err := p.outboxRepo.FetchPendingEvents(ctx, 50)
	if err != nil {
		return err
	}

	for _, ev := range events {
		if err := p.DispatchEvent(ctx, ev); err != nil {
			_ = p.outboxRepo.RecordDispatchFailure(ctx, ev.ID, err.Error())
		} else {
			_ = p.outboxRepo.MarkDispatched(ctx, ev.ID)
		}
	}

	return nil
}

func (p *OutboxPublisher) DispatchEvent(ctx context.Context, ev *models.OutboxEvent) error {
	// 1. If it's a job creation/queued event, publish to Redis Queue stream so workers can consume it
	if ev.EventType == "job.queued" {
		jobID, err := uuid.Parse(ev.AggregateID)
		if err == nil {
			// Extract payload details if present
			var meta map[string]interface{}
			_ = json.Unmarshal(ev.Payload, &meta)

			jobType, _ := meta["type"].(string)
			priority := 0
			if pVal, ok := meta["priority"].(float64); ok {
				priority = int(pVal)
			}

			var scheduledAt *time.Time
			if schedStr, ok := meta["scheduled_at"].(string); ok && schedStr != "" {
				if t, err := time.Parse(time.RFC3339, schedStr); err == nil && t.After(time.Now()) {
					scheduledAt = &t
				}
			}

			msg := &JobMessage{
				JobID:          jobID,
				OrganizationID: ev.OrganizationID,
				Type:           jobType,
				Priority:       priority,
				EnqueuedAt:     time.Now().UTC(),
			}

			if scheduledAt != nil {
				if err := p.queue.ScheduleJob(ctx, msg, *scheduledAt); err != nil {
					return fmt.Errorf("failed to schedule job: %w", err)
				}
			} else {
				if _, err := p.queue.Publish(ctx, msg); err != nil {
					return fmt.Errorf("failed to publish to stream: %w", err)
				}
			}
		}
	}

	// 2. Publish to Redis Pub/Sub for real-time WebSocket distribution to all connected server instances
	channel := fmt.Sprintf("%s%s", EventBroadcastChannelPrefix, ev.OrganizationID.String())
	broadcastMsg, err := json.Marshal(map[string]interface{}{
		"event_id":        ev.ID,
		"organization_id": ev.OrganizationID,
		"event_type":      ev.EventType,
		"aggregate_type":  ev.AggregateType,
		"aggregate_id":    ev.AggregateID,
		"payload":         json.RawMessage(ev.Payload),
		"timestamp":       ev.CreatedAt.Format(time.RFC3339Nano),
	})
	if err == nil {
		_ = p.redisClient.Publish(ctx, channel, broadcastMsg).Err()
	}

	return nil
}
