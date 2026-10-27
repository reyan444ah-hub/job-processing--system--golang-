package queue

import (
	"context"
	"encoding/json"
	"errors"
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

	var processingErrors []error
	for _, ev := range events {
		if err := p.DispatchEvent(ctx, ev); err != nil {
			if recordErr := p.outboxRepo.RecordDispatchFailure(ctx, ev.ID, err.Error()); recordErr != nil {
				processingErrors = append(processingErrors, fmt.Errorf("event %d dispatch failed: %v; recording failure also failed: %w", ev.ID, err, recordErr))
				continue
			}
			processingErrors = append(processingErrors, fmt.Errorf("event %d dispatch failed: %w", ev.ID, err))
		} else {
			if err := p.outboxRepo.MarkDispatched(ctx, ev.ID); err != nil {
				if recordErr := p.outboxRepo.RecordDispatchFailure(ctx, ev.ID, err.Error()); recordErr != nil {
					processingErrors = append(processingErrors, fmt.Errorf("event %d was dispatched but marking and failure recording failed: %w", ev.ID, errors.Join(err, recordErr)))
					continue
				}
				processingErrors = append(processingErrors, fmt.Errorf("event %d was dispatched but could not be marked: %w", ev.ID, err))
			}
		}
	}

	return errors.Join(processingErrors...)
}

func (p *OutboxPublisher) DispatchEvent(ctx context.Context, ev *models.OutboxEvent) error {
	// Queue transitions are relayed through the outbox so Redis failures can be retried.
	switch ev.EventType {
	case "job.queued", "job.retrying":
		jobID, err := uuid.Parse(ev.AggregateID)
		if err != nil {
			return fmt.Errorf("invalid job aggregate id %q: %w", ev.AggregateID, err)
		}
		var meta map[string]interface{}
		if err := json.Unmarshal(ev.Payload, &meta); err != nil {
			return fmt.Errorf("invalid job event payload: %w", err)
		}
		jobType, _ := meta["type"].(string)
		priority := 0
		if value, ok := meta["priority"].(float64); ok {
			priority = int(value)
		}

		msg := &JobMessage{
			JobID:          jobID,
			OrganizationID: ev.OrganizationID,
			Type:           jobType,
			Priority:       priority,
			EnqueuedAt:     ev.CreatedAt.UTC(),
		}
		scheduleField := "scheduled_at"
		if ev.EventType == "job.retrying" {
			scheduleField = "retry_at"
		}
		if scheduledAt, ok := meta[scheduleField].(string); ok && scheduledAt != "" {
			t, err := time.Parse(time.RFC3339Nano, scheduledAt)
			if err != nil {
				return fmt.Errorf("invalid %s in job event: %w", scheduleField, err)
			}
			if t.After(time.Now()) || ev.EventType == "job.retrying" {
				if err := p.queue.ScheduleJob(ctx, msg, t); err != nil {
					return fmt.Errorf("failed to schedule job: %w", err)
				}
			} else if _, err := p.queue.Publish(ctx, msg); err != nil {
				return fmt.Errorf("failed to publish to stream: %w", err)
			}
		} else if ev.EventType == "job.retrying" {
			return errors.New("retry event is missing retry_at")
		} else if _, err := p.queue.Publish(ctx, msg); err != nil {
			return fmt.Errorf("failed to publish to stream: %w", err)
		}

	case "job.dead_letter":
		jobID, err := uuid.Parse(ev.AggregateID)
		if err != nil {
			return fmt.Errorf("invalid job aggregate id %q: %w", ev.AggregateID, err)
		}
		var meta struct {
			AttemptCount int    `json:"attempt_count"`
			MaxAttempts  int    `json:"max_attempts"`
			Error        string `json:"error"`
		}
		if err := json.Unmarshal(ev.Payload, &meta); err != nil {
			return fmt.Errorf("invalid dead-letter event payload: %w", err)
		}
		if err := p.queue.SendToDLQ(ctx, &JobMessage{
			JobID:          jobID,
			OrganizationID: ev.OrganizationID,
			AttemptCount:   meta.AttemptCount,
			MaxAttempts:    meta.MaxAttempts,
			EnqueuedAt:     ev.CreatedAt,
		}, meta.Error); err != nil {
			return fmt.Errorf("failed to publish job to dead-letter queue: %w", err)
		}
	}

	// 2. Publish to Redis Pub/Sub for real-time WebSocket distribution to all connected server instances
	channel := fmt.Sprintf("%s%s", EventBroadcastChannelPrefix, ev.OrganizationID.String())
	var recipientUserID *uuid.UUID
	if ev.EventType == "job.completed" {
		var payload struct {
			UserID *uuid.UUID `json:"user_id"`
		}
		if err := json.Unmarshal(ev.Payload, &payload); err != nil {
			return fmt.Errorf("invalid job completion notification payload: %w", err)
		}
		recipientUserID = payload.UserID
	}
	broadcastMsg, err := json.Marshal(map[string]interface{}{
		"event_id":          ev.ID,
		"organization_id":   ev.OrganizationID,
		"event_type":        ev.EventType,
		"aggregate_type":    ev.AggregateType,
		"aggregate_id":      ev.AggregateID,
		"recipient_user_id": recipientUserID,
		"payload":           json.RawMessage(ev.Payload),
		"timestamp":         ev.CreatedAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal broadcast event: %w", err)
	}

	if err := p.redisClient.Publish(ctx, channel, broadcastMsg).Err(); err != nil {
		return fmt.Errorf("failed to broadcast event to Redis Pub/Sub: %w", err)
	}

	return nil
}
