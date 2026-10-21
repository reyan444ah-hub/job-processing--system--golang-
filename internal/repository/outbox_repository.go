package repository

import (
	"context"
	"time"

	"distributed-job-system/internal/database"
	"distributed-job-system/internal/models"
)

type OutboxRepository struct {
	db *database.DB
}

func NewOutboxRepository(db *database.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// FetchPendingEvents retrieves a batch of undispatched outbox events for publishing
func (r *OutboxRepository) FetchPendingEvents(ctx context.Context, batchSize int) ([]*models.OutboxEvent, error) {
	query := `
		SELECT id, organization_id, event_type, aggregate_type, aggregate_id, payload,
		       dispatched, dispatched_at, retry_count, last_error, created_at
		FROM outbox_events
		WHERE dispatched = FALSE
		ORDER BY id ASC
		LIMIT $1`

	rows, err := r.db.QueryContext(ctx, query, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*models.OutboxEvent
	for rows.Next() {
		ev := &models.OutboxEvent{}
		var rawPayload []byte
		err := rows.Scan(
			&ev.ID, &ev.OrganizationID, &ev.EventType, &ev.AggregateType, &ev.AggregateID, &rawPayload,
			&ev.Dispatched, &ev.DispatchedAt, &ev.RetryCount, &ev.LastError, &ev.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		ev.Payload = rawPayload
		events = append(events, ev)
	}

	return events, nil
}

// MarkDispatched marks an event as successfully published
func (r *OutboxRepository) MarkDispatched(ctx context.Context, id int64) error {
	now := time.Now().UTC()
	query := `UPDATE outbox_events SET dispatched = TRUE, dispatched_at = $1 WHERE id = $2`
	_, err := r.db.ExecContext(ctx, query, now, id)
	return err
}

// RecordDispatchFailure increments retry count and sets last error
func (r *OutboxRepository) RecordDispatchFailure(ctx context.Context, id int64, errMsg string) error {
	query := `UPDATE outbox_events SET retry_count = retry_count + 1, last_error = $1 WHERE id = $2`
	_, err := r.db.ExecContext(ctx, query, errMsg, id)
	return err
}

// StoreOutboxEventDirect allows inserting an outbox event standalone if needed
func (r *OutboxRepository) StoreOutboxEventDirect(ctx context.Context, ev *models.OutboxEvent) error {
	query := `
		INSERT INTO outbox_events (organization_id, event_type, aggregate_type, aggregate_id, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`
	now := time.Now().UTC()
	return r.db.QueryRowContext(ctx, query, ev.OrganizationID, ev.EventType, ev.AggregateType, ev.AggregateID, ev.Payload, now).Scan(&ev.ID)
}

// CleanUpDispatched removes old dispatched events older than a given retention threshold
func (r *OutboxRepository) CleanUpDispatched(ctx context.Context, retentionDuration time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-retentionDuration)
	query := `DELETE FROM outbox_events WHERE dispatched = TRUE AND dispatched_at < $1`
	res, err := r.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
