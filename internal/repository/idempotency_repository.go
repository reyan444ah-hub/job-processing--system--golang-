package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"distributed-job-system/internal/database"
	"distributed-job-system/internal/models"

	"github.com/google/uuid"
)

type IdempotencyRepository struct {
	db *database.DB
}

func NewIdempotencyRepository(db *database.DB) *IdempotencyRepository {
	return &IdempotencyRepository{db: db}
}

// GetRecord returns existing idempotency key record for an organization
func (r *IdempotencyRepository) GetRecord(ctx context.Context, orgID uuid.UUID, key string) (*models.IdempotencyKey, error) {
	record := &models.IdempotencyKey{}
	query := `
		SELECT id, organization_id, key, request_hash, status, response_code, response_body, created_at, updated_at
		FROM idempotency_keys
		WHERE organization_id = $1 AND key = $2`

	var rawResponse []byte
	err := r.db.QueryRowContext(ctx, query, orgID, key).Scan(
		&record.ID, &record.OrganizationID, &record.Key, &record.RequestHash,
		&record.Status, &record.ResponseCode, &rawResponse, &record.CreatedAt, &record.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	record.ResponseBody = rawResponse
	return record, nil
}

// CreatePending attempts to insert a 'pending' key. Returns ErrAlreadyExists if the key already exists for the org.
func (r *IdempotencyRepository) CreatePending(ctx context.Context, orgID uuid.UUID, key, reqHash string) (*models.IdempotencyKey, error) {
	record := &models.IdempotencyKey{
		ID:             uuid.New(),
		OrganizationID: orgID,
		Key:            key,
		RequestHash:    reqHash,
		Status:         "pending",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	query := `
		INSERT INTO idempotency_keys (id, organization_id, key, request_hash, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (organization_id, key) DO NOTHING
		RETURNING id, organization_id, key, request_hash, status, created_at, updated_at`

	err := r.db.QueryRowContext(ctx, query, record.ID, record.OrganizationID, record.Key, record.RequestHash, record.Status, record.CreatedAt, record.UpdatedAt).
		Scan(&record.ID, &record.OrganizationID, &record.Key, &record.RequestHash, &record.Status, &record.CreatedAt, &record.UpdatedAt)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrAlreadyExists
	}
	if err != nil {
		return nil, err
	}
	return record, nil
}

// SaveResponse updates an idempotency record with final status, HTTP code, and payload
func (r *IdempotencyRepository) SaveResponse(ctx context.Context, orgID uuid.UUID, key string, statusCode int, responseBody []byte) error {
	query := `
		UPDATE idempotency_keys
		SET status = 'processed', response_code = $1, response_body = $2, updated_at = NOW()
		WHERE organization_id = $3 AND key = $4`

	_, err := r.db.ExecContext(ctx, query, statusCode, responseBody, orgID, key)
	return err
}

// DeleteRecord deletes an idempotency key (used to rollback stuck pending keys upon creation failure)
func (r *IdempotencyRepository) DeleteRecord(ctx context.Context, orgID uuid.UUID, key string) error {
	query := `DELETE FROM idempotency_keys WHERE organization_id = $1 AND key = $2`
	_, err := r.db.ExecContext(ctx, query, orgID, key)
	return err
}
