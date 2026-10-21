package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	"distributed-job-system/internal/database"
	"distributed-job-system/internal/models"

	"github.com/google/uuid"
)

var (
	ErrNotFound      = errors.New("record not found")
	ErrAlreadyExists = errors.New("record already exists")
)

type AuthRepository struct {
	db *database.DB
}

func NewAuthRepository(db *database.DB) *AuthRepository {
	return &AuthRepository{db: db}
}

func (r *AuthRepository) CreateOrganization(ctx context.Context, name string) (*models.Organization, error) {
	org := &models.Organization{
		ID:        uuid.New(),
		Name:      name,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	query := `INSERT INTO organizations (id, name, created_at, updated_at) VALUES ($1, $2, $3, $4) RETURNING id, name, created_at, updated_at`
	err := r.db.QueryRowContext(ctx, query, org.ID, org.Name, org.CreatedAt, org.UpdatedAt).
		Scan(&org.ID, &org.Name, &org.CreatedAt, &org.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return org, nil
}

func (r *AuthRepository) GetOrganizationByID(ctx context.Context, id uuid.UUID) (*models.Organization, error) {
	org := &models.Organization{}
	query := `SELECT id, name, created_at, updated_at FROM organizations WHERE id = $1`
	err := r.db.QueryRowContext(ctx, query, id).Scan(&org.ID, &org.Name, &org.CreatedAt, &org.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return org, err
}

func (r *AuthRepository) CreateUser(ctx context.Context, orgID uuid.UUID, email, passwordHash, role string) (*models.User, error) {
	user := &models.User{
		ID:             uuid.New(),
		OrganizationID: orgID,
		Email:          email,
		PasswordHash:   passwordHash,
		Role:           role,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	query := `
		INSERT INTO users (id, organization_id, email, password_hash, role, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, organization_id, email, role, created_at, updated_at`

	err := r.db.QueryRowContext(ctx, query, user.ID, user.OrganizationID, user.Email, user.PasswordHash, user.Role, user.CreatedAt, user.UpdatedAt).
		Scan(&user.ID, &user.OrganizationID, &user.Email, &user.Role, &user.CreatedAt, &user.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return user, nil
}

func (r *AuthRepository) GetUserByEmail(ctx context.Context, email string) (*models.User, error) {
	user := &models.User{}
	query := `SELECT id, organization_id, email, password_hash, role, created_at, updated_at FROM users WHERE email = $1`
	err := r.db.QueryRowContext(ctx, query, email).
		Scan(&user.ID, &user.OrganizationID, &user.Email, &user.PasswordHash, &user.Role, &user.CreatedAt, &user.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return user, err
}

func (r *AuthRepository) GetUserByID(ctx context.Context, id uuid.UUID) (*models.User, error) {
	user := &models.User{}
	query := `SELECT id, organization_id, email, password_hash, role, created_at, updated_at FROM users WHERE id = $1`
	err := r.db.QueryRowContext(ctx, query, id).
		Scan(&user.ID, &user.OrganizationID, &user.Email, &user.PasswordHash, &user.Role, &user.CreatedAt, &user.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return user, err
}

func HashToken(rawToken string) string {
	hash := sha256.Sum256([]byte(rawToken))
	return hex.EncodeToString(hash[:])
}

func (r *AuthRepository) StoreRefreshToken(ctx context.Context, userID uuid.UUID, rawToken string, expiresAt time.Time) error {
	tokenHash := HashToken(rawToken)
	query := `INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) VALUES ($1, $2, $3, $4)`
	_, err := r.db.ExecContext(ctx, query, uuid.New(), userID, tokenHash, expiresAt)
	return err
}

func (r *AuthRepository) ValidateAndRevokeRefreshToken(ctx context.Context, rawToken string) (uuid.UUID, error) {
	tokenHash := HashToken(rawToken)
	query := `
		UPDATE refresh_tokens
		SET revoked_at = NOW()
		WHERE token_hash = $1 AND revoked_at IS NULL AND expires_at > NOW()
		RETURNING user_id`

	var userID uuid.UUID
	err := r.db.QueryRowContext(ctx, query, tokenHash).Scan(&userID)
	if errors.Is(err, sql.ErrNoRows) {
		return uuid.Nil, errors.New("invalid, expired, or revoked refresh token")
	}
	return userID, err
}

func (r *AuthRepository) RevokeAllUserTokens(ctx context.Context, userID uuid.UUID) error {
	query := `UPDATE refresh_tokens SET revoked_at = NOW() WHERE user_id = $1 AND revoked_at IS NULL`
	_, err := r.db.ExecContext(ctx, query, userID)
	return err
}
