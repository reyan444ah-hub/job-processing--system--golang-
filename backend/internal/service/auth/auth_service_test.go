package auth_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"distributed-job-system/internal/config"
	"distributed-job-system/internal/database"
	"distributed-job-system/internal/repository"
	"distributed-job-system/internal/service/auth"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthService_FullCycle(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbMock.Close()

	db := &database.DB{DB: dbMock}
	cfg := &config.Config{
		JWTSecret:        "test-secret-key-for-unit-tests-12345",
		JWTAccessExpiry:  15 * time.Minute,
		JWTRefreshExpiry: 7 * 24 * time.Hour,
	}

	repo := repository.NewAuthRepository(db)
	svc := auth.NewAuthService(repo, cfg)
	ctx := context.Background()

	orgID := uuid.New()
	userID := uuid.New()
	email := "intern@example.com"
	password := "SecretPass123!"

	// --- 1. Test Registration ---
	// Expect GetUserByEmail -> sql.ErrNoRows
	mock.ExpectQuery(`SELECT id, organization_id, email, password_hash, role, created_at, updated_at FROM users WHERE email = \$1`).
		WithArgs(email).
		WillReturnError(sql.ErrNoRows)

	// Expect CreateOrganization
	mock.ExpectQuery(`INSERT INTO organizations \(id, name, created_at, updated_at\) VALUES \(\$1, \$2, \$3, \$4\) RETURNING id, name, created_at, updated_at`).
		WithArgs(sqlmock.AnyArg(), "Acme Corp", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "created_at", "updated_at"}).
			AddRow(orgID, "Acme Corp", time.Now(), time.Now()))

	// Expect CreateUser
	mock.ExpectQuery(`INSERT INTO users \(id, organization_id, email, password_hash, role, created_at, updated_at\)`).
		WithArgs(sqlmock.AnyArg(), orgID, email, sqlmock.AnyArg(), "admin", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "email", "role", "created_at", "updated_at"}).
			AddRow(userID, orgID, email, "admin", time.Now(), time.Now()))

	// Expect StoreRefreshToken
	mock.ExpectExec(`INSERT INTO refresh_tokens \(id, user_id, token_hash, expires_at\) VALUES \(\$1, \$2, \$3, \$4\)`).
		WithArgs(sqlmock.AnyArg(), userID, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	user, org, access, refresh, err := svc.Register(ctx, "Acme Corp", email, password)
	assert.NoError(t, err)
	assert.NotNil(t, user)
	assert.NotNil(t, org)
	assert.NotEmpty(t, access)
	assert.NotEmpty(t, refresh)

	// --- 2. Test JWT Validation ---
	claims, err := svc.ValidateAccessToken(access)
	assert.NoError(t, err)
	assert.Equal(t, userID, claims.UserID)
	assert.Equal(t, orgID, claims.OrganizationID)
	assert.Equal(t, email, claims.Email)
	assert.Equal(t, "admin", claims.Role)

	wrongAlgorithm := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	wrongAlgorithmToken, err := wrongAlgorithm.SignedString([]byte(cfg.JWTSecret))
	require.NoError(t, err)
	_, err = svc.ValidateAccessToken(wrongAlgorithmToken)
	assert.Error(t, err, "JWT validation must reject algorithms other than HS256")

	// --- 3. Test Refresh Token Rotation ---
	mock.ExpectQuery(`UPDATE refresh_tokens SET revoked_at = NOW\(\) WHERE token_hash = \$1 AND revoked_at IS NULL AND expires_at > NOW\(\) RETURNING user_id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"user_id"}).AddRow(userID))

	mock.ExpectQuery(`SELECT id, organization_id, email, password_hash, role, created_at, updated_at FROM users WHERE id = \$1`).
		WithArgs(userID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "email", "password_hash", "role", "created_at", "updated_at"}).
			AddRow(userID, orgID, email, "hashed", "admin", time.Now(), time.Now()))

	mock.ExpectExec(`INSERT INTO refresh_tokens \(id, user_id, token_hash, expires_at\) VALUES \(\$1, \$2, \$3, \$4\)`).
		WithArgs(sqlmock.AnyArg(), userID, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	newAccess, newRefresh, err := svc.Refresh(ctx, refresh)
	assert.NoError(t, err)
	assert.NotEmpty(t, newAccess)
	assert.NotEmpty(t, newRefresh)
	assert.NotEqual(t, refresh, newRefresh, "Refresh token should rotate to a new value")

	// --- 4. Test Token Reuse (Revoked Token Rejection) ---
	mock.ExpectQuery(`UPDATE refresh_tokens SET revoked_at = NOW\(\) WHERE token_hash = \$1 AND revoked_at IS NULL AND expires_at > NOW\(\) RETURNING user_id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnError(sql.ErrNoRows) // already revoked

	_, _, err = svc.Refresh(ctx, refresh)
	assert.Error(t, err, "Reusing revoked refresh token must be rejected")

	// --- 5. Test Logout (Session Revocation) ---
	mock.ExpectExec(`UPDATE refresh_tokens SET revoked_at = NOW\(\) WHERE user_id = \$1 AND revoked_at IS NULL`).
		WithArgs(userID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = svc.Logout(ctx, userID)
	assert.NoError(t, err)
}

func TestAuthService_CreateMember(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbMock.Close()

	orgID := uuid.New()
	userID := uuid.New()
	email := "member@example.com"
	now := time.Now().UTC()

	mock.ExpectQuery(`SELECT id, organization_id, email, password_hash, role, created_at, updated_at FROM users WHERE email = \$1`).
		WithArgs(email).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO users \(id, organization_id, email, password_hash, role, created_at, updated_at\)`).
		WithArgs(sqlmock.AnyArg(), orgID, email, sqlmock.AnyArg(), "member", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "organization_id", "email", "role", "created_at", "updated_at"}).
			AddRow(userID, orgID, email, "member", now, now))

	repo := repository.NewAuthRepository(&database.DB{DB: dbMock})
	svc := auth.NewAuthService(repo, &config.Config{})
	user, err := svc.CreateMember(context.Background(), orgID, email, "SecretPass123!")

	require.NoError(t, err)
	assert.Equal(t, userID, user.ID)
	assert.Equal(t, "member", user.Role)
	assert.NotEmpty(t, user.PasswordHash)
	assert.NoError(t, mock.ExpectationsWereMet())
}
