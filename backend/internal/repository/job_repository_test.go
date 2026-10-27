package repository_test

import (
	"context"
	"testing"
	"time"

	"distributed-job-system/internal/database"
	"distributed-job-system/internal/models"
	"distributed-job-system/internal/repository"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestListJobsByUserScopesByOrganizationAndSubmitter(t *testing.T) {
	dbMock, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbMock.Close()

	orgID := uuid.New()
	userID := uuid.New()
	jobID := uuid.New()
	now := time.Now().UTC()

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM jobs WHERE organization_id = \$1 AND created_by_user_id = \$2 AND status = \$3`).
		WithArgs(orgID, userID, "completed").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT id, organization_id, idempotency_key, type, payload, status`).
		WithArgs(orgID, userID, "completed", 10, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "organization_id", "idempotency_key", "type", "payload", "status", "priority", "scheduled_at",
			"attempt_count", "max_attempts", "locked_by", "locked_at", "started_at", "completed_at",
			"result", "last_error", "version", "created_at", "updated_at",
		}).AddRow(
			jobID, orgID, nil, "email.send", []byte(`{"to":"member@example.com"}`), models.StatusCompleted, 0, nil,
			1, 3, nil, nil, nil, now, []byte(`{"sent":true}`), nil, 2, now, now,
		))

	jobs, total, err := repository.NewJobRepository(&database.DB{DB: dbMock}).ListJobsByUser(context.Background(), orgID, userID, "completed", 10, 0)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, 1, total)
	require.Equal(t, jobID, jobs[0].ID)
	require.Equal(t, models.StatusCompleted, jobs[0].Status)
	require.NoError(t, mock.ExpectationsWereMet())
}
