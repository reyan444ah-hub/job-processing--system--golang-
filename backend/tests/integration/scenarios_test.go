package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distributed-job-system/internal/api/handlers"
	"distributed-job-system/internal/api/middleware"
	"distributed-job-system/internal/cache"
	"distributed-job-system/internal/config"
	"distributed-job-system/internal/database"
	"distributed-job-system/internal/models"
	"distributed-job-system/internal/queue"
	rdb "distributed-job-system/internal/redis"
	"distributed-job-system/internal/repository"
	"distributed-job-system/internal/service"
	"distributed-job-system/internal/service/auth"
	"distributed-job-system/internal/worker"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestEnv struct {
	cfg         *config.Config
	db          *database.DB
	rdb         *rdb.Client
	authRepo    *repository.AuthRepository
	jobRepo     *repository.JobRepository
	outboxRepo  *repository.OutboxRepository
	jobCache    *cache.JobCache
	redisQueue  *queue.RedisQueue
	authService *auth.AuthService
	jobService  *service.JobService
	router      *gin.Engine
}

func setupTestEnv(t *testing.T) *TestEnv {
	cfg := config.LoadConfig()

	db, err := database.Connect(cfg.DatabaseURL)
	if err != nil {
		t.Skipf("Skipping integration test: PostgreSQL unavailable (%v)", err)
		return nil
	}

	require.NoError(t, db.RunMigrations("../../migrations/000001_init_schema.sql"))

	redisClient, err := rdb.Connect(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		db.Close()
		t.Skipf("Skipping integration test: Redis unavailable (%v)", err)
		return nil
	}

	authRepo := repository.NewAuthRepository(db)
	jobRepo := repository.NewJobRepository(db)
	outboxRepo := repository.NewOutboxRepository(db)
	jobCache := cache.NewJobCache(redisClient, cfg.CacheTTL)
	redisQueue := queue.NewRedisQueue(redisClient, "test:jobs:stream", "test:job-workers-group", "test:jobs:dlq")

	authService := auth.NewAuthService(authRepo, cfg)
	jobService := service.NewJobService(jobRepo, jobCache, redisQueue)

	gin.SetMode(gin.TestMode)
	router := gin.New()

	authHandler := handlers.NewAuthHandler(authService)
	jobHandler := handlers.NewJobHandler(jobService)

	api := router.Group("/api/v1")
	{
		authGroup := api.Group("/auth")
		{
			authGroup.POST("/register", authHandler.Register)
			authGroup.POST("/login", authHandler.Login)
			authGroup.POST("/refresh", authHandler.Refresh)
			authGroup.POST("/logout", middleware.AuthMiddleware(authService), authHandler.Logout)
		}

		jobsGroup := api.Group("/jobs", middleware.AuthMiddleware(authService))
		{
			jobsGroup.POST("", jobHandler.SubmitJob)
			jobsGroup.GET("", jobHandler.ListJobs)
			jobsGroup.GET("/:id", jobHandler.GetJob)
			jobsGroup.POST("/:id/cancel", jobHandler.CancelJob)
			jobsGroup.POST("/:id/retry", jobHandler.RetryJob)
			jobsGroup.GET("/:id/events", jobHandler.GetJobEvents)
		}
	}

	return &TestEnv{
		cfg:         cfg,
		db:          db,
		rdb:         redisClient,
		authRepo:    authRepo,
		jobRepo:     jobRepo,
		outboxRepo:  outboxRepo,
		jobCache:    jobCache,
		redisQueue:  redisQueue,
		authService: authService,
		jobService:  jobService,
		router:      router,
	}
}

// 1. Submit 100 jobs concurrently and verify that accepted jobs are eventually processed
func TestScenario_100ConcurrentJobsProcessing(t *testing.T) {
	env := setupTestEnv(t)
	if env == nil {
		return
	}
	defer env.db.Close()
	defer env.rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register organization
	org, err := env.authRepo.CreateOrganization(ctx, "PerfOrg-"+uuid.New().String()[:8])
	require.NoError(t, err)

	// Start Outbox Publisher
	publisher := queue.NewOutboxPublisher(env.outboxRepo, env.redisQueue, env.rdb.Client, 50*time.Millisecond)
	go publisher.Start(ctx)

	// Start Worker
	w := worker.NewWorker("test-worker-concurrent", 10, env.redisQueue, env.jobRepo, env.jobCache, 10*time.Second)
	w.Start(ctx)
	defer w.Stop()

	// Concurrently submit 100 jobs
	numJobs := 100
	jobIDs := make([]uuid.UUID, numJobs)
	var wg sync.WaitGroup

	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			payload, _ := json.Marshal(map[string]interface{}{"index": idx})
			job, _, err := env.jobService.SubmitJob(ctx, org.ID, "", &service.SubmitJobRequest{
				Type:        "generic.test",
				Payload:     payload,
				MaxAttempts: 3,
			})
			assert.NoError(t, err)
			assert.NotNil(t, job)
			jobIDs[idx] = job.ID
		}(i)
	}
	wg.Wait()

	// Verify all 100 jobs eventually reach 'completed'
	assert.Eventually(t, func() bool {
		completedCount := 0
		for _, jid := range jobIDs {
			job, err := env.jobRepo.GetJobByID(ctx, org.ID, jid)
			if err == nil && job.Status == models.StatusCompleted {
				completedCount++
			}
		}
		return completedCount == numJobs
	}, 20*time.Second, 500*time.Millisecond, "All 100 jobs should complete")
}

// 2. Submit the same idempotency key concurrently and verify that only one logical job is created
func TestScenario_ConcurrentIdempotencyKeyDeduplication(t *testing.T) {
	env := setupTestEnv(t)
	if env == nil {
		return
	}
	defer env.db.Close()
	defer env.rdb.Close()

	ctx := context.Background()
	org, err := env.authRepo.CreateOrganization(ctx, "IdempOrg-"+uuid.New().String()[:8])
	require.NoError(t, err)

	key := "idemp-key-" + uuid.New().String()
	concurrency := 20
	var wg sync.WaitGroup

	var createdCount int32
	var deduplicatedCount int32
	var errorCount int32
	var createdJobID uuid.UUID
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload, _ := json.Marshal(map[string]interface{}{"order_id": "12345"})
			job, dedup, err := env.jobService.SubmitJob(ctx, org.ID, key, &service.SubmitJobRequest{
				Type:        "order.process",
				Payload:     payload,
				MaxAttempts: 3,
			})
			if err == nil && job != nil {
				if dedup {
					atomic.AddInt32(&deduplicatedCount, 1)
				} else {
					atomic.AddInt32(&createdCount, 1)
					mu.Lock()
					createdJobID = job.ID
					mu.Unlock()
				}
			} else {
				atomic.AddInt32(&errorCount, 1)
			}
		}()
	}
	wg.Wait()

	// Exactly ONE job created, remaining requests received existing job or dedup
	assert.Equal(t, int32(1), createdCount, "Exactly one job should be newly created")
	assert.Equal(t, int32(concurrency-1), deduplicatedCount, "All duplicate requests should return the committed job")
	assert.Zero(t, errorCount, "Concurrent retries should not get stuck on a pending idempotency record")
	assert.NotEqual(t, uuid.Nil, createdJobID)
	_, err = env.db.ExecContext(ctx, `UPDATE outbox_events SET dispatched = TRUE WHERE organization_id = $1`, org.ID)
	require.NoError(t, err, "isolate this test's unrelayed queue events from later scenarios")
}

// 3. Kill a worker during processing and demonstrate recovery (PEL Claiming)
func TestScenario_WorkerKillAndCrashRecovery(t *testing.T) {
	env := setupTestEnv(t)
	if env == nil {
		return
	}
	defer env.db.Close()
	defer env.rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher := queue.NewOutboxPublisher(env.outboxRepo, env.redisQueue, env.rdb.Client, 50*time.Millisecond)
	go publisher.Start(ctx)

	org, err := env.authRepo.CreateOrganization(ctx, "CrashOrg-"+uuid.New().String()[:8])
	require.NoError(t, err)

	// Submit slow job
	payload, _ := json.Marshal(map[string]interface{}{"duration": "slow"})
	job, _, err := env.jobService.SubmitJob(ctx, org.ID, "", &service.SubmitJobRequest{
		Type:        "test.slow",
		Payload:     payload,
		MaxAttempts: 3,
	})
	require.NoError(t, err)

	require.NoError(t, env.redisQueue.EnsureConsumerGroup(ctx))
	assert.Eventually(t, func() bool {
		streamLength, err := env.rdb.Client.XLen(ctx, "test:jobs:stream").Result()
		return err == nil && streamLength > 0
	}, 5*time.Second, 50*time.Millisecond, "outbox relay should publish the job")
	pending, err := env.redisQueue.ConsumeNext(ctx, "crashed-worker", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, pending, 1, "crashed worker must own a pending stream entry")
	_, err = env.jobRepo.ClaimJobAtomic(ctx, job.ID, "crashed-worker", false, 100*time.Millisecond)
	require.NoError(t, err, "simulate worker crash immediately after its durable job claim")
	time.Sleep(150 * time.Millisecond)

	// Start Worker 2 with low claim idle time (100ms) to trigger PEL / crash recovery
	w2 := worker.NewWorker("recovery-worker", 2, env.redisQueue, env.jobRepo, env.jobCache, 100*time.Millisecond)
	w2.Start(ctx)
	defer w2.Stop()

	// Recovered job should complete
	assert.Eventually(t, func() bool {
		j, err := env.jobRepo.GetJobByID(ctx, org.ID, job.ID)
		return err == nil && j.Status == models.StatusCompleted
	}, 15*time.Second, 500*time.Millisecond, "Job abandoned by killed worker should be recovered and completed")
}

// 4. Force a job to fail repeatedly and verify that it reaches the dead-letter queue
func TestScenario_DeadLetterQueueExhaustion(t *testing.T) {
	env := setupTestEnv(t)
	if env == nil {
		return
	}
	defer env.db.Close()
	defer env.rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher := queue.NewOutboxPublisher(env.outboxRepo, env.redisQueue, env.rdb.Client, 50*time.Millisecond)
	go publisher.Start(ctx)
	initialDLQLength, err := env.rdb.Client.XLen(ctx, "test:jobs:dlq").Result()
	require.NoError(t, err)

	org, err := env.authRepo.CreateOrganization(ctx, "DLQOrg-"+uuid.New().String()[:8])
	require.NoError(t, err)

	// Submit failing job with max_attempts = 2
	job, _, err := env.jobService.SubmitJob(ctx, org.ID, "", &service.SubmitJobRequest{
		Type:        "test.fail",
		Payload:     []byte(`{"cause":"force_failure"}`),
		MaxAttempts: 2,
	})
	require.NoError(t, err)

	w := worker.NewWorker("dlq-worker", 2, env.redisQueue, env.jobRepo, env.jobCache, 1*time.Second)
	w.Start(ctx)
	defer w.Stop()

	// The outbox relay delivers the durable retry schedule and final DLQ entry.
	assert.Eventually(t, func() bool {
		j, err := env.jobRepo.GetJobByID(ctx, org.ID, job.ID)
		dlqLength, queueErr := env.rdb.Client.XLen(ctx, "test:jobs:dlq").Result()
		return err == nil && queueErr == nil && j.Status == models.StatusDeadLetter && dlqLength >= initialDLQLength+1
	}, 15*time.Second, 300*time.Millisecond, "Job should transition to dead_letter and reach Redis DLQ")
}

// 8. Attempt cross-organization access and verify that the request is rejected
func TestScenario_CrossOrganizationAccessRejected(t *testing.T) {
	env := setupTestEnv(t)
	if env == nil {
		return
	}
	defer env.db.Close()
	defer env.rdb.Close()

	ctx := context.Background()

	// Create Org A and User A
	_, _, tokenA, _, err := env.authService.Register(ctx, "TenantA", "alice@tenant-a.com", "password123")
	require.NoError(t, err)

	// Create Org B and User B
	_, _, tokenB, _, err := env.authService.Register(ctx, "TenantB", "bob@tenant-b.com", "password123")
	require.NoError(t, err)

	// User A creates a job
	reqBody := `{"type": "confidential.job", "payload": {"secret": "orgA_data"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(reqBody))
	req.Header.Set("Authorization", "Bearer "+tokenA)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	env.router.ServeHTTP(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)

	var resp struct {
		Job models.Job `json:"job"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	jobIDA := resp.Job.ID

	// User B attempts to access Org A's job via GET /api/v1/jobs/:id
	reqForbidden := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/jobs/%s", jobIDA), nil)
	reqForbidden.Header.Set("Authorization", "Bearer "+tokenB)
	wForbidden := httptest.NewRecorder()
	env.router.ServeHTTP(wForbidden, reqForbidden)

	// Must be 404 Not Found (or 403 Forbidden) to prevent information disclosure
	assert.Equal(t, http.StatusNotFound, wForbidden.Code, "User from Org B should NOT be able to view Org A's job")
}
