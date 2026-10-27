package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-job-system/internal/api/handlers"
	"distributed-job-system/internal/api/middleware"
	"distributed-job-system/internal/cache"
	"distributed-job-system/internal/config"
	"distributed-job-system/internal/database"
	"distributed-job-system/internal/queue"
	rdb "distributed-job-system/internal/redis"
	"distributed-job-system/internal/repository"
	"distributed-job-system/internal/service"
	"distributed-job-system/internal/service/auth"
	"distributed-job-system/internal/ws"

	"github.com/gin-gonic/gin"
)

func main() {
	cfg := config.LoadConfig()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// 1. Connect to PostgreSQL
	db, err := database.Connect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer db.Close()

	// Run migrations: must succeed before serving traffic
	if err := db.RunMigrations("migrations/000001_init_schema.sql"); err != nil {
		log.Fatalf("Fatal: Database migration failed: %v", err)
	}

	// 2. Connect to Redis
	redisClient, err := rdb.Connect(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Printf("Warning: Redis connection failed: %v. Running in degraded cache mode.", err)
	} else {
		defer redisClient.Close()
	}

	// 3. Initialize Repositories
	authRepo := repository.NewAuthRepository(db)
	jobRepo := repository.NewJobRepository(db)
	outboxRepo := repository.NewOutboxRepository(db)

	// 4. Initialize Cache & Queue Primitives
	jobCache := cache.NewJobCache(redisClient, cfg.CacheTTL)
	redisQueue := queue.NewRedisQueue(redisClient, cfg.StreamName, cfg.ConsumerGroup, cfg.DLQStreamName)

	// 5. Initialize Services
	authService := auth.NewAuthService(authRepo, cfg)
	jobService := service.NewJobService(jobRepo, jobCache, redisQueue)

	// 6. Initialize WebSocket Hub
	var wsHub *ws.Hub
	if redisClient != nil {
		wsHub = ws.NewHub(redisClient.Client)
		go wsHub.Run(context.Background())
	}

	// 7. Start Transactional Outbox Publisher Daemon
	if redisClient != nil {
		outboxPub := queue.NewOutboxPublisher(outboxRepo, redisQueue, redisClient.Client, cfg.OutboxPollInterval)
		go outboxPub.Start(context.Background())
	}

	// 8. Setup Gin Router
	if cfg.AppEnv == "production" {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.Default()

	// CORS Middleware
	router.Use(func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if !cfg.IsOriginAllowed(origin, c.Request.Host) {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		if origin != "" {
			c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
			c.Writer.Header().Add("Vary", "Origin")
			c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		}
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With, Idempotency-Key")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// Handlers
	authHandler := handlers.NewAuthHandler(authService)
	adminHandler := handlers.NewAdminHandler(authService)
	jobHandler := handlers.NewJobHandler(jobService)

	// Root health check: verifies PostgreSQL and Redis readiness
	router.GET("/health", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()

		pgOk := db.PingContext(ctx) == nil
		redisOk := jobCache.CheckRedisHealthy(ctx)

		status := "ok"
		httpCode := http.StatusOK
		if !pgOk || !redisOk {
			status = "degraded"
			httpCode = http.StatusServiceUnavailable
		}

		c.JSON(httpCode, gin.H{
			"status":   status,
			"postgres": pgOk,
			"redis":    redisOk,
			"time":     time.Now().UTC(),
		})
	})

	// WebSocket Endpoint: /api/v1/ws/jobs
	router.GET("/api/v1/ws/jobs", func(c *gin.Context) {
		if wsHub == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "real-time events unavailable: Redis disconnected"})
			return
		}
		ws.ServeWsWithOrigins(wsHub, authService, cfg.AllowedOrigins, c.Writer, c.Request)
	})

	// API Routes Group
	api := router.Group("/api/v1")
	{
		// Public Auth Endpoints
		authGroup := api.Group("/auth")
		{
			authGroup.POST("/register", authHandler.Register)
			authGroup.POST("/login", authHandler.Login)
			authGroup.POST("/refresh", authHandler.Refresh)
			authGroup.POST("/logout", middleware.AuthMiddleware(authService), authHandler.Logout)
		}

		// Members can submit and view their own jobs; admins manage the organization queue.
		api.POST("/jobs", middleware.AuthMiddleware(authService), jobHandler.SubmitJob)
		api.GET("/my/jobs", middleware.AuthMiddleware(authService), middleware.RequireRole("member"), jobHandler.ListMyJobs)

		adminGroup := api.Group("", middleware.AuthMiddleware(authService), middleware.RequireRole("admin"))
		{
			adminGroup.GET("/metrics", jobHandler.GetMetrics)
			adminGroup.GET("/admin/users", adminHandler.ListUsers)
			adminGroup.POST("/admin/users", adminHandler.CreateMember)
			adminGroup.GET("/jobs", jobHandler.ListJobs)
			adminGroup.GET("/jobs/:id", jobHandler.GetJob)
			adminGroup.POST("/jobs/:id/cancel", jobHandler.CancelJob)
			adminGroup.POST("/jobs/:id/retry", jobHandler.RetryJob)
			adminGroup.GET("/jobs/:id/events", jobHandler.GetJobEvents)
		}
	}

	// 9. Graceful HTTP Server Shutdown
	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.AppPort),
		Handler: router,
	}

	go func() {
		log.Printf("Starting API Server on port %s...\n", cfg.AppPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server listen failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down API Server gracefully...")

	ctxShutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctxShutdown); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exiting successfully.")
}
