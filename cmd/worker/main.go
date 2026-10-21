package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-job-system/internal/cache"
	"distributed-job-system/internal/config"
	"distributed-job-system/internal/database"
	"distributed-job-system/internal/queue"
	rdb "distributed-job-system/internal/redis"
	"distributed-job-system/internal/repository"
	"distributed-job-system/internal/worker"
)

func main() {
	cfg := config.LoadConfig()

	log.Printf("Starting worker process [%s]...\n", cfg.WorkerName)

	// 1. Connect to PostgreSQL
	db, err := database.Connect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("[%s] Database connection failed: %v", cfg.WorkerName, err)
	}
	defer db.Close()

	// 2. Connect to Redis
	redisClient, err := rdb.Connect(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("[%s] Redis connection failed: %v", cfg.WorkerName, err)
	}
	defer redisClient.Close()

	// 3. Initialize Repositories & Queue
	jobRepo := repository.NewJobRepository(db)
	jobCache := cache.NewJobCache(redisClient, cfg.CacheTTL)
	redisQueue := queue.NewRedisQueue(redisClient, cfg.StreamName, cfg.ConsumerGroup, cfg.DLQStreamName)

	// 4. Initialize Worker
	w := worker.NewWorker(
		cfg.WorkerName,
		cfg.WorkerConcurrency,
		redisQueue,
		jobRepo,
		jobCache,
		cfg.ClaimMinIdleTime,
	)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// 5. Handle Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("[%s] Received termination signal. Initiating graceful shutdown...\n", cfg.WorkerName)
	cancel()
	w.Stop()

	// Give time to flush any pending acks
	time.Sleep(1 * time.Second)
	log.Printf("[%s] Worker successfully stopped.\n", cfg.WorkerName)
}
