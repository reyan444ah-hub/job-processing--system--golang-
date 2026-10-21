package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"distributed-job-system/internal/cache"
	"distributed-job-system/internal/models"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/repository"

	"github.com/redis/go-redis/v9"
)

type Worker struct {
	workerName       string
	concurrency      int
	queue            *queue.RedisQueue
	jobRepo          *repository.JobRepository
	cache            *cache.JobCache
	claimMinIdleTime time.Duration
	stopChan         chan struct{}
	wg               sync.WaitGroup
}

func NewWorker(
	workerName string,
	concurrency int,
	q *queue.RedisQueue,
	jobRepo *repository.JobRepository,
	cache *cache.JobCache,
	claimMinIdleTime time.Duration,
) *Worker {
	if workerName == "" {
		workerName = fmt.Sprintf("worker-%d", time.Now().UnixNano()%10000)
	}
	if concurrency <= 0 {
		concurrency = 5
	}
	if claimMinIdleTime <= 0 {
		claimMinIdleTime = 30 * time.Second
	}

	return &Worker{
		workerName:       workerName,
		concurrency:      concurrency,
		queue:            q,
		jobRepo:          jobRepo,
		cache:            cache,
		claimMinIdleTime: claimMinIdleTime,
		stopChan:         make(chan struct{}),
	}
}

func (w *Worker) Start(ctx context.Context) {
	log.Printf("[%s] Starting worker with %d concurrent slots...\n", w.workerName, w.concurrency)

	// Ensure Redis Consumer Group exists
	if err := w.queue.EnsureConsumerGroup(ctx); err != nil {
		log.Printf("[%s] Warning: consumer group init error: %v\n", w.workerName, err)
	}

	// 1. Start worker heartbeat ticker
	go w.startHeartbeat(ctx)

	// 2. Start scheduled job sweeper
	go w.startScheduledSweeper(ctx)

	// 3. Start PEL (Pending Entries List) crash recovery sweeper
	go w.startPendingCrashRecovery(ctx)

	// 4. Start concurrent consumer slots
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.runConsumerLoop(ctx, i)
	}
}

func (w *Worker) Stop() {
	log.Printf("[%s] Gracefully stopping worker...\n", w.workerName)
	close(w.stopChan)
	w.wg.Wait()
	log.Printf("[%s] Worker shutdown complete.\n", w.workerName)
}

func (w *Worker) startHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopChan:
			return
		case <-ticker.C:
			_ = w.queue.Heartbeat(ctx, w.workerName)
		}
	}
}

func (w *Worker) startScheduledSweeper(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopChan:
			return
		case <-ticker.C:
			_, _ = w.queue.SweepScheduledJobs(ctx)
		}
	}
}

func (w *Worker) startPendingCrashRecovery(ctx context.Context) {
	// Reclaim messages pending in consumer group from crashed workers using XAUTOCLAIM
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopChan:
			return
		case <-ticker.C:
			claimedMsgs, err := w.queue.ClaimPendingMessages(ctx, w.workerName, w.claimMinIdleTime, 10)
			if err == nil && len(claimedMsgs) > 0 {
				log.Printf("[%s] Reclaimed %d abandoned messages from dead workers\n", w.workerName, len(claimedMsgs))
				for _, msg := range claimedMsgs {
					w.processRedisMessage(ctx, msg)
				}
			}
		}
	}
}

func (w *Worker) runConsumerLoop(ctx context.Context, slotID int) {
	defer w.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopChan:
			return
		default:
			// Read next message with blocking timeout 2s
			msgs, err := w.queue.ConsumeNext(ctx, w.workerName, 1, 2*time.Second)
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if len(msgs) == 0 {
				continue
			}

			for _, msg := range msgs {
				w.processRedisMessage(ctx, msg)
			}
		}
	}
}

func (w *Worker) processRedisMessage(ctx context.Context, xmsg redis.XMessage) {
	defer func() {
		// Always ACK the message from the stream group once dealt with
		_ = w.queue.Ack(ctx, xmsg.ID)
	}()

	rawPayload, ok := xmsg.Values["payload"].(string)
	if !ok {
		return
	}

	var msg queue.JobMessage
	if err := json.Unmarshal([]byte(rawPayload), &msg); err != nil {
		log.Printf("[%s] Corrupted message payload: %v\n", w.workerName, err)
		return
	}

	jobID := msg.JobID

	// 1. Atomic claim in PostgreSQL with row-level locking (guarantees safe concurrency across multiple workers)
	job, err := w.jobRepo.ClaimJobAtomic(ctx, jobID, w.workerName)
	if err != nil {
		// Job may have already been claimed, cancelled, or completed by another worker
		return
	}

	// Invalidate cache immediately on claim
	w.cache.Invalidate(ctx, job.OrganizationID, job.ID)

	// 2. Execute job handler
	result, err := w.executeJob(ctx, job)
	if err != nil {
		// Execution failed: evaluate retry vs dead-letter queue
		log.Printf("[%s] Job %s failed attempt %d: %v\n", w.workerName, job.ID, job.AttemptCount, err)

		backoff := queue.CalculateBackoff(job.AttemptCount, 2*time.Second, 1*time.Minute)
		nextRetry := time.Now().UTC().Add(backoff)

		newStatus, failErr := w.jobRepo.FailOrRetryJob(ctx, job.ID, job.Version, err.Error(), &nextRetry)
		if failErr != nil {
			log.Printf("[%s] Failed to record failure for job %s: %v\n", w.workerName, job.ID, failErr)
		}

		w.cache.Invalidate(ctx, job.OrganizationID, job.ID)

		if newStatus == models.StatusDeadLetter {
			log.Printf("[%s] Job %s exhausted retries. Routing to DLQ\n", w.workerName, job.ID)
			_ = w.queue.SendToDLQ(ctx, &msg, err.Error())
		} else if newStatus == models.StatusRetrying {
			// Schedule next retry
			_ = w.queue.ScheduleJob(ctx, &msg, nextRetry)
		}
		return
	}

	// 3. Complete job successfully
	resultBytes, _ := json.Marshal(result)
	if err := w.jobRepo.CompleteJob(ctx, job.ID, job.Version, resultBytes); err != nil {
		log.Printf("[%s] Failed to mark job %s as completed: %v\n", w.workerName, job.ID, err)
	}

	w.cache.Invalidate(ctx, job.OrganizationID, job.ID)
}

func (w *Worker) executeJob(ctx context.Context, job *models.Job) (map[string]interface{}, error) {
	// Parse payload
	var payload map[string]interface{}
	_ = json.Unmarshal(job.Payload, &payload)

	// Simulate different workloads based on job.Type
	switch job.Type {
	case "test.fail":
		return nil, errors.New("simulated intentional failure for DLQ test")

	case "test.slow":
		time.Sleep(1 * time.Second)
		return map[string]interface{}{"status": "slow_job_completed"}, nil

	case "email.send":
		// Simulated email dispatch
		recipient, _ := payload["to"].(string)
		return map[string]interface{}{
			"sent_to":      recipient,
			"delivered_at": time.Now().UTC().Format(time.RFC3339),
		}, nil

	case "report.generate":
		return map[string]interface{}{
			"report_url": "https://storage.local/reports/" + job.ID.String() + ".pdf",
			"pages":      42,
		}, nil

	default:
		// Generic execution
		time.Sleep(50 * time.Millisecond)
		return map[string]interface{}{
			"processed_by": w.workerName,
			"processed_at": time.Now().UTC().Format(time.RFC3339),
		}, nil
	}
}
