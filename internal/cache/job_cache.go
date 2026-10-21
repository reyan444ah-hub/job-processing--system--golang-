package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"distributed-job-system/internal/models"
	rdb "distributed-job-system/internal/redis"

	"github.com/google/uuid"
)

type JobCache struct {
	client *rdb.Client
	ttl    time.Duration
}

func NewJobCache(client *rdb.Client, ttl time.Duration) *JobCache {
	if ttl <= 0 {
		ttl = 1 * time.Minute
	}
	return &JobCache{
		client: client,
		ttl:    ttl,
	}
}

func (c *JobCache) jobKey(orgID, jobID uuid.UUID) string {
	return fmt.Sprintf("cache:job:%s:%s", orgID.String(), jobID.String())
}

// GetJob attempts to load a job from Redis cache. Returns nil if missing or error.
func (c *JobCache) GetJob(ctx context.Context, orgID, jobID uuid.UUID) (*models.Job, bool) {
	if c.client == nil {
		return nil, false
	}

	val, err := c.client.Get(ctx, c.jobKey(orgID, jobID)).Result()
	if err != nil {
		return nil, false
	}

	var job models.Job
	if err := json.Unmarshal([]byte(val), &job); err != nil {
		return nil, false
	}

	return &job, true
}

// SetJob caches the job with configured TTL
func (c *JobCache) SetJob(ctx context.Context, job *models.Job) {
	if c.client == nil || job == nil {
		return
	}

	data, err := json.Marshal(job)
	if err != nil {
		return
	}

	_ = c.client.Set(ctx, c.jobKey(job.OrganizationID, job.ID), data, c.ttl).Err()
}

// Invalidate removes cached job upon state changes
func (c *JobCache) Invalidate(ctx context.Context, orgID, jobID uuid.UUID) {
	if c.client == nil {
		return
	}
	_ = c.client.Del(ctx, c.jobKey(orgID, jobID)).Err()
}

// CheckRedisHealthy returns true if Redis is responding to pings
func (c *JobCache) CheckRedisHealthy(ctx context.Context) bool {
	if c.client == nil {
		return false
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	return c.client.Ping(ctxTimeout).Err() == nil
}
