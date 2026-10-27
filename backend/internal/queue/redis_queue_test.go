package queue_test

import (
	"testing"
	"time"

	"distributed-job-system/internal/queue"

	"github.com/stretchr/testify/assert"
)

func TestCalculateBackoff(t *testing.T) {
	base := 1 * time.Second
	maxBackoff := 30 * time.Second

	// Attempt 1: base ~ 1s + jitter
	d1 := queue.CalculateBackoff(1, base, maxBackoff)
	assert.GreaterOrEqual(t, d1, base)
	assert.LessOrEqual(t, d1, 3*time.Second)

	// Attempt 2: base * 2 ~ 2s + jitter
	d2 := queue.CalculateBackoff(2, base, maxBackoff)
	assert.GreaterOrEqual(t, d2, 1*time.Second)
	assert.LessOrEqual(t, d2, 4*time.Second)

	// High attempt: should cap near maxBackoff
	d10 := queue.CalculateBackoff(10, base, maxBackoff)
	assert.LessOrEqual(t, d10, maxBackoff+10*time.Second)
}
