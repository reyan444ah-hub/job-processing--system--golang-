package handlers

import (
	"net/http"
	"strconv"

	"distributed-job-system/internal/api/middleware"
	"distributed-job-system/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type JobHandler struct {
	jobService *service.JobService
}

func NewJobHandler(jobService *service.JobService) *JobHandler {
	return &JobHandler{jobService: jobService}
}

// SubmitJob handles POST /api/v1/jobs with Idempotency-Key header support
func (h *JobHandler) SubmitJob(c *gin.Context) {
	orgID, ok := middleware.GetOrgID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "organization context missing"})
		return
	}

	idempotencyKey := c.GetHeader("Idempotency-Key")

	var req service.SubmitJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job, deduplicated, err := h.jobService.SubmitJob(c.Request.Context(), orgID, idempotencyKey, &req)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	c.Header("X-Deduplicated", strconv.FormatBool(deduplicated))
	// Always returns 202 Accepted on durable acceptance
	c.JSON(http.StatusAccepted, gin.H{
		"message":      "job accepted for asynchronous processing",
		"deduplicated": deduplicated,
		"job":          job,
	})
}

// GetJob handles GET /api/v1/jobs/:id
func (h *JobHandler) GetJob(c *gin.Context) {
	orgID, ok := middleware.GetOrgID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "organization context missing"})
		return
	}

	jobIDStr := c.Param("id")
	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job id"})
		return
	}

	job, err := h.jobService.GetJob(c.Request.Context(), orgID, jobID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found or access denied"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"job": job})
}

// ListJobs handles GET /api/v1/jobs
func (h *JobHandler) ListJobs(c *gin.Context) {
	orgID, ok := middleware.GetOrgID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "organization context missing"})
		return
	}

	status := c.Query("status")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))

	jobs, total, err := h.jobService.ListJobs(c.Request.Context(), orgID, status, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list jobs"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"jobs":   jobs,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// CancelJob handles POST /api/v1/jobs/:id/cancel
func (h *JobHandler) CancelJob(c *gin.Context) {
	orgID, ok := middleware.GetOrgID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "organization context missing"})
		return
	}

	jobID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job id"})
		return
	}

	job, err := h.jobService.CancelJob(c.Request.Context(), orgID, jobID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "job cancelled successfully",
		"job":     job,
	})
}

// RetryJob handles POST /api/v1/jobs/:id/retry
func (h *JobHandler) RetryJob(c *gin.Context) {
	orgID, ok := middleware.GetOrgID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "organization context missing"})
		return
	}

	jobID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job id"})
		return
	}

	job, err := h.jobService.RetryJob(c.Request.Context(), orgID, jobID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "job re-enqueued successfully",
		"job":     job,
	})
}

// GetJobEvents handles GET /api/v1/jobs/:id/events (Event History Recovery for Reconnects)
func (h *JobHandler) GetJobEvents(c *gin.Context) {
	orgID, ok := middleware.GetOrgID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "organization context missing"})
		return
	}

	jobID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job id"})
		return
	}

	events, err := h.jobService.GetJobEvents(c.Request.Context(), orgID, jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to retrieve job events"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"job_id": jobID,
		"events": events,
	})
}

// GetMetrics handles GET /api/v1/metrics
func (h *JobHandler) GetMetrics(c *gin.Context) {
	metrics, err := h.jobService.GetMetrics(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to retrieve metrics"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"metrics": metrics})
}
