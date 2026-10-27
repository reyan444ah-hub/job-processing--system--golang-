package handlers

import (
	"errors"
	"net/http"

	"distributed-job-system/internal/api/middleware"
	"distributed-job-system/internal/service/auth"

	"github.com/gin-gonic/gin"
)

type AdminHandler struct {
	authService *auth.AuthService
}

func NewAdminHandler(authService *auth.AuthService) *AdminHandler {
	return &AdminHandler{authService: authService}
}

type CreateMemberRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required,min=6"`
}

func (h *AdminHandler) ListUsers(c *gin.Context) {
	orgID, ok := middleware.GetOrgID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "organization context missing"})
		return
	}

	users, err := h.authService.ListOrganizationUsers(c.Request.Context(), orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list organization users"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"users": users})
}

func (h *AdminHandler) CreateMember(c *gin.Context) {
	orgID, ok := middleware.GetOrgID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "organization context missing"})
		return
	}

	var req CreateMemberRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	user, err := h.authService.CreateMember(c.Request.Context(), orgID, req.Email, req.Password)
	if err != nil {
		if errors.Is(err, auth.ErrEmailAlreadyRegistered) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create member"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"user": user})
}
