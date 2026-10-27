package middleware

import (
	"net/http"
	"strings"

	"distributed-job-system/internal/service/auth"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	ContextUserIDKey = "userID"
	ContextOrgIDKey  = "orgID"
	ContextRoleKey   = "role"
	ContextEmailKey  = "email"
)

func AuthMiddleware(authService *auth.AuthService) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing authorization header",
			})
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid authorization format, expected 'Bearer <token>'",
			})
			return
		}

		tokenStr := parts[1]
		claims, err := authService.ValidateAccessToken(tokenStr)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid or expired token",
			})
			return
		}

		// Inject tenant context into request
		c.Set(ContextUserIDKey, claims.UserID)
		c.Set(ContextOrgIDKey, claims.OrganizationID)
		c.Set(ContextRoleKey, claims.Role)
		c.Set(ContextEmailKey, claims.Email)

		c.Next()
	}
}

func RequireRole(requiredRole string) gin.HandlerFunc {
	return func(c *gin.Context) {
		role, ok := GetRole(c)
		if !ok || role != requiredRole {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "insufficient permissions",
			})
			return
		}

		c.Next()
	}
}

func GetOrgID(c *gin.Context) (uuid.UUID, bool) {
	val, exists := c.Get(ContextOrgIDKey)
	if !exists {
		return uuid.Nil, false
	}
	orgID, ok := val.(uuid.UUID)
	return orgID, ok
}

func GetRole(c *gin.Context) (string, bool) {
	val, exists := c.Get(ContextRoleKey)
	if !exists {
		return "", false
	}
	role, ok := val.(string)
	return role, ok
}

func GetUserID(c *gin.Context) (uuid.UUID, bool) {
	val, exists := c.Get(ContextUserIDKey)
	if !exists {
		return uuid.Nil, false
	}
	userID, ok := val.(uuid.UUID)
	return userID, ok
}
