package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestRequireRole(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		role           string
		expectedStatus int
	}{
		{name: "allows required role", role: "admin", expectedStatus: http.StatusNoContent},
		{name: "rejects member", role: "member", expectedStatus: http.StatusForbidden},
		{name: "rejects missing role", expectedStatus: http.StatusForbidden},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router := gin.New()
			router.GET("/admin", func(c *gin.Context) {
				if test.role != "" {
					c.Set(ContextRoleKey, test.role)
				}
			}, RequireRole("admin"), func(c *gin.Context) {
				c.Status(http.StatusNoContent)
			})

			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin", nil))
			assert.Equal(t, test.expectedStatus, response.Code)
		})
	}
}
