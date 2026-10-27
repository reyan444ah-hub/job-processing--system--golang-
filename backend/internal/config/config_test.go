package config_test

import (
	"testing"

	"distributed-job-system/internal/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProductionConfigurationRequiresExplicitJWTSecret(t *testing.T) {
	cfg := &config.Config{AppEnv: "production"}
	assert.ErrorContains(t, cfg.Validate(), "JWT_SECRET")

	cfg.JWTSecret = "short"
	assert.ErrorContains(t, cfg.Validate(), "JWT_SECRET")

	cfg.JWTSecret = "unique-production-secret-with-at-least-32-characters"
	require.NoError(t, cfg.Validate())
}

func TestOriginAllowListAndSameOrigin(t *testing.T) {
	cfg := &config.Config{AllowedOrigins: []string{"https://trusted.example", "http://localhost:3000"}}
	assert.True(t, cfg.IsOriginAllowed("https://trusted.example", "api.example"))
	assert.True(t, cfg.IsOriginAllowed("https://api.example", "api.example"))
	assert.True(t, cfg.IsOriginAllowed("", "api.example"))
	assert.False(t, cfg.IsOriginAllowed("https://attacker.example", "api.example"))
}
