package config

import (
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	AppPort            string
	AppEnv             string
	DatabaseURL        string
	RedisAddr          string
	RedisPassword      string
	RedisDB            int
	JWTSecret          string
	AllowedOrigins     []string
	JWTAccessExpiry    time.Duration
	JWTRefreshExpiry   time.Duration
	WorkerConcurrency  int
	WorkerName         string
	StreamName         string
	ConsumerGroup      string
	DLQStreamName      string
	ClaimMinIdleTime   time.Duration
	OutboxPollInterval time.Duration
	CacheTTL           time.Duration
}

func LoadConfig() *Config {
	_ = godotenv.Load()

	appEnv := getEnv("APP_ENV", "development")
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" && appEnv != "production" {
		jwtSecret = "super-secret-jwt-key-distributed-platform-2026"
	}

	return &Config{
		AppPort:            getEnv("PORT", "8080"),
		AppEnv:             appEnv,
		DatabaseURL:        getEnv("DATABASE_URL", "postgres://postgres:postgrespassword@localhost:3000/distributed_jobs?sslmode=disable"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
		RedisDB:            getEnvAsInt("REDIS_DB", 0),
		JWTSecret:          jwtSecret,
		AllowedOrigins:     splitCSV(os.Getenv("ALLOWED_ORIGINS")),
		JWTAccessExpiry:    getEnvAsDuration("JWT_ACCESS_EXPIRY", 15*time.Minute),
		JWTRefreshExpiry:   getEnvAsDuration("JWT_REFRESH_EXPIRY", 7*24*time.Hour),
		WorkerConcurrency:  getEnvAsInt("WORKER_CONCURRENCY", 5),
		WorkerName:         getEnv("WORKER_NAME", "worker-1"),
		StreamName:         getEnv("STREAM_NAME", "jobs:stream"),
		ConsumerGroup:      getEnv("CONSUMER_GROUP", "job-workers-group"),
		DLQStreamName:      getEnv("DLQ_STREAM_NAME", "jobs:dlq"),
		ClaimMinIdleTime:   getEnvAsDuration("CLAIM_MIN_IDLE_TIME", 30*time.Second),
		OutboxPollInterval: getEnvAsDuration("OUTBOX_POLL_INTERVAL", 200*time.Millisecond),
		CacheTTL:           getEnvAsDuration("CACHE_TTL", 1*time.Minute),
	}
}

func (c *Config) Validate() error {
	if c.AppEnv == "production" {
		if len(c.JWTSecret) < 32 {
			return errors.New("JWT_SECRET must contain at least 32 characters in production")
		}
		if c.JWTSecret == "super-secret-jwt-key-distributed-platform-2026" {
			return errors.New("JWT_SECRET must not use the development default in production")
		}
	}
	return nil
}

func (c *Config) IsOriginAllowed(origin, host string) bool {
	if origin == "" {
		return true
	}
	for _, allowed := range c.AllowedOrigins {
		if origin == allowed {
			return true
		}
	}
	return sameOrigin(origin, host)
}

func sameOrigin(origin, host string) bool {
	parsed, err := url.Parse(origin)
	return err == nil && parsed.Host != "" && parsed.Path == "" && parsed.RawQuery == "" &&
		parsed.Fragment == "" && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host == host
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if origin := strings.TrimSpace(part); origin != "" {
			result = append(result, origin)
		}
	}
	return result
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	valStr := os.Getenv(key)
	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}
	return defaultVal
}

func getEnvAsDuration(key string, defaultVal time.Duration) time.Duration {
	valStr := os.Getenv(key)
	if val, err := time.ParseDuration(valStr); err == nil {
		return val
	}
	return defaultVal
}
