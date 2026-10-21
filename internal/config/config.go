package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	AppPort             string
	AppEnv              string
	DatabaseURL         string
	RedisAddr           string
	RedisPassword       string
	RedisDB             int
	JWTSecret           string
	JWTAccessExpiry     time.Duration
	JWTRefreshExpiry    time.Duration
	WorkerConcurrency   int
	WorkerName          string
	StreamName          string
	ConsumerGroup       string
	DLQStreamName       string
	ClaimMinIdleTime    time.Duration
	OutboxPollInterval  time.Duration
	CacheTTL            time.Duration
}

func LoadConfig() *Config {
	_ = godotenv.Load()

	return &Config{
		AppPort:            getEnv("PORT", "8080"),
		AppEnv:             getEnv("APP_ENV", "development"),
		DatabaseURL:        getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:3000/distributed_jobs?sslmode=disable"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
		RedisDB:            getEnvAsInt("REDIS_DB", 0),
		JWTSecret:          getEnv("JWT_SECRET", "super-secret-jwt-key-distributed-platform-2026"),
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
