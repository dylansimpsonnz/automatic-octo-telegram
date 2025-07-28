package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	MongoDB MongoDBConfig
	Kafka   KafkaConfig
	Buffer  BufferConfig
	Monitor MonitorConfig
}

type MongoDBConfig struct {
	URI        string
	Database   string
	Collection string
}

type KafkaConfig struct {
	Brokers []string
	Topic   string
	Retries int
	Timeout time.Duration
}

type BufferConfig struct {
	Path      string
	BatchSize int
}

type MonitorConfig struct {
	Interval        time.Duration
	ConnectTimeout  time.Duration
	MaxRetries      int
	BackoffInterval time.Duration
}

func Load() (*Config, error) {
	cfg := &Config{
		MongoDB: MongoDBConfig{
			URI:        getEnv("MONGODB_URI", "mongodb://localhost:27017"),
			Database:   getEnv("MONGODB_DATABASE", "testdb"),
			Collection: getEnv("MONGODB_COLLECTION", "events"),
		},
		Kafka: KafkaConfig{
			Brokers: []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
			Topic:   getEnv("KAFKA_TOPIC", "cdc-events"),
			Retries: getEnvInt("KAFKA_RETRIES", 3),
			Timeout: getEnvDuration("KAFKA_TIMEOUT", 30*time.Second),
		},
		Buffer: BufferConfig{
			Path:      getEnv("BUFFER_PATH", "./buffer.db"),
			BatchSize: getEnvInt("BUFFER_BATCH_SIZE", 100),
		},
		Monitor: MonitorConfig{
			Interval:        getEnvDuration("MONITOR_INTERVAL", 30*time.Second),
			ConnectTimeout:  getEnvDuration("CONNECT_TIMEOUT", 10*time.Second),
			MaxRetries:      getEnvInt("MAX_RETRIES", 5),
			BackoffInterval: getEnvDuration("BACKOFF_INTERVAL", 5*time.Second),
		},
	}
	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}