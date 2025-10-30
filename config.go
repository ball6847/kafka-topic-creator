package main

import (
	"fmt"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

// KafkaConfig holds the configuration for Kafka connection
type KafkaConfig struct {
	Server   string `envconfig:"KAFKA_SERVER" default:"localhost:9092"`
	Username string `envconfig:"KAFKA_USERNAME"`
	Password string `envconfig:"KAFKA_PASSWORD"`

	// Debug and logging configuration
	DebugEnabled bool   `envconfig:"KAFKA_DEBUG_ENABLED" default:"false"`
	Debug        string `envconfig:"KAFKA_DEBUG" default:""`
	LogLevel     int    `envconfig:"KAFKA_LOG_LEVEL" default:"6"` // 6=INFO, 7=DEBUG
}

// loadConfig loads configuration from .env file and environment variables
func loadConfig() (KafkaConfig, error) {
	// Load .env file if it exists (ignore error if file doesn't exist)
	_ = godotenv.Load()

	// Get Kafka configuration from environment
	var config KafkaConfig
	if err := envconfig.Process("", &config); err != nil {
		return config, fmt.Errorf("failed to process environment config: %w", err)
	}

	return config, nil
}
