package main

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// getKafkaAdmin creates a new Kafka admin client from the provided configuration
func getKafkaAdmin(config KafkaConfig) (*kafka.AdminClient, error) {
	fmt.Printf("🔧 Creating Kafka admin client with config:\n")
	fmt.Printf("   Server: %s\n", config.Server)

	// Create admin client configuration
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":       config.Server,
		"socket.keepalive.enable": true,
		"request.timeout.ms":      5000,  // 5 second timeout for requests
		"metadata.max.age.ms":     30000, // Cache metadata for 30 seconds
	}

	// Add debug configuration if enabled
	if config.DebugEnabled {
		if config.Debug != "" {
			configMap.SetKey("debug", config.Debug)
		} else {
			configMap.SetKey("debug", "broker,topic,protocol") // Default debug categories
		}
		configMap.SetKey("log_level", config.LogLevel)
		fmt.Printf("   Debug: %s (level %d)\n", config.Debug, config.LogLevel)
	} else {
		configMap.SetKey("log_level", 3) // INFO level for production
		fmt.Printf("   Debug: Disabled (log level 3)\n")
	}

	// Add SASL authentication if credentials are provided
	if config.Username != "" && config.Password != "" {
		configMap.SetKey("sasl.mechanisms", "PLAIN")
		configMap.SetKey("sasl.username", config.Username)
		configMap.SetKey("sasl.password", config.Password)
		configMap.SetKey("security.protocol", "SASL_PLAINTEXT")
		fmt.Printf("   Authentication: SASL PLAIN\n")
		fmt.Printf("   Username: %s\n", config.Username)
	} else {
		fmt.Printf("   Authentication: None (PLAINTEXT)\n")
		fmt.Printf("   ⚠️  WARNING: No authentication credentials provided!\n")
	}

	// Try SSL first, then fallback to PLAINTEXT
	if shouldUseSSL(config.Server) {
		fmt.Printf("   Security: SASL_SSL (trying SSL first)\n")
		configMap.SetKey("security.protocol", "SASL_SSL")
	} else {
		fmt.Printf("   Security: SASL_PLAINTEXT\n")
	}

	// Create admin client
	fmt.Printf("🔌 Connecting to Kafka cluster...\n")
	adminClient, err := kafka.NewAdminClient(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client: %w", err)
	}

	return adminClient, nil
}

// shouldUseSSL determines if SSL should be used based on the server URL
func shouldUseSSL(server string) bool {
	// Confluent Cloud typically uses SSL on port 9092
	return strings.Contains(server, "confluent.cloud") || strings.Contains(server, ":9092")
}
