package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// TopicManager handles Kafka topic operations
type TopicManager struct {
	adminClient *kafka.AdminClient
}

// NewTopicManager creates a new TopicManager with the given admin client
func NewTopicManager(adminClient *kafka.AdminClient) *TopicManager {
	return &TopicManager{
		adminClient: adminClient,
	}
}

// CreateTopics creates topics with predefined configurations using the admin client with retry logic
func (tm *TopicManager) CreateTopics(ctx context.Context, topicSpecs []kafka.TopicSpecification) error {
	return tm.createTopicsFromSpecs(ctx, topicSpecs)
}

// createTopicsFromSpecs creates topics from specifications with retry logic
func (tm *TopicManager) createTopicsFromSpecs(ctx context.Context, topicSpecs []kafka.TopicSpecification) error {
	topicCount := len(topicSpecs)

	// Retry logic for connection issues
	maxRetries := 2
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		fmt.Printf("Attempting to create topics (attempt %d/%d)...\n", attempt, maxRetries)

		// Create topics with timeout
		results, err := tm.adminClient.CreateTopics(ctx, topicSpecs, nil)
		if err != nil {
			lastErr = fmt.Errorf("failed to create topics on attempt %d: %w", attempt, err)
			log.Printf("Connection error: %v", err)

			// Check if it's a connection error that we should retry
			if attempt < maxRetries && isRetryableError(err) {
				waitTime := time.Duration(attempt) * 1 * time.Second
				fmt.Printf("Retrying in %v...\n", waitTime)
				time.Sleep(waitTime)
				continue
			}
			break
		}

		// Check results
		successCount := 0
		existsCount := 0
		errorCount := 0

		for _, result := range results {
			if result.Error.Code() == kafka.ErrNoError {
				fmt.Printf("✅ Successfully created topic '%s'\n", result.Topic)
				successCount++
				continue
			}

			// Topic might already exist, which is not an error for our purposes
			if result.Error.Code() == kafka.ErrTopicAlreadyExists {
				fmt.Printf("ℹ️  Topic '%s' already exists\n", result.Topic)
				existsCount++
				continue
			}

			// Handle other errors
			fmt.Printf("❌ Failed to create topic '%s': %v\n", result.Topic, result.Error)
			errorCount++
		}

		// Print summary
		fmt.Printf("📊 Topic creation summary: %d created, %d already exist, %d errors\n",
			successCount, existsCount, errorCount)

		// If we have no errors, return success
		if errorCount == 0 {
			return nil
		}

		lastErr = fmt.Errorf("some topics failed to create: %d errors out of %d topics",
			errorCount, topicCount)
		break
	}

	return lastErr
}

// isRetryableError determines if an error should trigger a retry
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	// Retry on connection-related errors
	retryableErrors := []string{
		"connection refused",
		"connection closed",
		"timeout",
		"network is unreachable",
		"no such host",
		"connection reset by peer",
		"broken pipe",
	}

	for _, retryable := range retryableErrors {
		if strings.Contains(strings.ToLower(errStr), retryable) {
			return true
		}
	}

	return false
}
