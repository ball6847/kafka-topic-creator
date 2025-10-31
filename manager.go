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

// GetExistingTopics retrieves metadata for all existing topics
func (tm *TopicManager) GetExistingTopics(ctx context.Context) (map[string]kafka.TopicMetadata, error) {
	metadata, err := tm.adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	topics := make(map[string]kafka.TopicMetadata)
	for _, topic := range metadata.Topics {
		topics[topic.Topic] = topic
	}

	return topics, nil
}

// SyncTopics synchronizes topics to match desired configurations (creates missing, updates existing)
func (tm *TopicManager) SyncTopics(ctx context.Context, topicSpecs []kafka.TopicSpecification) error {
	// Get existing topics metadata
	existingTopics, err := tm.GetExistingTopics(ctx)
	if err != nil {
		return fmt.Errorf("failed to get existing topics: %w", err)
	}

	// Track operation results
	var topicsToCreate []kafka.TopicSpecification
	var topicsToUpdate []topicUpdateInfo
	var cannotScaleDown []topicScaleDownInfo
	var unchangedCount int

	// Analyze each desired topic
	for _, spec := range topicSpecs {
		existing, exists := existingTopics[spec.Topic]

		if !exists {
			// Topic doesn't exist - add to creation list
			topicsToCreate = append(topicsToCreate, spec)
			continue
		}

		// Topic exists - check if updates are needed
		currentPartitions := len(existing.Partitions)
		needsUpdate := false
		updateInfo := topicUpdateInfo{
			topic:   spec.Topic,
			current: existing,
			desired: spec,
		}

		// Check partition changes
		if spec.NumPartitions > currentPartitions {
			// Need to increase partitions
			needsUpdate = true
			updateInfo.needsPartitionIncrease = true
		} else if spec.NumPartitions < currentPartitions {
			// Cannot decrease partitions - report this
			cannotScaleDown = append(cannotScaleDown, topicScaleDownInfo{
				topic:             spec.Topic,
				currentPartitions: currentPartitions,
				desiredPartitions: spec.NumPartitions,
			})
		}

		// Check replication factor changes (more complex, for now just report)
		if len(existing.Partitions) > 0 && int32(spec.ReplicationFactor) != existing.Partitions[0].Replicas[0] {
			// This would require more complex broker reassignment
			// For now, we'll note it but not implement
			fmt.Printf("⚠️  Topic '%s' replication factor change not yet implemented\n", spec.Topic)
		}

		if needsUpdate {
			topicsToUpdate = append(topicsToUpdate, updateInfo)
		} else if spec.NumPartitions == currentPartitions {
			fmt.Printf("ℹ️  Topic '%s' already matches desired configuration\n", spec.Topic)
			unchangedCount++
		}
	}

	// Execute operations
	createdCount, updatedCount, failedCount := 0, 0, 0

	// Create missing topics
	if len(topicsToCreate) > 0 {
		fmt.Printf("📋 Creating %d new topics...\n", len(topicsToCreate))
		err := tm.createTopicsFromSpecs(ctx, topicsToCreate)
		if err != nil {
			fmt.Printf("❌ Failed to create topics: %v\n", err)
			failedCount += len(topicsToCreate)
		} else {
			createdCount = len(topicsToCreate)
		}
	}

	// Update existing topics
	if len(topicsToUpdate) > 0 {
		fmt.Printf("🔄 Updating %d existing topics...\n", len(topicsToUpdate))
		for _, update := range topicsToUpdate {
			if update.needsPartitionIncrease {
				err := tm.increaseTopicPartitions(ctx, update.topic, update.desired.NumPartitions)
				if err != nil {
					fmt.Printf("❌ Failed to update partitions for topic '%s': %v\n", update.topic, err)
					failedCount++
				} else {
					fmt.Printf("✅ Successfully updated partitions for topic '%s'\n", update.topic)
					updatedCount++
				}
			}
		}
	}

	// Report topics that cannot be scaled down
	if len(cannotScaleDown) > 0 {
		fmt.Printf("⚠️  %d topics cannot be scaled down (Kafka limitation):\n", len(cannotScaleDown))
		for _, info := range cannotScaleDown {
			fmt.Printf("   - '%s': %d → %d partitions\n", info.topic, info.currentPartitions, info.desiredPartitions)
		}
	}

	// Print summary
	fmt.Printf("📊 Sync Summary: %d created, %d updated, %d unchanged, %d cannot scale down, %d failed\n",
		createdCount, updatedCount, unchangedCount, len(cannotScaleDown), failedCount)

	if failedCount > 0 {
		return fmt.Errorf("some operations failed: %d failures", failedCount)
	}

	return nil
}

// Helper types for sync operations
type topicUpdateInfo struct {
	topic                  string
	current                kafka.TopicMetadata
	desired                kafka.TopicSpecification
	needsPartitionIncrease bool
}

type topicScaleDownInfo struct {
	topic             string
	currentPartitions int
	desiredPartitions int
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

// increaseTopicPartitions increases the number of partitions for a topic
func (tm *TopicManager) increaseTopicPartitions(ctx context.Context, topicName string, newPartitionCount int) error {
	// Create partition specification
	partitionSpec := []kafka.PartitionsSpecification{
		{
			Topic:      topicName,
			IncreaseTo: newPartitionCount,
		},
	}

	// Alter topic to increase partitions
	results, err := tm.adminClient.CreatePartitions(ctx, partitionSpec, nil)
	if err != nil {
		return fmt.Errorf("failed to increase partitions for topic '%s': %w", topicName, err)
	}

	// Check results
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("failed to increase partitions for topic '%s': %v", result.Topic, result.Error)
		}
	}

	return nil
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
