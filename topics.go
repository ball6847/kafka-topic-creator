package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"gopkg.in/yaml.v2"
)

// TopicConfig represents a single topic configuration from YAML
type TopicConfig struct {
	Name              string `yaml:"name"`
	Partitions        int    `yaml:"partitions"`
	ReplicationFactor int    `yaml:"replication_factor"`
	Description       string `yaml:"description,omitempty"`
}

// TopicsConfig represents the complete YAML configuration
type TopicsConfig struct {
	Topics []TopicConfig `yaml:"topics"`
}

// GetAllTopicConfigs returns the list of all topics with their configurations from YAML file
func GetAllTopicConfigs(configFile string) ([]kafka.TopicSpecification, error) {
	// Read the YAML config file
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configFile, err)
	}

	// Parse the YAML content
	var config TopicsConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", configFile, err)
	}

	// Validate and convert to Kafka TopicSpecifications
	var topicSpecs []kafka.TopicSpecification
	for _, topic := range config.Topics {
		// Validate topic configuration
		if topic.Name == "" {
			return nil, fmt.Errorf("topic name cannot be empty")
		}
		if topic.Partitions <= 0 {
			return nil, fmt.Errorf("topic '%s' must have at least 1 partition", topic.Name)
		}
		if topic.ReplicationFactor <= 0 {
			return nil, fmt.Errorf("topic '%s' must have at least 1 replication factor", topic.Name)
		}

		topicSpecs = append(topicSpecs, kafka.TopicSpecification{
			Topic:             topic.Name,
			NumPartitions:     topic.Partitions,
			ReplicationFactor: topic.ReplicationFactor,
		})
	}

	return topicSpecs, nil
}
