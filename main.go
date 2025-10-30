package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Define command-line flags
	var (
		listTopics = flag.Bool("list", false, "List all available topics and exit")
		configFile = flag.String("config", "", "Path to topics configuration file (required)")
	)
	flag.Parse()

	// Validate that config file is provided
	if *configFile == "" {
		fmt.Println("❌ Error: -config flag is required")
		fmt.Printf("Usage: %s -config <config-file.yaml> [options]\n", os.Args[0])
		fmt.Printf("Example: %s -config topics.yaml\n", os.Args[0])
		os.Exit(1)
	}

	// Handle graceful shutdown with context cancellation
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create context that can be cancelled by signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := <-sigChan
		fmt.Printf("\n🛑 Received signal %v, cancelling operations...\n", sig)
		cancel()
	}()

	fmt.Println("🚀 Starting Kafka Topic Creation Tool")
	fmt.Println("Press Ctrl+C to cancel...")

	// Load topic configurations once
	topicConfigs, err := GetAllTopicConfigs(*configFile)
	if err != nil {
		log.Fatalf("❌ Failed to load topic configurations: %v", err)
	}

	// Handle listing topics
	if *listTopics {
		fmt.Println("📋 Available topics:")
		for _, ts := range topicConfigs {
			fmt.Printf("  %-40s Partitions: %-2d Replication: %d\n", ts.Topic, ts.NumPartitions, ts.ReplicationFactor)
		}
		return
	}

	config, err := loadConfig()
	if err != nil {
		log.Fatalf("❌ Failed to load configuration: %v", err)
	}

	fmt.Printf("📡 Connecting to Kafka at %s\n", config.Server)
	adminClient, err := getKafkaAdmin(config)
	if err != nil {
		log.Fatalf("❌ Failed to create Kafka admin client: %v", err)
	}
	defer adminClient.Close()

	topicManager := NewTopicManager(adminClient)

	topicCount := len(topicConfigs)
	fmt.Printf("📋 Creating %d topics with predefined configurations\n", topicCount)

	// Create topics with context for cancellation
	err = topicManager.CreateTopics(ctx, topicConfigs)
	if err != nil {
		if ctx.Err() == context.Canceled {
			fmt.Println("✅ Topic creation cancelled by user")
		} else {
			log.Fatalf("❌ Failed to create topics: %v", err)
		}
		return
	}

	fmt.Println("✅ Topic creation process completed successfully!")
}
