package main

import (
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
		configFile = flag.String("config", "", "Path to the topics configuration file (required)")
	)
	flag.Parse()

	// Validate that config file is provided
	if *configFile == "" {
		fmt.Println("❌ Error: -config flag is required")
		fmt.Printf("Usage: %s -config <config-file.yaml> [options]\n", os.Args[0])
		fmt.Printf("Example: %s -config topics.yaml\n", os.Args[0])
		os.Exit(1)
	}

	// Handle listing topics
	if *listTopics {
		fmt.Println("📋 Available topics:")
		topicSpecs, err := GetAllTopicConfigs(*configFile)
		if err != nil {
			log.Fatalf("❌ Failed to load topic configurations: %v", err)
		}
		for _, ts := range topicSpecs {
			fmt.Printf("  %-40s Partitions: %-2d Replication: %d\n", ts.Topic, ts.NumPartitions, ts.ReplicationFactor)
		}
		return
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("🚀 Starting Kafka Topic Creation Tool")
	fmt.Println("Press Ctrl+C to cancel...")

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

	// Create all topics with predefined configurations
	topicConfigs, err := GetAllTopicConfigs(*configFile)
	if err != nil {
		log.Fatalf("❌ Failed to load topic configurations: %v", err)
	}
	topicCount := len(topicConfigs)
	fmt.Printf("📋 Creating %d topics with predefined configurations\n", topicCount)

	// Run topic creation in a goroutine so we can handle signals
	errChan := make(chan error, 1)
	doneChan := make(chan bool, 1)

	go func() {
		err := topicManager.CreateTopics(topicConfigs)

		if err != nil {
			errChan <- err
		} else {
			doneChan <- true
		}
	}()

	// Wait for either completion, error, or signal
	select {
	case err := <-errChan:
		log.Fatalf("❌ Failed to create topics: %v", err)
	case <-doneChan:
		fmt.Println("✅ Topic creation process completed successfully!")
	case <-sigChan:
		fmt.Println("\n🛑 Received interrupt signal, shutting down gracefully...")
		fmt.Println("✅ Topic creation cancelled by user")
		os.Exit(0)
	}
}
