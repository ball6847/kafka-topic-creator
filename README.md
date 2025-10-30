# Kafka Topic Creator

This tool creates Kafka topics for the CheckinPlus platform using a configurable YAML file for topic definitions. The tool supports both predefined topic configurations and custom topic definitions.

## Installation

### Via go install

```bash
go install github.com/ball6847/kafka-topic-creator@latest
```

### Via go run

```bash
# Create all topics from config file
go run . -config topics.yaml

# List all available topics and their configurations
go run . -config topics.yaml -list

# Or build and run:
go build .
./kafka-topic-creator -config topics.yaml [flags]
```

**Note**: The `-config` flag is required. No default configuration file will be loaded.

## Command Line Flags

- `-config <file>`: Path to the topics configuration file (required)
- `-list`: List all available topics and exit

## Configuration

### Environment Variables

- `KAFKA_SERVER`: Kafka bootstrap servers (default: localhost:9092)
- `KAFKA_USERNAME`: Username for SASL authentication (optional)
- `KAFKA_PASSWORD`: Password for SASL authentication (optional)
- `KAFKA_DEBUG_ENABLED`: Enable debug logging (default: false)
- `KAFKA_DEBUG`: Debug categories (default: broker,topic,protocol)
- `KAFKA_LOG_LEVEL`: Log level (default: 6 for INFO, 7 for DEBUG)

### .env File Support

Copy `.env.example` to `.env` for local development:

```bash
cp .env.example .env
```

Then edit `.env` with your Kafka configuration. The application supports both `.env` files and environment variables, with environment variables taking precedence.

### Security and SSL

The tool automatically detects when to use SSL based on the server URL:
- Confluent Cloud servers (containing "confluent.cloud") use SASL_SSL
- Port 9092 connections attempt SSL first
- Falls back to PLAINTEXT for local development

When authentication credentials are provided, the tool uses SASL authentication.

## How it works

The tool follows a clean architecture pattern:

1. **Configuration Loading** - Loads config from .env file and environment variables
2. **CLI Parsing** - Parses command-line flags for topic configuration
3. **Config File Processing** - Reads and parses YAML configuration file
4. **Dependency Setup** - Creates Kafka admin client and topic manager
5. **Action Execution** - Creates topics using dependency injection

**This script is idempotent** - it can be run multiple times safely. If a topic already exists, it will skip it without error.

## Topic Configurations

The tool reads topic configurations from a YAML file. Each topic can have custom partition and replication factor settings.

### Example YAML Configuration

```yaml
topics:
  - name: "auth.login-otp"
    partitions: 1
    replication_factor: 1
  - name: "property.property_create"
    partitions: 1
    replication_factor: 1
  - name: "room_availability.room_availability_update"
    partitions: 12
    replication_factor: 1
```

### Configuration Guidelines

- **High-throughput topics** like `room_availability.room_availability_update` use 12+ partitions for better parallelism
- **Medium-volume topics** use 6 partitions
- **Low-volume topics** use 3 partitions or 1 partition
- All topics use replication factor of 1 by default for development environments
- For production, consider using replication factor of 3 for fault tolerance

These configurations are defined in YAML files and serve as the source of truth for infrastructure changes. All topic configurations are tracked in files, ensuring consistent and auditable infrastructure management.

## Architecture Benefits

- **Clean Separation** - Each file has a single responsibility
- **Testability** - Components can be unit tested independently
- **Maintainability** - Easy to modify specific functionality
- **Reusability** - Components can be imported by other applications
- **Modern Go** - Uses envconfig, godotenv, and dependency injection
- **Flexible Configuration** - Supports both predefined and custom topic configurations

## Updating Topics

When new topics are added to the shared libraries, update the topic configurations in your YAML configuration file to include the new topics with appropriate partition and replication factor settings. Each environment can have its own configuration file with different settings optimized for that environment's requirements.
