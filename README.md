# Buffered CDC Service

A Go-based service that syncs MongoDB change streams to Kafka with offline buffering and retry logic.

## Features

- **MongoDB Change Streams**: Monitors data changes in real-time
- **Offline Buffering**: Uses BoltDB for durable local storage when offline
- **Kafka Sync**: Publishes events to Kafka with retry logic and exponential backoff
- **Connectivity Monitoring**: Automatically detects online/offline status
- **Task Scheduling**: Cron-based scheduler for maintenance tasks
- **Delayed Message Delivery**: Schedule messages for future delivery with `requestedReadyTime`
- **Graceful Shutdown**: Ensures data integrity during service stops

## Architecture

```
MongoDB -> Change Stream -> Local Buffer (BoltDB) -> Kafka
                                 ^                      ^
                                 |                      |
                         Offline Storage      Online Sync Worker
```

## Getting Started

### Quick Start with Docker

The easiest way to run the service is using Docker Compose, which includes MongoDB, Kafka, and all dependencies:

```bash
git clone <repository>
cd buffered-cdc

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f buffered-cdc

# Stop all services
docker-compose down
```

This will start:
- MongoDB with replica set (required for change streams)
- Kafka with Zookeeper
- The buffered CDC service
- Kafka UI (http://localhost:8080) for monitoring
- MongoDB Express (http://localhost:8081) for database management

### Prerequisites

- Docker and Docker Compose
- Or for local development:
  - Go 1.23+
  - MongoDB with replica set enabled
  - Kafka cluster

### Local Development

```bash
git clone <repository>
cd buffered-cdc
go mod download
go build -o buffered-cdc .
```

### Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
# Edit .env with your configuration
```

### Running Locally

```bash
./buffered-cdc
```

## Configuration Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `MONGODB_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGODB_DATABASE` | `testdb` | Database to monitor |
| `MONGODB_COLLECTION` | `events` | Collection to monitor |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC` | `cdc-events` | Target Kafka topic |
| `KAFKA_RETRIES` | `3` | Number of retry attempts |
| `KAFKA_TIMEOUT` | `30s` | Kafka write timeout |
| `BUFFER_PATH` | `./buffer.db` | Local buffer database path |
| `BUFFER_BATCH_SIZE` | `100` | Batch size for processing |
| `MONITOR_INTERVAL` | `30s` | Connectivity check interval |
| `CONNECT_TIMEOUT` | `10s` | Connection timeout |
| `MAX_RETRIES` | `5` | Maximum retry attempts |
| `BACKOFF_INTERVAL` | `5s` | Base backoff interval |

## Data Flow

1. **Change Detection**: MongoDB change streams detect document changes
2. **Scheduling Logic**: Events with `requestedReadyTime` >30 minutes are delayed
3. **Local Buffering**: Events are stored in BoltDB for durability
4. **Connectivity Check**: Service monitors Kafka connectivity
5. **Batch Processing**: When online, ready events are sent to Kafka in batches
6. **Retry Logic**: Failed events are retried with exponential backoff
7. **Cleanup**: Successfully sent events are removed from buffer

## Scheduled Tasks

The service includes several scheduled maintenance tasks:

- **Buffer Stats** (every 5 minutes): Logs buffer statistics
- **Cleanup** (daily at 2 AM): Removes old failed events (>10 retries, >24h old)
- **Health Check** (every minute): Monitors buffer size and alerts on issues
- **Scheduled Events** (every minute): Processes delayed events that are now ready

## Event Format

Events sent to Kafka have the following structure:

```json
{
  "id": "unique-event-id",
  "operation": "insert|update|delete|replace",
  "timestamp": "2024-01-01T00:00:00Z",
  "requestedReadyTime": "2024-01-01T12:00:00Z",
  "data": {
    "documentKey": {...},
    "fullDocument": {...},
    "clusterTime": {...},
    "operationType": "..."
  },
  "retries": 0
}
```

## Error Handling

- **Connection Failures**: Events are buffered locally until connectivity is restored
- **Kafka Failures**: Automatic retry with exponential backoff
- **Buffer Full**: Configurable cleanup policies for old events
- **Graceful Shutdown**: Ensures all in-flight operations complete safely

## Monitoring

The service provides built-in monitoring:

- Connection status logging
- Buffer size monitoring
- Sync statistics
- Failed event tracking
- Health check alerts

## Development

### Project Structure

```
├── main.go                    # Application entry point
├── Dockerfile                 # Docker image definition
├── docker-compose.yml         # Full stack deployment
├── .dockerignore              # Docker ignore file
├── scripts/                   # Setup scripts
│   ├── mongo-init.js         # MongoDB initialization
│   └── setup-replica-set.js  # Replica set setup
├── internal/
│   ├── config/               # Configuration management
│   ├── buffer/               # BoltDB buffer implementation
│   ├── monitor/              # MongoDB and connectivity monitoring
│   ├── sync/                 # Kafka sync worker
│   ├── scheduler/            # Cron-based task scheduler
│   └── service/              # Main service orchestration
└── README.md
```

### Building

```bash
# Local build
go build -o buffered-cdc .

# Docker build
docker build -t buffered-cdc .
```

### Testing the Service

1. **Start the stack:**
   ```bash
   docker-compose up -d
   ```

2. **Insert test data into MongoDB:**
   ```bash
   # Connect to MongoDB
   docker exec -it mongo mongosh -u admin -p password --authenticationDatabase admin

   # Switch to testdb and insert data
   use testdb
   
   # Immediate message (sent right away)
   db.events.insertOne({
     message: "Immediate test message", 
     timestamp: new Date()
   })
   
   # Delayed message (sent in 2 hours)
   db.events.insertOne({
     message: "Delayed test message",
     requestedReadyTime: new Date(Date.now() + 2 * 60 * 60 * 1000).toISOString(),
     timestamp: new Date()
   })
   
   # Update existing document
   db.events.updateOne(
     {message: "Immediate test message"}, 
     {$set: {updated: true, requestedReadyTime: new Date(Date.now() + 45 * 60 * 1000).toISOString()}}
   )
   ```

3. **Check Kafka for events:**
   - Visit Kafka UI at http://localhost:8080
   - Navigate to Topics → cdc-events
   - View messages to see the CDC events
   - Note: Delayed messages will only appear after their `requestedReadyTime`

### Running Tests

```bash
go test ./...
```

## Delayed Message Delivery

The service supports delayed message delivery using the `requestedReadyTime` field:

### How It Works

- **Immediate Delivery**: Documents without `requestedReadyTime` or with time ≤30 minutes in future are sent immediately
- **Delayed Delivery**: Documents with `requestedReadyTime` >30 minutes in future are stored and scheduled
- **Scheduling**: A background task runs every minute to check for ready delayed messages

### Document Format

Add `requestedReadyTime` as an ISO 8601 string to your MongoDB documents:

```javascript
{
  message: "Hello World",
  requestedReadyTime: "2025-01-15T14:30:00Z",  // RFC3339 format
  otherField: "value"
}
```

### Use Cases

- **Scheduled Notifications**: Send alerts at specific times
- **Delayed Processing**: Batch processing at optimal times  
- **Event Scheduling**: Trigger events in the future
- **Rate Limiting**: Spread message delivery over time

## License

MIT License