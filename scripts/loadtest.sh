#!/bin/bash

# Load testing script for Buffered CDC Service
set -e

# Default values
WORKERS=10
MESSAGES=1000
BATCH_SIZE=10
DELAYED_PERCENT=30
MAX_DELAY_HOURS=24
MONGO_URI="mongodb://admin:password@localhost:27017/?authSource=admin&replicaSet=rs0"
DATABASE="testdb"
COLLECTION="events"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -w|--workers)
      WORKERS="$2"
      shift 2
      ;;
    -m|--messages)
      MESSAGES="$2"
      shift 2
      ;;
    -b|--batch-size)
      BATCH_SIZE="$2"
      shift 2
      ;;
    -d|--delayed-percent)
      DELAYED_PERCENT="$2"
      shift 2
      ;;
    --max-delay-hours)
      MAX_DELAY_HOURS="$2"
      shift 2
      ;;
    --mongo-uri)
      MONGO_URI="$2"
      shift 2
      ;;
    --database)
      DATABASE="$2"
      shift 2
      ;;
    --collection)
      COLLECTION="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  -w, --workers WORKERS              Number of concurrent workers (default: 10)"
      echo "  -m, --messages MESSAGES            Total number of messages (default: 1000)"
      echo "  -b, --batch-size BATCH_SIZE        Batch size for insertions (default: 10)"
      echo "  -d, --delayed-percent PERCENT      Percentage of delayed messages (default: 30)"
      echo "      --max-delay-hours HOURS        Maximum delay in hours (default: 24)"
      echo "      --mongo-uri URI                MongoDB connection URI"
      echo "      --database DATABASE            Database name (default: testdb)"
      echo "      --collection COLLECTION        Collection name (default: events)"
      echo "  -h, --help                         Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0 --workers 20 --messages 5000"
      echo "  $0 --delayed-percent 50 --max-delay-hours 12"
      echo "  $0 --mongo-uri mongodb://user:pass@host:27017/db"
      exit 0
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

echo "=== Buffered CDC Load Test ==="
echo "Workers: $WORKERS"
echo "Messages: $MESSAGES"
echo "Batch Size: $BATCH_SIZE"
echo "Delayed Messages: $DELAYED_PERCENT%"
echo "Max Delay: $MAX_DELAY_HOURS hours"
echo "Database: $DATABASE.$COLLECTION"
echo "================================"

# Check if loadtest binary exists
if [ ! -f "./cmd/loadtest/loadtest" ]; then
    echo "Building load test binary..."
    cd cmd/loadtest
    go build -o loadtest .
    cd ../..
fi

# Run the load test
echo "Starting load test..."
./cmd/loadtest/loadtest \
    --workers="$WORKERS" \
    --messages="$MESSAGES" \
    --batch-size="$BATCH_SIZE" \
    --delayed-percent="$DELAYED_PERCENT" \
    --max-delay-hours="$MAX_DELAY_HOURS" \
    --mongo-uri="$MONGO_URI" \
    --database="$DATABASE" \
    --collection="$COLLECTION"

echo ""
echo "Load test completed!"
echo ""
echo "To monitor the results:"
echo "1. Check Kafka UI: http://localhost:8080"
echo "2. Check MongoDB Express: http://localhost:8081"
echo "3. View CDC service logs: docker-compose logs -f buffered-cdc"