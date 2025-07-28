package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LoadTestConfig struct {
	MongoURI       string
	Database       string
	Collection     string
	Workers        int
	TotalMessages  int
	BatchSize      int
	DelayedPercent int
	MaxDelayHours  int
}

type TestMessage struct {
	ID           string     `bson:"_id,omitempty"`
	Message      string     `bson:"message"`
	Timestamp    time.Time  `bson:"timestamp"`
	DelayedUntil *time.Time `bson:"delayedUntil,omitempty"`
	LoadTestBatch int       `bson:"loadTestBatch"`
	MessageType  string     `bson:"messageType"`
	Payload      map[string]interface{} `bson:"payload"`
}

func main() {
	config := parseFlags()
	
	log.Printf("Starting load test with %d workers, %d total messages", config.Workers, config.TotalMessages)
	log.Printf("MongoDB: %s/%s.%s", config.MongoURI, config.Database, config.Collection)
	log.Printf("Delayed messages: %d%%, max delay: %d hours", config.DelayedPercent, config.MaxDelayHours)
	
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(config.MongoURI))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background())
	
	collection := client.Database(config.Database).Collection(config.Collection)
	
	startTime := time.Now()
	
	runLoadTest(collection, config)
	
	duration := time.Since(startTime)
	throughput := float64(config.TotalMessages) / duration.Seconds()
	
	log.Printf("Load test completed in %v", duration)
	log.Printf("Throughput: %.2f messages/second", throughput)
}

func parseFlags() *LoadTestConfig {
	config := &LoadTestConfig{}
	
	flag.StringVar(&config.MongoURI, "mongo-uri", "mongodb://admin:password@mongo:27017/?authSource=admin&replicaSet=rs0", "MongoDB connection URI")
	flag.StringVar(&config.Database, "database", "testdb", "Database name")
	flag.StringVar(&config.Collection, "collection", "events", "Collection name")
	flag.IntVar(&config.Workers, "workers", 10, "Number of concurrent workers")
	flag.IntVar(&config.TotalMessages, "messages", 1000, "Total number of messages to insert")
	flag.IntVar(&config.BatchSize, "batch-size", 10, "Batch size for insertions")
	flag.IntVar(&config.DelayedPercent, "delayed-percent", 30, "Percentage of messages with delayed delivery")
	flag.IntVar(&config.MaxDelayHours, "max-delay-hours", 24, "Maximum delay in hours for delayed messages")
	
	flag.Parse()
	
	if config.TotalMessages < config.Workers {
		config.Workers = config.TotalMessages
	}
	
	return config
}

func runLoadTest(collection *mongo.Collection, config *LoadTestConfig) {
	var wg sync.WaitGroup
	messagesChan := make(chan int, config.TotalMessages)
	
	// Fill the channel with message indices
	for i := 0; i < config.TotalMessages; i++ {
		messagesChan <- i
	}
	close(messagesChan)
	
	// Start workers
	for i := 0; i < config.Workers; i++ {
		wg.Add(1)
		go worker(i, collection, config, messagesChan, &wg)
	}
	
	wg.Wait()
}

func worker(workerID int, collection *mongo.Collection, config *LoadTestConfig, messagesChan <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	
	batch := make([]interface{}, 0, config.BatchSize)
	batchNum := 0
	
	for msgIndex := range messagesChan {
		message := generateTestMessage(msgIndex, config)
		batch = append(batch, message)
		
		if len(batch) >= config.BatchSize {
			insertBatch(collection, batch, workerID, batchNum)
			batch = batch[:0]
			batchNum++
		}
	}
	
	// Insert remaining messages
	if len(batch) > 0 {
		insertBatch(collection, batch, workerID, batchNum)
	}
}

func generateTestMessage(index int, config *LoadTestConfig) *TestMessage {
	now := time.Now()
	message := &TestMessage{
		Message:       fmt.Sprintf("Load test message #%d", index),
		Timestamp:     now,
		LoadTestBatch: index / config.BatchSize,
		Payload: map[string]interface{}{
			"index":     index,
			"worker":    index % config.Workers,
			"timestamp": now.Unix(),
			"random":    rand.Intn(1000),
		},
	}
	
	// Determine if this should be a delayed message
	if rand.Intn(100) < config.DelayedPercent {
		// Random delay between 1 minute and max delay hours
		minDelayMinutes := 1
		maxDelayMinutes := config.MaxDelayHours * 60
		delayMinutes := minDelayMinutes + rand.Intn(maxDelayMinutes-minDelayMinutes)
		
		delayTime := now.Add(time.Duration(delayMinutes) * time.Minute)
		message.DelayedUntil = &delayTime
		message.MessageType = "delayed"
	} else {
		message.MessageType = "immediate"
	}
	
	return message
}

func insertBatch(collection *mongo.Collection, batch []interface{}, workerID, batchNum int) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	result, err := collection.InsertMany(ctx, batch)
	if err != nil {
		log.Printf("Worker %d: Failed to insert batch %d: %v", workerID, batchNum, err)
		return
	}
	
	log.Printf("Worker %d: Inserted batch %d with %d messages (IDs: %d)", 
		workerID, batchNum, len(result.InsertedIDs), len(result.InsertedIDs))
}