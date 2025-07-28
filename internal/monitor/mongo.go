package monitor

import (
	"context"
	"fmt"
	"log"
	"time"

	"buffered-cdc/internal/buffer"
	"buffered-cdc/internal/config"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoMonitor struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	buffer     *buffer.Buffer
	config     *config.MongoDBConfig
}

type ChangeStreamEvent struct {
	ID            interface{}            `bson:"_id"`
	OperationType string                 `bson:"operationType"`
	FullDocument  map[string]interface{} `bson:"fullDocument,omitempty"`
	DocumentKey   map[string]interface{} `bson:"documentKey"`
	ClusterTime   interface{}            `bson:"clusterTime"`
}

func NewMongoMonitor(cfg *config.Config, buf *buffer.Buffer) (*MongoMonitor, error) {
	clientOptions := options.Client().ApplyURI(cfg.MongoDB.URI)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	database := client.Database(cfg.MongoDB.Database)
	collection := database.Collection(cfg.MongoDB.Collection)

	return &MongoMonitor{
		client:     client,
		database:   database,
		collection: collection,
		buffer:     buf,
		config:     &cfg.MongoDB,
	}, nil
}

func (mm *MongoMonitor) Start(ctx context.Context) error {
	log.Println("Starting MongoDB change stream monitor")

	pipeline := mongo.Pipeline{}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	changeStream, err := mm.collection.Watch(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("failed to create change stream: %w", err)
	}
	defer changeStream.Close(ctx)

	for changeStream.Next(ctx) {
		var event ChangeStreamEvent
		if err := changeStream.Decode(&event); err != nil {
			log.Printf("Failed to decode change stream event: %v", err)
			continue
		}

		if err := mm.handleChangeEvent(&event); err != nil {
			log.Printf("Failed to handle change event: %v", err)
		}
	}

	if err := changeStream.Err(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}

	return nil
}

func (mm *MongoMonitor) handleChangeEvent(event *ChangeStreamEvent) error {
	var requestedReadyTime *time.Time
	
	// Extract requestedReadyTime from fullDocument if it exists
	if event.FullDocument != nil {
		if readyTimeVal, exists := event.FullDocument["requestedReadyTime"]; exists && readyTimeVal != nil {
			if readyTimeStr, ok := readyTimeVal.(string); ok {
				if parsedTime, err := time.Parse(time.RFC3339, readyTimeStr); err == nil {
					requestedReadyTime = &parsedTime
				}
			}
		}
	}

	bufferEvent := &buffer.Event{
		ID:                 fmt.Sprintf("%v", event.ID),
		Operation:          event.OperationType,
		Timestamp:          time.Now(),
		RequestedReadyTime: requestedReadyTime,
		Data: map[string]interface{}{
			"documentKey":   event.DocumentKey,
			"fullDocument":  event.FullDocument,
			"clusterTime":   event.ClusterTime,
			"operationType": event.OperationType,
		},
		Retries: 0,
	}

	// Check if we should send immediately or delay
	shouldDelay := false
	if requestedReadyTime != nil {
		// Delay if requestedReadyTime > current time + 30 minutes
		threshold := time.Now().Add(30 * time.Minute)
		if requestedReadyTime.After(threshold) {
			shouldDelay = true
		}
	}

	if shouldDelay {
		// Store in buffer for delayed processing
		if err := mm.buffer.Store(bufferEvent); err != nil {
			return fmt.Errorf("failed to store delayed event in buffer: %w", err)
		}
		log.Printf("Stored delayed change event: %s for document %v, ready at %v", 
			event.OperationType, event.DocumentKey, requestedReadyTime)
	} else {
		// Send immediately (for now, still store in buffer - the sync service will handle immediate sending)
		if err := mm.buffer.Store(bufferEvent); err != nil {
			return fmt.Errorf("failed to store immediate event in buffer: %w", err)
		}
		log.Printf("Stored immediate change event: %s for document %v", 
			event.OperationType, event.DocumentKey)
	}

	return nil
}

func (mm *MongoMonitor) Close() error {
	if mm.client != nil {
		return mm.client.Disconnect(context.Background())
	}
	return nil
}