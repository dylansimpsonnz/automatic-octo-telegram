package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"buffered-cdc/internal/buffer"
	"buffered-cdc/internal/config"
	"buffered-cdc/internal/monitor"

	"github.com/segmentio/kafka-go"
)

type KafkaSync struct {
	buffer     *buffer.Buffer
	config     *config.KafkaConfig
	connMonitor *monitor.ConnectivityMonitor
	writer     *kafka.Writer
}

func NewKafkaSync(cfg *config.Config, buf *buffer.Buffer, connMonitor *monitor.ConnectivityMonitor) *KafkaSync {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    cfg.Buffer.BatchSize,
		RequiredAcks: kafka.RequireOne,
		WriteTimeout: cfg.Kafka.Timeout,
	}

	return &KafkaSync{
		buffer:      buf,
		config:      &cfg.Kafka,
		connMonitor: connMonitor,
		writer:      writer,
	}
}

func (ks *KafkaSync) Start(ctx context.Context) {
	log.Println("Starting Kafka sync worker")
	
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	statusCh := ks.connMonitor.Subscribe()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ks.connMonitor.IsOnline() {
				if err := ks.syncBatch(ctx); err != nil {
					log.Printf("Failed to sync batch: %v", err)
				}
			}
		case status := <-statusCh:
			if status == monitor.StatusOnline {
				log.Println("Connectivity restored, starting sync process")
				if err := ks.syncBatch(ctx); err != nil {
					log.Printf("Failed to sync batch after connectivity restore: %v", err)
				}
			}
		}
	}
}

func (ks *KafkaSync) syncBatch(ctx context.Context) error {
	events, err := ks.buffer.GetReadyEvents(ks.config.Retries)
	if err != nil {
		return fmt.Errorf("failed to get ready events from buffer: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	log.Printf("Syncing %d events to Kafka", len(events))

	var messages []kafka.Message
	for _, event := range events {
		value, err := json.Marshal(event)
		if err != nil {
			log.Printf("Failed to marshal event %s: %v", event.ID, err)
			continue
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(event.ID),
			Value: value,
			Headers: []kafka.Header{
				{Key: "operation", Value: []byte(event.Operation)},
				{Key: "timestamp", Value: []byte(event.Timestamp.Format(time.RFC3339))},
			},
		})
	}

	if len(messages) == 0 {
		return nil
	}

	err = ks.writeWithRetry(ctx, messages, events)
	if err != nil {
		return fmt.Errorf("failed to write messages to Kafka: %w", err)
	}

	for _, event := range events {
		if err := ks.buffer.Delete(event.ID, event.Timestamp); err != nil {
			log.Printf("Failed to delete event %s from buffer: %v", event.ID, err)
		}
	}

	log.Printf("Successfully synced %d events to Kafka", len(events))
	return nil
}

func (ks *KafkaSync) writeWithRetry(ctx context.Context, messages []kafka.Message, events []*buffer.Event) error {
	backoff := time.Second

	for attempt := 0; attempt < ks.config.Retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
		}

		err := ks.writer.WriteMessages(ctx, messages...)
		if err == nil {
			return nil
		}

		log.Printf("Kafka write attempt %d failed: %v", attempt+1, err)

		if !ks.connMonitor.IsOnline() {
			log.Println("Connection lost during Kafka write, will retry when online")
			break
		}
	}

	for _, event := range events {
		if err := ks.buffer.UpdateRetries(event.ID, event.Timestamp, event.Retries+1); err != nil {
			log.Printf("Failed to update retry count for event %s: %v", event.ID, err)
		}
	}

	return fmt.Errorf("failed to write to Kafka after %d retries", ks.config.Retries)
}

func (ks *KafkaSync) Close() error {
	if ks.writer != nil {
		return ks.writer.Close()
	}
	return nil
}