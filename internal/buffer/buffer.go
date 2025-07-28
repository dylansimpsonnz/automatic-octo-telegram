package buffer

import (
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

const (
	eventsBucket = "events"
)

type Event struct {
	ID                 string                 `json:"id"`
	Operation          string                 `json:"operation"`
	Timestamp          time.Time              `json:"timestamp"`
	Data               map[string]interface{} `json:"data"`
	Retries            int                    `json:"retries"`
	RequestedReadyTime *time.Time             `json:"requestedReadyTime"`
}

type Buffer struct {
	db *bbolt.DB
}

func New(path string) (*Buffer, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open buffer database: %w", err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(eventsBucket))
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	return &Buffer{db: db}, nil
}

func (b *Buffer) Store(event *Event) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(eventsBucket))
		
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		key := fmt.Sprintf("%d_%s", event.Timestamp.UnixNano(), event.ID)
		return bucket.Put([]byte(key), data)
	})
}

func (b *Buffer) GetBatch(batchSize int) ([]*Event, error) {
	var events []*Event

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(eventsBucket))
		cursor := bucket.Cursor()

		count := 0
		for key, value := cursor.First(); key != nil && count < batchSize; key, value = cursor.Next() {
			var event Event
			if err := json.Unmarshal(value, &event); err != nil {
				continue
			}
			events = append(events, &event)
			count++
		}

		return nil
	})

	return events, err
}

func (b *Buffer) GetReadyEvents(batchSize int) ([]*Event, error) {
	var events []*Event
	now := time.Now()

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(eventsBucket))
		cursor := bucket.Cursor()

		count := 0
		for key, value := cursor.First(); key != nil && count < batchSize; key, value = cursor.Next() {
			var event Event
			if err := json.Unmarshal(value, &event); err != nil {
				continue
			}
			
			// Include events that are ready (null requestedReadyTime or requestedReadyTime <= now + 30 minutes)
			if event.RequestedReadyTime == nil {
				events = append(events, &event)
				count++
			} else {
				threshold := now.Add(30 * time.Minute)
				if event.RequestedReadyTime.Before(threshold) || event.RequestedReadyTime.Equal(threshold) {
					events = append(events, &event)
					count++
				}
			}
		}

		return nil
	})

	return events, err
}

func (b *Buffer) Delete(eventID string, timestamp time.Time) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(eventsBucket))
		key := fmt.Sprintf("%d_%s", timestamp.UnixNano(), eventID)
		return bucket.Delete([]byte(key))
	})
}

func (b *Buffer) UpdateRetries(eventID string, timestamp time.Time, retries int) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(eventsBucket))
		key := fmt.Sprintf("%d_%s", timestamp.UnixNano(), eventID)
		
		value := bucket.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("event not found")
		}

		var event Event
		if err := json.Unmarshal(value, &event); err != nil {
			return err
		}

		event.Retries = retries
		data, err := json.Marshal(event)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(key), data)
	})
}

func (b *Buffer) Count() (int, error) {
	var count int
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(eventsBucket))
		count = bucket.Stats().KeyN
		return nil
	})
	return count, err
}

func (b *Buffer) Close() error {
	return b.db.Close()
}