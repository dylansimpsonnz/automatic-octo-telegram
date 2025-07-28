package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	"buffered-cdc/internal/buffer"

	"github.com/robfig/cron/v3"
)

type Task func(ctx context.Context) error

type Scheduler struct {
	cron   *cron.Cron
	buffer *buffer.Buffer
	tasks  map[string]Task
}

func New(buf *buffer.Buffer) *Scheduler {
	c := cron.New(cron.WithSeconds())

	return &Scheduler{
		cron:   c,
		buffer: buf,
		tasks:  make(map[string]Task),
	}
}

func (s *Scheduler) Start() {
	log.Println("Starting task scheduler")
	s.registerDefaultTasks()
	s.cron.Start()
}

func (s *Scheduler) Stop() {
	log.Println("Stopping task scheduler")
	s.cron.Stop()
}

func (s *Scheduler) AddTask(name, cronSpec string, task Task) error {
	_, err := s.cron.AddFunc(cronSpec, func() {
		ctx := context.Background()
		if err := task(ctx); err != nil {
			log.Printf("Task %s failed: %v", name, err)
		}
	})

	if err != nil {
		return fmt.Errorf("failed to add task %s: %w", name, err)
	}

	s.tasks[name] = task
	log.Printf("Added scheduled task: %s with spec: %s", name, cronSpec)
	return nil
}

func (s *Scheduler) registerDefaultTasks() {
	s.AddTask("buffer_stats", "0 */5 * * * *", s.bufferStatsTask)

	s.AddTask("cleanup_old_events", "0 0 2 * * *", s.cleanupTask)

	s.AddTask("health_check", "0 */1 * * * *", s.healthCheckTask)

	s.AddTask("process_scheduled_events", "* * * * * *", s.processScheduledEventsTask)
}

func (s *Scheduler) bufferStatsTask(ctx context.Context) error {
	count, err := s.buffer.Count()
	if err != nil {
		return fmt.Errorf("failed to get buffer count: %w", err)
	}

	log.Printf("Buffer statistics - Events in queue: %d", count)
	return nil
}

func (s *Scheduler) cleanupTask(ctx context.Context) error {
	log.Println("Running cleanup task - checking for old failed events")

	events, err := s.buffer.GetBatch(1000)
	if err != nil {
		return fmt.Errorf("failed to get events for cleanup: %w", err)
	}

	cutoff := time.Now().Add(-24 * time.Hour)
	cleanedCount := 0

	for _, event := range events {
		if event.Retries > 10 && event.Timestamp.Before(cutoff) {
			if err := s.buffer.Delete(event.ID, event.Timestamp); err != nil {
				log.Printf("Failed to delete old event %s: %v", event.ID, err)
				continue
			}
			cleanedCount++
		}
	}

	if cleanedCount > 0 {
		log.Printf("Cleaned up %d old failed events", cleanedCount)
	}

	return nil
}

func (s *Scheduler) healthCheckTask(ctx context.Context) error {
	count, err := s.buffer.Count()
	if err != nil {
		return fmt.Errorf("health check failed - buffer error: %w", err)
	}

	if count > 10000 {
		log.Printf("WARNING: Buffer contains %d events - consider investigating connectivity issues", count)
	}

	return nil
}

func (s *Scheduler) processScheduledEventsTask(ctx context.Context) error {
	events, err := s.buffer.GetBatch(1000)
	if err != nil {
		return fmt.Errorf("failed to get events for scheduled processing: %w", err)
	}

	now := time.Now()
	threshold := now.Add(30 * time.Minute)
	processedCount := 0

	for _, event := range events {
		// Check if this event has a requestedReadyTime and if it's ready
		if event.RequestedReadyTime != nil {
			// Event is ready if requestedReadyTime <= current time + 30 minutes
			if event.RequestedReadyTime.After(threshold) {
				// Event is not ready yet, skip
				continue
			}

			// Event is ready to be processed - the sync service will pick it up via GetReadyEvents
			log.Printf("Scheduled event %s is now ready for processing (readyTime: %v, threshold: %v)", 
				event.ID, event.RequestedReadyTime, threshold)
			processedCount++
		}
	}

	if processedCount > 0 {
		log.Printf("Processed %d scheduled events that are now ready", processedCount)
	}

	return nil
}

func (s *Scheduler) GetTaskNames() []string {
	var names []string
	for name := range s.tasks {
		names = append(names, name)
	}
	return names
}
