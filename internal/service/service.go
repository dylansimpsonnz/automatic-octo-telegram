package service

import (
	"context"
	"fmt"
	"log"
	"sync"

	"buffered-cdc/internal/buffer"
	"buffered-cdc/internal/config"
	"buffered-cdc/internal/monitor"
	"buffered-cdc/internal/scheduler"
	kafkasync "buffered-cdc/internal/sync"
)

type Service struct {
	config          *config.Config
	buffer          *buffer.Buffer
	mongoMonitor    *monitor.MongoMonitor
	connMonitor     *monitor.ConnectivityMonitor
	kafkaSync       *kafkasync.KafkaSync
	scheduler       *scheduler.Scheduler
	
	cancelFuncs     []context.CancelFunc
	wg              sync.WaitGroup
}

func New(cfg *config.Config) (*Service, error) {
	buf, err := buffer.New(cfg.Buffer.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffer: %w", err)
	}

	mongoMonitor, err := monitor.NewMongoMonitor(cfg, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to create mongo monitor: %w", err)
	}

	connMonitor := monitor.NewConnectivityMonitor(cfg)
	kafkaSync := kafkasync.NewKafkaSync(cfg, buf, connMonitor)
	sched := scheduler.New(buf)

	return &Service{
		config:       cfg,
		buffer:       buf,
		mongoMonitor: mongoMonitor,
		connMonitor:  connMonitor,
		kafkaSync:    kafkaSync,
		scheduler:    sched,
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	log.Println("Starting buffered CDC service")

	s.scheduler.Start()

	s.startComponent("connectivity monitor", func(ctx context.Context) {
		s.connMonitor.Start(ctx)
	})

	s.startComponent("kafka sync", func(ctx context.Context) {
		s.kafkaSync.Start(ctx)
	})

	s.startComponent("mongo monitor", func(ctx context.Context) {
		if err := s.mongoMonitor.Start(ctx); err != nil {
			log.Printf("MongoDB monitor error: %v", err)
		}
	})

	<-ctx.Done()
	log.Println("Shutdown signal received, stopping service...")

	return s.shutdown()
}

func (s *Service) startComponent(name string, fn func(context.Context)) {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelFuncs = append(s.cancelFuncs, cancel)
	
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		log.Printf("Starting %s", name)
		fn(ctx)
		log.Printf("Stopped %s", name)
	}()
}

func (s *Service) shutdown() error {
	log.Println("Initiating graceful shutdown...")

	for _, cancel := range s.cancelFuncs {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All components stopped gracefully")
	}

	s.scheduler.Stop()

	if err := s.kafkaSync.Close(); err != nil {
		log.Printf("Error closing Kafka sync: %v", err)
	}

	if err := s.mongoMonitor.Close(); err != nil {
		log.Printf("Error closing MongoDB monitor: %v", err)
	}

	if err := s.buffer.Close(); err != nil {
		log.Printf("Error closing buffer: %v", err)
	}

	log.Println("Service shutdown complete")
	return nil
}