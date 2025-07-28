package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"buffered-cdc/internal/config"
	"buffered-cdc/internal/service"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	svc, err := service.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutdown signal received")
		cancel()
	}()

	if err := svc.Start(ctx); err != nil {
		log.Fatalf("Service failed: %v", err)
	}

	log.Println("Service stopped gracefully")
}