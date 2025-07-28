package monitor

import (
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"buffered-cdc/internal/config"
)

type ConnectivityStatus int

const (
	StatusOffline ConnectivityStatus = iota
	StatusOnline
)

type ConnectivityMonitor struct {
	config   *config.MonitorConfig
	kafka    *config.KafkaConfig
	status   ConnectivityStatus
	mu       sync.RWMutex
	watchers []chan ConnectivityStatus
}

func NewConnectivityMonitor(cfg *config.Config) *ConnectivityMonitor {
	return &ConnectivityMonitor{
		config: &cfg.Monitor,
		kafka:  &cfg.Kafka,
		status: StatusOffline,
	}
}

func (cm *ConnectivityMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(cm.config.Interval)
	defer ticker.Stop()

	cm.checkConnectivity()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cm.checkConnectivity()
		}
	}
}

func (cm *ConnectivityMonitor) checkConnectivity() {
	isOnline := cm.checkKafkaConnectivity()
	
	cm.mu.Lock()
	oldStatus := cm.status
	if isOnline {
		cm.status = StatusOnline
	} else {
		cm.status = StatusOffline
	}
	
	if oldStatus != cm.status {
		log.Printf("Connectivity status changed: %s", cm.statusString())
		cm.notifyWatchers()
	}
	cm.mu.Unlock()
}

func (cm *ConnectivityMonitor) checkKafkaConnectivity() bool {
	for _, broker := range cm.kafka.Brokers {
		host := strings.Split(broker, ":")[0]
		port := "9092"
		if parts := strings.Split(broker, ":"); len(parts) > 1 {
			port = parts[1]
		}

		conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), cm.config.ConnectTimeout)
		if err != nil {
			continue
		}
		conn.Close()
		return true
	}
	return false
}

func (cm *ConnectivityMonitor) IsOnline() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.status == StatusOnline
}

func (cm *ConnectivityMonitor) Subscribe() <-chan ConnectivityStatus {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	ch := make(chan ConnectivityStatus, 1)
	ch <- cm.status
	cm.watchers = append(cm.watchers, ch)
	return ch
}

func (cm *ConnectivityMonitor) notifyWatchers() {
	for _, watcher := range cm.watchers {
		select {
		case watcher <- cm.status:
		default:
		}
	}
}

func (cm *ConnectivityMonitor) statusString() string {
	if cm.status == StatusOnline {
		return "ONLINE"
	}
	return "OFFLINE"
}

func (cm *ConnectivityMonitor) WaitForOnline(ctx context.Context) error {
	if cm.IsOnline() {
		return nil
	}

	statusCh := cm.Subscribe()
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case status := <-statusCh:
			if status == StatusOnline {
				return nil
			}
		}
	}
}