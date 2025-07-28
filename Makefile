.PHONY: build test clean loadtest loadtest-build docker-build docker-up docker-down

# Build the main application
build:
	go build -o buffered-cdc .

# Build the load test tool
loadtest-build:
	cd cmd/loadtest && go build -o loadtest .

# Run tests
test:
	go test ./...

# Clean build artifacts
clean:
	rm -f buffered-cdc
	rm -f cmd/loadtest/loadtest
	rm -f buffer.db

# Load testing targets
loadtest: loadtest-build
	./scripts/loadtest.sh

loadtest-small: loadtest-build
	./scripts/loadtest.sh --workers 5 --messages 100 --delayed-percent 20

loadtest-medium: loadtest-build
	./scripts/loadtest.sh --workers 10 --messages 1000 --delayed-percent 30

loadtest-large: loadtest-build
	./scripts/loadtest.sh --workers 20 --messages 5000 --delayed-percent 40

loadtest-stress: loadtest-build
	./scripts/loadtest.sh --workers 50 --messages 10000 --delayed-percent 50

# Docker-based load testing (runs inside container network)
docker-loadtest:
	./scripts/docker-loadtest.sh

docker-loadtest-small:
	./scripts/docker-loadtest.sh --workers 5 --messages 100 --delayed-percent 20

docker-loadtest-medium:
	./scripts/docker-loadtest.sh --workers 10 --messages 1000 --delayed-percent 30

docker-loadtest-large:
	./scripts/docker-loadtest.sh --workers 20 --messages 5000 --delayed-percent 40

docker-loadtest-stress:
	./scripts/docker-loadtest.sh --workers 50 --messages 10000 --delayed-percent 50

# Docker targets
docker-build:
	docker build -t buffered-cdc .

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f buffered-cdc

# Development helpers
dev-setup: docker-up loadtest-build
	@echo "Development environment ready!"
	@echo "Services: http://localhost:8080 (Kafka UI), http://localhost:8081 (MongoDB Express)"

# Quick test with immediate monitoring  
quick-test: docker-loadtest-small
	@echo ""
	@echo "Quick test completed. Monitor results at:"
	@echo "- Kafka UI: http://localhost:8080"
	@echo "- MongoDB Express: http://localhost:8081"
	@echo "- Logs: make docker-logs"