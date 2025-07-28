# Build stage
FROM golang:1.23-alpine AS builder

# Install git and ca-certificates (needed for go modules)
RUN apk add --no-cache git ca-certificates

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o buffered-cdc .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/buffered-cdc .

# Create directory for buffer database
RUN mkdir -p /data

# Expose no ports (this is a background service)

# Set environment variables with defaults
ENV MONGODB_URI=mongodb://mongo:27017
ENV MONGODB_DATABASE=testdb
ENV MONGODB_COLLECTION=events
ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_TOPIC=cdc-events
ENV BUFFER_PATH=/data/buffer.db

# Run the application
CMD ["./buffered-cdc"]