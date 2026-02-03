# Build Stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git make

# Copy module files first (caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build binaries
RUN go build -o /bin/scheduler ./cmd/scheduler
RUN go build -o /bin/worker ./cmd/worker
RUN go build -o /bin/astractl ./cmd/ctl

# Runner Stage
FROM alpine:latest

WORKDIR /app

# Install runtime dependencies (TLS cert tools, etc if needed)
RUN apk add --no-cache ca-certificates bash curl

# Copy binaries
COPY --from=builder /bin/scheduler /usr/local/bin/scheduler
COPY --from=builder /bin/worker /usr/local/bin/worker
COPY --from=builder /bin/astractl /usr/local/bin/astractl

# Create data directory
RUN mkdir -p /app/data /app/certs

# Copy entrypoint script (we will create this next)
COPY deploy/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Environment defaults
ENV DATA_DIR=/app/data

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
