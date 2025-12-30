# --- Stage 1: Builder (Compiles Go Binaries) ---
FROM golang:1.22 AS builder

WORKDIR /app

# 1. Cache Go Dependencies
COPY go.mod go.sum ./
RUN go mod download

# 2. Copy Source Code
COPY . .

# 3. Build Static Binaries (Scheduler, Worker, CLI)
# CGO_ENABLED=0 ensures they run on Alpine/Slim Linux without dependency issues
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/scheduler cmd/scheduler/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/worker cmd/worker/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/astractl cmd/ctl/main.go

# --- Stage 2: Runtime Image (Final Product) ---
FROM python:3.11-slim

WORKDIR /app

# 1. Install System Dependencies
RUN apt-get update && apt-get install -y curl openssl ca-certificates && rm -rf /var/lib/apt/lists/*

# 2. Setup Python AI Agent
# Copy requirements first to cache pip install
COPY reasoning/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY reasoning/ ./reasoning/

# 3. Copy Compiled Binaries from Builder
COPY --from=builder /bin/scheduler /usr/local/bin/
COPY --from=builder /bin/worker /usr/local/bin/
COPY --from=builder /bin/astractl /usr/local/bin/

# 4. Setup Directories & Permissions
RUN mkdir -p /app/data /app/certs
# Copy your certificate generation script just in case
COPY scripts/generate-certs.sh ./scripts/
RUN chmod +x ./scripts/generate-certs.sh

# 5. Default Environment Variables
ENV PATH="/usr/local/bin:${PATH}"

# Default command (can be overridden)
CMD ["scheduler"]