#!/bin/bash
set -e

# Build Binaries first for reliable process management
echo "🔨 Building binaries..."
go build -o bin/scheduler cmd/scheduler/main.go
go build -o bin/worker cmd/worker/main.go

# Cleanup function
cleanup() {
    echo "🧹 Clean up..."
    pkill -x "scheduler" || true
    pkill -x "worker" || true
    docker stop astra-nats || true
    docker rm -f astra-nats || true
    rm -rf data
}
trap cleanup EXIT

# Kill previous jobs aggressively
pkill -x "scheduler" || true
pkill -x "worker" || true
pkill -f "go run" || true
docker rm -f astra-nats || true
rm -rf data

# Start NATS
echo "Starting Fresh NATS..."
docker run -d --name astra-nats -p 4222:4222 -p 8222:8222 nats:latest -js > /dev/null
sleep 3 # Wait for NATS

# Export common env vars
export NATS_URL=nats://localhost:4222
export ASTRA_AUTH_TOKEN=my-secret-key
# Dummy cert paths if needed (scheduler might check existence even if ignored in insecure?)
# Scheduler checks: if certFile == "" log.Fatal.
# We must provide cert files OR run in potential insecure mode if supported?
# The code I saw earlier:
# if certFile == "" || keyFile == "" || caFile == "" { log.Fatal(...) }
# So I MUST provide cert files.
# The original script pointed to certs/server.crt etc.
export SCHED_CERT_FILE=certs/server.crt
export SCHED_KEY_FILE=certs/server.key
export SCHED_CA_FILE=certs/ca.crt
export WORKER_CERT_FILE=certs/worker.crt
export WORKER_KEY_FILE=certs/worker.key
export CLIENT_CERT_FILE=certs/client.crt
export CLIENT_KEY_FILE=certs/client.key
export CLIENT_CA_FILE=certs/ca.crt

# Scheduler Args
# The scheduler code requires certs. Ensure they exist.
if [ ! -f certs/server.crt ]; then
    echo "⚠️  Certs missing. Generating..."
    bash scripts/generate-certs.sh > /dev/null 2>&1 || true
fi

# Start Scheduler
echo "Starting Scheduler..."
./bin/scheduler -id node-1 -port :50055 -raft localhost:6005 -bootstrap true > scheduler.log 2>&1 &
SCHED_PID=$!
echo "Scheduler PID: $SCHED_PID"
sleep 5

# Start Worker
echo "Starting Worker 1 (worker-1)..."
# Worker flags: -id <string> [-oneshot]
# No -server flag. It connects to NATS.
./bin/worker -id worker-verify-1 > worker1.log 2>&1 &
WORKER_PID=$!
echo "Worker PID: $WORKER_PID"
sleep 10 # Wait for heartbeats

# Check Heartbeats
echo "Checking Scheduler Logs for Heartbeats..."
if grep -q "heartbeats" scheduler.log; then
    echo "✅ Heartbeat stream created."
else
    echo "⚠️  No heartbeat stream log found."
fi

# Submit Task
echo "Submitting Task..."
go run scripts/submit_task.go > submit.log 2>&1 &
SUBMIT_PID=$!
wait $SUBMIT_PID
if [ $? -ne 0 ]; then
    echo "❌ Submit failed. Log:"
    cat submit.log
    exit 1
fi
TASK_ID=$(grep "Success! Task ID:" submit.log | awk '{print $4}')
echo "Task ID: $TASK_ID"

sleep 5

# Check logs
echo "Checking Scheduler Logs for Scoring..."
if grep -q "Scoring Algorithm selected" scheduler.log; then
    echo "✅ Task $TASK_ID assigned to a node!"
    grep "Scoring Algorithm selected" scheduler.log
elif grep -q "No suitable nodes found" scheduler.log; then
    echo "⚠️  No suitable nodes found. Registry might be empty."
    echo "Registry content hint:"
    grep -i "registry" scheduler.log || echo "No registry logs"
else
     echo "❌ Scoring log not found"
fi

echo "Checking Worker 1 Logs..."
if grep -q "Received TARGETED task" worker1.log; then
    echo "✅ Worker received targeted task!"
elif grep -q "Worker received task ID" worker1.log; then
    echo "⚠️  Worker received task via BROADCAST (fallback)."
else
    echo "❌ Worker did not receive task"
fi

# Assertions
if grep -q "Scoring Algorithm selected" scheduler.log; then
    echo "🎉 VERIFICATION PASSED: Node Registry is working!"
    exit 0
else
    echo "❌ VERIFICATION FAILED: Node Registry issue."
    exit 1
fi
