#!/bin/bash

# --- 1. CLEANUP: Delete old state and kill stale processes ---
echo "🧹 Wiping old Raft data and processes..."
rm -rf ./data/node-1 ./data/node-2 ./data/node-3
pkill -f "go run cmd/scheduler/main.go" || true
sleep 1

# --- 2. ENVIRONMENT: Correct certificate mapping ---
IP="192.168.68.110"
export ASTRA_AUTH_TOKEN="my-secret-key"
export SCHED_CA_FILE="$PWD/certs/ca.crt"

# Schedulers must use SERVER certs, not client certs
export SCHED_CERT_FILE="$PWD/certs/server.crt"
export SCHED_KEY_FILE="$PWD/certs/server.key"

# Worker exports
export WORKER_CERT_FILE="$PWD/certs/worker.crt"
export WORKER_KEY_FILE="$PWD/certs/worker.key"

# Tell astractl about all scheduler nodes
export ASTRA_SCHED_ADDRS="$IP:50051,$IP:50052,$IP:50053"

echo "🚀 Launching 3-Node Raft Cluster on $IP..."

# Node 1: The Bootstrap Leader
gnome-terminal --title="NODE 1 (LEADER)" -- bash -c "go run cmd/scheduler/main.go --id node-1 --raft $IP:6000 --bootstrap=true; exec bash"

sleep 7 # Give node-1 ample time to bootstrap and elect itself leader

# Node 2: Follower
gnome-terminal --title="NODE 2" -- bash -c "go run cmd/scheduler/main.go --id node-2 --port :50052 --raft $IP:6001 --join $IP:50051; exec bash"

# Node 3: Follower
gnome-terminal --title="NODE 3" -- bash -c "go run cmd/scheduler/main.go --id node-3 --port :50053 --raft $IP:6002 --join $IP:50051; exec bash"

echo "✅ Cluster initialization sequence complete."