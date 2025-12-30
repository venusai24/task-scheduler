#!/bin/bash
# filepath: /home/venu/DevProjects/task-sched/scripts/manager.sh

BASE_PORT=50051
BASE_RAFT=6000
IP="localhost" # Change to your server IP if needed

# Ensure certificates exist
if [ ! -f "certs/server.crt" ]; then
    echo "❌ Certs missing! Run: ./scripts/generate-certs.sh"
    exit 1
fi

# Export required env vars for all nodes
export ASTRA_AUTH_TOKEN="${ASTRA_AUTH_TOKEN:-my-secret-key}"
export SCHED_CA_FILE="$PWD/certs/ca.crt"
export SCHED_CERT_FILE="$PWD/certs/server.crt"
export SCHED_KEY_FILE="$PWD/certs/server.key"
export WORKER_CERT_FILE="$PWD/certs/worker.crt"
export WORKER_KEY_FILE="$PWD/certs/worker.key"

# State file to track running nodes
STATE_FILE=".cluster_state"

start_node() {
    local ID=$1
    local PORT=$2
    local RAFT=$3
    local JOIN_ARG=$4
    
    echo "🚀 Starting $ID (Port: $PORT, Raft: $RAFT)..."
    
    # Create data dir
    mkdir -p "data/$ID"
    
    # Build command
    CMD="go run cmd/scheduler/main.go -id $ID -port :$PORT -raft $IP:$RAFT $JOIN_ARG"
    
    # Launch in background
    nohup $CMD > "data/$ID/node.log" 2>&1 &
    PID=$!
    
    echo "$ID,$PID,$PORT,$RAFT" >> $STATE_FILE
    echo "   -> PID: $PID. Log: data/$ID/node.log"
}

case "$1" in
    "start")
        if [ -z "$2" ]; then
            read -p "Enter number of nodes to start: " NUM_NODES
        else
            NUM_NODES=$2
        fi

        echo "🧹 Cleaning up old cluster..."
        pkill -f "cmd/scheduler/main.go" 2>/dev/null || true
        rm -rf data/node-* $STATE_FILE
        touch $STATE_FILE

        echo "🏗️  Bootstrapping $NUM_NODES nodes..."

        # Node 1 (Bootstrap Leader)
        start_node "node-1" $BASE_PORT $BASE_RAFT "-bootstrap=true"
        echo "⏳ Waiting 5s for leader election..."
        sleep 5

        # Followers
        for (( i=2; i<=NUM_NODES; i++ ))
        do
            PORT=$((BASE_PORT + i - 1))
            RAFT=$((BASE_RAFT + i - 1))
            start_node "node-$i" $PORT $RAFT "-join $IP:$BASE_PORT"
            sleep 2
        done
        
        echo "✅ Cluster is UP with $NUM_NODES nodes!"
        ;;

    "add")
        if [ ! -f $STATE_FILE ]; then echo "❌ Cluster not running!"; exit 1; fi
        
        # Calculate next index
        LAST_NODE=$(tail -n 1 $STATE_FILE | cut -d',' -f1)
        LAST_NUM=${LAST_NODE#node-}
        NEXT_NUM=$((LAST_NUM + 1))
        
        PORT=$((BASE_PORT + NEXT_NUM - 1))
        RAFT=$((BASE_RAFT + NEXT_NUM - 1))
        
        start_node "node-$NEXT_NUM" $PORT $RAFT "-join $IP:$BASE_PORT"
        echo "✅ Added node-$NEXT_NUM"
        ;;

    "delete")
        NODE_ID=$2
        if [ -z "$NODE_ID" ]; then echo "Usage: ./manager.sh delete node-X"; exit 1; fi
        
        echo "🔻 Decommissioning $NODE_ID..."
        
        # 1. Tell Leader to remove from Raft config
        go run cmd/ctl/main.go leave -node $NODE_ID
        
        # 2. Kill Process
        LINE=$(grep "^$NODE_ID," $STATE_FILE)
        if [ -n "$LINE" ]; then
            PID=$(echo $LINE | cut -d',' -f2)
            kill $PID 2>/dev/null || true
            echo "💀 Killed process $PID"
            
            # Remove from state file
            grep -v "^$NODE_ID," $STATE_FILE > "$STATE_FILE.tmp" && mv "$STATE_FILE.tmp" $STATE_FILE
        else
            echo "⚠️  Node ID not found in state file."
        fi
        
        echo "✅ Node $NODE_ID removed"
        ;;

    "list")
        if [ ! -f $STATE_FILE ]; then echo "No cluster state found"; exit 0; fi
        echo "📊 Running Nodes:"
        printf "%-10s %-10s %-10s %-10s\n" "ID" "PID" "PORT" "RAFT"
        echo "-------------------------------------------"
        while IFS=, read -r ID PID PORT RAFT; do
            if kill -0 $PID 2>/dev/null; then
                STATUS="✅"
            else
                STATUS="❌"
            fi
            printf "%s %-10s %-10s %-10s %-10s\n" "$STATUS" "$ID" "$PID" "$PORT" "$RAFT"
        done < $STATE_FILE
        ;;

    "stop")
        echo "🛑 Stopping all nodes..."
        pkill -f "cmd/scheduler/main.go" 2>/dev/null || true
        rm -f $STATE_FILE
        echo "✅ Cluster stopped"
        ;;

    *)
        echo "Usage: $0 {start [N] | add | delete <node-id> | list | stop}"
        exit 1
        ;;
esac