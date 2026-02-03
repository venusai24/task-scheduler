#!/bin/bash
set -e

# Helper to generate self-signed certs if missing (Development convenience)
generate_certs() {
    if [ ! -f "$SCHED_CERT_FILE" ]; then
        echo "🔒 Generating self-signed development certificates..."
        mkdir -p $(dirname "$SCHED_CERT_FILE")
        
        # Simplified generation for dev (uses openssl if installed, or handled by external mount)
        # For now, we assume certs are mounted via Volume in production/compose.
        # Use existing script if available
        if [ -f "/app/scripts/gen_server_cert.sh" ]; then
            cd /app
            ./scripts/generate-certs.sh
        fi
    fi
}

# Distinguish role based on command
if [ "$1" = "scheduler" ]; then
    echo "🚀 Starting Scheduler..."
    sleep 10
    exec scheduler "$@"
elif [ "$1" = "worker" ]; then
    echo "👷 Starting Worker..."
    exec worker "$@"
elif [ "$1" = "agent" ]; then
    echo "🤖 Starting Agent..."
    # Agent typically runs in its own python container, but if binary wrapper existed:
    echo "Agent runs in a separate python image."
    exit 1
else
    # Default pass-through
    exec "$@"
fi
