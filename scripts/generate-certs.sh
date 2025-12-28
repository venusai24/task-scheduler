#!/bin/bash
set -euo pipefail

# Optional: pass IP as first arg, otherwise auto-detect primary IP
IP="${1:-$(hostname -I | awk '{print $1}')}"
CERT_DIR="./certs"

mkdir -p "$CERT_DIR"

echo "🔐 Generating mTLS certificates for AstraSched..."
echo "Using IP for SAN: $IP"
echo

# 1. Root CA
echo "👉 Generating Root CA..."
openssl req -x509 -newkey rsa:4096 -days 365 -nodes \
  -keyout "$CERT_DIR/ca.key" \
  -out "$CERT_DIR/ca.crt" \
  -subj "/CN=AstraSched-Root-CA/O=AstraSched"

echo "✅ Root CA created at $CERT_DIR/ca.crt"
echo

# 2. Scheduler (Server) Certificate
echo "👉 Generating Scheduler (Server) certificate..."
openssl req -newkey rsa:4096 -nodes \
  -keyout "$CERT_DIR/server.key" \
  -out "$CERT_DIR/server.csr" \
  -subj "/CN=localhost/O=AstraSched-Server"

openssl x509 -req -in "$CERT_DIR/server.csr" \
  -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
  -CAcreateserial -out "$CERT_DIR/server.crt" \
  -days 365 \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:%s\n" "$IP")

echo "✅ Scheduler (Server) certificate created at $CERT_DIR/server.crt"
echo

# 3. Worker Certificate
echo "👉 Generating Worker certificate..."
openssl req -newkey rsa:4096 -nodes \
  -keyout "$CERT_DIR/worker.key" \
  -out "$CERT_DIR/worker.csr" \
  -subj "/CN=worker/O=AstraSched-Worker"

openssl x509 -req -in "$CERT_DIR/worker.csr" \
  -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
  -CAcreateserial -out "$CERT_DIR/worker.crt" \
  -days 365 \
  -extfile <(printf "subjectAltName=DNS:worker,IP:%s\n" "$IP")

echo "✅ Worker certificate created at $CERT_DIR/worker.crt"
echo

# 4. CLI Client Certificate
echo "👉 Generating CLI Client certificate..."
openssl req -newkey rsa:4096 -nodes \
  -keyout "$CERT_DIR/client.key" \
  -out "$CERT_DIR/client.csr" \
  -subj "/CN=astractl/O=AstraSched-Client"

openssl x509 -req -in "$CERT_DIR/client.csr" \
  -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
  -CAcreateserial -out "$CERT_DIR/client.crt" \
  -days 365 \
  -extfile <(printf "subjectAltName=DNS:astractl,IP:127.0.0.1,IP:%s\n" "$IP")

echo "✅ CLI Client certificate created at $CERT_DIR/client.crt"
echo

echo "🎉 All certificates generated in $CERT_DIR"
echo
echo "Run these in your shell before starting services:"
echo
echo "# --- Shared ---"
echo "export SCHED_CA_FILE=\"\$PWD/$CERT_DIR/ca.crt\""
echo "export ASTRA_AUTH_TOKEN=\"my-secret-key\""
echo
echo "# --- Scheduler (Control Plane) ---"
echo "export SCHED_CERT_FILE=\"\$PWD/$CERT_DIR/server.crt\""
echo "export SCHED_KEY_FILE=\"\$PWD/$CERT_DIR/server.key\""
echo
echo "# --- Worker (Execution Plane) ---"
echo "export WORKER_CERT_FILE=\"\$PWD/$CERT_DIR/worker.crt\""
echo "export WORKER_KEY_FILE=\"\$PWD/$CERT_DIR/worker.key\""
echo
echo "# --- CLI (astractl) ---"
echo "export SCHED_CERT_FILE=\"\$PWD/$CERT_DIR/client.crt\""
echo "export SCHED_KEY_FILE=\"\$PWD/$CERT_DIR/client.key\""
echo
echo "You can also pass IP explicitly: ./scripts/generate-certs.sh 192.168.68.110"