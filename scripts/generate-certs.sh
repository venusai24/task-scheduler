#!/bin/bash

# Generate certificates for mTLS authentication
# Creates: CA cert, server cert/key, client cert/key

CERT_DIR="./certs"
mkdir -p $CERT_DIR

echo "🔐 Generating certificates for mTLS..."

# 1. Generate Root CA
openssl req -x509 -newkey rsa:4096 -days 365 -nodes \
  -keyout $CERT_DIR/ca.key \
  -out $CERT_DIR/ca.crt \
  -subj "/CN=AstraSched-CA/O=AstraSched"

echo "✅ CA certificate created"

# 2. Generate Server Certificate
openssl req -newkey rsa:4096 -nodes \
  -keyout $CERT_DIR/server.key \
  -out $CERT_DIR/server.csr \
  -subj "/CN=localhost/O=AstraSched-Server"

openssl x509 -req -in $CERT_DIR/server.csr \
  -CA $CERT_DIR/ca.crt -CAkey $CERT_DIR/ca.key \
  -CAcreateserial -out $CERT_DIR/server.crt \
  -days 365 \
  -extfile <(echo "subjectAltName=DNS:localhost,IP:127.0.0.1")

echo "✅ Server certificate created"

# 3. Generate Client Certificate
openssl req -newkey rsa:4096 -nodes \
  -keyout $CERT_DIR/client.key \
  -out $CERT_DIR/client.csr \
  -subj "/CN=astractl/O=AstraSched-Client"

openssl x509 -req -in $CERT_DIR/client.csr \
  -CA $CERT_DIR/ca.crt -CAkey $CERT_DIR/ca.key \
  -CAcreateserial -out $CERT_DIR/client.crt \
  -days 365

echo "✅ Client certificate created"

echo ""
echo "🎉 Certificate generation complete!"
echo ""
echo "Set these environment variables:"
echo "  export SCHED_CERT_FILE=$CERT_DIR/server.crt"
echo "  export SCHED_KEY_FILE=$CERT_DIR/server.key"
echo "  export SCHED_CA_FILE=$CERT_DIR/ca.crt"
echo "  export SCHED_AUTH_TOKEN=your-secret-token"
echo "  export NATS_TOKEN=your-nats-token"
