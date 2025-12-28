#!/bin/bash

IP="192.168.68.110"

openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout server.key -out server.crt \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:$IP"

echo "✅ Certificate generated: server.crt (SAN: 127.0.0.1, $IP)"