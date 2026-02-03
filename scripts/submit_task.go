package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	pb "github.com/venusai24/task-scheduler/proto"
)

func main() {
	// 1. Get Cert Paths from Env (set by test script)
	certFile := os.Getenv("CLIENT_CERT_FILE")
	keyFile := os.Getenv("CLIENT_KEY_FILE")
	caFile := os.Getenv("CLIENT_CA_FILE")

	if certFile == "" {
		certFile = "certs/client.crt"
		keyFile = "certs/client.key"
		caFile = "certs/ca.crt"
	}

	// 2. Load mTLS Config
	clientCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("Failed to load client certs (%s, %s): %v", certFile, keyFile, err)
	}

	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Failed to read CA (%s): %v", caFile, err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caBytes) {
		log.Fatal("Failed to append CA cert")
	}

	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      certPool,
		ServerName:   "localhost",
	})

	// 3. Connect to Scheduler
	conn, err := grpc.Dial("localhost:50055", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("Failed to connect to scheduler: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Add Auth Token
	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		token = "my-secret-key"
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "auth-token", token)

	// 4. Submit Intent
	// Simple YAML payload. The logic downstream handles parsing.
	jobYaml := `
name: verify-task
command: echo "Hello Targeted Worker"
image: alpine
resources:
  cpu: 0.1
  memory: 64MB
`
	req := &pb.SubmitRequest{
		YamlContent: jobYaml,
		DryRun:      false,
	}

	resp, err := client.SubmitIntent(ctx, req)
	if err != nil {
		log.Fatalf("SubmitIntent failed: %v", err)
	}

	fmt.Printf("Success! Task ID: %s\n", resp.TaskId)
}
