package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"time"

	pb "github.com/venusai24/task-scheduler/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

func main() {
	certFile := os.Getenv("CLIENT_CERT_FILE")
	keyFile := os.Getenv("CLIENT_KEY_FILE")
	caFile := os.Getenv("CLIENT_CA_FILE")

	if certFile == "" {
		// Fallback for dev
		certFile = "certs/client.crt"
		keyFile = "certs/client.key"
		caFile = "certs/ca.crt"
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("Load key pair: %v", err)
	}

	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Read CA: %v", err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caBytes)

	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      certPool,
		ServerName:   "localhost",
	})

	// Connect to Scheduler
	schedAddr := os.Getenv("SCHED_ADDR")
	if schedAddr == "" {
		schedAddr = "localhost:50051"
	}

	conn, err := grpc.Dial(schedAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewSchedServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Auth Token
	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		token = "my-secret-key"
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "auth-token", token)

	log.Println("Submitting intent...")
	r, err := c.SubmitIntent(ctx, &pb.SubmitRequest{
		YamlContent: "kind: Intent\nname: test-job-001\nspec:\n  script: echo 'Hello World'",
	})
	if err != nil {
		log.Fatalf("could not submit: %v", err)
	}
	log.Printf("Success! Task ID: %s", r.TaskId)
}
