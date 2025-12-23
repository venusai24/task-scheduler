package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/venusai24/task-scheduler/proto"
)

func main() {
	// Connect to the Scheduler via gRPC
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewSchedServiceClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	log.Println("Submitting intent...")
	r, err := c.SubmitIntent(ctx, &pb.SubmitRequest{
		YamlContent: "kind: Intent\nname: test-job-001\nspec:\n  image: alpine:latest",
	})
	if err != nil {
		log.Fatalf("could not submit: %v", err)
	}
	log.Printf("Success! Task ID: %s", r.TaskId)
}