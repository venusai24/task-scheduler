package main

import (
	"context"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/venusai24/task-scheduler/proto"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Please provide a Task ID (e.g., go run cmd/test_status/main.go <ID>)")
	}
	taskID := os.Args[1]

	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewSchedServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	log.Printf("Checking status for %s...", taskID)
	r, err := c.GetTask(ctx, &pb.TaskRequest{TaskId: taskID})
	if err != nil {
		log.Fatalf("could not get task: %v", err)
	}
	
	log.Printf("Task State: %s", r.Task.State)
}