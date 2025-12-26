package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/venusai24/task-scheduler/proto"
)

func main() {
	// Define Subcommands
	submitCmd := flag.NewFlagSet("submit", flag.ExitOnError)
	filePtr := submitCmd.String("f", "", "Path to YAML intent file")

	getCmd := flag.NewFlagSet("get", flag.ExitOnError)
	idPtr := getCmd.String("id", "", "Task ID to fetch")

	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}

	// Connect to Scheduler (Control Plane)
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect to Scheduler: %v", err)
	}
	defer conn.Close()
	client := pb.NewSchedServiceClient(conn)

	// Switch on Subcommand
	switch os.Args[1] {
	case "submit":
		submitCmd.Parse(os.Args[2:])
		if *filePtr == "" {
			fmt.Println("Error: Please provide a file using -f <file>")
			return
		}
		handleSubmit(client, *filePtr)
	case "get":
		getCmd.Parse(os.Args[2:])
		if *idPtr == "" {
			fmt.Println("Error: Please provide an ID using -id <task_id>")
			return
		}
		handleGet(client, *idPtr)
	default:
		printHelp()
	}
}

func handleSubmit(client pb.SchedServiceClient, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	fmt.Println("🚀 Submitting Intent to AstraSched Control Plane...")
	resp, err := client.SubmitIntent(ctx, &pb.SubmitRequest{
		YamlContent: string(data),
	})
	if err != nil {
		log.Fatalf("Submission failed: %v", err)
	}
	fmt.Printf("✅ Success! Task ID: %s\n", resp.TaskId)
}

func handleGet(client pb.SchedServiceClient, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.GetTask(ctx, &pb.TaskRequest{TaskId: id})
	if err != nil {
		log.Fatalf("Failed to get task: %v", err)
	}

	t := resp.Task
	fmt.Println("\n📋 AstraSched Task Report")
	fmt.Println("========================")
	fmt.Printf("ID:          %s\n", t.Id)
	fmt.Printf("State:       %s\n", t.State)
	fmt.Printf("Retry Count: %d\n", t.RetryCount)
	fmt.Printf("Gov Mode:    %s\n", t.Mode)
	fmt.Println("========================")
}

func printHelp() {
	fmt.Println("AstraSched CLI (astractl)")
	fmt.Println("Usage:")
	fmt.Println("  astractl submit -f <file.yaml>   # Submit a new task")
	fmt.Println("  astractl get -id <task_id>       # Check task status")
}