package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/venusai24/task-scheduler/proto"
)

func main() {
	// Define Subcommands
	submitCmd := flag.NewFlagSet("submit", flag.ExitOnError)
	filePtr := submitCmd.String("f", "", "Path to YAML intent file")
	dryRun := submitCmd.Bool("dry-run", false, "Verify intent without execution")

	getCmd := flag.NewFlagSet("get", flag.ExitOnError)
	idPtr := getCmd.String("id", "", "Task ID to fetch")

	approveCmd := flag.NewFlagSet("approve", flag.ExitOnError)
	appIDPtr := approveCmd.String("id", "", "Task ID to approve")

	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}

	// Connect to Scheduler with mTLS if configured
	var conn *grpc.ClientConn
	var err error // Add this line
	caFile := os.Getenv("SCHED_CA_FILE")

	if caFile != "" {
		creds, err := credentials.NewClientTLSFromFile(caFile, "localhost")
		if err != nil {
			log.Fatalf("Failed to load CA cert: %v", err)
		}
		conn, err = grpc.Dial("localhost:50051", grpc.WithTransportCredentials(creds))
		if err != nil {
			log.Fatalf("Did not connect to Scheduler: %v", err)
		}
		log.Println("Connected with mTLS")
	} else {
		conn, err = grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Did not connect to Scheduler: %v", err)
		}
		log.Println("⚠️  Connected without TLS (dev mode)")
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
		handleSubmit(client, *filePtr, *dryRun)
	case "get":
		getCmd.Parse(os.Args[2:])
		if *idPtr == "" {
			fmt.Println("Error: Please provide an ID using -id <task_id>")
			return
		}
		handleGet(client, *idPtr)
	case "approve":
		approveCmd.Parse(os.Args[2:])
		if *appIDPtr == "" {
			fmt.Println("Error: Please provide an ID using -id <task_id>")
			return
		}
		handleApprove(client, *appIDPtr)
	default:
		printHelp()
	}
}

func handleSubmit(client pb.SchedServiceClient, path string, dryRun bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	// Require auth token from environment
	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		log.Fatal("SECURE ERROR: ASTRA_AUTH_TOKEN is not set!")
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "auth-token", token)
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	if dryRun {
		fmt.Println("🔍 SIMULATION MODE: Task will be validated but not executed")
	}
	fmt.Println("🚀 Submitting Intent to AstraSched Control Plane...")

	resp, err := client.SubmitIntent(ctx, &pb.SubmitRequest{
		YamlContent: string(data),
		DryRun:      dryRun,
	})
	if err != nil {
		log.Fatalf("Submission failed: %v", err)
	}

	if dryRun {
		fmt.Printf("✅ Simulation Success! Task ID: %s (will not execute)\n", resp.TaskId)
	} else {
		fmt.Printf("✅ Success! Task ID: %s\n", resp.TaskId)
	}
}

func handleGet(client pb.SchedServiceClient, id string) {
	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		log.Fatal("SECURE ERROR: ASTRA_AUTH_TOKEN is not set!")
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "auth-token", token)
	ctx, cancel := context.WithTimeout(ctx, time.Second)
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

func handleApprove(client pb.SchedServiceClient, id string) {
	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		log.Fatal("SECURE ERROR: ASTRA_AUTH_TOKEN is not set!")
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "auth-token", token)
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	fmt.Printf("🔄 Approving task %s...\n", id)
	resp, err := client.ApproveTask(ctx, &pb.ApproveRequest{TaskId: id})
	if err != nil {
		log.Fatalf("Approval failed: %v", err)
	}

	if resp.Success {
		fmt.Printf("✅ Task %s Approved. Rescheduling...\n", id)
	} else {
		log.Fatalf("Approval failed: %s", resp.Message)
	}
}

func printHelp() {
	fmt.Println("AstraSched CLI (astractl)")
	fmt.Println("Usage:")
	fmt.Println("  astractl submit -f <file.yaml> [--dry-run]  # Submit a new task")
	fmt.Println("  astractl get -id <task_id>                  # Check task status")
	fmt.Println("  astractl approve -id <task_id>              # Approve a pending task")
}