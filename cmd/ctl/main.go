package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/venusai24/task-scheduler/proto"
)

func main() {
	// Define Subcommands
	submitCmd := flag.NewFlagSet("submit", flag.ExitOnError)
	filePtr := submitCmd.String("f", "", "Path to YAML intent file")
	dryRun := submitCmd.Bool("dry-run", false, "Verify intent without execution")
	dependsOnPtr := submitCmd.String("depends-on", "", "ID of the parent task to wait for")

	getCmd := flag.NewFlagSet("get", flag.ExitOnError)
	idPtr := getCmd.String("id", "", "Task ID to fetch")

	approveCmd := flag.NewFlagSet("approve", flag.ExitOnError)
	appIDPtr := approveCmd.String("id", "", "Task ID to approve")

	rollbackCmd := flag.NewFlagSet("rollback", flag.ExitOnError)
	rollbackIDPtr := rollbackCmd.String("id", "", "Task ID to rollback")

	explainCmd := flag.NewFlagSet("explain", flag.ExitOnError)
	explainIDPtr := explainCmd.String("id", "", "Task ID to explain")

	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}

	// Build list of scheduler addresses
	addrsEnv := os.Getenv("ASTRA_SCHED_ADDRS")
	var addrs []string
	if addrsEnv != "" {
		for _, a := range strings.Split(addrsEnv, ",") {
			a = strings.TrimSpace(a)
			if a != "" {
				addrs = append(addrs, a)
			}
		}
	} else {
		// default: 3-node local cluster
		addrs = []string{"localhost:50051", "localhost:50052", "localhost:50053"}
	}

	caFile := os.Getenv("SCHED_CA_FILE")
	certFile := os.Getenv("SCHED_CERT_FILE") // CLI client cert
	keyFile := os.Getenv("SCHED_KEY_FILE")   // CLI client key
	if caFile == "" || certFile == "" || keyFile == "" {
		log.Fatal("SECURE ERROR: SCHED_CA_FILE, SCHED_CERT_FILE, and SCHED_KEY_FILE must be set for mTLS.")
	}

	// Small helper: run an RPC against the cluster, trying all nodes
	nextAddrIdx := 0

	withClient := func(fn func(pb.SchedServiceClient) error) error {
		// TLS setup (shared for all dials)
		clientCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("load client cert/key: %w", err)
		}
		ca, err := os.ReadFile(caFile)
		if err != nil {
			return fmt.Errorf("read CA: %w", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)

		creds := credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      pool,
			ServerName:   "localhost", // CN on server cert
		})

		if len(addrs) == 0 {
			return fmt.Errorf("no scheduler addresses configured")
		}

		maxAttempts := len(addrs) * 3
		var lastErr error

		for attempt := 0; attempt < maxAttempts; attempt++ {
			idx := (nextAddrIdx + attempt) % len(addrs)
			addr := addrs[idx]

			// Block until connection is established (or timeout)
			dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
			conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(creds), grpc.WithBlock())
			dialCancel()
			if err != nil {
				lastErr = fmt.Errorf("dial %s: %w", addr, err)
				continue
			}
			log.Printf("Connected to Scheduler %s with mTLS", addr)
			client := pb.NewSchedServiceClient(conn)

			err = fn(client)
			conn.Close()

			if err == nil {
				nextAddrIdx = (idx + 1) % len(addrs)
				return nil
			}

			st, _ := status.FromError(err)
			if strings.Contains(strings.ToLower(st.Message()), "not leader") {
				log.Printf("Node %s is not leader, trying next...", addr)
				lastErr = err
				continue
			}

			nextAddrIdx = (idx + 1) % len(addrs)
			return err
		}

		nextAddrIdx = (nextAddrIdx + 1) % len(addrs)
		if lastErr == nil {
			lastErr = fmt.Errorf("no scheduler addresses configured")
		}
		return lastErr
	}

	cmd := os.Args[1]

	switch cmd {
	case "submit":
		submitCmd.Parse(os.Args[2:])
		if *filePtr == "" {
			fmt.Println("Error: Please provide a file using -f <file>")
			return
		}
		err := withClient(func(c pb.SchedServiceClient) error {
			return handleSubmit(c, *filePtr, *dependsOnPtr, *dryRun)
		})
		if err != nil {
			log.Fatalf("Submission failed: %v", err)
		}
	case "get":
		getCmd.Parse(os.Args[2:])
		if *idPtr == "" {
			fmt.Println("Error: Please provide an ID using -id <task_id>")
			return
		}
		err := withClient(func(c pb.SchedServiceClient) error {
			return handleGet(c, *idPtr)
		})
		if err != nil {
			log.Fatalf("Failed to get task: %v", err)
		}
	case "approve":
		approveCmd.Parse(os.Args[2:])
		if *appIDPtr == "" {
			fmt.Println("Error: Please provide an ID using -id <task_id>")
			return
		}
		err := withClient(func(c pb.SchedServiceClient) error {
			return handleApprove(c, *appIDPtr)
		})
		if err != nil {
			log.Fatalf("Approval failed: %v", err)
		}
	case "rollback":
		rollbackCmd.Parse(os.Args[2:])
		if *rollbackIDPtr == "" {
			fmt.Println("Error: Please provide an ID using -id <task_id>")
			return
		}
		err := withClient(func(c pb.SchedServiceClient) error {
			return handleRollback(c, *rollbackIDPtr)
		})
		if err != nil {
			log.Fatalf("Rollback failed: %v", err)
		}
	case "explain":
		explainCmd.Parse(os.Args[2:])
		if *explainIDPtr == "" {
			fmt.Println("Error: Please provide an ID using -id <task_id>")
			return
		}
		err := withClient(func(c pb.SchedServiceClient) error {
			return handleExplain(c, *explainIDPtr)
		})
		if err != nil {
			log.Fatalf("Failed to fetch task details: %v", err)
		}
	default:
		printHelp()
		os.Exit(1)
	}
}

func handleSubmit(client pb.SchedServiceClient, path string, dependsOn string, dryRun bool) error {
	if path == "" {
		return fmt.Errorf("path to YAML intent file is required (-f)")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	yamlContent := string(data)

	// Replace existing depends_on line if it exists, or append if not found
	if dependsOn != "" {
		lines := strings.Split(yamlContent, "\n")
		var newLines []string
		found := false
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "depends_on:") {
				// Overwrite the line with the flag value
				newLines = append(newLines, fmt.Sprintf("depends_on: %s", dependsOn))
				found = true
			} else {
				newLines = append(newLines, line)
			}
		}
		if !found {
			// Append if not found
			newLines = append(newLines, fmt.Sprintf("depends_on: %s", dependsOn))
		}
		yamlContent = strings.Join(newLines, "\n")
	}

	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		return fmt.Errorf("ASTRA_AUTH_TOKEN must be set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "auth-token", token)

	fmt.Println("🚀 Submitting Intent to AstraSched Control Plane...")
	resp, err := client.SubmitIntent(ctx, &pb.SubmitRequest{
		YamlContent: yamlContent,
		DryRun:      dryRun,
	})
	if err == nil {
		fmt.Printf("✅ Success! Task ID: %s\n", resp.TaskId)
	}
	return err
}

func handleGet(client pb.SchedServiceClient, id string) error {
	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		return fmt.Errorf("ASTRA_AUTH_TOKEN is not set")
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "auth-token", token)
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	resp, err := client.GetTask(ctx, &pb.TaskRequest{TaskId: id})
	if err != nil {
		return err
	}

	t := resp.Task
	fmt.Println("\n AstraSched Task Report")
	fmt.Println("========================")
	fmt.Printf("ID:          %s\n", t.Id)
	fmt.Printf("State:       %s\n", t.State)
	fmt.Printf("Retry Count: %d\n", t.RetryCount)
	fmt.Printf("Gov Mode:    %s\n", t.Mode)
	if t.AiInsight != "" {
		fmt.Printf("AI Insight:  %s\n", t.AiInsight)
	}
	if len(t.Logs) > 0 {
		fmt.Println("\nExecution Logs:")
		for _, logEntry := range t.Logs {
			fmt.Printf("  • %s\n", logEntry)
		}
	}
	fmt.Println("========================")
	return nil
}

func handleApprove(client pb.SchedServiceClient, id string) error {
	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		return fmt.Errorf("ASTRA_AUTH_TOKEN is not set")
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "auth-token", token)
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	fmt.Printf(" Approving task %s...\n", id)
	resp, err := client.ApproveTask(ctx, &pb.ApproveRequest{TaskId: id})
	if err != nil {
		return err
	}

	if resp.Success {
		fmt.Printf(" Task %s Approved. Rescheduling...\n", id)
	} else {
		return fmt.Errorf("Approval failed: %s", resp.Message)
	}
	return nil
}

func handleRollback(client pb.SchedServiceClient, id string) error {
	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		return fmt.Errorf("ASTRA_AUTH_TOKEN is not set")
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "auth-token", token)
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	fmt.Printf(" Rolling back task %s to initial state...\n", id)
	resp, err := client.RollbackTask(ctx, &pb.RollbackRequest{TaskId: id})
	if err != nil {
		return err
	}

	if resp.Success {
		fmt.Printf(" Task %s rolled back successfully\n", id)
	} else {
		return fmt.Errorf("Rollback failed: %s", resp.Message)
	}
	return nil
}

func handleExplain(client pb.SchedServiceClient, id string) error {
	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		return fmt.Errorf("ASTRA_AUTH_TOKEN is not set")
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "auth-token", token)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := client.GetTask(ctx, &pb.TaskRequest{TaskId: id})
	if err != nil {
		return err
	}

	t := resp.Task
	fmt.Println("\n🔍 AstraSched Root Cause Analysis")
	fmt.Println("====================================")
	fmt.Printf("Task ID:    %s\n", t.Id)
	fmt.Printf("Final State: %s\n", t.State)

	if t.AiInsight != "" {
		fmt.Printf("\n🤖 AI Semantic Analysis:\n%s\n", t.AiInsight)
	} else {
		fmt.Println("\n🤖 AI Semantic Analysis: No AI verdict recorded for this task.")
	}

	if len(t.Logs) > 0 {
		fmt.Println("\n📈 Execution Journey (Audit Log):")
		for _, entry := range t.Logs {
			fmt.Printf("  • %s\n", entry)
		}
	}
	fmt.Println("====================================")
	return nil
}

func printHelp() {
	fmt.Println("AstraSched CLI (astractl)")
	fmt.Println("Usage:")
	fmt.Println("  astractl submit -f <file.yaml> [--depends-on <ID>] [--dry-run]  # Submit a new task")
	fmt.Println("  astractl get -id <task_id>                                     # Check task status")
	fmt.Println("  astractl approve -id <task_id>                                 # Approve a pending task")
	fmt.Println("  astractl rollback -id <task_id>                                # Rollback task to initial state")
	fmt.Println("  astractl explain -id <task_id>                                 # Show Root Cause Analysis report")
}