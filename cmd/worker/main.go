package main

import (
	"context"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	pb "github.com/venusai24/task-scheduler/proto"
)

func main() {
	// 1. Connect to NATS with Token Auth
	natsToken := os.Getenv("NATS_TOKEN")
	var nc *nats.Conn
	var err error

	if natsToken != "" {
		nc, err = nats.Connect("nats://localhost:4222", nats.Token(natsToken))
		log.Println("Connected to NATS with token authentication")
	} else {
		nc, err = nats.Connect("nats://localhost:4222")
		log.Println("⚠️  Connected to NATS without authentication (dev mode)")
	}

	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Worker connected to NATS JetStream.")

	// 2. Connect to Scheduler gRPC with TLS support
	var conn *grpc.ClientConn
	caFile := os.Getenv("SCHED_CA_FILE")

	if caFile != "" {
		creds, err := credentials.NewClientTLSFromFile(caFile, "localhost")
		if err != nil {
			log.Fatalf("Worker failed to load CA cert: %v", err)
		}
		conn, err = grpc.Dial("localhost:50051", grpc.WithTransportCredentials(creds))
		log.Println("Worker connected with TLS.")
	} else {
		conn, err = grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
		log.Println("⚠️  Worker connected without TLS (dev mode)")
	}
	if err != nil {
		log.Fatalf("Failed to connect to Scheduler: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedServiceClient(conn)
	log.Println("Worker connected to Scheduler gRPC.")

	// 3. Create the Stream
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TASKS",
		Subjects: []string{"tasks.>"},
	})
	if err != nil {
		log.Printf("Stream might already exist: %v", err)
	}

	// 4. Subscribe
	sub, err := js.QueueSubscribe("tasks.scheduled", "worker-group", func(m *nats.Msg) {
		taskID := string(m.Data)
		log.Printf("Worker received task ID: %s", taskID)

		// FETCH: Use gRPC to get full task details with Auth Token
		authToken := os.Getenv("SCHED_AUTH_TOKEN")
		if authToken == "" {
			authToken = "my-secret-key" // Matches Scheduler default
		}

		ctx := metadata.AppendToOutgoingContext(context.Background(), "auth-token", authToken)
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		resp, err := client.GetTask(ctx, &pb.TaskRequest{TaskId: taskID})
		if err != nil {
			log.Printf("Failed to fetch task details: %v", err)
			return
		}

		// EXECUTE with full task object
		executeTask(resp.Task, js)
		m.Ack()

	}, nats.Durable("worker-monitor"), nats.ManualAck())

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Worker listening for tasks... (Ctrl+C to quit)")

	// Keep running until interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	sub.Unsubscribe()
	log.Println("Worker shutting down.")
}

func executeTask(task *pb.Task, js nats.JetStreamContext) {
	// Check simulation mode
	if task.IsSimulation {
		log.Printf("🔍 SIMULATION MODE: Skipping execution for task %s", task.Id)
		log.Printf("   Intent validated. No actual work performed.")
		_, err := js.Publish("tasks.events.completed", []byte(task.Id))
		if err != nil {
			log.Printf("Failed to publish completion: %v", err)
		}
		return
	}

	log.Printf(">>> STARTING Execution for %s", task.Id)

	// Quality Gate: Check for pre-run conditions
	if strings.Contains(task.IntentYaml, "check_file:") {
		// Extract filename (simplified parsing)
		if strings.Contains(task.IntentYaml, "check_file: required_file.txt") {
			if _, err := os.Stat("required_file.txt"); os.IsNotExist(err) {
				log.Printf("❌ Quality Gate Failed: required_file.txt missing for task %s", task.Id)
				_, err := js.Publish("tasks.events.failed", []byte(task.Id))
				if err != nil {
					log.Printf("Failed to publish failure event: %v", err)
				}
				return
			}
			log.Printf("✅ Quality Gate Passed: required_file.txt exists")
		}
	}

	// Extract and execute script from YAML
	script := extractScript(task.IntentYaml)
	if script == "" {
		log.Printf("⚠️  No script found in intent for task %s", task.Id)
		_, err := js.Publish("tasks.events.failed", []byte(task.Id))
		if err != nil {
			log.Printf("Failed to publish failure event: %v", err)
		}
		return
	}

	// ACTUAL EXECUTION
	log.Printf("Executing script for task %s", task.Id)
	cmd := exec.Command("sh", "-c", script)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("!!! Execution FAILED for %s: %v", task.Id, err)
		_, pubErr := js.Publish("tasks.events.failed", []byte(task.Id))
		if pubErr != nil {
			log.Printf("Failed to publish failure event: %v", pubErr)
		}
	} else {
		log.Printf("✅ Task %s SUCCEEDED!", task.Id)
		_, pubErr := js.Publish("tasks.events.completed", []byte(task.Id))
		if pubErr != nil {
			log.Printf("Failed to publish completion event: %v", pubErr)
		}
	}
}

// extractScript parses the YAML and extracts the script field
func extractScript(yaml string) string {
	// Simplified parsing - in production use a proper YAML parser
	lines := strings.Split(yaml, "\n")
	for i, line := range lines {
		if strings.Contains(line, "script:") {
			// Get the script value (handle both inline and multiline)
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				script := strings.TrimSpace(parts[1])
				if script != "" && script != "|" {
					return script
				}
				// Handle multiline script
				if i+1 < len(lines) {
					return strings.TrimSpace(lines[i+1])
				}
			}
		}
	}
	return ""
}