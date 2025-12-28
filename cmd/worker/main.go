package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

	// 2. Connect to Scheduler gRPC with MANDATORY mTLS
	certFile := os.Getenv("WORKER_CERT_FILE")
	keyFile := os.Getenv("WORKER_KEY_FILE")
	caFile := os.Getenv("SCHED_CA_FILE")

	if certFile == "" || keyFile == "" || caFile == "" {
		log.Fatal("SECURE ERROR: WORKER_CERT_FILE, WORKER_KEY_FILE, and SCHED_CA_FILE must be set.")
	}

	clientCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("Failed to load worker cert: %v", err)
	}

	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Failed to read CA: %v", err)
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

	// ✅ ADD: Try multiple scheduler nodes until we find the leader
	schedulerAddrs := []string{
		"localhost:50051",
		"localhost:50052", 
		"localhost:50053",
	}

	var conn *grpc.ClientConn
	var connErr error

	for _, addr := range schedulerAddrs {
		log.Printf("Attempting to connect to scheduler at %s...", addr)
		conn, connErr = grpc.Dial(addr, 
			grpc.WithTransportCredentials(creds),
			grpc.WithBlock(),
			grpc.WithTimeout(2*time.Second),
		)
		if connErr == nil {
			log.Printf("✅ Connected to scheduler at %s", addr)
			break
		}
		log.Printf("⚠️  Failed to connect to %s: %v", addr, connErr)
	}

	if connErr != nil {
		log.Fatal("Failed to connect to any scheduler node")
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
		token := os.Getenv("ASTRA_AUTH_TOKEN")
		if token == "" {
			token = "my-secret-key" // Matches Scheduler default
		}

		ctx := metadata.AppendToOutgoingContext(context.Background(), "auth-token", token)
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var resp *pb.TaskResponse
		var fetchErr error
		for attempt := 1; attempt <= 5; attempt++ {
			resp, fetchErr = client.GetTask(ctx, &pb.TaskRequest{TaskId: taskID})
			if fetchErr == nil {
				break
			}
			if strings.Contains(strings.ToLower(fetchErr.Error()), "task not found") {
				log.Printf("Task %s not found yet (attempt %d/5), retrying...", taskID, attempt)
				time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				continue
			}
			break
		}
		if fetchErr != nil {
			log.Printf("Failed to fetch task details: %v", fetchErr)
			m.Ack()
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

	if !executeHook(task.Id, "Pre-Run", task.PreRunScript, js) {
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

	processedScript := injectSecrets(script)

	// ACTUAL EXECUTION
	log.Printf("Executing secure script for task %s", task.Id)
	
	var out bytes.Buffer
	cmd := exec.Command("sh", "-c", processedScript)
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		log.Printf("!!! Execution FAILED for %s: %v", task.Id, err)
		
		// Create a JSON failure payload with error context
		failurePayload := map[string]string{
			"task_id": task.Id,
			"error":   out.String(),
		}
		data, _ := json.Marshal(failurePayload)
		_, pubErr := js.Publish("tasks.events.failed", data)
		if pubErr != nil {
			log.Printf("Failed to publish failure event: %v", pubErr)
		}
		return
	}

	if !executeHook(task.Id, "Post-Run", task.PostRunScript, js) {
		return
	}

	log.Printf("✅ Task %s SUCCEEDED!", task.Id)
	_, pubErr := js.Publish("tasks.events.completed", []byte(task.Id))
	if pubErr != nil {
		log.Printf("Failed to publish completion event: %v", pubErr)
	}
}

// extractScript parses the YAML and extracts the script field
func extractScript(yamlStr string) string {
	lines := strings.Split(yamlStr, "\n")
	foundScript := false
	var scriptLines []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "script:") {
			foundScript = true
			// Handle inline script: "script: echo hello"
			parts := strings.SplitN(trimmed, ":", 2)
			if len(parts) == 2 && strings.TrimSpace(parts[1]) != "|" && strings.TrimSpace(parts[1]) != "" {
				return strings.TrimSpace(parts[1])
			}
			continue
		}
		// If we found the script header and it's indented, it's part of the multi-line block
		if foundScript {
			if strings.HasPrefix(line, "    ") || strings.HasPrefix(line, "\t") || trimmed == "" {
				scriptLines = append(scriptLines, trimmed)
			} else if trimmed != "" {
				// We hit another field, stop collecting
				break
			}
		}
	}
	return strings.TrimSpace(strings.Join(scriptLines, "\n"))
}

func injectSecrets(script string) string {
	return os.Expand(script, func(key string) string {
		val := os.Getenv(key)
		if val == "" {
			log.Printf("⚠️  Warning: Secret %s not found in environment", key)
			return fmt.Sprintf("<MISSING_SECRET_%s>", key)
		}
		return val
	})
}

func executeHook(taskID, stage, script string, js nats.JetStreamContext) bool {
	if script == "" {
		return true
	}

	log.Printf("Executing %s Quality Gate for task %s", stage, taskID)
	processed := injectSecrets(script)
	cmd := exec.Command("sh", "-c", processed)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("❌ %s Quality Gate Failed for %s: %v", stage, taskID, err)
		if _, pubErr := js.Publish("tasks.events.failed", []byte(taskID)); pubErr != nil {
			log.Printf("Failed to publish failure event: %v", pubErr)
		}
		return false
	}

	return true
}