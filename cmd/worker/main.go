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

// Global variables to manage connection state
var (
	conn   *grpc.ClientConn
	client pb.SchedServiceClient
	creds  credentials.TransportCredentials
)

// List of all potential scheduler nodes
var schedulerAddrs = []string{
	"localhost:50051",
	"localhost:50052",
	"localhost:50053",
}

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
		log.Fatal("NATS Connection failed: ", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal("JetStream init failed: ", err)
	}
	log.Println("✅ Worker connected to NATS JetStream.")

	// 2. Load mTLS Credentials ONE TIME
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

	creds = credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      certPool,
		ServerName:   "localhost",
	})

	// 3. Initial Connection to Scheduler
	connectToScheduler()
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	// 4. Create the Stream
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TASKS",
		Subjects: []string{"tasks.>"},
	})
	if err != nil {
		log.Printf("Stream might already exist: %v", err)
	}

	// 5. Subscribe to Tasks
	sub, err := js.QueueSubscribe("tasks.scheduled", "worker-group", func(m *nats.Msg) {
		taskID := string(m.Data)
		log.Printf("📥 Worker received task ID: %s", taskID)

		// RETRY LOOP: Resilience against Leader Failover
		var task *pb.Task
		var fetchErr error

		for attempt := 1; attempt <= 10; attempt++ {
			task, fetchErr = fetchTask(taskID)
			if fetchErr == nil {
				break
			}

			log.Printf("⚠️  Fetch attempt %d/10 failed: %v", attempt, fetchErr)

			// If connection refused/closed, force reconnection
			if strings.Contains(fetchErr.Error(), "connection refused") ||
				strings.Contains(fetchErr.Error(), "transport is closing") ||
				strings.Contains(fetchErr.Error(), "unavailable") {
				log.Println("🔄 Detected dead connection. Hunting for new Leader...")
				connectToScheduler()
			}

			time.Sleep(1 * time.Second)
		}

		if fetchErr != nil {
			log.Printf("❌ ABORT: Could not fetch task %s after retries", taskID)
			m.Ack() // Ack to clear it from NATS
			return
		}

		executeTask(task, js)
		m.Ack()

	}, nats.Durable("worker-monitor"), nats.ManualAck())

	if err != nil {
		log.Fatal(err)
	}

	log.Println("🚀 Worker running. Waiting for tasks... (Ctrl+C to quit)")

	// Keep running until interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	sub.Unsubscribe()
	log.Println("Worker shutting down.")
}

// connectToScheduler cycles through nodes until it finds a live one
func connectToScheduler() {
	// Close existing if open
	if conn != nil {
		conn.Close()
	}

	for _, addr := range schedulerAddrs {
		log.Printf("Trying to connect to Scheduler at %s...", addr)
		var err error
		// Short timeout for failover speed
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err = grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(creds),
			grpc.WithBlock(),
		)
		cancel()

		if err == nil {
			client = pb.NewSchedServiceClient(conn)
			log.Printf("✅ Connected to Scheduler Node: %s", addr)
			return
		}
		log.Printf("⚠️  Failed to connect to %s: %v", addr, err)
	}
	log.Println("❌ CRITICAL: All Scheduler nodes appear down!")
}

// fetchTask wraps the gRPC call
func fetchTask(id string) (*pb.Task, error) {
	if client == nil {
		return nil, fmt.Errorf("no scheduler connection")
	}

	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		token = "my-secret-key"
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "auth-token", token)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := client.GetTask(ctx, &pb.TaskRequest{TaskId: id})
	if err != nil {
		return nil, err
	}
	return resp.Task, nil
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
	scriptIndentLevel := -1
	var scriptLines []string

	for _, line := range lines {
		// Keep the raw line to preserve indentation
		rawLine := line
		trimmed := strings.TrimSpace(rawLine)

		// Calculate indentation level
		currentIndent := 0
		for _, char := range rawLine {
			if char == ' ' {
				currentIndent++
			} else if char == '\t' {
				currentIndent += 4
			} else {
				break
			}
		}

		if !foundScript {
			if strings.HasPrefix(trimmed, "script:") {
				foundScript = true
				scriptIndentLevel = currentIndent

				parts := strings.SplitN(trimmed, ":", 2)
				if len(parts) == 2 {
					val := strings.TrimSpace(parts[1])
					// Handle inline script
					if val != "|" && val != ">" && val != "" {
						if len(val) >= 2 {
							first, last := val[0], val[len(val)-1]
							if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
								return val[1 : len(val)-1]
							}
						}
						return val
					}
				}
			}
			continue
		}

		if foundScript {
			if trimmed == "" {
				scriptLines = append(scriptLines, rawLine)
				continue
			}
			// Stop if we hit a sibling key (same or less indentation than 'script:')
			if currentIndent <= scriptIndentLevel {
				break
			}
			scriptLines = append(scriptLines, rawLine)
		}
	}
	return strings.Join(scriptLines, "\n")
}

func injectSecrets(script string) string {
	return os.Expand(script, func(key string) string {
		val := os.Getenv(key)
		if val == "" {
			// If not found in env, assume it's a local script variable.
			// Return it with the $ prefix to preserve it.
			return "$" + key
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