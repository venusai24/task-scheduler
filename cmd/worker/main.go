package main

import (
	"context"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"github.com/nats-io/nats.go"
	pb "github.com/venusai24/task-scheduler/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v2"
)

func main() {
	// 1. Connect to gRPC Scheduler
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to scheduler: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedServiceClient(conn)

	// Add auth token to context (optional for dev)
	authToken := os.Getenv("ASTRA_AUTH_TOKEN")
	if authToken != "" {
		log.Println("Worker connected to Scheduler via gRPC with authentication")
	} else {
		log.Println("⚠️  Worker connected to Scheduler via gRPC without authentication (dev mode)")
	}

	// 2. Connect to NATS with Token Auth
	natsToken := os.Getenv("NATS_TOKEN")
	var nc *nats.Conn

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
		log.Printf("Received Task ID: %s", taskID)

		// Create context with auth for each request
		var reqCtx context.Context
		if authToken != "" {
			reqCtx = metadata.AppendToOutgoingContext(context.Background(), "auth-token", authToken)
		} else {
			reqCtx = context.Background()
		}

		// 1. Fetch the full task from the Scheduler via gRPC
		resp, err := client.GetTask(reqCtx, &pb.TaskRequest{TaskId: taskID})
		if err != nil {
			log.Printf("ERR: Could not fetch task details for %s: %v", taskID, err)
			return
		}

		// 2. Now you have the YAML in resp.Task.IntentYaml to execute!
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

// Fixed struct to match your YAML structure
type Intent struct {
	Spec struct {
		Script     string `yaml:"script"`
		Governance struct {
			Mode string `yaml:"mode"`
		} `yaml:"governance"`
	} `yaml:"spec"`
}

func executeTask(task *pb.Task, js nats.JetStreamContext) {
	id := task.Id

	// Check simulation mode
	if task.IsSimulation {
		log.Printf("🔍 SIMULATION MODE: Skipping execution for task %s", id)
		js.Publish("tasks.events.completed", []byte(id))
		return
	}

	// Parse the YAML to extract the script
	var intent Intent
	if err := yaml.Unmarshal([]byte(task.IntentYaml), &intent); err != nil {
		log.Printf("ERR: Failed to parse YAML: %v", err)
		log.Printf("DEBUG: YAML content was: %s", task.IntentYaml)
		js.Publish("tasks.events.failed", []byte(id))
		return
	}

	script := strings.TrimSpace(intent.Spec.Script)
	if script == "" {
		log.Printf("ERR: No script found for task %s", id)
		log.Printf("DEBUG: Parsed intent: %+v", intent)
		js.Publish("tasks.events.failed", []byte(id))
		return
	}

	log.Printf(">>> EXECUTING: %s (Task: %s)", script, id)

	// Execute the script
	cmd := exec.Command("sh", "-c", script)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()

	if err != nil {
		log.Printf("❌ Task %s FAILED: %v", id, err)
		js.Publish("tasks.events.failed", []byte(id))
	} else {
		log.Printf("✅ Task %s COMPLETED", id)
		js.Publish("tasks.events.completed", []byte(id))
	}
}