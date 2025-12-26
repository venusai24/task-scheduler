package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nats-io/nats.go"
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

	// 2. Create the Stream
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TASKS",
		Subjects: []string{"tasks.>"},
	})
	if err != nil {
		log.Printf("Stream might already exist: %v", err)
	}

	// 3. Subscribe
	sub, err := js.QueueSubscribe("tasks.scheduled", "worker-group", func(m *nats.Msg) {
		taskID := string(m.Data)
		log.Printf("Received Task ID: %s", taskID)

		// 4. EXECUTE (now with simulation support)
		executeTask(taskID, js, nc)

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

func executeTask(id string, js nats.JetStreamContext, nc *nats.Conn) {
	// Fetch task metadata to check simulation mode
	// In production, retrieve from store via gRPC or KV
	// For now, we'll use NATS KV or assume we can query

	// SIMPLIFIED: Check for simulation flag via a separate KV or assume metadata
	// Here we'll simulate by checking a config subject
	msg, err := nc.Request("tasks.metadata."+id, nil, 1*time.Second)
	var task pb.Task
	isSimulation := false

	if err == nil && len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &task); err == nil {
			isSimulation = task.IsSimulation
		}
	}

	if isSimulation {
		log.Printf("🔍 SIMULATION MODE: Skipping execution for task %s", id)
		log.Printf("   Intent validated. No actual work performed.")
		// Mark as completed immediately
		_, err := js.Publish("tasks.events.completed", []byte(id))
		if err != nil {
			log.Printf("Failed to publish completion: %v", err)
		}
		return
	}

	log.Printf(">>> STARTING Execution for %s", id)
	time.Sleep(1 * time.Second)

	// SIMULATED LOGIC: Succeed on retry, fail on first attempt
	// In production, this would be replaced with actual task execution

	// TODO: Get task from store to check RetryCount
	// For now, we'll simulate: first attempt fails, retry succeeds

	// FORCE FAILURE for first attempt (testing governance modes)
	log.Printf("!!! SIMULATING FAILURE for %s", id)
	_, err = js.Publish("tasks.events.failed", []byte(id))
	if err != nil {
		log.Printf("Failed to publish failure event: %v", err)
	}

	// NOTE: To test retry success, you would check task.RetryCount:
	// if task.RetryCount > 0 {
	//     log.Printf("✅ Task %s SUCCEEDED on retry!", id)
	//     js.Publish("tasks.events.completed", []byte(id))
	//     return
	// }
}