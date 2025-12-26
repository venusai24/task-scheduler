package main

import (
	"log"
	"os"
	"os/signal"
	"strings" // <--- This was missing
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	// 1. Connect to NATS
	nc, err := nats.Connect("nats://localhost:4222")
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

		// 4. EXECUTE
		executeTask(taskID, js)

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

func executeTask(id string, js nats.JetStreamContext) {
	log.Printf(">>> STARTING Execution for %s", id)
	time.Sleep(1 * time.Second)

	// CHECK: Only fail if the ID contains the text "fail"
	// Since generated IDs are random alphanumeric, they usually won't contain "fail",
	// so tasks will SUCCEED by default.
	if strings.Contains(id, "fail") {
		log.Printf("!!! TASK FAILED (Simulated) for %s", id)
		_, err := js.Publish("tasks.events.failed", []byte(id))
		if err != nil {
			log.Printf("Failed to publish failure event: %v", err)
		}
		return
	}

	// SUCCESS PATH
	log.Printf("<<< FINISHED Execution for %s", id)
	_, err := js.Publish("tasks.events.completed", []byte(id))
	if err != nil {
		log.Printf("Failed to publish completion event: %v", err)
	}
}