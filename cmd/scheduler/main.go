package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"google.golang.org/grpc"

	"github.com/venusai24/task-scheduler/internal/store"
	pb "github.com/venusai24/task-scheduler/proto"
)

// server implements the gRPC SchedService
type server struct {
	pb.UnimplementedSchedServiceServer
	store *store.Store
	js    nats.JetStreamContext
}

// SubmitIntent handles new task submissions
func (s *server) SubmitIntent(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	taskID := nuid.Next()

	task := &pb.Task{
		Id:         taskID,
		IntentYaml: req.YamlContent,
		State:      pb.TaskState_CREATED,
		Logs:       []string{},
		Mode:       pb.GovernanceMode_ADVISORY_ONLY, // Default to Safe Mode
	}

	log.Printf("Received Intent. Assigning ID: %s. Applying to Raft...", taskID)
	if err := s.store.Set(task); err != nil {
		return nil, fmt.Errorf("failed to persist task: %v", err)
	}

	msg := []byte(task.Id)
	_, err := s.js.Publish("tasks.scheduled", msg)
	if err != nil {
		log.Printf("ERR: Failed to publish task %s to NATS: %v", task.Id, err)
		return nil, fmt.Errorf("failed to schedule task: %v", err)
	}

	log.Printf("Task %s scheduled via NATS!", task.Id)
	return &pb.SubmitResponse{TaskId: taskID}, nil
}

// GetTask retrieves the current state
func (s *server) GetTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	task, err := s.store.Get(req.TaskId)
	if err != nil {
		return nil, err
	}
	return &pb.TaskResponse{Task: task}, nil
}

func main() {
	// 1. NATS
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to init JetStream: %v", err)
	}
	log.Println("Connected to NATS JetStream.")

	// 2. Raft Store
	dataDir := "./data/node-1"
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}

	st := store.NewStore()
	raftAddr := "localhost:6000"
	if err := st.Open("node-1", dataDir, raftAddr); err != nil {
		log.Fatalf("Failed to open Raft store: %v", err)
	}
	log.Printf("Raft storage started on %s", raftAddr)
	time.Sleep(2 * time.Second) // Wait for Leader Election

	// 3. LISTEN FOR EVENTS (The Brain)
	
	// A. Completion Listener
	_, err = js.Subscribe("tasks.events.completed", func(m *nats.Msg) {
		taskID := string(m.Data)
		log.Printf("EVENT: Received Completion for %s", taskID)
		if err := st.TransitionState(taskID, pb.TaskState_COMPLETED); err != nil {
			log.Printf("ERR: Failed to mark task complete: %v", err)
		} else {
			log.Printf("SUCCESS: Task %s marked as COMPLETED in Raft.", taskID)
		}
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to completed events: %v", err)
	}

	// B. Failure Listener (GOVERNANCE LOGIC)
	_, err = js.Subscribe("tasks.events.failed", func(m *nats.Msg) {
		taskID := string(m.Data)
		log.Printf("EVENT: Received FAILURE for %s", taskID)

		// 1. Get current state
		task, err := st.Get(taskID)
		if err != nil {
			log.Printf("ERR: Unknown task %s failed", taskID)
			return
		}

		// 2. Check Governance Mode
		const MaxRetries = 3

		if task.Mode == pb.GovernanceMode_ADVISORY_ONLY {
			if task.RetryCount < MaxRetries {
				log.Printf("GOVERNANCE: Advisory Mode. Retrying task %s (%d/%d)", taskID, task.RetryCount+1, MaxRetries)
				
				// Update DB (Increment Retry Count)
				_, err := st.IncrementRetry(taskID)
				if err != nil {
					log.Printf("ERR: Failed to update retry count: %v", err)
					return
				}

				// Re-Queue to NATS
				_, err = js.Publish("tasks.scheduled", []byte(taskID))
				if err != nil {
					log.Printf("ERR: Failed to re-queue task: %v", err)
				}

			} else {
				log.Printf("GOVERNANCE: Max retries exhausted for %s. Marking FAILED.", taskID)
				st.TransitionState(taskID, pb.TaskState_FAILED)
			}
		} else {
			log.Println("GOVERNANCE: Autonomous mode not implemented yet.")
		}
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to failure events: %v", err)
	}

	// 4. gRPC Server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSchedServiceServer(grpcServer, &server{store: st, js: js})

	log.Println("Scheduler Control Plane listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}