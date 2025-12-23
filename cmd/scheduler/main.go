package main

import (
	"context"
	"encoding/json"
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

// Verdict matches the JSON sent by the Python Agent
type Verdict struct {
	TaskID   string `json:"task_id"`
	Decision string `json:"decision"` // "RETRY" or "STOP"
	Reason   string `json:"reason"`
}

type server struct {
	pb.UnimplementedSchedServiceServer
	store *store.Store
	js    nats.JetStreamContext
}

func (s *server) SubmitIntent(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	taskID := nuid.Next()

	// NOTE: We are setting MODE to AUTONOMOUS for this test!
	// In a real CLI, this comes from the YAML content.
	task := &pb.Task{
		Id:         taskID,
		IntentYaml: req.YamlContent,
		State:      pb.TaskState_CREATED,
		Logs:       []string{},
		Mode:       pb.GovernanceMode_AUTONOMOUS, // <--- CHANGED FOR DEMO
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
	time.Sleep(2 * time.Second) 

	// 3. LISTENERS

	// A. Completed
	_, err = js.Subscribe("tasks.events.completed", func(m *nats.Msg) {
		taskID := string(m.Data)
		log.Printf("EVENT: Received Completion for %s", taskID)
		st.TransitionState(taskID, pb.TaskState_COMPLETED)
	})

	// B. Verdict Listener (THE NEW PART)
	// Consumes analysis results from the Python Agent
	_, err = js.Subscribe("tasks.governance.verdict", func(m *nats.Msg) {
		var v Verdict
		if err := json.Unmarshal(m.Data, &v); err != nil {
			log.Printf("ERR: Bad verdict JSON: %v", err)
			return
		}
		
		log.Printf("🤖 AI VERDICT for %s: %s (%s)", v.TaskID, v.Decision, v.Reason)

		if v.Decision == "RETRY" {
			// Autonomous Retry
			_, err := st.IncrementRetry(v.TaskID)
			if err != nil {
				log.Printf("ERR: Failed to increment retry: %v", err)
				return
			}
			// Re-queue
			js.Publish("tasks.scheduled", []byte(v.TaskID))
			log.Printf("   -> Task re-queued automatically.")
		} else {
			// Autonomous Stop
			st.TransitionState(v.TaskID, pb.TaskState_FAILED)
			log.Printf("   -> Task HALTED to save costs.")
		}
	})

	// C. Failure Listener
	_, err = js.Subscribe("tasks.events.failed", func(m *nats.Msg) {
		taskID := string(m.Data)
		log.Printf("EVENT: Received FAILURE for %s", taskID)

		task, err := st.Get(taskID)
		if err != nil {
			return
		}

		// LOGIC SWITCH
		if task.Mode == pb.GovernanceMode_ADVISORY_ONLY {
			// ... Old Logic (Blind Retry) ...
			if task.RetryCount < 3 {
				log.Printf("GOVERNANCE: Advisory Mode. Retrying...")
				st.IncrementRetry(taskID)
				js.Publish("tasks.scheduled", []byte(taskID))
			} else {
				log.Printf("GOVERNANCE: Max retries exhausted.")
				st.TransitionState(taskID, pb.TaskState_FAILED)
			}
		} else {
			// NEW: AUTONOMOUS MODE
			// 1. Mark as Analyzing
			log.Printf("GOVERNANCE: Autonomous Mode. Requesting AI Analysis...")
			st.TransitionState(taskID, pb.TaskState_ANALYZING)
			
			// 2. The Python agent is already listening to 'tasks.events.failed',
			// so we don't need to send a separate request. It will pick this up automatically.
		}
	})

	// 4. Server
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