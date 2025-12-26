package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

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

// authInterceptor validates the auth token in metadata
func authInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("missing metadata")
	}
	
	tokens := md.Get("auth-token")
	expectedToken := os.Getenv("SCHED_AUTH_TOKEN")
	if expectedToken == "" {
		expectedToken = "my-secret-key" // Default for development
	}
	
	if len(tokens) == 0 || tokens[0] != expectedToken {
		return nil, fmt.Errorf("unauthorized: invalid or missing auth token")
	}
	
	return handler(ctx, req)
}

func (s *server) SubmitIntent(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	taskID := nuid.Next()

	// Improved detection logic - handle both quoted and unquoted YAML values
	mode := pb.GovernanceMode_ADVISORY_ONLY
	
	if strings.Contains(req.YamlContent, "mode: \"HUMAN_GATE\"") || strings.Contains(req.YamlContent, "mode: HUMAN_GATE") {
		mode = pb.GovernanceMode_HUMAN_GATE
		log.Printf("Detected HUMAN_GATE mode for task %s", taskID)
	} else if strings.Contains(req.YamlContent, "mode: \"AUTONOMOUS\"") || strings.Contains(req.YamlContent, "mode: AUTONOMOUS") {
		mode = pb.GovernanceMode_AUTONOMOUS
		log.Printf("Detected AUTONOMOUS mode for task %s", taskID)
	} else {
		log.Printf("Using default ADVISORY_ONLY mode for task %s", taskID)
	}

	task := &pb.Task{
		Id:           taskID,
		IntentYaml:   req.YamlContent,
		State:        pb.TaskState_CREATED,
		Logs:         []string{},
		Mode:         mode,
		IsSimulation: req.DryRun, // <--- Add this
	}

	if req.DryRun {
		log.Printf("🔍 SIMULATION MODE: Task %s will not execute actual work", taskID)
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

// Add the ApproveTask handler to the gRPC server implementation:
func (s *server) ApproveTask(ctx context.Context, req *pb.ApproveRequest) (*pb.ApproveResponse, error) {
	if req.TaskId == "" {
		return &pb.ApproveResponse{
			Success: false,
			Message: "task_id is required",
		}, nil
	}

	// Increment retry count
	if _, err := s.store.IncrementRetry(req.TaskId); err != nil {
		return &pb.ApproveResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to increment retry: %v", err),
		}, nil
	}

	// FIX: Publish only the task ID string (not the marshaled protobuf)
	// The worker expects: taskID := string(m.Data)
	if _, err := s.js.Publish("tasks.scheduled", []byte(req.TaskId)); err != nil {
		return &pb.ApproveResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to republish task: %v", err),
		}, nil
	}

	log.Printf("Task %s approved and republished", req.TaskId)
	return &pb.ApproveResponse{
		Success: true,
		Message: "Task approved and rescheduled",
	}, nil
}

func main() {
	// 1. NATS Connection with Token Auth
	natsToken := os.Getenv("NATS_TOKEN")
	var nc *nats.Conn
	var err error
	
	if natsToken != "" {
		nc, err = nats.Connect("nats://localhost:4222", nats.Token(natsToken))
		log.Println("Connecting to NATS with token authentication")
	} else {
		nc, err = nats.Connect("nats://localhost:4222")
		log.Println("Connecting to NATS without authentication (dev mode)")
	}
	
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to init JetStream: %v", err)
	}
	log.Println("Connected to NATS JetStream.")

	// 2. Raft Store Initialization
	dataDir := "./data/node-1"
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}

	st := store.NewStore()
	raftAddr := "localhost:6000"
	if err := st.Open("node-1", dataDir, raftAddr); err != nil {
		log.Fatalf("Failed to open Raft store: %v", err)
	}
	defer st.Close() // Ensure cleanup on exit
	
	log.Printf("Raft storage started on %s", raftAddr)
	
	// 3. Wait for Raft Leader Election
	log.Println("Waiting for Raft leader election...")
	if err := waitForLeader(st, 30*time.Second); err != nil {
		log.Fatalf("Raft failed to elect leader: %v", err)
	}
	log.Printf("✅ Raft node is now LEADER (state: %s)", st.GetRaftState())
	
	// 4. Verify Store with Test Operation
	testTask := &pb.Task{
		Id:    "init-test",
		State: pb.TaskState_CREATED,
		Mode:  pb.GovernanceMode_ADVISORY_ONLY,
	}
	if err := st.Set(testTask); err != nil {
		log.Fatalf("Store not ready after leader election: %v", err)
	}
	log.Println("✅ Raft store verified and ready")

	// 5. NOW Safe to Subscribe to NATS Events
	log.Println("Subscribing to NATS event streams...")

	// A. Completed Events
	_, err = js.Subscribe("tasks.events.completed", func(m *nats.Msg) {
		// Readiness gate: Only process if we're still the leader
		if !st.IsLeader() {
			log.Printf("⚠️  Not leader, deferring completion event")
			return // NATS will redeliver
		}
		
		taskID := string(m.Data)
		log.Printf("EVENT: Received Completion for %s", taskID)
		st.TransitionState(taskID, pb.TaskState_COMPLETED)
		m.Ack()
	}, nats.DeliverNew(), nats.StartTime(time.Now())) // ADD THIS
	if err != nil {
		log.Fatalf("Failed to subscribe to completed events: %v", err)
	}

	// B. Verdict Listener (AI Agent)
	_, err = js.Subscribe("tasks.governance.verdict", func(m *nats.Msg) {
		if !st.IsLeader() {
			log.Printf("⚠️  Not leader, deferring verdict event")
			return
		}
		
		var v Verdict
		if err := json.Unmarshal(m.Data, &v); err != nil {
			log.Printf("ERR: Bad verdict JSON: %v", err)
			m.Ack()
			return
		}
		
		log.Printf("🤖 AI VERDICT for %s: %s (%s)", v.TaskID, v.Decision, v.Reason)

		task, err := st.Get(v.TaskID)
		if err != nil {
			log.Printf("❌ Task %s not found: %v", v.TaskID, err)
			m.Ack()
			return
		}

		// SAFETY CHECK: If mode is HUMAN_GATE or ADVISORY_ONLY,
		// we only record the AI's advice; we DO NOT change the state.
		if task.Mode == pb.GovernanceMode_HUMAN_GATE || task.Mode == pb.GovernanceMode_ADVISORY_ONLY {
			log.Printf("🤖 AI advice received for %s, but ignored due to %v mode.", v.TaskID, task.Mode)
			task.AiInsight = fmt.Sprintf("AI Recommendation: %s - %s", v.Decision, v.Reason)
			st.Set(task)
			m.Ack()
			return
		}

		// AUTONOMOUS MODE: Proceed with AI decision, but respect safety limits
		if v.Decision == "RETRY" {
			// SAFETY GATE: Check if we have already hit the retry limit
			const maxRetries = 3
			if task.RetryCount >= maxRetries {
				log.Printf("⚠️ SAFETY LIMIT REACHED for %s (RetryCount: %d/%d). Ignoring AI retry request.", 
					v.TaskID, task.RetryCount, maxRetries)
				st.TransitionState(v.TaskID, pb.TaskState_FAILED)
				m.Ack()
				return
			}

			// Autonomous Retry - within safety limits
			log.Printf("🤖 AI VERDICT: RETRY accepted for %s (Attempt %d/%d)", 
				v.TaskID, task.RetryCount+1, maxRetries)
			_, err := st.IncrementRetry(v.TaskID)
			if err != nil {
				log.Printf("ERR: Failed to increment retry: %v", err)
				m.Ack()
				return
			}
			js.Publish("tasks.scheduled", []byte(v.TaskID))
			log.Printf("   -> Task re-queued automatically.")
		} else {
			st.TransitionState(v.TaskID, pb.TaskState_FAILED)
			log.Printf("   -> Task HALTED to save costs.")
		}
		m.Ack()
	}, nats.DeliverNew(), nats.StartTime(time.Now())) // ADD THIS
	if err != nil {
		log.Fatalf("Failed to subscribe to verdict events: %v", err)
	}

	// C. Failure Events
	_, err = js.Subscribe("tasks.events.failed", func(m *nats.Msg) {
		if !st.IsLeader() {
			log.Printf("⚠️  Not leader, deferring failure event")
			return
		}
		
		taskID := string(m.Data)
		log.Printf("EVENT: Received FAILURE for %s", taskID)

		task, err := st.Get(taskID)
		if err != nil {
			log.Printf("ERR: Failed to get task %s: %v", taskID, err)
			m.Ack() // Ack to avoid infinite retry on missing task
			return
		}

		// LOGIC SWITCH
		switch task.Mode {
		case pb.GovernanceMode_ADVISORY_ONLY:
			log.Printf("[ADVISORY] Task %s failed. Manual intervention required.", taskID)
			// No automatic retry - just log and wait for manual action
		case pb.GovernanceMode_AUTONOMOUS:
			log.Printf("[AUTONOMOUS] Task %s failed. Moving to ANALYZING state...", taskID)
			// Transition to ANALYZING - AI agent will handle the retry decision
			if err := st.TransitionState(taskID, pb.TaskState_ANALYZING); err != nil {
				log.Printf("ERR: Failed to update task %s to ANALYZING: %v", taskID, err)
			}
			// DO NOT publish to tasks.scheduled here - wait for AI verdict
		case pb.GovernanceMode_HUMAN_GATE:
			log.Printf("[HUMAN_GATE] Task %s failed. Awaiting approval...", taskID)
			if err := st.TransitionState(taskID, pb.TaskState_NEEDS_APPROVAL); err != nil {
				log.Printf("ERR: Failed to update task %s to NEEDS_APPROVAL: %v", taskID, err)
			}
		default:
			log.Printf("ERR: Unknown governance mode for task %s", taskID)
		}
		m.Ack()
	}, nats.DeliverNew(), nats.StartTime(time.Now())) // ADD THIS
	if err != nil {
		log.Fatalf("Failed to subscribe to failure events: %v", err)
	}

	log.Println("✅ All NATS subscriptions active")

	// 6. Start gRPC Server with mTLS and Auth
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}
	
	// Load TLS credentials if certificates exist
	var grpcServer *grpc.Server
	certFile := os.Getenv("SCHED_CERT_FILE")
	keyFile := os.Getenv("SCHED_KEY_FILE")
	
	if certFile != "" && keyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			log.Fatalf("Failed to load TLS credentials: %v", err)
		}
		grpcServer = grpc.NewServer(
			grpc.Creds(creds),
			grpc.UnaryInterceptor(authInterceptor),
		)
		log.Println("gRPC server starting with mTLS and auth interceptor")
	} else {
		grpcServer = grpc.NewServer(grpc.UnaryInterceptor(authInterceptor))
		log.Println("⚠️  gRPC server starting WITHOUT mTLS (dev mode)")
	}
	
	pb.RegisterSchedServiceServer(grpcServer, &server{store: st, js: js})

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		log.Println("Shutting down gracefully...")
		grpcServer.GracefulStop()
		st.Close()
		nc.Close()
		os.Exit(0)
	}()

	log.Println("🚀 Scheduler Control Plane Ready - Listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

// waitForLeader blocks until the Raft node becomes leader or timeout
func waitForLeader(st *store.Store, timeout time.Duration) error {
    deadline := time.Now().Add(timeout)
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()

    for {
        if st.IsLeader() {
            return nil
        }
        
        if time.Now().After(deadline) {
            return fmt.Errorf("timeout waiting for leader election")
        }
        
        <-ticker.C
    }
}