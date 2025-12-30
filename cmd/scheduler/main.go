package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
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
	expectedToken := os.Getenv("ASTRA_AUTH_TOKEN")
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

	mode := pb.GovernanceMode_ADVISORY_ONLY
	if strings.Contains(req.YamlContent, "mode: \"HUMAN_GATE\"") || strings.Contains(req.YamlContent, "mode: HUMAN_GATE") {
		mode = pb.GovernanceMode_HUMAN_GATE
		log.Printf("Detected HUMAN_GATE mode for task %s", taskID)
	} else if strings.Contains(req.YamlContent, "mode: \"AUTONOMOUS\"") || strings.Contains(req.YamlContent, "mode: AUTONOMOUS") {
		mode = pb.GovernanceMode_AUTONOMOUS
		log.Printf("Detected AUTONOMOUS mode for task %s", taskID)
	}

	// Extract dependency
	dependsOn := extractField(req.YamlContent, "depends_on:")
	
	initialState := pb.TaskState_CREATED
	shouldScheduleNow := true

	if dependsOn != "" {
		// Check if the parent is ALREADY completed
		parentTask, err := s.store.Get(dependsOn)
		if err == nil && parentTask.State == pb.TaskState_COMPLETED {
			log.Printf("🔗 Parent %s is ALREADY finished. Scheduling %s immediately.", dependsOn, taskID)
			initialState = pb.TaskState_CREATED
			shouldScheduleNow = true
		} else {
			initialState = pb.TaskState_AWAITING_PREREQUISITE
			shouldScheduleNow = false
			log.Printf("🔗 Task %s waiting for parent: %s", taskID, dependsOn)
		}
	}

	task := &pb.Task{
		Id:            taskID,
		IntentYaml:    req.YamlContent,
		State:         initialState,
		Logs:          []string{},
		Mode:          mode,
		IsSimulation:  req.DryRun,
		PreRunScript:  extractField(req.YamlContent, "pre_run:"),
		PostRunScript: extractField(req.YamlContent, "post_run:"),
		DependsOn:     dependsOn,
	}

	if req.DryRun {
		log.Printf("🔍 SIMULATION MODE: Task %s will not execute actual work", taskID)
	}

	if err := s.store.Set(task); err != nil {
		return nil, fmt.Errorf("failed to persist task: %v", err)
	}

	// Schedule if no dependency OR dependency is already met
	if shouldScheduleNow {
		msg := []byte(task.Id)
		_, err := s.js.Publish("tasks.scheduled", msg)
		if err != nil {
			log.Printf("ERR: Failed to publish task %s to NATS: %v", task.Id, err)
			return nil, fmt.Errorf("failed to schedule task: %v", err)
		}
		log.Printf("Task %s scheduled via NATS!", task.Id)
	} else {
		log.Printf("Task %s persisted but HELD (waiting for %s)", task.Id, dependsOn)
	}

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
		return &pb.ApproveResponse{Success: false, Message: "task_id is required"}, nil
	}
	if _, err := s.store.IncrementRetry(req.TaskId); err != nil {
		return &pb.ApproveResponse{Success: false, Message: fmt.Sprintf("Failed: %v", err)}, nil
	}
	if _, err := s.js.Publish("tasks.scheduled", []byte(req.TaskId)); err != nil {
		return &pb.ApproveResponse{Success: false, Message: fmt.Sprintf("Failed to republish: %v", err)}, nil
	}
	log.Printf("Task %s approved and republished", req.TaskId)
	return &pb.ApproveResponse{Success: true, Message: "Task approved"}, nil
}

func (s *server) RollbackTask(ctx context.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	if req.TaskId == "" {
		return &pb.RollbackResponse{Success: false, Message: "task_id is required"}, nil
	}
	if err := s.store.Rollback(req.TaskId); err != nil {
		return &pb.RollbackResponse{Success: false, Message: fmt.Sprintf("Rollback failed: %v", err)}, nil
	}
	return &pb.RollbackResponse{Success: true, Message: "Rolled back successfully"}, nil
}

func (s *server) JoinCluster(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	if req.NodeId == "" || req.Address == "" {
		return &pb.JoinResponse{Success: false, Message: "node_id and address are required"}, nil
	}

	if !s.store.IsLeader() {
		return &pb.JoinResponse{Success: false, Message: "not leader"}, nil
	}

	if err := s.store.AddVoter(req.NodeId, req.Address); err != nil {
		return &pb.JoinResponse{
			Success: false,
			Message: fmt.Sprintf("failed to add node: %v", err),
		}, nil
	}

	log.Printf("Added node %s at %s to cluster", req.NodeId, req.Address)
	return &pb.JoinResponse{Success: true, Message: "node added"}, nil
}

func main() {
	// ADD CLI FLAGS
	nodeID := flag.String("id", "node-1", "Node identifier")
	grpcPort := flag.String("port", ":50051", "gRPC listen port")
	raftAddr := flag.String("raft", "localhost:6000", "Raft bind address")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap cluster as leader")
	joinAddr := flag.String("join", "", "Address of leader to join")
	flag.Parse()

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

	// FIX: Create NATS streams with valid names (no dots in stream names)
	streamConfigs := []struct {
		name    string
		subject string
	}{
		{"TASKS", "tasks.scheduled"},
		{"TASKS_EVENTS", "tasks.events.>"},
		{"TASKS_GOVERNANCE", "tasks.governance.>"},
	}

	for _, cfg := range streamConfigs {
		_, err := js.StreamInfo(cfg.name)
		if err != nil {
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     cfg.name,
				Subjects: []string{cfg.subject},
			})
			if err != nil {
				log.Fatalf("Failed to create stream %s: %v", cfg.name, err)
			}
			log.Printf("Created stream: %s (subject: %s)", cfg.name, cfg.subject)
		}
	}

	// FIX: Use node-specific data directory
	dataDir := filepath.Join("./data", *nodeID)
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}

	st := store.NewStore()
	if err := st.Open(*nodeID, dataDir, *raftAddr, *bootstrap); err != nil {
		log.Fatalf("Failed to open Raft store: %v", err)
	}
	defer st.Close()

	log.Printf("Raft storage started on %s", *raftAddr)

	// 1. Prepare gRPC Listener EARLY (before Raft operations)
	lis, err := net.Listen("tcp", *grpcPort)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", *grpcPort, err)
	}

	certFile := os.Getenv("SCHED_CERT_FILE")
	keyFile := os.Getenv("SCHED_KEY_FILE")
	caFile := os.Getenv("SCHED_CA_FILE")

	if certFile == "" || keyFile == "" || caFile == "" {
		log.Fatal("SECURE ERROR: SCHED_CERT_FILE, SCHED_KEY_FILE, and SCHED_CA_FILE must be set. Insecure mode is disabled.")
	}

	serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("Failed to load server key pair: %v", err)
	}

	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		log.Fatalf("Failed to read CA cert: %v", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caBytes) {
		log.Fatal("Failed to append CA cert")
	}

	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
	})

	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
		grpc.UnaryInterceptor(authInterceptor),
	)

	pb.RegisterSchedServiceServer(grpcServer, &server{store: st, js: js})

	// 2. START gRPC SERVER IN BACKGROUND (Critical!)
	go func() {
		log.Printf("🚀 gRPC Server listening on %s", *grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// 3. Attempt cluster join with retry logic
	if *joinAddr != "" {
		go func() {
			backoff := 3 * time.Second
			maxBackoff := 1 * time.Minute
			attempt := 0

			for {
				// Stop trying if this node became leader (split-brain prevention)
				if st.IsLeader() {
					log.Println("This node became leader, stopping join attempts")
					return
				}

				attempt++
				log.Printf("Attempting to join cluster via %s (Attempt %d)...", *joinAddr, attempt)

				if err := joinCluster(*joinAddr, *nodeID, *raftAddr); err == nil {
					log.Println("✅ Successfully joined cluster")
					return
				} else {
					log.Printf("Join attempt failed: %v, retrying in %v", err, backoff)
				}

				time.Sleep(backoff)

				// Exponential backoff to reduce load during prolonged outages
				backoff = backoff * 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}()

		// Give initial join attempts time to complete
		time.Sleep(5 * time.Second)
	}

	// 4. Wait for Raft Leader Election
	// Bootstrap node: Give it time to self-elect
	if *bootstrap {
		log.Println("Bootstrap node - waiting for self-election...")
		time.Sleep(3 * time.Second)

		if st.IsLeader() {
			log.Printf("✅ Bootstrap node elected as leader (state: %s)", st.GetRaftState())
		} else {
			log.Printf("⚠️  Bootstrap node not yet leader (state: %s), continuing...", st.GetRaftState())
		}
	} else {
		// Joining node: Wait for cluster to have a leader (not for self to become leader)
		log.Println("Waiting for cluster to elect a leader...")
		if err := waitForClusterReady(st, 30*time.Second); err != nil {
			log.Printf("⚠️  Cluster status: %v (state: %s)", err, st.GetRaftState())
		} else {
			log.Printf("✅ Raft follower ready (state: %s, leader present)", st.GetRaftState())
		}
	}

	// 5. Verify Store with Test Operation (only if leader)
	if st.IsLeader() {
		testTask := &pb.Task{
			Id:    "init-test",
			State: pb.TaskState_CREATED,
			Mode:  pb.GovernanceMode_ADVISORY_ONLY,
		}
		if err := st.Set(testTask); err != nil {
			log.Fatalf("Store not ready after leader election: %v", err)
		}
		log.Println("✅ Raft store verified and ready")
	}

	// 6. NOW Safe to Subscribe to NATS Events
	log.Println("Subscribing to NATS event streams...")

	// A. Completed Events - NOW WITH DEPENDENCY COORDINATION
	_, err = js.Subscribe("tasks.events.completed", func(m *nats.Msg) {
		if !st.IsLeader() {
			log.Printf("⚠️  Not leader, deferring completion event")
			return
		}

		taskID := string(m.Data)
		log.Printf("EVENT: Received Completion for %s", taskID)
		st.TransitionState(taskID, pb.TaskState_COMPLETED)

		// 🚀 TRIGGER DEPENDENTS
		dependents := st.GetDependentTasks(taskID)
		if len(dependents) > 0 {
			log.Printf("🔗 Found %d dependent tasks waiting for %s", len(dependents), taskID)
			for _, dep := range dependents {
				log.Printf("   -> Unleashing Dependent Task: %s", dep.Id)
				
				// 1. Update State in Raft
				if err := st.TransitionState(dep.Id, pb.TaskState_PENDING); err != nil {
					log.Printf("ERR: Failed to transition dependent task %s: %v", dep.Id, err)
					continue
				}

				// 2. Publish to NATS for execution
				if _, err := js.Publish("tasks.scheduled", []byte(dep.Id)); err != nil {
					log.Printf("ERR: Failed to publish dependent task %s: %v", dep.Id, err)
				}
			}
		}

		m.Ack()
	}, nats.DeliverNew())
	if err != nil {
		log.Fatalf("Failed to subscribe to completed events: %v", err)
	}

	// B. Verdict Listener (AI Agent) - USE DURABLE QUEUE
	// Ensure existing durable consumer (if any) matches our desired deliver policy.
	// If it doesn't, delete it so we can recreate with the correct configuration.
	{
		const verdictStream = "TASKS_GOVERNANCE"
		const verdictDurable = "verdict-processor"
		desired := nats.DeliverNewPolicy

		if ci, err := js.ConsumerInfo(verdictStream, verdictDurable); err == nil {
			if ci.Config.DeliverPolicy != desired {
				log.Printf("⚠️  Conflicting consumer '%s' on stream '%s' (deliver=%d). Deleting to recreate...",
					verdictDurable, verdictStream, ci.Config.DeliverPolicy)
				if err := js.DeleteConsumer(verdictStream, verdictDurable); err != nil {
					log.Fatalf("Failed to delete conflicting consumer %s: %v", verdictDurable, err)
				}
			} else {
				log.Printf("Consumer '%s' exists with matching config, reusing.", verdictDurable)
			}
		} else if err != nats.ErrConsumerNotFound {
			log.Fatalf("Failed to query consumer info for %s: %v", verdictDurable, err)
		}
	}

	_, err = js.QueueSubscribe("tasks.governance.verdict", "sched-cluster", func(m *nats.Msg) {
		// Only the leader should process
		if !st.IsLeader() {
			// Don't Nak - let another node handle it, or delay
			m.NakWithDelay(2 * time.Second)
			return
		}

		var v Verdict
		if err := json.Unmarshal(m.Data, &v); err != nil {
			log.Printf("ERR: Invalid verdict JSON: %v", err)
			m.Term() // Terminate - bad message
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

			// FIX 1: Record AI insight BEFORE retry (for audit trail)
			task.AiInsight = fmt.Sprintf("AI Recommendation: %s - %s", v.Decision, v.Reason)
			if err := st.Set(task); err != nil {
				log.Printf("ERR: Failed to persist AI insight for %s: %v", v.TaskID, err)
			}

			// FIX 2: Transition to PENDING first (for complete audit trail)
			if err := st.TransitionState(v.TaskID, pb.TaskState_PENDING); err != nil {
				log.Printf("ERR: Failed to transition %s to PENDING: %v", v.TaskID, err)
			}

			// Increment retry count AFTER state transition
			if _, err := st.IncrementRetry(v.TaskID); err != nil {
				log.Printf("ERR: Failed to increment retry: %v", err)
				m.Ack()
				return
			}

			if _, err := js.Publish("tasks.scheduled", []byte(v.TaskID)); err != nil {
				log.Printf("ERR: Failed to republish task %s: %v", v.TaskID, err)
				m.Ack()
				return
			}

			log.Printf("   -> Task re-queued automatically.")
		} else {
			// FIX 1: Record AI insight for STOP decision too
			task.AiInsight = fmt.Sprintf("AI Recommendation: %s - %s", v.Decision, v.Reason)
			if err := st.Set(task); err != nil {
				log.Printf("ERR: Failed to persist AI insight for %s: %v", v.TaskID, err)
			}
			
			st.TransitionState(v.TaskID, pb.TaskState_FAILED)
			log.Printf("   -> Task HALTED to save costs.")
		}
		m.Ack()
	}, nats.Durable("verdict-processor"), nats.AckExplicit(), nats.MaxDeliver(5), nats.DeliverNew())
	if err != nil {
		log.Fatalf("Failed to subscribe to verdict events: %v", err)
	}

	// C. Failure Events
	_, err = js.Subscribe("tasks.events.failed", func(m *nats.Msg) {
		if !st.IsLeader() {
			log.Printf("⚠️  Not leader, deferring failure event")
			return
		}

		// Parse the JSON payload from the worker
		var payload struct {
			TaskID string `json:"task_id"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(m.Data, &payload); err != nil {
			// Fallback for raw ID messages (backward compatibility)
			payload.TaskID = string(m.Data)
			log.Printf("⚠️  Failed to parse JSON, treating as raw task ID: %s", payload.TaskID)
		}

		log.Printf("EVENT: Received FAILURE for %s (Error: %s)", payload.TaskID, payload.Error)

		task, err := st.Get(payload.TaskID)
		if err != nil {
			log.Printf("ERR: Failed to get task %s: %v", payload.TaskID, err)
			m.Ack() // Ack to avoid infinite retry on missing task
			return
		}

		// LOGIC SWITCH
		switch task.Mode {
		case pb.GovernanceMode_ADVISORY_ONLY:
			log.Printf("[ADVISORY] Task %s failed. Manual intervention required.", payload.TaskID)
			// No automatic retry - just log and wait for manual action
		case pb.GovernanceMode_AUTONOMOUS:
			log.Printf("[AUTONOMOUS] Task %s failed. Moving to ANALYZING state...", payload.TaskID)
			// Transition to ANALYZING - AI agent will handle the retry decision
			if err := st.TransitionState(payload.TaskID, pb.TaskState_ANALYZING); err != nil {
				log.Printf("ERR: Failed to update task %s to ANALYZING: %v", payload.TaskID, err)
			}
			// DO NOT publish to tasks.scheduled here - wait for AI verdict
		case pb.GovernanceMode_HUMAN_GATE:
			log.Printf("[HUMAN_GATE] Task %s failed. Awaiting approval...", payload.TaskID)
			if err := st.TransitionState(payload.TaskID, pb.TaskState_NEEDS_APPROVAL); err != nil {
				log.Printf("ERR: Failed to update task %s to NEEDS_APPROVAL: %v", payload.TaskID, err)
			}
		default:
			log.Printf("ERR: Unknown governance mode for task %s", payload.TaskID)
		}
		m.Ack()
	}, nats.DeliverNew())
	if err != nil {
		log.Fatalf("Failed to subscribe to failure events: %v", err)
	}

	log.Println("✅ All NATS subscriptions active")

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

	log.Printf("✅ Scheduler [%s] Ready - gRPC on %s, Raft on %s", *nodeID, *grpcPort, *raftAddr)

	// Keep main goroutine alive
	select {}
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

// waitForClusterReady blocks until a leader exists in the cluster (or timeout)
func waitForClusterReady(st *store.Store, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		// Check if there's a leader in the cluster (not if WE are the leader)
		leaderAddr := st.GetLeaderAddr()
		if leaderAddr != "" {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for cluster leader")
		}

		<-ticker.C
	}
}

func extractField(content, key string) string {
	lines := strings.Split(content, "\n")
	key = strings.TrimSpace(key)
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, key) {
			continue
		}

		value := strings.TrimSpace(strings.TrimPrefix(trimmed, key))
		value = strings.Trim(value, "\"'")

		if value == "" || value == "|" {
			if i+1 < len(lines) {
				next := strings.TrimSpace(lines[i+1])
				return strings.Trim(next, "\"'")
			}
			return ""
		}

		return value
	}
	return ""
}

// ADD: Helper to join cluster via gRPC
func joinCluster(leaderAddr, nodeID, raftAddr string) error {
	// Load client certs (same TLS setup as server)
	certFile := os.Getenv("SCHED_CERT_FILE")
	keyFile := os.Getenv("SCHED_KEY_FILE")
	caFile := os.Getenv("SCHED_CA_FILE")

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}

	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		return err
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caBytes)

	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      certPool,
	})

	conn, err := grpc.Dial(leaderAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewSchedServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Add auth token to context
	token := os.Getenv("ASTRA_AUTH_TOKEN")
	if token == "" {
		token = "my-secret-key"
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "auth-token", token)

	resp, err := client.JoinCluster(ctx, &pb.JoinRequest{
		NodeId:  nodeID,
		Address: raftAddr,
	})
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("join rejected: %s", resp.Message)
	}

	return nil
}