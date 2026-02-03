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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"sync"

	"github.com/venusai24/task-scheduler/internal/executor" // <--- ADD THIS
	"github.com/venusai24/task-scheduler/internal/store"
	pb "github.com/venusai24/task-scheduler/proto"
)

// Verdict matches the JSON sent by the Python Agent
type Verdict struct {
	TaskID   string `json:"task_id"`
	Decision string `json:"decision"` // "RETRY" or "STOP"
	Reason   string `json:"reason"`
}

type NodeMetrics struct {
	NodeID        string  `json:"node_id"`
	CPUPercent    float64 `json:"cpu_percent"`
	MemoryPercent float64 `json:"memory_percent"`
	LastSeen      time.Time
}

type server struct {
	pb.UnimplementedSchedServiceServer
	store        *store.Store
	js           nats.JetStreamContext
	exec         executor.Executor
	nodeRegistry map[string]NodeMetrics
	mu           sync.RWMutex
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
		// SCORE NODES
		bestNode := s.selectBestNode()
		if bestNode != "" {
			task.AssignedNode = bestNode
			log.Printf("🎯 Scoring Algorithm selected %s for task %s", bestNode, taskID)
		} else {
			log.Println("⚠️  No suitable nodes found (or no heatbeats yet). Broadcasting to all.")
		}

		// USE EXECUTOR INTERFACE
		if err := s.exec.SubmitTask(ctx, task); err != nil {
			log.Printf("ERR: Failed to submit task %s: %v", task.Id, err)
			return nil, fmt.Errorf("failed to schedule task: %v", err)
		}
		log.Printf("Task %s scheduled via Executor!", task.Id)
	} else {
		log.Printf("Task %s persisted but HELD (waiting for %s)", task.Id, dependsOn)
	}

	return &pb.SubmitResponse{TaskId: taskID}, nil
}

func (s *server) GetTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	// Follower Read: No "IsLeader" check needed here!
	// Any node can answer from its local FSM state.
	task, err := s.store.Get(req.TaskId)
	if err != nil {
		return nil, err
	}
	return &pb.TaskResponse{Task: task}, nil
}

func (s *server) GetTaskLogs(ctx context.Context, req *pb.LogRequest) (*pb.LogResponse, error) {
	logs, err := s.store.GetLogs(req.TaskId)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch logs: %v", err)
	}
	return &pb.LogResponse{Logs: logs}, nil
}

// Add the ApproveTask handler to the gRPC server implementation:
func (s *server) ApproveTask(ctx context.Context, req *pb.ApproveRequest) (*pb.ApproveResponse, error) {
	if req.TaskId == "" {
		return &pb.ApproveResponse{Success: false, Message: "task_id is required"}, nil
	}
	if _, err := s.store.IncrementRetry(req.TaskId); err != nil {
		return &pb.ApproveResponse{Success: false, Message: fmt.Sprintf("Failed: %v", err)}, nil
	}
	// Re-fetch task to get latest state/assigned node if any (though we might want to re-score)
	task, err := s.store.Get(req.TaskId)
	if err != nil {
		return &pb.ApproveResponse{Success: false, Message: fmt.Sprintf("Task not found: %v", err)}, nil
	}
	// Re-score on approval? simpler to just resubmit
	// Update: We should re-assess the node in case the old one is dead, but strictly speaking
	// we can just pass the task. If AssignedNode is old, it might fail.
	// Let's re-score if it was assigned.
	if task.AssignedNode != "" {
		// Optional: clear it to allow re-scoring or keep it.
		// For now, let's clear it to allow fresh scoring if logic allows,
		// but wait, SubmitTask in interface takes task.
		// If we want to re-score, we need to do it here.
		bestNode := s.selectBestNode()
		if bestNode != "" {
			task.AssignedNode = bestNode
		}
	}

	if err := s.exec.SubmitTask(ctx, task); err != nil {
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
	// 1. Connect to NATS (with Retry)
	var nc *nats.Conn
	var err error
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	for i := 0; i < 30; i++ {
		if natsToken != "" {
			nc, err = nats.Connect(natsURL, nats.Token(natsToken))
		} else {
			nc, err = nats.Connect(natsURL)
		}

		if err == nil {
			log.Println("✅ Connected to NATS")
			break
		}
		log.Printf("⚠️  Failed to connect to NATS (Attempt %d/30): %v. Retrying in 2s...", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatal("❌ NATS Connection failed after retries: ", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to init JetStream: %v", err)
	}
	log.Println("Connected to NATS JetStream.")

	// FIX: Create NATS streams with valid names (no dots in stream names)
	streamConfigs := []struct {
		name     string
		subjects []string
	}{
		{"TASKS", []string{"tasks.scheduled", "tasks.scheduled.>"}}, // Cover both broadcast and targeted
		{"TASKS_EVENTS", []string{"tasks.events.>"}},
		{"TASKS_GOVERNANCE", []string{"tasks.governance.>"}},
		{"SCHEDULER_HEARTBEATS", []string{"scheduler.heartbeats"}},
	}

	for _, cfg := range streamConfigs {
		_, err := js.StreamInfo(cfg.name)
		if err != nil {
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     cfg.name,
				Subjects: cfg.subjects,
			})
			if err != nil {
				log.Fatalf("Failed to create stream %s: %v", cfg.name, err)
			}
			log.Printf("Created stream: %s (subjects: %v)", cfg.name, cfg.subjects)
		}
	}

	// FIX: Use node-specific data directory
	dataDir := filepath.Join("./data", *nodeID)
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}

	// Initialize Log Storage (Filesystem by default)
	logStore := store.NewFileLogStore(dataDir)

	st := store.NewStore(logStore)
	if err := st.Open(*nodeID, dataDir, *raftAddr, *bootstrap); err != nil {
		log.Fatalf("Failed to open Raft store: %v", err)
	}
	defer st.Close()

	log.Printf("Raft storage started on %s", *raftAddr)

	// --- NATS KV Node Registry Setup ---
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "scheduler_nodes",
		TTL:    45 * time.Second, // Auto-expire nodes after 45s of silence
	})
	if err != nil {
		log.Fatalf("Failed to create KV bucket: %v", err)
	}

	// Watch for changes to update local registry cache
	watcher, err := kv.WatchAll()
	if err != nil {
		log.Fatalf("Failed to watch KV bucket: %v", err)
	}
	defer watcher.Stop()

	// Registry Mirror (Background Sync)
	// We need to initialize srv before we can use it in the goroutine,
	// but srv needs other things. So we set up srv first, then start the loop.

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

	// 1.5 Initialize Executor
	var execStrategy executor.Executor
	strategyEnv := os.Getenv("EXECUTION_STRATEGY")
	if strategyEnv == "DOCKER" {
		log.Println("🐳 Using DOCKER execution strategy (Dynamic Scaling)")
		dockerExec, err := executor.NewDockerExecutor()
		if err != nil {
			log.Fatalf("Failed to init Docker executor: %v", err)
		}
		execStrategy = dockerExec
	} else {
		log.Println("📥 Using NATS execution strategy (Static Workers)")
		execStrategy = executor.NewNatsExecutor(js)
	}

	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
		grpc.UnaryInterceptor(authInterceptor),
	)

	srv := &server{
		store:        st,
		js:           js,
		exec:         execStrategy,
		nodeRegistry: make(map[string]NodeMetrics),
	}
	pb.RegisterSchedServiceServer(grpcServer, srv)

	// Start KV Watcher Loop
	go func() {
		for entry := range watcher.Updates() {
			if entry == nil {
				continue
			}

			nodeID := entry.Key()

			if entry.Operation() == nats.KeyValueDelete || entry.Operation() == nats.KeyValuePurge {
				srv.mu.Lock()
				delete(srv.nodeRegistry, nodeID)
				srv.mu.Unlock()
				log.Printf("📉 Node %s removed from registry (expired/deleted)", nodeID)
				continue
			}

			// Parse Value
			var m NodeMetrics
			if err := json.Unmarshal(entry.Value(), &m); err != nil {
				log.Printf("ERR: Failed to unmarshal registry update for %s: %v", nodeID, err)
				continue
			}

			srv.mu.Lock()
			srv.nodeRegistry[nodeID] = m
			srv.mu.Unlock()
		}
	}()

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

		var payload struct {
			TaskID string `json:"task_id"`
			Logs   string `json:"logs"`
		}
		// Try parsing JSON first
		if err := json.Unmarshal(m.Data, &payload); err != nil {
			// Backward compatibility: raw string ID
			payload.TaskID = string(m.Data)
			payload.Logs = "(No logs provided)"
		}

		log.Printf("EVENT: Received Completion for %s", payload.TaskID)

		// WRITE STATUS + LOGS
		if err := st.TransitionState(payload.TaskID, pb.TaskState_COMPLETED, payload.Logs); err != nil {
			log.Printf("ERR: Failed to transition state for %s: %v", payload.TaskID, err)
			return
		}

		// 🚀 TRIGGER DEPENDENTS
		dependents := st.GetDependentTasks(payload.TaskID)
		if len(dependents) > 0 {
			log.Printf("🔗 Found %d dependent tasks waiting for %s", len(dependents), payload.TaskID)
			for _, dep := range dependents {
				log.Printf("   -> Unleashing Dependent Task: %s", dep.Id)

				// 1. Update State in Raft
				if err := st.TransitionState(dep.Id, pb.TaskState_PENDING, "Dependency met. Activated."); err != nil {
					log.Printf("ERR: Failed to transition dependent task %s: %v", dep.Id, err)
					continue
				}

				if err := execStrategy.SubmitTask(context.Background(), dep); err != nil {
					log.Printf("ERR: Failed to publish dependent task %s: %v", dep.Id, err)
				}
			}
		}

		m.Ack()
	}, nats.DeliverNew())
	if err != nil {
		log.Fatalf("Failed to subscribe to completed events: %v", err)
	}

	// ... [Verdict Processor section omitted for brevity, logic remains similar] ...

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
			payload.TaskID = string(m.Data)
			payload.Error = "Unknown error (parsing failed)"
			log.Printf("⚠️  Failed to parse JSON, treating as raw task ID: %s", payload.TaskID)
		}

		log.Printf("EVENT: Received FAILURE for %s (Error: %s)", payload.TaskID, payload.Error)

		task, err := st.Get(payload.TaskID)
		if err != nil {
			log.Printf("ERR: Failed to get task %s: %v", payload.TaskID, err)
			m.Ack()
			return
		}

		// LOGIC SWITCH
		switch task.Mode {
		case pb.GovernanceMode_ADVISORY_ONLY:
			log.Printf("[ADVISORY] Task %s failed. Manual intervention required.", payload.TaskID)
			st.TransitionState(payload.TaskID, pb.TaskState_FAILED, fmt.Sprintf("FAILED: %s", payload.Error))

		case pb.GovernanceMode_AUTONOMOUS:
			log.Printf("[AUTONOMOUS] Task %s failed. Moving to ANALYZING state...", payload.TaskID)
			// Transition to ANALYZING - AI agent will handle the retry decision
			if err := st.TransitionState(payload.TaskID, pb.TaskState_ANALYZING, fmt.Sprintf("FAILED: %s. Analyzing...", payload.Error)); err != nil {
				log.Printf("ERR: Failed to update task %s to ANALYZING: %v", payload.TaskID, err)
			}
			// DO NOT publish to tasks.scheduled here - wait for AI verdict
		case pb.GovernanceMode_HUMAN_GATE:
			log.Printf("[HUMAN_GATE] Task %s failed. Awaiting approval...", payload.TaskID)
			if err := st.TransitionState(payload.TaskID, pb.TaskState_NEEDS_APPROVAL, fmt.Sprintf("FAILED: %s. Waiting for approval...", payload.Error)); err != nil {
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

	// D. Heartbeat Listener
	_, err = js.Subscribe("scheduler.heartbeats", func(m *nats.Msg) {
		var metrics NodeMetrics
		if err := json.Unmarshal(m.Data, &metrics); err != nil {
			log.Printf("ERR: Invalid heartbeat JSON: %v", err)
			return
		}
		metrics.LastSeen = time.Now()

		// Persist to NATS KV (Watcher will update local Map)
		val, _ := json.Marshal(metrics)
		if _, err := kv.Put(metrics.NodeID, val); err != nil {
			log.Printf("ERR: Failed to put node %s to KV: %v", metrics.NodeID, err)
		}

		// Debug log (throttled)
		// log.Printf("💓 Heartbeat processed for %s", metrics.NodeID)
		m.Ack()
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to heartbeats: %v", err)
	}

	log.Println("✅ All NATS subscriptions active")

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down gracefully...")
		grpcServer.GracefulStop()
		watcher.Stop() // Stop watcher
		st.Close()
		nc.Close()
		os.Exit(0)
	}()

	log.Printf("✅ Scheduler [%s] Ready - gRPC on %s, Raft on %s", *nodeID, *grpcPort, *raftAddr)

	// Keep main goroutine alive
	select {}
}

// selectBestNode implements Ranked-Choice Voting
// It scores nodes based on availability (Lower Load = Higher Score)
func (s *server) selectBestNode() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	type candidate struct {
		id    string
		score float64
	}

	var candidates []candidate
	// NATS KV TTL handles pruning, so we just iterate what we have in the map
	// The map is kept in sync by the KV Watcher.

	for id, m := range s.nodeRegistry {
		// Simple Scoring: Score = (100 - CPU) + (100 - Mem)
		// Higher is better.
		score := (100 - m.CPUPercent) + (100 - m.MemoryPercent)
		candidates = append(candidates, candidate{id: id, score: score})
	}

	if len(candidates) == 0 {
		return ""
	}

	// Sort Descending
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	// Return top candidate
	return candidates[0].id
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
