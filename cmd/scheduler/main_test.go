package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/venusai24/task-scheduler/internal/store"
	pb "github.com/venusai24/task-scheduler/proto"
)

// --- Mocks ---

type MockExecutor struct {
	CapturedTask *pb.Task
	SubmitError  error
}

func (m *MockExecutor) SubmitTask(ctx context.Context, task *pb.Task) error {
	m.CapturedTask = task
	return m.SubmitError
}

type MockJetStream struct {
	nats.JetStreamContext
	PublishedMsgs map[string][]byte
}

func (m *MockJetStream) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	if m.PublishedMsgs == nil {
		m.PublishedMsgs = make(map[string][]byte)
	}
	m.PublishedMsgs[subj] = data
	return &nats.PubAck{}, nil
}

// --- Helpers ---

func setupTestServer() (*server, *store.Store, *MockExecutor) {
	// Use an in-memory store or a temp file store
	// For simplicity in this environment, we'll rely on the fact that store.NewStore()
	// might need disk access. We'll use a temp dir.
	tmpDir := "/tmp/scheduler_test_" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Fix: create a LogStorage instance
	logStore := store.NewFileLogStore(tmpDir)

	st := store.NewStore(logStore)
	// Open with bootstrap=true to be leader
	_ = st.Open("test-node", tmpDir, "localhost:0", true)
	// Wait for leader
	time.Sleep(3 * time.Second)

	mockExec := &MockExecutor{}

	srv := &server{
		store:        st,
		js:           &MockJetStream{},
		exec:         mockExec,
		nodeRegistry: make(map[string]NodeMetrics),
	}

	return srv, st, mockExec
}

// --- Tests ---

func TestSubmitIntent_HappyPath(t *testing.T) {
	srv, st, mockExec := setupTestServer()
	defer st.Close()

	ctx := context.Background()
	req := &pb.SubmitRequest{
		YamlContent: "name: test-task\ncmd: echo hello",
		DryRun:      false,
	}

	resp, err := srv.SubmitIntent(ctx, req)
	if err != nil {
		t.Fatalf("SubmitIntent failed: %v", err)
	}

	if resp.TaskId == "" {
		t.Error("Expected TaskID, got empty")
	}

	// Verify persistence
	task, err := st.Get(resp.TaskId)
	if err != nil {
		t.Fatalf("Task not persisted: %v", err)
	}
	if task.State != pb.TaskState_CREATED {
		t.Errorf("Expected State CREATED, got %s", task.State)
	}

	// Verify Execution
	if mockExec.CapturedTask == nil {
		t.Error("Task was not submitted to executor")
	} else if mockExec.CapturedTask.Id != resp.TaskId {
		t.Errorf("Executor received wrong task ID: %s vs %s", mockExec.CapturedTask.Id, resp.TaskId)
	}
}

func TestSubmitIntent_Dependencies(t *testing.T) {
	srv, st, mockExec := setupTestServer()
	defer st.Close()
	ctx := context.Background()

	// 1. Create Parent Task
	parentReq := &pb.SubmitRequest{YamlContent: "name: parent"}
	parentResp, err := srv.SubmitIntent(ctx, parentReq)
	if err != nil {
		t.Fatalf("Failed to submit parent task: %v", err)
	}

	// Reset mock to ensure we don't count parent execution
	mockExec.CapturedTask = nil

	// 2. Create Child Task depending on Parent
	childYaml := fmt.Sprintf("name: child\ndepends_on: %s", parentResp.TaskId)
	childReq := &pb.SubmitRequest{YamlContent: childYaml}

	childResp, err := srv.SubmitIntent(ctx, childReq)
	if err != nil {
		t.Fatalf("SubmitIntent failed: %v", err)
	}

	// 3. Verify Child is HELD
	task, _ := st.Get(childResp.TaskId)
	if task.State != pb.TaskState_AWAITING_PREREQUISITE {
		t.Errorf("Expected AWAITING_PREREQUISITE, got %s", task.State)
	}

	if mockExec.CapturedTask != nil {
		t.Error("Child task should NOT be executed yet")
	}

	// 4. Simulate Parent Completion (Manually transition parent)
	st.TransitionState(parentResp.TaskId, pb.TaskState_COMPLETED, "Done")

	// NOTE: unique logic in main.go relies on NATS events 'tasks.events.completed'
	// to trigger the dependency check.
	// Since we are unit testing the Logic, we might need to invoke the logic manually or refactor.
	// Looking at main.go: The event handler calls `st.GetDependentTasks` and then execs them.
	// We can test `st.GetDependentTasks` behaves or simulate the event handler logic here.

	dependents := st.GetDependentTasks(parentResp.TaskId)
	if len(dependents) != 1 {
		t.Errorf("Expected 1 dependent, got %d", len(dependents))
	} else {
		if dependents[0].Id != childResp.TaskId {
			t.Errorf("Expected dependent %s, got %s", childResp.TaskId, dependents[0].Id)
		}
	}
}

func TestSubmitIntent_Governance(t *testing.T) {
	srv, st, _ := setupTestServer()
	defer st.Close()
	ctx := context.Background()

	tests := []struct {
		name         string
		yaml         string
		expectedMode pb.GovernanceMode
	}{
		{"Default", "name: task1", pb.GovernanceMode_ADVISORY_ONLY},
		{"HumanGate", "name: task2\nmode: HUMAN_GATE", pb.GovernanceMode_HUMAN_GATE},
		{"Autonomous", "name: task3\nmode: \"AUTONOMOUS\"", pb.GovernanceMode_AUTONOMOUS},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := srv.SubmitIntent(ctx, &pb.SubmitRequest{YamlContent: tt.yaml})
			if err != nil {
				t.Fatalf("SubmitIntent failed: %v", err)
			}
			task, _ := st.Get(resp.TaskId)
			if task.Mode != tt.expectedMode {
				t.Errorf("Expected mode %v, got %v", tt.expectedMode, task.Mode)
			}
		})
	}
}

func TestSelectBestNode(t *testing.T) {
	srv, st, _ := setupTestServer()
	defer st.Close()

	// Manually populate registry
	srv.nodeRegistry["node-high-load"] = NodeMetrics{
		NodeID: "node-high-load", CPUPercent: 90, MemoryPercent: 90,
	} // Score: 10 + 10 = 20
	srv.nodeRegistry["node-low-load"] = NodeMetrics{
		NodeID: "node-low-load", CPUPercent: 10, MemoryPercent: 10,
	} // Score: 90 + 90 = 180

	best := srv.selectBestNode()
	if best != "node-low-load" {
		t.Errorf("Expected node-low-load, got %s", best)
	}

	// Edge case: No nodes
	srv.nodeRegistry = make(map[string]NodeMetrics)
	best = srv.selectBestNode()
	if best != "" {
		t.Errorf("Expected empty string for no nodes, got %s", best)
	}
}

func TestApproveTask(t *testing.T) {
	srv, st, mockExec := setupTestServer()
	defer st.Close()
	ctx := context.Background()

	// Create task in NEEDS_APPROVAL (simulate failure first or just set it)
	task := &pb.Task{
		Id:    "task-approval",
		State: pb.TaskState_NEEDS_APPROVAL,
		Mode:  pb.GovernanceMode_HUMAN_GATE,
	}
	st.Set(task)

	// Approve
	resp, err := srv.ApproveTask(ctx, &pb.ApproveRequest{TaskId: "task-approval"})
	if err != nil {
		t.Fatalf("ApproveTask failed: %v", err)
	}
	if !resp.Success {
		t.Errorf("Approve failed: %s", resp.Message)
	}

	// Verify Execution
	if mockExec.CapturedTask == nil || mockExec.CapturedTask.Id != "task-approval" {
		t.Error("Approved task was not re-submitted")
	}
}
