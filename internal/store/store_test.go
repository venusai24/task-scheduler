package store

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/venusai24/task-scheduler/proto"
)

// MockLogStorage for testing
type MockLogStorage struct {
	logs map[string][]string
}

func NewMockLogStorage() *MockLogStorage {
	return &MockLogStorage{
		logs: make(map[string][]string),
	}
}

func (m *MockLogStorage) AppendLog(taskID, msg string) error {
	m.logs[taskID] = append(m.logs[taskID], msg)
	return nil
}

func (m *MockLogStorage) GetLogs(taskID string) ([]string, error) {
	return m.logs[taskID], nil
}

func setupTestStore(t *testing.T) (*Store, func()) {
	// 1. Create In-Memory Stores
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapStore := raft.NewInmemSnapshotStore()

	// 2. Create In-Memory Transport
	_, transport := raft.NewInmemTransport(raft.ServerAddress("localhost:0"))

	// 3. Create Scheduler Store
	mockLogStorage := NewMockLogStorage()
	s := NewStore(mockLogStorage)

	// 4. Config
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID("test-node")
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond

	// 5. Create Raft
	ra, err := raft.NewRaft(config, s, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("Failed to create raft: %v", err)
	}

	s.SetRaft(ra)

	// 6. Bootstrap
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}
	ra.BootstrapCluster(configuration)

	// Wait for leader
	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for leader")
		default:
			if ra.State() == raft.Leader {
				goto Ready
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

Ready:
	return s, func() {
		// Cleanup
		ra.Shutdown()
	}
}

// MockSnapshotSink implementation
type MockSnapshotSink struct {
	*raft.DiscardSnapshotSink
	id       string
	contents []byte
}

func NewMockSnapshotSink(id string) *MockSnapshotSink {
	return &MockSnapshotSink{
		DiscardSnapshotSink: &raft.DiscardSnapshotSink{},
		id:                  id,
	}
}

func (m *MockSnapshotSink) Write(p []byte) (n int, err error) {
	m.contents = append(m.contents, p...)
	return len(p), nil
}

func (m *MockSnapshotSink) ID() string {
	return m.id
}

func (m *MockSnapshotSink) Cancel() error {
	return nil
}

func (m *MockSnapshotSink) Close() error {
	return nil
}

// Reader returns a ByteReader for contents
type ByteReader struct {
	data   []byte
	offset int64
}

func (b *ByteReader) Read(p []byte) (n int, err error) {
	if b.offset >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n = copy(p, b.data[b.offset:])
	b.offset += int64(n)
	return n, nil
}

func (b *ByteReader) Close() error {
	return nil
}

func TestStore_SetGet(t *testing.T) {
	s, cleanup := setupTestStore(t)
	defer cleanup()

	task := &pb.Task{
		Id:    "task-1",
		State: pb.TaskState_CREATED,
	}

	if err := s.Set(task); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify Get
	got, err := s.Get("task-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Id != task.Id || got.State != task.State {
		t.Errorf("Mismatch. Got %+v, want %+v", got, task)
	}
}

func TestStore_TransitionState(t *testing.T) {
	s, cleanup := setupTestStore(t)
	defer cleanup()

	task := &pb.Task{
		Id:    "task-2",
		State: pb.TaskState_CREATED,
	}
	s.Set(task)

	if err := s.TransitionState("task-2", pb.TaskState_RUNNING, "Started"); err != nil {
		t.Fatalf("TransitionState failed: %v", err)
	}

	got, _ := s.Get("task-2")
	if got.State != pb.TaskState_RUNNING {
		t.Errorf("Expected RUNNING, got %s", got.State)
	}

	// Verify Logs
	logs, _ := s.GetLogs("task-2")
	if len(logs) == 0 {
		t.Error("Expected logs, got none")
	} else {
		if logs[0] != "Started" {
			t.Errorf("Expected log 'Started', got '%s'", logs[0])
		}
	}
}

func TestStore_RetryLogic(t *testing.T) {
	s, cleanup := setupTestStore(t)
	defer cleanup()

	task := &pb.Task{
		Id:         "task-retry",
		State:      pb.TaskState_FAILED,
		RetryCount: 0,
	}
	s.Set(task)

	newCount, err := s.IncrementRetry("task-retry")
	if err != nil {
		t.Fatalf("IncrementRetry failed: %v", err)
	}
	if newCount != 1 {
		t.Errorf("Expected retry count 1, got %d", newCount)
	}

	got, _ := s.Get("task-retry")
	if got.State != pb.TaskState_PENDING {
		t.Errorf("Expected PENDING after retry, got %s", got.State)
	}
}

func TestStore_Rollback(t *testing.T) {
	s, cleanup := setupTestStore(t)
	defer cleanup()

	task := &pb.Task{
		Id:         "task-rollback",
		State:      pb.TaskState_FAILED,
		RetryCount: 5,
		AiInsight:  "Bad logic",
	}
	s.Set(task)

	if err := s.Rollback("task-rollback"); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	got, _ := s.Get("task-rollback")
	if got.State != pb.TaskState_CREATED {
		t.Errorf("Expected CREATED, got %s", got.State)
	}
	if got.RetryCount != 0 {
		t.Errorf("Expected RetryCount 0, got %d", got.RetryCount)
	}
	if got.AiInsight != "" {
		t.Errorf("Expected empty AI insight, got %s", got.AiInsight)
	}
}

func TestStore_GetDependentTasks(t *testing.T) {
	s, cleanup := setupTestStore(t)
	defer cleanup()

	parent := &pb.Task{Id: "parent"}
	s.Set(parent)

	child1 := &pb.Task{Id: "c1", DependsOn: "parent", State: pb.TaskState_AWAITING_PREREQUISITE}
	s.Set(child1)

	child2 := &pb.Task{Id: "c2", DependsOn: "parent", State: pb.TaskState_CREATED} // Not awaiting
	s.Set(child2)

	child3 := &pb.Task{Id: "c3", DependsOn: "other", State: pb.TaskState_AWAITING_PREREQUISITE}
	s.Set(child3)

	dependents := s.GetDependentTasks("parent")
	if len(dependents) != 1 {
		t.Errorf("Expected 1 dependent, got %d", len(dependents))
	} else if dependents[0].Id != "c1" {
		t.Errorf("Expected c1, got %s", dependents[0].Id)
	}
}

func TestStore_SnapshotRestore(t *testing.T) {
	s, cleanup := setupTestStore(t)
	defer cleanup()

	// Fill data
	for i := 0; i < 10; i++ {
		s.Set(&pb.Task{Id: fmt.Sprintf("t-%d", i), State: pb.TaskState_CREATED})
	}

	// Trigger Snapshot manually
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Create sink
	sink := NewMockSnapshotSink("test-snap")
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}
	sink.Close()

	// Restore
	reader := &ByteReader{data: sink.contents}

	// Create blank store
	s2 := NewStore(NewMockLogStorage())
	if err := s2.Restore(io.NopCloser(reader)); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify s2 has data
	if len(s2.tasks) != 10 {
		t.Errorf("Expected 10 tasks in restored store, got %d", len(s2.tasks))
	}
	if _, ok := s2.tasks["t-9"]; !ok {
		t.Error("Missing t-9 in restored store")
	}
}
