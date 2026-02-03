package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	pb "github.com/venusai24/task-scheduler/proto"
	"go.etcd.io/bbolt"
)

// Store holds the actual data and the Raft instance
type Store struct {
	mu          sync.RWMutex
	db          *bbolt.DB
	raft        *raft.Raft
	logStore    *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
	transport   *raft.NetworkTransport
	localID     raft.ServerID
	tasks       map[string]*pb.Task

	// Separate store for heavy logs
	logStorage LogStorage

	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

// NewStore initializes the memory map
// NewStore initializes the memory map
func NewStore(logStorage LogStorage) *Store {
	return &Store{
		tasks:      make(map[string]*pb.Task),
		logStorage: logStorage,
	}
}

// SetRaft safely sets the Raft instance
func (s *Store) SetRaft(r *raft.Raft) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raft = r
}

// Open initializes the Raft node
// Note: This replaces the Open in raft.go if they share package,
// but Go doesn't allow duplicate method declarations across files in same package if not careful.
// Wait, methods are attached to types. Splitting methods across files is fine.
// BUT `Open` was in `raft.go`. If I redefine it here, it works ONLY if I remove it from `raft.go` or if `raft.go` is deleted.
// The user confirmed `raft.go` is part of the package. I should NOT redefine `Open` here if it's already in `raft.go`.
// Let me CHECK `raft.go` content again. `Open` IS in `raft.go`.
// So I should NOT include `Open` and `Close` and `startFailureDetector` in this file if they are in `raft.go`.
// I will REMOVE them from this file `store.go` and let `raft.go` handle the Raft lifecycle.
// I only need to ensure `Store` struct fields match what `raft.go` expects.

// Re-checking `raft.go` view...
// `raft.go` uses `s.transport`, `s.localID`, `s.shutdownCtx`.
// My new `Store` struct definition includes these.
// So I should NOT paste `Open`, `Close`, `startFailureDetector`, `AddVoter`, `RemoveServer` here.
// I will stick to the FSM and Data access methods.

// Apply is called by Raft when a log is committed.
func (s *Store) Apply(l *raft.Log) interface{} {
	var task pb.Task
	if err := json.Unmarshal(l.Data, &task); err != nil {
		return fmt.Errorf("failed to unmarshal task: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.Id] = &task
	return nil
}

// Snapshot ...
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	clone := make(map[string]*pb.Task)
	for k, v := range s.tasks {
		clone[k] = v
	}
	return &fsmSnapshot{store: clone}, nil
}

// Restore ...
func (s *Store) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	o := make(map[string]*pb.Task)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}
	s.mu.Lock()
	s.tasks = o
	s.mu.Unlock()
	return nil
}

type fsmSnapshot struct {
	store map[string]*pb.Task
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
	}
	return err
}

func (f *fsmSnapshot) Release() {}

// -- PUBLIC API --

func (s *Store) Set(t *pb.Task) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return s.raft.Apply(b, 10*time.Second).Error()
}

func (s *Store) TransitionState(id string, newState pb.TaskState, logMsg string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	if logMsg != "" {
		if err := s.logStorage.AppendLog(id, logMsg); err != nil {
			fmt.Printf("ERR: Failed to write log for %s: %v\n", id, err)
		}
	}

	s.mu.RLock()
	task, exists := s.tasks[id]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", id)
	}

	updatedTask := *task
	updatedTask.State = newState
	b, err := json.Marshal(&updatedTask)
	if err != nil {
		return err
	}
	return s.raft.Apply(b, 10*time.Second).Error()
}

func (s *Store) Get(id string) (*pb.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, fmt.Errorf("task not found")
	}
	return t, nil
}

func (s *Store) GetLogs(id string) ([]string, error) {
	return s.logStorage.GetLogs(id)
}

func (s *Store) GetDependentTasks(parentID string) []*pb.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var dependents []*pb.Task
	for _, task := range s.tasks {
		if task.DependsOn == parentID && task.State == pb.TaskState_AWAITING_PREREQUISITE {
			dependents = append(dependents, task)
		}
	}
	return dependents
}

func (s *Store) IncrementRetry(id string) (int32, error) {
	if s.raft.State() != raft.Leader {
		return 0, fmt.Errorf("not leader")
	}
	s.mu.RLock()
	task, exists := s.tasks[id]
	s.mu.RUnlock()
	if !exists {
		return 0, fmt.Errorf("task not found")
	}

	updatedTask := *task
	updatedTask.RetryCount++
	updatedTask.State = pb.TaskState_PENDING

	s.logStorage.AppendLog(id, fmt.Sprintf("Retry #%d triggered", updatedTask.RetryCount))

	b, err := json.Marshal(&updatedTask)
	if err != nil {
		return 0, err
	}
	if err := s.raft.Apply(b, 10*time.Second).Error(); err != nil {
		return 0, err
	}
	return updatedTask.RetryCount, nil
}

func (s *Store) Rollback(id string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}
	s.mu.RLock()
	task, exists := s.tasks[id]
	s.mu.RUnlock()
	if !exists {
		return fmt.Errorf("task %s not found", id)
	}

	updated := *task
	updated.State = pb.TaskState_CREATED
	updated.RetryCount = 0
	updated.AiInsight = ""

	s.logStorage.AppendLog(id, "⏪ Task Rolled Back to Initial State")

	b, err := json.Marshal(&updated)
	if err != nil {
		return err
	}
	return s.raft.Apply(b, 10*time.Second).Error()
}

func (s *Store) GetLeaderAddr() string {
	return string(s.raft.Leader())
}

func (s *Store) GetRaftState() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raft == nil {
		return "Shutdown"
	}
	return s.raft.State().String()
}

func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}
