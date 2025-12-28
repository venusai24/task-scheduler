package store

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/venusai24/task-scheduler/proto"
)

// Store holds the actual data and the Raft instance
type Store struct {
	mu    sync.RWMutex
	tasks map[string]*pb.Task // The "State" we are protecting

	raft *raft.Raft // The Consensus mechanism
}

// NewStore initializes the memory map
func NewStore() *Store {
	return &Store{
		tasks: make(map[string]*pb.Task),
	}
}

// SetRaft safely sets the Raft instance
func (s *Store) SetRaft(r *raft.Raft) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raft = r
}

// -- RAFT FSM IMPLEMENTATION --

// Apply is called by Raft when a log is committed.
func (s *Store) Apply(l *raft.Log) interface{} {
	var task pb.Task

	// We assume the log data is just the JSON of the Task
	if err := json.Unmarshal(l.Data, &task); err != nil {
		// Return error but don't panic - Raft will handle it
		return fmt.Errorf("failed to unmarshal task: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Apply the change to the local state
	s.tasks[task.Id] = &task
	return nil
}

// Snapshot is used to compact logs
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Clone the map
	clone := make(map[string]*pb.Task)
	for k, v := range s.tasks {
		clone[k] = v
	}
	return &fsmSnapshot{store: clone}, nil
}

// Restore loads the state from a snapshot
func (s *Store) Restore(rc io.ReadCloser) error {
	defer rc.Close() // Ensure the reader is always closed

	o := make(map[string]*pb.Task)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	s.mu.Lock()
	s.tasks = o
	s.mu.Unlock()

	return nil
}

// -- SNAPSHOT HELPER --

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

// Set adds a task to the distributed store
func (s *Store) Set(t *pb.Task) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	b, err := json.Marshal(t)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, 10*time.Second)
	return f.Error()
}

// TransitionState updates just the state of a task
func (s *Store) TransitionState(id string, newState pb.TaskState) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	// 1. Get current task to ensure it exists
	s.mu.RLock()
	task, exists := s.tasks[id]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", id)
	}

	// 2. Clone and Update (Immutability pattern)
	updatedTask := *task
	updatedTask.State = newState

	// 3. Commit to Raft
	b, err := json.Marshal(&updatedTask)
	if err != nil {
		return err
	}
	return s.raft.Apply(b, 10*time.Second).Error()
}

// Get reads a task (local read)
func (s *Store) Get(id string) (*pb.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.tasks[id]
	if !ok {
		return nil, fmt.Errorf("task not found")
	}
	return t, nil
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

	// Clone and Update
	updatedTask := *task
	updatedTask.RetryCount++
	updatedTask.State = pb.TaskState_PENDING // Reset to pending so it can be picked up

	// Append log to history for debugging
	updatedTask.Logs = append(updatedTask.Logs, fmt.Sprintf("Retry #%d triggered at %s", updatedTask.RetryCount, time.Now().Format(time.RFC3339)))

	b, err := json.Marshal(&updatedTask)
	if err != nil {
		return 0, err
	}

	if err := s.raft.Apply(b, 10*time.Second).Error(); err != nil {
		return 0, err
	}

	return updatedTask.RetryCount, nil
}

// Rollback resets a task to its initial state
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

	// Clone and reset to initial state
	updated := *task
	updated.State = pb.TaskState_CREATED
	updated.RetryCount = 0
	updated.Logs = append(updated.Logs, fmt.Sprintf("⏪ Task Rolled Back to Initial State at %s", time.Now().Format(time.RFC3339)))

	// Clear AI insight on rollback
	updated.AiInsight = ""

	b, err := json.Marshal(&updated)
	if err != nil {
		return err
	}

	return s.raft.Apply(b, 10*time.Second).Error()
}

// GetRaftState returns the current Raft state (Follower, Candidate, Leader, Shutdown)
func (s *Store) GetRaftState() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.raft == nil {
		return "Shutdown"
	}
	return s.raft.State().String()
}

// IsLeader returns true if this node is the Raft leader
func (s *Store) IsLeader() bool {
	return s.GetRaftState() == "Leader"
}