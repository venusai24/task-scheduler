package executor

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	pb "github.com/venusai24/task-scheduler/proto"
)

// MockJetStream implements nats.JetStreamContext partially for testing
type MockJetStream struct {
	nats.JetStreamContext
	PublishedSubject string
	PublishedData    []byte
}

func (m *MockJetStream) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	m.PublishedSubject = subj
	m.PublishedData = data
	return &nats.PubAck{}, nil
}

func TestSubmitTask_NATS(t *testing.T) {
	mockJS := &MockJetStream{}
	exec := NewNatsExecutor(mockJS)

	// Test case 1: Broadcast (No Assigned Node)
	task1 := &pb.Task{Id: "task-1"}
	if err := exec.SubmitTask(context.Background(), task1); err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}

	if mockJS.PublishedSubject != "tasks.scheduled" {
		t.Errorf("Expected subject tasks.scheduled, got %s", mockJS.PublishedSubject)
	}
	if string(mockJS.PublishedData) != "task-1" {
		t.Errorf("Expected data task-1, got %s", string(mockJS.PublishedData))
	}

	// Test case 2: Targeted (Assigned Node)
	task2 := &pb.Task{Id: "task-2", AssignedNode: "worker-X"}
	if err := exec.SubmitTask(context.Background(), task2); err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}

	if mockJS.PublishedSubject != "tasks.scheduled.worker-X" {
		t.Errorf("Expected subject tasks.scheduled.worker-X, got %s", mockJS.PublishedSubject)
	}
}
