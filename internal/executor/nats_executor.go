package executor

import (
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	pb "github.com/venusai24/task-scheduler/proto"
)

type NatsExecutor struct {
	js nats.JetStreamContext
}

func NewNatsExecutor(js nats.JetStreamContext) *NatsExecutor {
	return &NatsExecutor{js: js}
}

func (e *NatsExecutor) SubmitTask(ctx context.Context, task *pb.Task) error {
	subject := "tasks.scheduled"
	if task.AssignedNode != "" {
		subject = fmt.Sprintf("tasks.scheduled.%s", task.AssignedNode)
		log.Printf("📥 [Executor-Static] Targeted Publish: task %s -> %s", task.Id, subject)
	} else {
		log.Printf("📥 [Executor-Static] Broadcasting task %s to shared queue...", task.Id)
	}

	_, err := e.js.Publish(subject, []byte(task.Id))
	if err != nil {
		return fmt.Errorf("failed to publish to NATS: %w", err)
	}
	return nil
}
