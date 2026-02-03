package executor

import (
	"context"

	pb "github.com/venusai24/task-scheduler/proto"
)

// Executor defines how a task intention is executed.
// It abstracts between "Sending to NATS Queue" (Static) and "Spawning a Container" (Dynamic).
type Executor interface {
	// SubmitTask schedules a task for execution.
	// For NATS: Publishes ID to JetStream.
	// For Docker: Starts a container with --oneshot --id=taskID.
	SubmitTask(ctx context.Context, task *pb.Task) error
}
