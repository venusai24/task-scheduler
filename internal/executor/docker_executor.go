package executor

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	pb "github.com/venusai24/task-scheduler/proto"
)

type DockerExecutor struct {
	cli   client.APIClient
	image string
}

func NewDockerExecutor() (*DockerExecutor, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}

	image := os.Getenv("WORKER_IMAGE")
	if image == "" {
		image = "astra-core:latest" // Default fallback
	}

	return &DockerExecutor{
		cli:   cli,
		image: image,
	}, nil
}

func (e *DockerExecutor) SubmitTask(ctx context.Context, task *pb.Task) error {
	taskID := task.Id
	log.Printf("🐳 [Executor-Dynamic] Spawning Container for task %s...", taskID)

	// Define container config
	config := &container.Config{
		Image: e.image,
		Cmd:   []string{"worker", "--oneshot", "--id", taskID},
		Env: []string{
			"WORKER_CERT_FILE=/app/certs/worker.crt",
			"WORKER_KEY_FILE=/app/certs/worker.key",
			"SCHED_CA_FILE=/app/certs/ca.crt",
			"ASTRA_AUTH_TOKEN=" + os.Getenv("ASTRA_AUTH_TOKEN"),
			"NATS_URL=" + os.Getenv("NATS_URL"),
			// Point to Scheduler Nodes (internal docker DNS)
			"ASTRA_SCHED_ADDRS=node-1:50051,node-2:50052,node-3:50053",
		},
	}

	hostConfig := &container.HostConfig{
		AutoRemove: true, // IMPORTANT: Cleanup after exit
		Binds: []string{
			// Mount certs so the worker can talk to Scheduler
			// Warning: This assumes the host path matches internal path in DIND capability
			"/app/certs:/app/certs:ro",
		},
	}

	// Connect to the same network as scheduler
	netConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			"astra-net":            {}, // MUST match docker-compose network name
			"task-sched_astra-net": {}, // Docker Compose often prefixes network names
		},
	}

	resp, err := e.cli.ContainerCreate(ctx, config, hostConfig, netConfig, nil, "worker-"+taskID)
	if err != nil {
		// Try to fallback if network name is different (common compose issue)
		// Or just return error
		return fmt.Errorf("failed to create container: %w", err)
	}

	if err := e.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	log.Printf("✅ Started worker container %s for task %s", resp.ID[:12], taskID)
	return nil
}
