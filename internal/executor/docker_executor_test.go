package executor

import (
	"context"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	pb "github.com/venusai24/task-scheduler/proto"
)

// MockDockerClient implements client.APIClient for testing
type MockDockerClient struct {
	client.APIClient
	CreatedContainerName string
	StartedContainerID   string
}

func (m *MockDockerClient) ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error) {
	m.CreatedContainerName = containerName
	return container.CreateResponse{ID: "mock-container-id"}, nil
}

func (m *MockDockerClient) ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error {
	m.StartedContainerID = containerID
	return nil
}

func TestSubmitTask_Docker(t *testing.T) {
	mockCli := &MockDockerClient{}

	// Inject mock client manually since NewDockerExecutor uses real client
	exec := &DockerExecutor{
		cli:   mockCli,
		image: "test-image",
	}

	task := &pb.Task{Id: "task-123"}

	if err := exec.SubmitTask(context.Background(), task); err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}

	if mockCli.CreatedContainerName != "worker-task-123" {
		t.Errorf("Expected container name worker-task-123, got %s", mockCli.CreatedContainerName)
	}

	if mockCli.StartedContainerID != "mock-container-id" {
		t.Errorf("Expected started container ID mock-container-id, got %s", mockCli.StartedContainerID)
	}
}
