package store

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogStorage defines the interface for storing task logs.
// This allows swapping backend implementations (File, S3, Loki, etc.)
type LogStorage interface {
	AppendLog(taskID string, message string) error
	GetLogs(taskID string) ([]string, error)
}

// FileLogStore implements LogStorage using the local filesystem.
type FileLogStore struct {
	baseDir string
	mu      sync.RWMutex
}

// NewFileLogStore creates a new FileLogStore.
func NewFileLogStore(baseDir string) *FileLogStore {
	return &FileLogStore{baseDir: baseDir}
}

func (ls *FileLogStore) AppendLog(taskID string, message string) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	logDir := filepath.Join(ls.baseDir, "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	f, err := os.OpenFile(filepath.Join(logDir, taskID+".log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	timestamp := time.Now().Format(time.RFC3339)
	entry := fmt.Sprintf("[%s] %s\n", timestamp, message)

	if _, err := f.WriteString(entry); err != nil {
		return err
	}
	return nil
}

func (ls *FileLogStore) GetLogs(taskID string) ([]string, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	path := filepath.Join(ls.baseDir, "logs", taskID+".log")
	f, err := os.Open(path)
	// If file doesn't exist, return empty logs instead of error
	if os.IsNotExist(err) {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var logs []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		logs = append(logs, scanner.Text())
	}
	return logs, scanner.Err()
}
