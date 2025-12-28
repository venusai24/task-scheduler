package store

import (
    "context"
    "fmt"
    "log"
    "net"
    "os"
    "path/filepath"
    "time"

    "github.com/hashicorp/raft"
    raftboltdb "github.com/hashicorp/raft-boltdb/v2"
    "go.etcd.io/bbolt"
)

// Open initializes the Raft node
func (s *Store) Open(localID, raftDir, raftAddr string, bootstrap bool) error {
    // 1. Setup Raft Configuration (tuned for production)
    config := raft.DefaultConfig()
    config.LocalID = raft.ServerID(localID)
    config.HeartbeatTimeout = 500 * time.Millisecond
    config.ElectionTimeout = 1500 * time.Millisecond
    config.CommitTimeout = 50 * time.Millisecond
    config.LeaderLeaseTimeout = 500 * time.Millisecond
    config.LogLevel = "WARN"
    config.SnapshotThreshold = 8192
    config.SnapshotInterval = 120 * time.Second

    // keep localID on Store for internal checks (raft.Raft doesn't export LocalID)
    s.localID = config.LocalID

    // 2. Setup Transport (TCP)
    transport, err := raft.NewTCPTransport(raftAddr, nil, 3, 10*time.Second, os.Stderr)
    if err != nil {
        return err
    }
    s.transport = transport  // ← SAVE IT

    // 3. Setup Log Store (BoltDB) with optimized settings
    snapshots, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
    if err != nil {
        return err
    }
	
	// Create log store with optimized BoltDB settings
	logStore, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(raftDir, "raft-log.bolt"),
		BoltOptions: &bbolt.Options{
			NoFreelistSync: true,
			Timeout:        1 * time.Second,
		},
	})
	if err != nil {
		return err
	}
	
	// Create stable store with optimized BoltDB settings
	stableStore, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(raftDir, "raft-stable.bolt"),
		BoltOptions: &bbolt.Options{
			NoFreelistSync: true,
			Timeout:        1 * time.Second,
		},
	})
	if err != nil {
		return err
	}

    // 4. Instantiate Raft
    ra, err := raft.NewRaft(config, s, logStore, stableStore, snapshots, transport)
    if err != nil {
        return err
    }
    s.raft = ra

    // start background failure detector
    s.shutdownCtx, s.shutdownCancel = context.WithCancel(context.Background())
    go s.startFailureDetector(s.shutdownCtx)

    // 5. Bootstrap Cluster (If we are the first node)
    if bootstrap {
        log.Println("Attempting to bootstrap Raft cluster...")
        configuration := raft.Configuration{
            Servers: []raft.Server{
                {
                    ID:      config.LocalID,
                    Address: transport.LocalAddr(),
                },
            },
        }
		
		// BootstrapCluster returns an error if state already exists, which we handle
		future := s.raft.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			if err == raft.ErrCantBootstrap {
				log.Println("Raft state already exists, skipping bootstrap.")
			} else {
				return fmt.Errorf("bootstrap failed: %w", err)
			}
		} else {
			log.Println("✅ Raft cluster bootstrapped successfully.")
		}
    }

    return nil
}

// Close gracefully shuts down Raft and associated stores
func (s *Store) Close() error {
    // cancel background detector
    if s.shutdownCancel != nil {
        s.shutdownCancel()
    }

    if s.raft != nil {
        // Shutdown Raft
        future := s.raft.Shutdown()
        if err := future.Error(); err != nil {
            log.Printf("Error shutting down raft: %v", err)
        }
    }

    // Close the transport to release the port
    if s.transport != nil {
        if err := s.transport.Close(); err != nil {
            log.Printf("Error closing transport: %v", err)
        }
    }

    // Close BoltDB stores
    if s.logStore != nil {
        if err := s.logStore.Close(); err != nil {
            log.Printf("Error closing log store: %v", err)
        }
    }

    if s.stableStore != nil {
        if err := s.stableStore.Close(); err != nil {
            log.Printf("Error closing stable store: %v", err)
        }
    }

    return nil
}

// startFailureDetector periodically checks peer reachability and removes persistently dead peers.
// Only the leader performs removals; removal is skipped if doing so would lose quorum.
func (s *Store) startFailureDetector(ctx context.Context) {
    ticker := time.NewTicker(15 * time.Second)
    defer ticker.Stop()

    failureCount := make(map[raft.ServerID]int)
    const maxFailures = 6

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            if s.raft == nil {
                continue
            }
            // leader-only for changes
            if s.raft.State() != raft.Leader {
                // reset counts when not leader to avoid accidental removals when leadership changes
                failureCount = make(map[raft.ServerID]int)
                continue
            }

            cfgFuture := s.raft.GetConfiguration()
            if err := cfgFuture.Error(); err != nil {
                log.Printf("failure-detector: get configuration error: %v", err)
                continue
            }
            cfg := cfgFuture.Configuration()
            totalServers := len(cfg.Servers)
            if totalServers <= 1 {
                continue
            }

            // compute unreachable counts but ensure we do not remove if doing so loses quorum
            unreach := make(map[raft.ServerID]bool)
            for _, srv := range cfg.Servers {
                if srv.ID == s.localID {
                    continue
                }
                addr := string(srv.Address)
                if !s.canReachPeer(addr) {
                    unreach[srv.ID] = true
                    failureCount[srv.ID]++
                    log.Printf("failure-detector: peer %s unreachable (%d/%d)", srv.ID, failureCount[srv.ID], maxFailures)
                    if failureCount[srv.ID] >= maxFailures {
                        // if removing would make alive < quorum, skip
                        alive := 0
                        for _, s2 := range cfg.Servers {
                            if s2.ID == srv.ID {
                                continue
                            }
                            // when counting alive:
                            if s2.ID == s.localID || s.canReachPeer(string(s2.Address)) {
                                alive++
                            }
                        }
                        quorum := (totalServers/2) + 1
                        if alive < quorum {
                            log.Printf("failure-detector: skipping removal of %s: removal would lose quorum (alive=%d, quorum=%d)", srv.ID, alive, quorum)
                            continue
                        }
                        log.Printf("failure-detector: removing dead peer %s", srv.ID)
                        if err := s.raft.RemoveServer(srv.ID, 0, 0).Error(); err != nil {
                            log.Printf("failure-detector: failed to remove %s: %v", srv.ID, err)
                        } else {
                            // reset counter after successful removal
                            delete(failureCount, srv.ID)
                        }
                    }
                } else {
                    // reachable -> reset counter
                    failureCount[srv.ID] = 0
                }
            }
        }
    }
}

func (s *Store) canReachPeer(addr string) bool {
    conn, err := net.DialTimeout("tcp", addr, 1500*time.Millisecond)
    if err != nil {
        return false
    }
    _ = conn.Close()
    return true
}

func hasExistingRaftState(raftDir string) bool {
    if _, err := os.Stat(filepath.Join(raftDir, "raft-log.bolt")); err == nil {
        return true
    }
    if _, err := os.Stat(filepath.Join(raftDir, "raft-stable.bolt")); err == nil {
        return true
    }
    return false
}

func (s *Store) AddVoter(nodeID, address string) error {
    if s.raft == nil {
        return fmt.Errorf("raft not initialized")
    }
    future := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, 0)
    return future.Error()
}

// RemoveServer removes a node from the Raft cluster
func (s *Store) RemoveServer(nodeID string) error {
    if s.raft.State() != raft.Leader {
        return fmt.Errorf("not the leader")
    }
    
    future := s.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
    return future.Error()
}