// raftkv: a replicated in-memory key-value store on top of the raft package.
//
// Run three nodes (separate terminals):
//
//	raftkv -id n1 -peers n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003 -http :8001 -data ./data/n1
//	raftkv -id n2 -peers ...same... -http :8002 -data ./data/n2
//	raftkv -id n3 -peers ...same... -http :8003 -data ./data/n3
//
//	curl -X PUT localhost:8001/kv/greeting -d hello   # 421 + Leader header if not leader
//	curl localhost:8003/kv/greeting                    # replicated
//	curl localhost:8002/status
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dwijamahardika/raft"
)

type command struct {
	Op    string `json:"op"` // put | del
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// kvStore is the replicated state machine: it applies committed commands in
// log order and lets HTTP handlers wait for a specific index.
type kvStore struct {
	mu      sync.Mutex
	data    map[string]string
	applied uint64
	waiters map[uint64][]chan uint64 // index -> chans receiving the term applied at that index
}

func newKV() *kvStore {
	return &kvStore{data: map[string]string{}, waiters: map[uint64][]chan uint64{}}
}

func (s *kvStore) apply(m raft.ApplyMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !m.Noop {
		var c command
		if err := json.Unmarshal(m.Command, &c); err == nil {
			switch c.Op {
			case "put":
				s.data[c.Key] = c.Value
			case "del":
				delete(s.data, c.Key)
			}
		}
	}
	s.applied = m.Index
	for _, ch := range s.waiters[m.Index] {
		ch <- m.Term
	}
	delete(s.waiters, m.Index)
}

// wait blocks until index is applied and reports whether it carried term
// (a different term means our proposal was overwritten by a new leader).
func (s *kvStore) wait(ctx context.Context, index, term uint64) error {
	ch := make(chan uint64, 1)
	s.mu.Lock()
	s.waiters[index] = append(s.waiters[index], ch)
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case got := <-ch:
		if got != term {
			return errors.New("proposal lost to leader change; retry")
		}
		return nil
	}
}

func (s *kvStore) get(k string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[k]
	return v, ok
}

func main() {
	id := flag.String("id", "", "this node's id")
	peers := flag.String("peers", "", "id=host:port,... for all nodes (raft transport)")
	httpAddr := flag.String("http", ":8001", "HTTP listen address")
	dataDir := flag.String("data", "./data", "directory for persisted raft state")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	addrs := map[string]string{}
	var ids []string
	for _, p := range strings.Split(*peers, ",") {
		nid, addr, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok {
			log.Error("bad -peers entry", "entry", p)
			os.Exit(2)
		}
		addrs[nid] = addr
		ids = append(ids, nid)
	}
	if _, ok := addrs[*id]; !ok {
		log.Error("-id must appear in -peers")
		os.Exit(2)
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Error("data dir", "err", err)
		os.Exit(1)
	}

	kv := newKV()
	node, err := raft.New(raft.Config{
		ID: *id, Peers: ids,
		Transport:       raft.NewRPCTransport(addrs, 500*time.Millisecond),
		Persister:       raft.FilePersister{Path: filepath.Join(*dataDir, "raft.state")},
		ElectionTimeout: 300 * time.Millisecond, HeartbeatInterval: 100 * time.Millisecond,
		Logger: log,
	})
	if err != nil {
		log.Error("raft", "err", err)
		os.Exit(1)
	}
	go func() {
		for m := range node.ApplyCh() {
			kv.apply(m)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ln, err := net.Listen("tcp", addrs[*id])
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	go func() {
		if err := raft.Serve(ctx, ln, node); err != nil {
			log.Error("rpc serve", "err", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		propose(w, r, node, kv, command{Op: "put", Key: r.PathValue("key"), Value: string(body)})
	})
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		propose(w, r, node, kv, command{Op: "del", Key: r.PathValue("key")})
	})
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		// Local read: fast, but may lag the leader by an RPC. For linearizable
		// reads route to the leader (or implement ReadIndex; see README).
		v, ok := kv.get(r.PathValue("key"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, v)
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		term, state, leader := node.Status()
		kv.mu.Lock()
		applied, keys := kv.applied, len(kv.data)
		kv.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": *id, "term": term, "state": state.String(), "leader": leader,
			"commit_index": node.CommitIndex(), "applied": applied, "keys": keys,
		})
	})
	srv := &http.Server{Addr: *httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("http listening", "addr", *httpAddr, "raft", addrs[*id])
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
		}
	}()

	<-ctx.Done()
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shCtx)
	node.Stop()
	log.Info("bye")
}

func propose(w http.ResponseWriter, r *http.Request, node *raft.Raft, kv *kvStore, c command) {
	body, _ := json.Marshal(c)
	index, term, err := node.Propose(body)
	if errors.Is(err, raft.ErrNotLeader) {
		_, _, leader := node.Status()
		w.Header().Set("Leader", leader)
		http.Error(w, fmt.Sprintf("not the leader; try %q", leader), http.StatusMisdirectedRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := kv.wait(ctx, index, term); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("X-Raft-Index", fmt.Sprint(index))
	w.WriteHeader(http.StatusNoContent)
}
