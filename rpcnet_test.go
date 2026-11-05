package raft

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// countingListener counts accepted connections, i.e. how often a peer dialled.
type countingListener struct {
	net.Listener
	accepted atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return c, err
}

// serveTCP serves r's RPCs on a loopback port until the test ends.
func serveTCP(t *testing.T, r *Raft) *countingListener {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cl := &countingListener{Listener: ln}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, cl, r) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	return cl
}

// Both RPCs round-trip over TCP, and once the node halts the caller gets an
// error rather than a reply, as if the node had crashed.
func TestRPCTransportRoundTrip(t *testing.T) {
	v := startManual(t, NewMemNetwork(), "v", []string{"c", "v"}, &MemPersister{})
	ln := serveTCP(t, v)
	tr := NewRPCTransport(map[string]string{"v": ln.Addr().String()}, time.Second)
	ctx := context.Background()

	vote, err := tr.RequestVote(ctx, "v", &RequestVoteArgs{Term: 1, CandidateID: "c"})
	if err != nil || !vote.VoteGranted || vote.Term != 1 {
		t.Fatalf("RequestVote = %+v, %v; want vote granted in term 1", vote, err)
	}
	app, err := tr.AppendEntries(ctx, "v", &AppendEntriesArgs{
		Term: 1, LeaderID: "c", Entries: []LogEntry{{Term: 1, Index: 1, Command: []byte("x")}},
	})
	if err != nil || !app.Success {
		t.Fatalf("AppendEntries = %+v, %v; want success", app, err)
	}
	if e, ok := logAt(v, 1); !ok || string(e.Command) != "x" {
		t.Fatalf("entry 1 = %+v (ok=%v), want command \"x\"", e, ok)
	}

	v.Stop()
	if reply, err := tr.RequestVote(ctx, "v", &RequestVoteArgs{Term: 2, CandidateID: "c"}); err == nil {
		t.Fatalf("halted node replied: %+v", reply)
	}
}

// The dial is bounded by the RPC's context, not only by the transport's dial
// timeout: an RPC whose context is already done must not dial at all.
func TestRPCTransportDialHonoursContext(t *testing.T) {
	v := startManual(t, NewMemNetwork(), "v", []string{"c", "v"}, &MemPersister{})
	ln := serveTCP(t, v)
	tr := NewRPCTransport(map[string]string{"v": ln.Addr().String()}, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tr.RequestVote(ctx, "v", &RequestVoteArgs{Term: 1, CandidateID: "c"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RequestVote with a cancelled context: %v, want context.Canceled", err)
	}
	// This call dials once. The server accepts connections in order, so had
	// the cancelled call dialled too, both would be counted by now.
	if _, err := tr.RequestVote(context.Background(), "v", &RequestVoteArgs{Term: 1, CandidateID: "c"}); err != nil {
		t.Fatal(err)
	}
	if n := ln.accepted.Load(); n != 1 {
		t.Fatalf("server accepted %d connections, want 1: the cancelled RPC dialled", n)
	}
}
