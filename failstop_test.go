package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// Raft's safety argument assumes a node's vote and log are on stable storage
// before any reply that depends on them leaves the node. These tests inject
// persist failures and check that a node never acts on state it could not
// make durable.

var errDiskFull = errors.New("injected: no space left on device")

// flakyPersister is a MemPersister whose Save can be made to fail on demand,
// standing in for a full disk or an fsync EIO. A failed Save leaves the
// previously saved state untouched, like FilePersister's tmp+rename.
type flakyPersister struct {
	mem  MemPersister
	mu   sync.Mutex
	fail error
}

func (p *flakyPersister) Save(b []byte) error {
	p.mu.Lock()
	err := p.fail
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.mem.Save(b)
}

func (p *flakyPersister) Load() ([]byte, error) { return p.mem.Load() }

func (p *flakyPersister) failWith(err error) {
	p.mu.Lock()
	p.fail = err
	p.mu.Unlock()
}

// durable decodes what is actually on "disk", i.e. what a restart would see.
func durable(t *testing.T, p Persister) persistentState {
	t.Helper()
	var ps persistentState
	b, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&ps); err != nil {
			t.Fatal(err)
		}
	}
	return ps
}

// startManual starts a node whose election timer never fires during a test,
// so elections happen only when the test calls campaign. Heartbeats stay
// fast so a leader replicates promptly.
func startManual(t *testing.T, net *MemNetwork, id string, peers []string, p Persister) *Raft {
	t.Helper()
	r, err := New(Config{
		ID: id, Peers: peers, Transport: net.Transport(id), Persister: p,
		ElectionTimeout: time.Hour, HeartbeatInterval: 10 * time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	net.Register(id, r)
	t.Cleanup(r.Stop)
	return r
}

func campaign(r *Raft) {
	r.mu.Lock()
	r.startElectionLocked()
	r.mu.Unlock()
}

func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// assertFailStopped checks the fail-stop contract after a persist failure:
// the error reaches the embedder, the node has left the protocol, and it
// stays out even once the disk recovers.
func assertFailStopped(t *testing.T, r *Raft, p *flakyPersister, rpcErr error) {
	t.Helper()
	if !errors.Is(rpcErr, ErrStopped) || !errors.Is(rpcErr, errDiskFull) {
		t.Fatalf("caller got %v, want an error wrapping ErrStopped and the disk error", rpcErr)
	}
	select {
	case <-r.Done():
	default:
		t.Fatal("Done not closed after persist failure")
	}
	if err := r.Err(); !errors.Is(err, ErrStopped) || !errors.Is(err, errDiskFull) {
		t.Fatalf("Err() = %v, want the persist error", err)
	}
	if _, st, _ := r.Status(); st != Stopped {
		t.Fatalf("state %v, want stopped", st)
	}
	p.failWith(nil)
	if _, err := r.HandleRequestVote(&RequestVoteArgs{Term: 99, CandidateID: "z"}); !errors.Is(err, ErrStopped) {
		t.Fatalf("halted node answered RequestVote: %v", err)
	}
	if _, err := r.HandleAppendEntries(&AppendEntriesArgs{Term: 99, LeaderID: "z"}); !errors.Is(err, ErrStopped) {
		t.Fatalf("halted node answered AppendEntries: %v", err)
	}
	if _, _, err := r.Propose([]byte("late")); !errors.Is(err, ErrStopped) {
		t.Fatalf("halted node accepted a proposal: %v", err)
	}
	r.Stop() // must not hang after a fail-stop
}

func logAt(r *Raft, index uint64) (LogEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index >= uint64(len(r.log)) {
		return LogEntry{}, false
	}
	return r.log[index], true
}

func TestVoteNotGrantedWhenPersistFails(t *testing.T) {
	net := NewMemNetwork()
	p := &flakyPersister{}
	v := startManual(t, net, "v", []string{"a", "b", "v"}, p)

	p.failWith(errDiskFull)
	reply, err := net.Transport("a").RequestVote(context.Background(), "v", &RequestVoteArgs{Term: 1, CandidateID: "a"})
	if err == nil && reply.VoteGranted {
		ps := durable(t, p)
		t.Fatalf("vote granted in term 1 although persisting it failed (durable: term=%d votedFor=%q)", ps.CurrentTerm, ps.VotedFor)
	}
	assertFailStopped(t, v, p, err)
}

func TestAppendNotAckedWhenPersistFails(t *testing.T) {
	net := NewMemNetwork()
	p := &flakyPersister{}
	f := startManual(t, net, "f", []string{"l", "f"}, p)
	tr := net.Transport("l")

	// Healthy heartbeat: f adopts term 1 and persists it.
	if reply, err := tr.AppendEntries(context.Background(), "f", &AppendEntriesArgs{Term: 1, LeaderID: "l"}); err != nil || !reply.Success {
		t.Fatalf("heartbeat: %+v %v", reply, err)
	}
	p.failWith(errDiskFull)
	reply, err := tr.AppendEntries(context.Background(), "f", &AppendEntriesArgs{
		Term: 1, LeaderID: "l", Entries: []LogEntry{{Term: 1, Index: 1, Command: []byte("x")}},
	})
	if err == nil && reply.Success {
		t.Fatalf("append acked although persisting it failed (durable log: %d entries)", len(durable(t, p).Log))
	}
	assertFailStopped(t, f, p, err)
}

func TestProposeFailsWhenLeaderPersistFails(t *testing.T) {
	net := NewMemNetwork()
	p := &flakyPersister{}
	l := startManual(t, net, "l", []string{"l"}, p)
	campaign(l) // single-node cluster: wins immediately
	if !l.IsLeader() {
		t.Fatal("single node did not become leader")
	}
	p.failWith(errDiskFull)
	idx, _, err := l.Propose([]byte("x"))
	if err == nil {
		t.Fatalf("proposal acknowledged at index %d although persisting it failed", idx)
	}
	assertFailStopped(t, l, p, err)
}

// A candidate's vote for itself counts toward its majority, so it must be
// durable before any RequestVote goes out; otherwise a restart could let the
// node vote for a rival in the same term.
func TestCandidateDoesNotCampaignWhenPersistFails(t *testing.T) {
	net := NewMemNetwork()
	p := &flakyPersister{}
	c := startManual(t, net, "c", []string{"a", "b", "c"}, p)
	p.failWith(errDiskFull)
	campaign(c)
	time.Sleep(50 * time.Millisecond) // vote requests are sent from goroutines
	if sent, _ := net.Counts(); sent != 0 {
		ps := durable(t, p)
		t.Fatalf("candidate sent %d RequestVotes although its self-vote is not durable (durable: term=%d votedFor=%q)", sent, ps.CurrentTerm, ps.VotedFor)
	}
	assertFailStopped(t, c, p, c.Err())
}

// A plain Stop halts the node the same way but is not an error.
func TestStopHaltsWithoutError(t *testing.T) {
	r := startManual(t, NewMemNetwork(), "x", []string{"x", "y"}, &MemPersister{})
	r.Stop()
	select {
	case <-r.Done():
	default:
		t.Fatal("Done not closed after Stop")
	}
	if err := r.Err(); err != nil {
		t.Fatalf("Err() after Stop = %v, want nil", err)
	}
	if _, err := r.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "y"}); !errors.Is(err, ErrStopped) {
		t.Fatalf("stopped node answered RequestVote: %v", err)
	}
	if _, _, err := r.Propose(nil); !errors.Is(err, ErrStopped) {
		t.Fatalf("stopped node: Propose returned %v, want ErrStopped", err)
	}
}

// Two leaders in one term. v's disk fails while it votes for a; v crashes and
// restarts from disk, which does not record that vote, and votes for b in the
// same term. a+v and b+v are both majorities of three.
func TestNoTwoLeadersAfterFailedVotePersist(t *testing.T) {
	net := NewMemNetwork()
	peers := []string{"a", "b", "v"}
	a := startManual(t, net, "a", peers, &MemPersister{})
	b := startManual(t, net, "b", peers, &MemPersister{})
	vp := &flakyPersister{}
	v := startManual(t, net, "v", peers, vp)

	net.Partition([]string{"a", "v"}, []string{"b"})
	vp.failWith(errDiskFull)
	campaign(a)
	waitFor(300*time.Millisecond, a.IsLeader)
	if err := v.Err(); !errors.Is(err, errDiskFull) {
		t.Fatalf("v should have fail-stopped on the disk error, Err() = %v", err)
	}

	// v crashes; its disk recovers; it restarts from what is durable.
	v.Stop()
	vp.failWith(nil)
	startManual(t, net, "v", peers, vp)

	net.Partition([]string{"b", "v"}, []string{"a"})
	campaign(b)
	if !waitFor(2*time.Second, b.IsLeader) {
		t.Fatal("b should win term 1 with the vote of the restarted v")
	}
	ta, sa, _ := a.Status()
	tb, _, _ := b.Status()
	if sa == Leader && ta == tb {
		t.Fatalf("two leaders in term %d: a and b (v voted for both)", ta)
	}
}

// A committed entry is lost. f1 acks an entry it failed to persist, the
// leader counts that ack toward a majority and commits; after the leader dies
// and f1 restarts, f1 and f2 elect a leader that never had the entry.
func TestCommittedEntrySurvivesFailedFollowerPersist(t *testing.T) {
	net := NewMemNetwork()
	peers := []string{"l", "f1", "f2"}
	l := startManual(t, net, "l", peers, &MemPersister{})
	f1p := &flakyPersister{}
	f1 := startManual(t, net, "f1", peers, f1p)
	f2 := startManual(t, net, "f2", peers, &MemPersister{})

	campaign(l)
	if !waitFor(2*time.Second, func() bool { return l.IsLeader() && f1.CommitIndex() >= 1 && f2.CommitIndex() >= 1 }) {
		t.Fatal("initial leader and no-op not established")
	}

	net.Disconnect("f2")
	f1p.failWith(errDiskFull)
	idx, _, err := l.Propose([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	committed := waitFor(300*time.Millisecond, func() bool { return l.CommitIndex() >= idx })
	if err := f1.Err(); !errors.Is(err, errDiskFull) {
		t.Fatalf("f1 should have fail-stopped on the disk error, Err() = %v", err)
	}

	// The leader dies for good; f1 restarts from disk; f2 comes back.
	net.Disconnect("l")
	l.Stop()
	f1.Stop()
	f1p.failWith(nil)
	startManual(t, net, "f1", peers, f1p)
	net.Connect("f2")
	campaign(f2)
	if !waitFor(2*time.Second, f2.IsLeader) {
		t.Fatal("f2 should win with the restarted f1's vote")
	}
	if committed {
		if e, ok := logAt(f2, idx); !ok || string(e.Command) != "x" {
			t.Fatalf("entry %d (\"x\") was committed by l but new leader f2 has %+v (ok=%v): committed entry lost", idx, e, ok)
		}
	}
}
