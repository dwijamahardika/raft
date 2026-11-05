package raft

import (
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// cluster is the test harness: N nodes on a MemNetwork, each with an applier
// goroutine that records committed commands so tests can assert agreement.
type cluster struct {
	t       *testing.T
	net     *MemNetwork
	ids     []string
	nodes   map[string]*Raft
	persist map[string]*MemPersister
	mu      sync.Mutex
	applied map[string][]ApplyMsg
	stopApp map[string]chan struct{}
}

const (
	electionTO = 150 * time.Millisecond
	heartbeat  = 40 * time.Millisecond
)

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{t: t, net: NewMemNetwork(), nodes: map[string]*Raft{}, persist: map[string]*MemPersister{}, applied: map[string][]ApplyMsg{}, stopApp: map[string]chan struct{}{}}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.persist[id] = &MemPersister{}
		c.start(id)
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) start(id string) {
	r, err := New(Config{
		ID: id, Peers: c.ids, Transport: c.net.Transport(id), Persister: c.persist[id],
		ElectionTimeout: electionTO, HeartbeatInterval: heartbeat,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.net.Register(id, r)
	c.mu.Lock()
	c.nodes[id] = r
	stop := make(chan struct{})
	c.stopApp[id] = stop
	c.mu.Unlock()
	go func() {
		for {
			select {
			case <-stop:
				return
			case m := <-r.ApplyCh():
				c.mu.Lock()
				prev := c.applied[id]
				if len(prev) > 0 && prev[len(prev)-1].Index+1 != m.Index {
					c.t.Errorf("%s: apply gap: %d then %d", id, prev[len(prev)-1].Index, m.Index)
				}
				c.applied[id] = append(prev, m)
				c.mu.Unlock()
			}
		}
	}()
}

// crash stops a node (state kept in its persister); restart brings it back.
func (c *cluster) crash(id string) {
	c.mu.Lock()
	r := c.nodes[id]
	close(c.stopApp[id])
	delete(c.nodes, id)
	c.mu.Unlock()
	c.net.Disconnect(id)
	r.Stop()
}

func (c *cluster) restart(id string) {
	c.mu.Lock()
	c.applied[id] = nil // a restarted state machine replays from the log
	c.mu.Unlock()
	c.start(id)
	c.net.Connect(id)
}

func (c *cluster) shutdown() {
	c.mu.Lock()
	nodes := make([]*Raft, 0, len(c.nodes))
	for id, r := range c.nodes {
		nodes = append(nodes, r)
		close(c.stopApp[id])
	}
	c.mu.Unlock()
	for _, r := range nodes {
		r.Stop()
	}
}

// waitLeader waits for exactly one leader among connected nodes and returns it.
func (c *cluster) waitLeader() *Raft {
	c.t.Helper()
	deadline := time.Now().Add(20 * electionTO)
	for time.Now().Before(deadline) {
		leaders := map[uint64][]string{}
		c.mu.Lock()
		for id, r := range c.nodes {
			if term, st, _ := r.Status(); st == Leader {
				leaders[term] = append(leaders[term], id)
			}
		}
		c.mu.Unlock()
		if len(leaders) > 0 {
			var maxTerm uint64
			for t := range leaders {
				maxTerm = max(maxTerm, t)
			}
			if len(leaders[maxTerm]) > 1 {
				c.t.Fatalf("two leaders in term %d: %v", maxTerm, leaders[maxTerm])
			}
			c.mu.Lock()
			l := c.nodes[leaders[maxTerm][0]]
			c.mu.Unlock()
			return l
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return nil
}

// propose submits via the current leader, retrying across leader changes.
func (c *cluster) propose(cmd string) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(20 * electionTO)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		var leader *Raft
		var best uint64
		for _, r := range c.nodes {
			// A leader stranded in a minority partition still believes it
			// leads; the one with the highest term is the real one.
			if term, st, _ := r.Status(); st == Leader && term > best {
				leader, best = r, term
			}
		}
		c.mu.Unlock()
		if leader != nil {
			if idx, _, err := leader.Propose([]byte(cmd)); err == nil {
				return idx
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("could not propose %q", cmd)
	return 0
}

// waitCommitted waits until `want` servers have applied index with the same command.
func (c *cluster) waitCommitted(index uint64, want int) string {
	c.t.Helper()
	deadline := time.Now().Add(20 * electionTO)
	for time.Now().Before(deadline) {
		cmds := map[string]int{}
		c.mu.Lock()
		for _, msgs := range c.applied {
			for _, m := range msgs {
				if m.Index == index {
					cmds[string(m.Command)]++
				}
			}
		}
		c.mu.Unlock()
		if len(cmds) > 1 {
			c.t.Fatalf("servers disagree at index %d: %v", index, cmds)
		}
		for cmd, n := range cmds {
			if n >= want {
				return cmd
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("index %d not committed on %d servers", index, want)
	return ""
}

func (c *cluster) one(cmd string, want int) {
	c.t.Helper()
	deadline := time.Now().Add(30 * electionTO)
	for time.Now().Before(deadline) {
		idx := c.propose(cmd)
		// Wait a bit for it to commit; if a leader change discarded it, retry.
		inner := time.Now().Add(10 * electionTO)
		for time.Now().Before(inner) {
			c.mu.Lock()
			n := 0
			for _, msgs := range c.applied {
				for _, m := range msgs {
					if m.Index == idx && string(m.Command) == cmd {
						n++
					}
				}
			}
			c.mu.Unlock()
			if n >= want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	c.t.Fatalf("%q never committed on %d servers", cmd, want)
}

// ---- tests ----

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader()
	term1, _, _ := l.Status()
	time.Sleep(4 * electionTO)
	l2 := c.waitLeader()
	term2, _, _ := l2.Status()
	if term1 != term2 || l != l2 {
		t.Fatalf("leadership unstable without failures: term %d->%d", term1, term2)
	}
	for _, r := range c.nodes {
		if term, _, leader := r.Status(); term != term1 || leader != l.cfg.ID {
			t.Fatalf("node %s: term=%d leader=%s", r.cfg.ID, term, leader)
		}
	}
}

func TestReElectionAfterLeaderIsolated(t *testing.T) {
	c := newCluster(t, 3)
	l1 := c.waitLeader()
	c.net.Disconnect(l1.cfg.ID)
	l2 := c.waitLeaderExcluding(l1.cfg.ID)
	if l2 == l1 {
		t.Fatal("isolated leader should not remain the cluster's leader")
	}
	t1, _, _ := l1.Status()
	t2, _, _ := l2.Status()
	if t2 <= t1 {
		t.Fatalf("new leader must have a higher term: %d vs %d", t2, t1)
	}
	// Old leader rejoins: it must step down to the higher term.
	c.net.Connect(l1.cfg.ID)
	time.Sleep(4 * electionTO)
	if _, st, _ := l1.Status(); st == Leader {
		if _, st2, _ := l2.Status(); st2 == Leader {
			t.Fatal("two leaders after heal")
		}
	}
	c.waitLeader()
}

func (c *cluster) waitLeaderExcluding(id string) *Raft {
	c.t.Helper()
	deadline := time.Now().Add(10 * electionTO)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for nid, r := range c.nodes {
			if nid != id && r.IsLeader() {
				c.mu.Unlock()
				return r
			}
		}
		c.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no new leader")
	return nil
}

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader()
	for i := 1; i <= 5; i++ {
		idx := c.propose(fmt.Sprintf("cmd%d", i))
		if got := c.waitCommitted(idx, 3); got != fmt.Sprintf("cmd%d", i) {
			t.Fatalf("index %d committed %q", idx, got)
		}
	}
}

func TestAgreementWithOneFollowerDown(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader()
	var follower string
	for _, id := range c.ids {
		if id != l.cfg.ID {
			follower = id
			break
		}
	}
	c.net.Disconnect(follower)
	c.one("a", 2)
	c.one("b", 2)
	c.net.Connect(follower)
	c.one("c", 3)
	// The rejoined follower must have caught up on a and b too.
	c.waitCommittedOn(follower, 3)
}

func (c *cluster) waitCommittedOn(id string, count int) {
	c.t.Helper()
	deadline := time.Now().Add(20 * electionTO)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		n := len(cmds(c.applied[id]))
		c.mu.Unlock()
		if n >= count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("%s applied fewer than %d entries", id, count)
}

func TestNoAgreementWithoutMajority(t *testing.T) {
	c := newCluster(t, 5)
	l := c.waitLeader()
	cut := 0
	for _, id := range c.ids {
		if id != l.cfg.ID && cut < 3 {
			c.net.Disconnect(id)
			cut++
		}
	}
	idx, _, err := l.Propose([]byte("doomed"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * electionTO)
	c.mu.Lock()
	for id, msgs := range c.applied {
		for _, m := range msgs {
			if m.Index == idx {
				c.mu.Unlock()
				t.Fatalf("%s committed without a majority", id)
			}
		}
	}
	c.mu.Unlock()
	// Reconnect: something eventually commits at that index (this or a later
	// leader's entry), and all servers agree on it.
	for _, id := range c.ids {
		c.net.Connect(id)
	}
	c.one("after", 5)
}

func TestConcurrentProposals(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader()
	const n = 30
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.one(fmt.Sprintf("p%d", i), 3)
		}(i)
	}
	wg.Wait()
	// All servers hold identical logs.
	c.mu.Lock()
	defer c.mu.Unlock()
	ref := cmds(c.applied[c.ids[0]])
	for _, id := range c.ids[1:] {
		got := cmds(c.applied[id])
		if len(got) < len(ref) {
			t.Fatalf("%s applied %d < %d", id, len(got), len(ref))
		}
		for i := range ref {
			if got[i] != ref[i] {
				t.Fatalf("divergence at %d: %q vs %q", i, got[i], ref[i])
			}
		}
	}
	if len(ref) < n {
		t.Fatalf("only %d of %d committed", len(ref), n)
	}
}

func TestPersistenceAcrossCrash(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader()
	c.one("x", 3)
	c.one("y", 3)
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.restart(id)
	}
	c.waitLeader()
	// Replayed from persisted log, then a new entry on top.
	for _, id := range c.ids {
		c.waitCommittedOn(id, 2)
	}
	c.one("z", 3)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range c.ids {
		got := cmds(c.applied[id])
		if len(got) != 3 || got[0] != "x" || got[1] != "y" || got[2] != "z" {
			t.Fatalf("%s log after restart: %v", id, got)
		}
	}
}

// cmds returns the applied commands, skipping leader no-ops.
func cmds(m []ApplyMsg) []string {
	out := make([]string, 0, len(m))
	for i := range m {
		if !m[i].Noop {
			out = append(out, string(m[i].Command))
		}
	}
	return out
}

// The scenario Raft's election restriction exists for: a leader in a minority
// partition keeps accepting proposals that can never commit. When the
// partition heals, its uncommitted entries must be overwritten by the
// majority's log, never the other way round.
func TestMinorityLeaderEntriesAreOverwritten(t *testing.T) {
	c := newCluster(t, 5)
	l := c.waitLeader()
	c.one("base", 5)

	var minority, majority []string
	minority = append(minority, l.cfg.ID)
	for _, id := range c.ids {
		if id == l.cfg.ID {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	c.net.Partition(minority, majority)

	// Old leader accepts entries it cannot commit.
	for i := 0; i < 3; i++ {
		if _, _, err := l.Propose([]byte(fmt.Sprintf("lost%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Majority elects a new leader and commits real entries.
	time.Sleep(3 * electionTO)
	c.one("real1", 3)
	c.one("real2", 3)

	c.net.Heal()
	c.one("after-heal", 5)

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range c.ids {
		for _, m := range c.applied[id] {
			if len(m.Command) >= 4 && string(m.Command[:4]) == "lost" {
				t.Fatalf("%s applied uncommittable entry %q", id, m.Command)
			}
		}
	}
	ref := cmds(c.applied[majority[0]])
	for _, id := range c.ids {
		got := cmds(c.applied[id])
		if len(got) < len(ref) {
			t.Fatalf("%s behind: %v vs %v", id, got, ref)
		}
		for i := range ref {
			if got[i] != ref[i] {
				t.Fatalf("%s diverged: %v vs %v", id, got, ref)
			}
		}
	}
}

func TestUnreliableNetwork(t *testing.T) {
	c := newCluster(t, 5)
	c.net.SetUnreliable(0.15, 15*time.Millisecond)
	c.waitLeader()
	for i := 0; i < 20; i++ {
		c.one(fmt.Sprintf("u%d", i), 3)
	}
	c.net.SetUnreliable(0, 0)
	c.one("final", 5)
	sent, lost := c.net.Counts()
	if lost == 0 || lost*100/sent < 5 {
		t.Fatalf("fault injection ineffective: %d/%d lost", lost, sent)
	}
}

func TestFollowerRejectsStaleTerm(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader()
	term, _, _ := l.Status()
	var f *Raft
	for _, r := range c.nodes {
		if r != l {
			f = r
			break
		}
	}
	reply, err := f.HandleAppendEntries(&AppendEntriesArgs{Term: term - 1, LeaderID: "ghost"})
	if err != nil || reply.Success || reply.Term != term {
		t.Fatalf("stale AppendEntries accepted: %+v %v", reply, err)
	}
	vote, err := f.HandleRequestVote(&RequestVoteArgs{Term: term - 1, CandidateID: "ghost"})
	if err != nil || vote.VoteGranted {
		t.Fatalf("voted for stale candidate: %+v %v", vote, err)
	}
}

func TestConflictBackup(t *testing.T) {
	// Directly exercise the fast-backup hint: a follower with entries from an
	// old term must report the first index of that term.
	p := &MemPersister{}
	r, _ := New(Config{ID: "x", Peers: []string{"x"}, Transport: NewMemNetwork().Transport("x"), Persister: p,
		ElectionTimeout: time.Hour, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	defer r.Stop()
	r.mu.Lock()
	r.currentTerm = 5
	r.log = append(r.log, LogEntry{Term: 2, Index: 1}, LogEntry{Term: 3, Index: 2}, LogEntry{Term: 3, Index: 3}, LogEntry{Term: 3, Index: 4})
	r.mu.Unlock()

	reply := mustAppend(t, r, &AppendEntriesArgs{Term: 5, LeaderID: "l", PrevLogIndex: 4, PrevLogTerm: 4})
	if reply.Success || reply.ConflictTerm != 3 || reply.ConflictIndex != 2 {
		t.Fatalf("want conflict term 3 first index 2, got %+v", reply)
	}
	reply = mustAppend(t, r, &AppendEntriesArgs{Term: 5, LeaderID: "l", PrevLogIndex: 9, PrevLogTerm: 4})
	if reply.Success || reply.ConflictTerm != 0 || reply.ConflictIndex != 5 {
		t.Fatalf("want missing-index hint 5, got %+v", reply)
	}
	// Matching prefix + conflicting suffix is truncated and replaced.
	reply = mustAppend(t, r, &AppendEntriesArgs{Term: 5, LeaderID: "l", PrevLogIndex: 1, PrevLogTerm: 2,
		Entries: []LogEntry{{Term: 5, Index: 2, Command: []byte("new")}}, LeaderCommit: 2})
	if !reply.Success {
		t.Fatalf("append should succeed: %+v", reply)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.log) != 3 || r.log[2].Term != 5 || r.commitIndex != 2 {
		t.Fatalf("log not truncated/replaced: %+v commit=%d", r.log, r.commitIndex)
	}
}

func mustAppend(t *testing.T, r *Raft, args *AppendEntriesArgs) *AppendEntriesReply {
	t.Helper()
	reply, err := r.HandleAppendEntries(args)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}
