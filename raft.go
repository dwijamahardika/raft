// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", 2014): leader
// election, log replication, and the safety rules that make a replicated log
// linearizable. It is transport- and storage-agnostic; see MemNetwork for a
// simulated cluster and RPCTransport for a real one.
//
// Scope: the core protocol (§5 of the paper) including the election
// restriction, commit-index advancement only for current-term entries, and
// the conflict-term fast backup. Snapshots and membership changes are out of
// scope and called out in the README.
package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
	// Stopped means the node no longer takes part in the protocol, either
	// because Stop was called or because it failed to persist its state
	// (see Raft.Err).
	Stopped
)

func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	case Stopped:
		return "stopped"
	default:
		return "follower"
	}
}

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// ApplyMsg is delivered on the apply channel once an entry is committed.
// Noop marks the empty entry a new leader appends on election; state machines
// must skip it.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
	Noop    bool
}

// ---- RPC types ----

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// Fast backup (§5.3 optimisation): on conflict the follower reports the
	// term of the conflicting entry and the first index it holds for that
	// term, so the leader skips a whole term per round trip instead of one
	// entry.
	ConflictTerm  uint64
	ConflictIndex uint64
}

// Transport sends RPCs to peers. Implementations must be safe for concurrent use.
type Transport interface {
	RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// Persister stores the durable state (currentTerm, votedFor, log). Raft
// persists before responding to any RPC or acknowledging a proposal, which is
// what allows a crashed node to rejoin without violating safety.
//
// Save must not return nil until the state is on stable storage. If Save
// returns an error the node fail-stops: it sends no reply that depended on
// the unsaved state and halts (see Raft.Err).
type Persister interface {
	Save(state []byte) error
	Load() ([]byte, error)
}

type Config struct {
	ID                string
	Peers             []string // all cluster members including self
	Transport         Transport
	Persister         Persister
	ElectionTimeout   time.Duration // base; actual is [base, 2*base)
	HeartbeatInterval time.Duration
	Logger            *slog.Logger
	// ApplyBuffer sizes the apply channel; a slow consumer blocks commits
	// from being delivered but never blocks the protocol itself.
	ApplyBuffer int
}

type Raft struct {
	cfg    Config
	logger *slog.Logger

	mu sync.Mutex
	// persistent
	currentTerm uint64
	votedFor    string
	log         []LogEntry // log[0] is a sentinel with Index 0, Term 0
	// volatile
	state       State
	leaderID    string
	commitIndex uint64
	lastApplied uint64
	lastHeard   time.Time
	// leader only
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	stopCh    chan struct{} // closed when the node halts; returned by Done
	stopped   bool
	err       error // why the node halted, if not via Stop
	wg        sync.WaitGroup
	rng       *rand.Rand
}

var (
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrStopped is returned by Propose and the RPC handlers once the node
	// has halted. If it halted because persisting state failed, the returned
	// error also wraps the storage error (see Raft.Err).
	ErrStopped = errors.New("raft: node stopped")
)

func New(cfg Config) (*Raft, error) {
	if cfg.ID == "" || len(cfg.Peers) == 0 || cfg.Transport == nil || cfg.Persister == nil {
		return nil, errors.New("raft: ID, Peers, Transport and Persister are required")
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 150 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = cfg.ElectionTimeout / 3
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ApplyBuffer <= 0 {
		cfg.ApplyBuffer = 256
	}
	r := &Raft{
		cfg:        cfg,
		logger:     cfg.Logger.With("node", cfg.ID),
		log:        []LogEntry{{}},
		state:      Follower,
		lastHeard:  time.Now(),
		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
		applyCh:    make(chan ApplyMsg, cfg.ApplyBuffer),
		stopCh:     make(chan struct{}),
		// Seed from the process-wide, randomly seeded source. Seeding from the
		// clock is a classic mistake: nodes created in the same instant get
		// identical election timeouts and split every vote, forever.
		rng: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
	r.applyCond = sync.NewCond(&r.mu)
	if err := r.restore(); err != nil {
		return nil, err
	}
	r.wg.Add(3)
	go r.electionLoop()
	go r.heartbeatLoop()
	go r.applyLoop()
	return r, nil
}

// ---- persistence ----

type persistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []LogEntry
}

// persistLocked saves currentTerm, votedFor and the log. It must be called
// with mu held, after changing any of them and before anything that depends
// on the change leaves the node: an RPC reply, an outgoing RPC, or a
// proposal ack.
//
// On failure the node fail-stops (haltLocked) and the error is returned; the
// caller must return without sending anything. Carrying on is unsafe: a node
// that grants a vote it could not save may, after a restart, grant another
// in the same term (two leaders); a follower that acks entries it could not
// save lets a leader commit them on a majority that does not hold them. A
// halted node is just a crashed node, which Raft is built to tolerate.
func (r *Raft) persistLocked() error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(persistentState{CurrentTerm: r.currentTerm, VotedFor: r.votedFor, Log: r.log})
	if err == nil {
		err = r.cfg.Persister.Save(buf.Bytes())
	}
	if err != nil {
		err = fmt.Errorf("%w: persisting state: %w", ErrStopped, err)
		r.logger.Error("persist failed; halting node", "err", err)
		r.haltLocked(err)
		return err
	}
	return nil
}

// haltLocked takes the node out of the protocol: background loops exit, RPC
// handlers and Propose return an error, and in-flight goroutines discard
// their results. It does not wait for goroutines to exit (mu is held); Stop
// does.
func (r *Raft) haltLocked(cause error) {
	if r.stopped {
		return
	}
	r.stopped = true
	r.err = cause
	r.state = Stopped
	r.leaderID = ""
	close(r.stopCh)
	r.applyCond.Broadcast()
}

func (r *Raft) stoppedErrLocked() error {
	if r.err != nil {
		return r.err
	}
	return ErrStopped
}

func (r *Raft) restore() error {
	data, err := r.cfg.Persister.Load()
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	var ps persistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&ps); err != nil {
		return err
	}
	r.currentTerm, r.votedFor, r.log = ps.CurrentTerm, ps.VotedFor, ps.Log
	if len(r.log) == 0 {
		r.log = []LogEntry{{}}
	}
	return nil
}

// ---- public API ----

// Propose appends a command to the log if this node is the leader. It returns
// immediately; commitment is signalled on ApplyCh.
func (r *Raft) Propose(cmd []byte) (index, term uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return 0, 0, r.stoppedErrLocked()
	}
	if r.state != Leader {
		return 0, 0, ErrNotLeader
	}
	e := LogEntry{Term: r.currentTerm, Index: r.lastIndexLocked() + 1, Command: cmd}
	r.log = append(r.log, e)
	if err := r.persistLocked(); err != nil {
		return 0, 0, err
	}
	r.matchIndex[r.cfg.ID] = e.Index
	r.nextIndex[r.cfg.ID] = e.Index + 1
	return e.Index, e.Term, nil
}

func (r *Raft) ApplyCh() <-chan ApplyMsg { return r.applyCh }

// Status returns (term, state, known leader).
func (r *Raft) Status() (uint64, State, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.state, r.leaderID
}

func (r *Raft) IsLeader() bool {
	_, s, _ := r.Status()
	return s == Leader
}

func (r *Raft) CommitIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex
}

// Done returns a channel that is closed when the node halts: after Stop, or
// when it fail-stops because persisting its state failed.
func (r *Raft) Done() <-chan struct{} { return r.stopCh }

// Err reports why the node halted. It is nil while the node runs and after a
// plain Stop. After a persist failure it returns that error (wrapping
// ErrStopped and the Persister's error): the node has stopped replying to
// peers and clients, and to rejoin it must be recreated with New - usually by
// restarting the process once storage is fixed - which reloads whatever the
// Persister last saved successfully.
func (r *Raft) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// Stop halts the node and waits for its background goroutines to exit. It is
// safe to call more than once, and after the node has fail-stopped.
func (r *Raft) Stop() {
	r.mu.Lock()
	r.haltLocked(nil)
	r.mu.Unlock()
	r.wg.Wait()
}

// ---- helpers (mu held) ----

func (r *Raft) lastIndexLocked() uint64 { return r.log[len(r.log)-1].Index }
func (r *Raft) lastTermLocked() uint64  { return r.log[len(r.log)-1].Term }

func (r *Raft) termAtLocked(index uint64) (uint64, bool) {
	if index >= uint64(len(r.log)) {
		return 0, false
	}
	return r.log[index].Term, true
}

// becomeFollowerLocked returns a non-nil error only if persisting a new term
// failed, in which case the node has halted.
func (r *Raft) becomeFollowerLocked(term uint64, leader string) error {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
		if err := r.persistLocked(); err != nil {
			return err
		}
	}
	if r.state != Follower {
		r.logger.Info("stepping down", "term", term, "from", r.state.String())
	}
	r.state = Follower
	r.leaderID = leader
	return nil
}

func (r *Raft) electionTimeoutLocked() time.Duration {
	base := r.cfg.ElectionTimeout
	return base + time.Duration(r.rng.Int64N(int64(base)))
}

func (r *Raft) majority() int { return len(r.cfg.Peers)/2 + 1 }

// ---- RPC handlers (invoked by the transport) ----
//
// A handler returns an error instead of a reply once the node has halted,
// including when persisting the state the reply would depend on fails. The
// transport must then send no reply (the caller sees a failed RPC, exactly
// as if the node had crashed).

func (r *Raft) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, r.stoppedErrLocked()
	}
	reply := &RequestVoteReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply, nil
	}
	if args.Term > r.currentTerm {
		if err := r.becomeFollowerLocked(args.Term, ""); err != nil {
			return nil, err
		}
		reply.Term = r.currentTerm
	}
	// Election restriction (§5.4.1): only vote for a candidate whose log is at
	// least as up to date as ours. This is what guarantees a leader holds
	// every committed entry.
	upToDate := args.LastLogTerm > r.lastTermLocked() ||
		(args.LastLogTerm == r.lastTermLocked() && args.LastLogIndex >= r.lastIndexLocked())
	if (r.votedFor == "" || r.votedFor == args.CandidateID) && upToDate {
		r.votedFor = args.CandidateID
		if err := r.persistLocked(); err != nil {
			return nil, err
		}
		r.lastHeard = time.Now()
		reply.VoteGranted = true
	}
	return reply, nil
}

func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, r.stoppedErrLocked()
	}
	reply := &AppendEntriesReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply, nil
	}
	// A valid leader for this term exists: reset the election clock and
	// (if we were candidate/leader for an older term) step down.
	if err := r.becomeFollowerLocked(args.Term, args.LeaderID); err != nil {
		return nil, err
	}
	reply.Term = r.currentTerm
	r.lastHeard = time.Now()

	// Consistency check (§5.3).
	prevTerm, ok := r.termAtLocked(args.PrevLogIndex)
	if !ok {
		reply.ConflictIndex = r.lastIndexLocked() + 1
		reply.ConflictTerm = 0
		return reply, nil
	}
	if prevTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevTerm
		i := args.PrevLogIndex
		for i > 1 && r.log[i-1].Term == prevTerm {
			i--
		}
		reply.ConflictIndex = i
		return reply, nil
	}
	// Append new entries, truncating on the first conflict. Entries that
	// already match are left alone so a stale duplicate RPC cannot roll back
	// entries a later RPC already appended.
	changed := false
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= r.lastIndexLocked() {
			if r.log[idx].Term == e.Term {
				continue
			}
			r.log = r.log[:idx] // conflict: drop this entry and all that follow it
		}
		r.log = append(r.log, args.Entries[i:]...)
		changed = true
		break
	}
	if changed {
		if err := r.persistLocked(); err != nil {
			return nil, err
		}
	}
	if args.LeaderCommit > r.commitIndex {
		last := args.PrevLogIndex + uint64(len(args.Entries))
		r.commitIndex = min(args.LeaderCommit, last)
		r.applyCond.Broadcast()
	}
	reply.Success = true
	return reply, nil
}

// ---- election ----

func (r *Raft) electionLoop() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		timeout := r.electionTimeoutLocked()
		r.mu.Unlock()
		select {
		case <-r.stopCh:
			return
		case <-time.After(timeout / 4):
		}
		r.mu.Lock()
		if !r.stopped && r.state != Leader && time.Since(r.lastHeard) >= timeout {
			r.startElectionLocked()
		}
		r.mu.Unlock()
	}
}

func (r *Raft) startElectionLocked() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.cfg.ID
	r.leaderID = ""
	r.lastHeard = time.Now()
	// The self-vote counts toward the majority, so it must be durable before
	// any RequestVote goes out.
	if err := r.persistLocked(); err != nil {
		return
	}
	term := r.currentTerm
	args := &RequestVoteArgs{Term: term, CandidateID: r.cfg.ID, LastLogIndex: r.lastIndexLocked(), LastLogTerm: r.lastTermLocked()}
	r.logger.Debug("starting election", "term", term)

	votes := 1
	var once sync.Once
	for _, peer := range r.cfg.Peers {
		if peer == r.cfg.ID {
			continue
		}
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), r.cfg.ElectionTimeout)
			defer cancel()
			reply, err := r.cfg.Transport.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.stopped {
				return
			}
			if reply.Term > r.currentTerm {
				_ = r.becomeFollowerLocked(reply.Term, "") // on error the node has halted
				return
			}
			if r.state != Candidate || r.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes >= r.majority() {
				once.Do(r.becomeLeaderLocked)
			}
		}(peer)
	}
	if votes >= r.majority() { // single-node cluster
		once.Do(r.becomeLeaderLocked)
	}
}

func (r *Raft) becomeLeaderLocked() {
	r.state = Leader
	r.leaderID = r.cfg.ID
	next := r.lastIndexLocked() + 1
	for _, p := range r.cfg.Peers {
		r.nextIndex[p] = next
		r.matchIndex[p] = 0
	}
	// Append a no-op for the new term. A leader may only advance commitIndex
	// over entries of its own term (§5.4.2), so without this, entries left
	// over from a previous leader would stay uncommitted until the next client
	// proposal - which could be never. etcd and others do the same.
	noop := LogEntry{Term: r.currentTerm, Index: r.lastIndexLocked() + 1}
	r.log = append(r.log, noop)
	if err := r.persistLocked(); err != nil {
		return // halted; never announce leadership on a log we could not save
	}
	r.matchIndex[r.cfg.ID] = r.lastIndexLocked()
	r.nextIndex[r.cfg.ID] = r.lastIndexLocked() + 1
	r.logger.Info("became leader", "term", r.currentTerm, "lastIndex", r.lastIndexLocked())
	// Send an immediate heartbeat to assert leadership and ship the no-op.
	r.broadcastLocked()
}

// ---- replication ----

func (r *Raft) heartbeatLoop() {
	defer r.wg.Done()
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
		}
		r.mu.Lock()
		if !r.stopped && r.state == Leader {
			r.broadcastLocked()
		}
		r.mu.Unlock()
	}
}

func (r *Raft) broadcastLocked() {
	for _, peer := range r.cfg.Peers {
		if peer != r.cfg.ID {
			go r.replicate(peer)
		}
	}
}

func (r *Raft) replicate(peer string) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	term := r.currentTerm
	next := r.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	if next > r.lastIndexLocked()+1 {
		next = r.lastIndexLocked() + 1
	}
	prevIndex := next - 1
	prevTerm, _ := r.termAtLocked(prevIndex)
	entries := append([]LogEntry(nil), r.log[next:]...)
	args := &AppendEntriesArgs{Term: term, LeaderID: r.cfg.ID, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: r.commitIndex}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.ElectionTimeout)
	defer cancel()
	reply, err := r.cfg.Transport.AppendEntries(ctx, peer, args)
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	if reply.Term > r.currentTerm {
		_ = r.becomeFollowerLocked(reply.Term, "") // on error the node has halted
		return
	}
	if r.state != Leader || r.currentTerm != term {
		return
	}
	if reply.Success {
		newMatch := prevIndex + uint64(len(entries))
		if newMatch > r.matchIndex[peer] {
			r.matchIndex[peer] = newMatch
		}
		r.nextIndex[peer] = max(r.nextIndex[peer], newMatch+1)
		r.advanceCommitLocked()
		return
	}
	// Conflict: back up using the follower's hint.
	if reply.ConflictTerm == 0 {
		r.nextIndex[peer] = max(1, reply.ConflictIndex)
	} else {
		// If we have entries of ConflictTerm, resume after the last one;
		// otherwise jump to the follower's first index of that term.
		last := uint64(0)
		for i := len(r.log) - 1; i >= 1; i-- {
			if r.log[i].Term == reply.ConflictTerm {
				last = r.log[i].Index
				break
			}
		}
		if last > 0 {
			r.nextIndex[peer] = last + 1
		} else {
			r.nextIndex[peer] = max(1, reply.ConflictIndex)
		}
	}
	go r.replicate(peer) // retry immediately rather than waiting a heartbeat
}

// advanceCommitLocked implements §5.4.2: a leader commits an entry only once
// a majority stores it *and* the entry is from the leader's current term.
// Older-term entries are committed indirectly when a current-term entry is.
func (r *Raft) advanceCommitLocked() {
	matches := make([]uint64, 0, len(r.cfg.Peers))
	for _, p := range r.cfg.Peers {
		matches = append(matches, r.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	n := matches[r.majority()-1]
	if n > r.commitIndex {
		if t, ok := r.termAtLocked(n); ok && t == r.currentTerm {
			r.commitIndex = n
			r.applyCond.Broadcast()
		}
	}
}

func (r *Raft) applyLoop() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		for r.lastApplied >= r.commitIndex && !r.stopped {
			r.applyCond.Wait()
		}
		if r.stopped {
			r.mu.Unlock()
			return
		}
		r.lastApplied++
		e := r.log[r.lastApplied]
		r.mu.Unlock()
		select {
		case r.applyCh <- ApplyMsg{Index: e.Index, Term: e.Term, Command: e.Command, Noop: e.Command == nil}:
		case <-r.stopCh:
			return
		}
	}
}
