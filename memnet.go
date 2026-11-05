package raft

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// MemNetwork is an in-process transport with fault injection: partitions,
// message loss, and latency. Tests use it to drive the protocol through the
// scenarios that matter (leader isolation, minority partitions, flaky links)
// without sockets.
type MemNetwork struct {
	mu         sync.Mutex
	nodes      map[string]*Raft
	cut        map[string]bool    // node fully disconnected
	blocked    map[[2]string]bool // directed link blocked
	dropRate   float64
	maxDelay   time.Duration
	rng        *rand.Rand
	sent, lost int
}

var errUnreachable = errors.New("memnet: unreachable")

// NewMemNetwork returns a fully connected, lossless network with no nodes.
// Its random source has a fixed seed, so the sequence of drop and delay
// decisions is reproducible; which message each one hits still depends on
// goroutine scheduling.
func NewMemNetwork() *MemNetwork {
	return &MemNetwork{nodes: map[string]*Raft{}, cut: map[string]bool{}, blocked: map[[2]string]bool{}, rng: rand.New(rand.NewPCG(1, 2))}
}

// Transport returns the per-node view of the network (so the sender is known).
func (n *MemNetwork) Transport(from string) Transport { return &memTransport{net: n, from: from} }

// Register makes r reachable as id. Registering an id again replaces the
// previous node, which is how tests restart a crashed node at its address.
func (n *MemNetwork) Register(id string, r *Raft) {
	n.mu.Lock()
	n.nodes[id] = r
	n.mu.Unlock()
}

// Disconnect isolates a node in both directions, as if its cable were
// pulled; the node itself keeps running.
func (n *MemNetwork) Disconnect(id string) { n.mu.Lock(); n.cut[id] = true; n.mu.Unlock() }

// Connect undoes Disconnect. Links blocked by Partition stay blocked.
func (n *MemNetwork) Connect(id string) { n.mu.Lock(); delete(n.cut, id); n.mu.Unlock() }

// Partition splits the cluster into groups that cannot talk across groups.
func (n *MemNetwork) Partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = map[[2]string]bool{}
	for i, a := range groups {
		for j, b := range groups {
			if i == j {
				continue
			}
			for _, x := range a {
				for _, y := range b {
					n.blocked[[2]string{x, y}] = true
				}
			}
		}
	}
}

// Heal removes the partition set by Partition. Nodes isolated with
// Disconnect stay isolated.
func (n *MemNetwork) Heal() { n.mu.Lock(); n.blocked = map[[2]string]bool{}; n.mu.Unlock() }

// SetUnreliable drops a fraction of messages and adds random delay.
func (n *MemNetwork) SetUnreliable(dropRate float64, maxDelay time.Duration) {
	n.mu.Lock()
	n.dropRate, n.maxDelay = dropRate, maxDelay
	n.mu.Unlock()
}

// Counts reports how many requests were sent and how many of them were lost
// to a disconnect, a partition or a random drop. Replies dropped after the
// handler ran are not counted as lost.
func (n *MemNetwork) Counts() (sent, lost int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sent, n.lost
}

// deliver decides whether a message from->to gets through and returns the
// target node. Reply loss is modelled by a second roll after the handler runs.
func (n *MemNetwork) deliver(from, to string) (*Raft, time.Duration, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sent++
	target, ok := n.nodes[to]
	if !ok || n.cut[from] || n.cut[to] || n.blocked[[2]string{from, to}] {
		n.lost++
		return nil, 0, errUnreachable
	}
	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		n.lost++
		return nil, 0, errUnreachable
	}
	var delay time.Duration
	if n.maxDelay > 0 {
		delay = time.Duration(n.rng.Int64N(int64(n.maxDelay)))
	}
	return target, delay, nil
}

func (n *MemNetwork) replyLost() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dropRate > 0 && n.rng.Float64() < n.dropRate
}

type memTransport struct {
	net  *MemNetwork
	from string
}

func (t *memTransport) RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	target, delay, err := t.net.deliver(t.from, peer)
	if err != nil {
		return nil, err
	}
	if err := sleepCtx(ctx, delay); err != nil {
		return nil, err
	}
	reply, err := target.HandleRequestVote(args)
	if err != nil {
		return nil, err // the node halted: no reply, as if it had crashed
	}
	if t.net.replyLost() {
		return nil, errUnreachable
	}
	return reply, nil
}

func (t *memTransport) AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	target, delay, err := t.net.deliver(t.from, peer)
	if err != nil {
		return nil, err
	}
	if err := sleepCtx(ctx, delay); err != nil {
		return nil, err
	}
	reply, err := target.HandleAppendEntries(args)
	if err != nil {
		return nil, err // the node halted: no reply, as if it had crashed
	}
	if t.net.replyLost() {
		return nil, errUnreachable
	}
	return reply, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
