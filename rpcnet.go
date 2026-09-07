package raft

import (
	"context"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// RPCTransport carries Raft RPCs over TCP with the standard library's net/rpc
// (gob encoding). Connections are cached per peer and re-dialled on failure.
type RPCTransport struct {
	addrs   map[string]string // peer id -> host:port
	timeout time.Duration
	mu      sync.Mutex
	clients map[string]*rpc.Client
}

func NewRPCTransport(addrs map[string]string, dialTimeout time.Duration) *RPCTransport {
	if dialTimeout <= 0 {
		dialTimeout = 500 * time.Millisecond
	}
	return &RPCTransport{addrs: addrs, timeout: dialTimeout, clients: map[string]*rpc.Client{}}
}

func (t *RPCTransport) client(peer string) (*rpc.Client, error) {
	t.mu.Lock()
	if c, ok := t.clients[peer]; ok {
		t.mu.Unlock()
		return c, nil
	}
	t.mu.Unlock()
	conn, err := net.DialTimeout("tcp", t.addrs[peer], t.timeout)
	if err != nil {
		return nil, err
	}
	c := rpc.NewClient(conn)
	t.mu.Lock()
	if existing, ok := t.clients[peer]; ok {
		t.mu.Unlock()
		c.Close()
		return existing, nil
	}
	t.clients[peer] = c
	t.mu.Unlock()
	return c, nil
}

func (t *RPCTransport) drop(peer string, c *rpc.Client) {
	t.mu.Lock()
	if t.clients[peer] == c {
		delete(t.clients, peer)
		c.Close()
	}
	t.mu.Unlock()
}

func (t *RPCTransport) call(ctx context.Context, peer, method string, args, reply any) error {
	c, err := t.client(peer)
	if err != nil {
		return err
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-ctx.Done():
		t.drop(peer, c)
		return ctx.Err()
	case done := <-call.Done:
		if done.Error != nil {
			t.drop(peer, c)
		}
		return done.Error
	}
}

func (t *RPCTransport) RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	var reply RequestVoteReply
	if err := t.call(ctx, peer, "RaftRPC.RequestVote", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *RPCTransport) AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	var reply AppendEntriesReply
	if err := t.call(ctx, peer, "RaftRPC.AppendEntries", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

// RaftRPC is the net/rpc receiver; register it with rpc.RegisterName("RaftRPC", &RaftRPC{r}).
type RaftRPC struct{ R *Raft }

func (s *RaftRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	*reply = *s.R.HandleRequestVote(args)
	return nil
}

func (s *RaftRPC) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	*reply = *s.R.HandleAppendEntries(args)
	return nil
}

// Serve accepts RPC connections until ctx is done.
func Serve(ctx context.Context, ln net.Listener, r *Raft) error {
	srv := rpc.NewServer()
	if err := srv.RegisterName("RaftRPC", &RaftRPC{R: r}); err != nil {
		return err
	}
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go srv.ServeConn(conn)
	}
}
