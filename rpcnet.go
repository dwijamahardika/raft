package raft

import (
	"context"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// RPCServiceName is the net/rpc service name under which Serve registers
// RPCService and to which RPCTransport sends its calls. It is part of the
// wire protocol: nodes only interoperate if they agree on it.
const RPCServiceName = "RaftRPC"

// RPCTransport carries Raft RPCs over TCP with the standard library's net/rpc
// (gob encoding). Connections are cached per peer and re-dialled on failure.
type RPCTransport struct {
	addrs   map[string]string // peer id -> host:port
	dialer  net.Dialer
	mu      sync.Mutex
	clients map[string]*rpc.Client
}

// NewRPCTransport returns a transport that reaches each peer at addrs[peer].
// A dial is bounded by dialTimeout (500ms if <= 0) and by the context of the
// RPC that triggered it. addrs must not be modified afterwards.
func NewRPCTransport(addrs map[string]string, dialTimeout time.Duration) *RPCTransport {
	if dialTimeout <= 0 {
		dialTimeout = 500 * time.Millisecond
	}
	return &RPCTransport{addrs: addrs, dialer: net.Dialer{Timeout: dialTimeout}, clients: map[string]*rpc.Client{}}
}

func (t *RPCTransport) client(ctx context.Context, peer string) (*rpc.Client, error) {
	t.mu.Lock()
	if c, ok := t.clients[peer]; ok {
		t.mu.Unlock()
		return c, nil
	}
	t.mu.Unlock()
	conn, err := t.dialer.DialContext(ctx, "tcp", t.addrs[peer])
	if err != nil {
		return nil, err
	}
	c := rpc.NewClient(conn)
	t.mu.Lock()
	if existing, ok := t.clients[peer]; ok {
		t.mu.Unlock()
		_ = c.Close() // lost a dial race; this client never carried a call
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
		_ = c.Close() // discarding a broken or timed-out connection; the caller already has the RPC's error
	}
	t.mu.Unlock()
}

func (t *RPCTransport) call(ctx context.Context, peer, method string, args, reply any) error {
	c, err := t.client(ctx, peer)
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

// RequestVote implements Transport. Any failure, including the peer having
// halted, is returned as an error; Raft treats it as a lost message.
func (t *RPCTransport) RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	var reply RequestVoteReply
	if err := t.call(ctx, peer, RPCServiceName+".RequestVote", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

// AppendEntries implements Transport, with the same error semantics as
// RequestVote.
func (t *RPCTransport) AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	var reply AppendEntriesReply
	if err := t.call(ctx, peer, RPCServiceName+".AppendEntries", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

// RPCService is the net/rpc receiver that exposes a node's RPC handlers.
// Serve registers it; to share an existing rpc.Server instead, register it
// with srv.RegisterName(RPCServiceName, &RPCService{R: r}).
type RPCService struct{ R *Raft }

// RequestVote serves Raft.HandleRequestVote. A handler error (the node has
// halted) is returned to net/rpc, which then sends the caller an error
// instead of a reply, exactly as if the node had crashed.
func (s *RPCService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rep, err := s.R.HandleRequestVote(args)
	if err != nil {
		return err
	}
	*reply = *rep
	return nil
}

// AppendEntries serves Raft.HandleAppendEntries, with the same error
// semantics as RequestVote.
func (s *RPCService) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rep, err := s.R.HandleAppendEntries(args)
	if err != nil {
		return err
	}
	*reply = *rep
	return nil
}

// Serve answers r's RPCs on connections accepted from ln until ctx is done,
// then closes ln and returns nil. Any other Accept error is returned.
func Serve(ctx context.Context, ln net.Listener, r *Raft) error {
	srv := rpc.NewServer()
	if err := srv.RegisterName(RPCServiceName, &RPCService{R: r}); err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close() // only to unblock Accept; the loop below reports why it stopped
	}()
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
