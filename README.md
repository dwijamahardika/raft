# raft

An implementation of the **Raft consensus algorithm** in Go — leader election, log replication, and the safety rules that make a replicated log behave like a single machine — with a fault-injecting network simulator that puts the protocol through the failures it exists to survive.

```
go test ./...     # ~15s: elections, partitions, crashes, 15% packet loss
```

The library is transport- and storage-agnostic. `cmd/raftkv` runs a real 3-node replicated key-value store over TCP (`net/rpc`) with a small HTTP front end.

## What it demonstrates

| Concern | Solution | Where |
|---|---|---|
| Leader election | Randomised timeouts, term-based voting, **election restriction** (§5.4.1: only vote for a candidate whose log is at least as up to date), immediate step-down on seeing a higher term. | `HandleRequestVote`, `startElectionLocked` |
| Log replication | `AppendEntries` with consistency check, truncate-on-conflict that never rolls back already-matching entries (so a stale duplicate RPC is harmless), **fast backup** via conflict term/index (§5.3) so a lagging follower catches up one term per round trip, not one entry. | `HandleAppendEntries`, `replicate` |
| Commit safety | A leader advances `commitIndex` only over entries of **its own term** stored on a majority (§5.4.2, the "Figure 8" rule). A **no-op entry on election** ensures earlier-term entries get committed promptly instead of waiting for the next client write — the same trick etcd uses. | `advanceCommitLocked`, `becomeLeaderLocked` |
| Durability | `currentTerm`, `votedFor` and the log are persisted **before** any RPC reply or proposal ack. `FilePersister` writes atomically (tmp + fsync + rename). Nodes crash and restart mid-test without violating safety. | `persistLocked`, `persister.go` |
| Fault injection | `MemNetwork`: per-node disconnect, arbitrary **partitions**, probabilistic message loss (request *and* reply), random latency, message counters. Deterministic seed. | `memnet.go` |
| Real transport | `net/rpc` over TCP with per-peer connection cache and re-dial on failure; context-driven timeouts. | `rpcnet.go` |
| Client semantics | `Propose` returns `(index, term)`; the demo waits for that index to apply and checks the term still matches — if a leader change overwrote the entry, the client is told to retry rather than getting a silent false ack. | `cmd/raftkv` |
| Testing | Stable initial election; re-election after leader isolation and old-leader step-down; agreement with a follower down and catch-up on rejoin; **no agreement without a majority**; 30 concurrent proposals with identical logs on all nodes; persistence across a full-cluster crash; **minority-leader entries overwritten** after partition heals; agreement under 15% loss + jitter; unit tests for stale-term rejection and conflict hints. Every test asserts "at most one leader per term" and "no gaps in applied indexes". | `raft_test.go` |

Zero dependencies.

## Design notes

**The bug the simulator found.** The first version seeded each node's random generator from the wall clock. Nodes created in the same instant got identical election timeouts and split the vote *every* round — forever. The reliable-network tests passed by luck (scheduler jitter); the lossy test failed deterministically. It's a nice illustration of why Raft's randomised timeouts are load-bearing, and why fault injection belongs in the test suite, not in production.

**Why is `commitIndex` only advanced for current-term entries?** Figure 8 of the paper: an entry replicated on a majority can still be overwritten if it belongs to an older term and a new leader with a different log wins. Committing it would break the "committed entries are never lost" guarantee. The no-op on election exists precisely so this rule doesn't leave old entries in limbo.

**Why not `httputil`-style retries in the transport?** Raft already retries at the protocol level (heartbeats re-drive replication every interval). Transport-level retries would only add duplicate RPCs, which the handlers must tolerate anyway — and do.

**Not implemented, deliberately:** log compaction / `InstallSnapshot` (§7), membership changes (§6), `ReadIndex` for linearizable reads, pre-vote. Each is an incremental addition; none changes the structure here.

## Layout

```
raft.go        protocol: state, election, replication, commit, apply loop
memnet.go      simulated network with partitions / loss / delay
rpcnet.go      net/rpc transport + server
persister.go   in-memory and atomic-file persisters
raft_test.go   cluster harness and scenario tests
cmd/raftkv/    3-node replicated KV over TCP with HTTP API
```

## Try the demo

```bash
P=n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003
go run ./cmd/raftkv -id n1 -peers $P -http :8001 -data ./data/n1 &
go run ./cmd/raftkv -id n2 -peers $P -http :8002 -data ./data/n2 &
go run ./cmd/raftkv -id n3 -peers $P -http :8003 -data ./data/n3 &

curl -si -X PUT localhost:8001/kv/a -d 1        # 204, or 421 + "Leader: nX"
curl localhost:8003/kv/a                        # 1 (replicated)
curl localhost:8002/status | jq
kill %1 && sleep 1 && curl localhost:8002/status | jq .leader   # re-elected
```
