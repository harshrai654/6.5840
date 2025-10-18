# RAFT Consensus Algorithm Implementation

A complete implementation of the RAFT consensus algorithm for building fault-tolerant, replicated systems.

## Overview

This is an implementation of **RAFT** (Raft Consensus Algorithm), a consensus protocol designed to manage replicated logs in distributed systems. RAFT simplifies consensus by breaking it down into three independent, manageable subproblems:

1. **Leader Election** - Electing a new leader when the current leader fails
2. **Log Replication** - The leader replicates log entries across the cluster
3. **Safety** - Ensuring consistency: if a server applies an entry at a given index, all servers must apply the same entry at that index

## Core Features

### Leader Election

- **Candidate-based voting**: Servers transition to candidates and request votes from peers
- **Term-based randomized timeouts**: Prevents split-vote scenarios and ensures liveness
- **Majority-based leader establishment**: A candidate needs votes from a majority to become leader

### Log Replication

- **AppendEntries RPC**: Used for both log replication and heartbeats
- **Log consistency checks**: `PrevLogIndex` and `PrevLogTerm` ensure followers have matching log prefixes
- **Automatic log reconciliation**: Followers repair log conflicts by following the leader's log
- **Snapshot-based log compaction**: Efficient recovery of followers from large log divergences

### Commitment & Application

- **Majority-based commitment**: Log entries are committed only after safe replication to a majority
- **Current term constraint**: Entries committed only if they're from the leader's current term
- **Applier goroutine**: Dedicated thread applies committed entries to the state machine

## Implementation Highlights

- **Context-based leader tracking**: Uses Go's `context.Context` for coordinated leader step-down
- **Condition variables for signaling**: Efficient coordination between leader and replicator goroutines
- **Persistence layer**: Durable state storage for recovery
- **RPC timeout handling**: Resilient to network delays

## References

- **Original RAFT Paper**: [In Search of an Understandable Consensus Algorithm](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)
- **Interactive Visualization**: [Raft Visualization](https://raft.github.io/raftscope/index.html)
- **Blog Series**:
  - [Building Fault Tolerant KV Storage System - Part 1](https://harshrai654.github.io/blogs/building-fault-tolerant-kv-storage-system---part-1/)
  - [Building Fault Tolerant KV Storage System - Part 2](https://harshrai654.github.io/blogs/building-fault-tolerant-kv-storage-system---part-2/)

## Testing

The implementation passes all standard RAFT tests including:

- Basic agreement (reliable network)
- RPC byte count optimization
- Progressive failure of followers
- Leader failure handling
- Concurrent Start() operations
- Partition recovery

## Key Insights

- **Why Current Term Matters**: Old term entries cannot be committed because a server with a newer term could become leader and overwrite them
- **Leader Initialization**: Uses goroutines for each peer to manage replication independently
- **Safety Through Majority**: Commitment and leadership decisions rely on majority consensus to prevent split-brain scenarios

---

_For detailed walkthroughs of election, log replication, and commitment logic, refer to the [blog series](https://harshrai654.github.io/blogs/)._
