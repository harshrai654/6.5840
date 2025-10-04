package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type ServerState int
type HeartBeatStatus int

const (
	StateFollower ServerState = iota
	StateLeader
	StateCandidate
)

const HEARTBEAT_TIMEOUT = 150 * time.Millisecond

const DEBUG = false

type LogEntry struct {
	Command interface{}
	Term    int
}

// dprintf logs conditionally based on DEBUG. It mirrors fmt.Printf's signature.
func dprintf(format string, v ...any) {
	if DEBUG {
		log.Printf(format, v...)
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persisted State
	currentTerm          int        // latest term server has seen
	votedFor             int        // candidateId that received vote in current term
	log                  []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader
	snapshotLastLogIndex int
	snapshotLastLogTerm  int
	// snapshot             []byte

	// Volatile state
	commitIndex           int                   // index of highest log entry known to be committed
	lastApplied           int                   // index of highest log entry applied to state machine
	state                 ServerState           // role of this server
	lastContactFromLeader time.Time             // Last timestamp at which leader sent heartbeat to current server
	electionTimeout       time.Duration         // time duration since last recieved heartbeat after which election will be trigerred by this server
	applyCh               chan raftapi.ApplyMsg // Channel where a raft server sends it commands to be applied to state machine

	// Volatile leader state
	nextIndex  []int      //	for each server, index of the next log entry to send to that server
	matchIndex []int      //	for each server, index of highest log entry known to be replicated on server
	applyCond  *sync.Cond // Condition validable to signal applier channel to send commands to apply channel

	leaderCancelFunc context.CancelFunc // Go context for a leader, called when we need to cancel leader's context and leader is stepping down
	replicatorCond   *sync.Cond         // Leader's conditon variable to signal replicator threads for each peer to either send heartbeat or new logs to each peer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == StateLeader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// func (rf *Raft) persist() {
// 	// Your code here (3C).
// 	// Example:
// 	// w := new(bytes.Buffer)
// 	// e := labgob.NewEncoder(w)
// 	// e.Encode(rf.xxx)
// 	// e.Encode(rf.yyy)
// 	// raftstate := w.Bytes()
// 	// rf.persister.Save(raftstate, nil)

// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)

// 	e.Encode(rf.currentTerm)
// 	e.Encode(rf.votedFor)
// 	e.Encode(rf.log)
// 	e.Encode(rf.snapshotLastLogIndex)
// 	e.Encode(rf.snapshotLastLogTerm)

// 	raftstate := w.Bytes()
// 	rf.persister.Save(raftstate, nil)

// 	dprintf("[Persist: %d]: Persisted state.\n", rf.me)
// }

func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	if len(snapshot) == 0 {
		snapshot = rf.persister.ReadSnapshot()
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastLogIndex)
	e.Encode(rf.snapshotLastLogTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)

	dprintf("[Persist: %d]: Persisted state with snapshot till index: %d, term: %d.\n", rf.me, rf.snapshotLastLogIndex, rf.snapshotLastLogTerm)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var log []LogEntry
	var currentTerm int
	var votedFor int
	var snapshotLastLogIndex int
	var snapshotLastLogTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&snapshotLastLogIndex) != nil || d.Decode(&snapshotLastLogTerm) != nil {
		dprintf("[Read Persist: %d]: Error decoding last persisted state.\n", rf.me)
	} else {
		rf.log = log
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.snapshotLastLogIndex = snapshotLastLogIndex
		rf.snapshotLastLogTerm = snapshotLastLogTerm
	}

	dprintf("[Read Persist: %d]: Raft state loaded from last persisted state.\n", rf.me)
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.snapshotLastLogIndex {
		return
	}
	snapshotLog := rf.log[index-rf.snapshotLastLogIndex]

	dprintf("[Peer: %d | Snapshot]: Taking snapshot till log index: %d with log at that term: %d.\n", rf.me, index, snapshotLog.Term)

	newLog := make([]LogEntry, 1, len(rf.log[index+1-rf.snapshotLastLogIndex:])+1)
	newLog[0] = LogEntry{Term: 0}
	newLog = append(newLog, rf.log[index+1-rf.snapshotLastLogIndex:]...)
	rf.log = newLog

	rf.snapshotLastLogTerm = snapshotLog.Term
	rf.snapshotLastLogIndex = index

	rf.persist(snapshot)

	dprintf("[Peer: %d | Snapshot]: Snapshot persisted, log truncated, snapshot last included log index: %d.\n", rf.me, rf.snapshotLastLogIndex)
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //currentTerm, for candidate to update itself in case someone else is leader now
	VoteGranted bool // true means candidate received vote
}

// Invoked by leader to replicate log entries; also used as heartbeat.
type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // Followers index which is conflicting with leader's prevLogIndex
	ConflictTerm  int  // Followers term of conflicting log
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	if rf.killed() {
		dprintf("[Peer Start: %d]: Killed peer cannot initiate log replication.\n", rf.me)
		return index, term, isLeader
	}

	rf.mu.Lock()

	isLeader = rf.state == StateLeader

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	term = rf.currentTerm

	// Your code here (3B).
	// Append the command to current logs.
	// We want to call AppendEntries RPC call to each peer and handle it response.
	// We want to block on `applyCh` so that the caller can wait for replication.
	// Once the replication is done on majority of peers we apply the command to state machine from last commitIndex to current logIndex
	// and updtae commit index and send the response back to client
	// Leader waits (and retires indefinetly) for AppendEntries response from all peers event if it responded to client

	// 1. Append the command as a new log entry to our logs
	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.persist(nil)

	dprintf("[Leader Start: %d]: Sending AppendEntries RPC broadcast to all peer specific threads.\n", rf.me)
	// 2. Signal all peer go-routines to send AppentEntries RPC
	// with entries according to their nextIndexes
	rf.replicatorCond.Broadcast()

	// Apply the log??
	// Waiting on the applyCh??

	index = len(rf.log) + rf.snapshotLastLogIndex - 1

	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// Broadcast to wake up any waiting goroutines so they can check killed()
	rf.replicatorCond.Broadcast()
	rf.applyCond.Broadcast()

	if rf.leaderCancelFunc != nil {
		rf.leaderCancelFunc()
	}

	dprintf("[Raft Instance %d]: Killed.\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	dprintf("[Peer: %d | RequestVote]: Candidate %d seeking vote for term: %d.\n", rf.me, args.CandidateId, args.Term)

	// Election voting restrictions for follower
	// - Candidate's term is older than follower from whom it is seeking vote
	// - Follower already voted
	// - Candidate's log is older then the follower
	// In all the above cases follower will not vote for the candidate and respond back with its current term
	// for the candidate to roll back to follower
	isCandidateOfOlderTerm := args.Term < rf.currentTerm

	if isCandidateOfOlderTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		dprintf("[Peer: %d | RequestVote]: Candidate %d is of older term. Candidate's term: %d | My current term %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)

		return
	} else {
		dprintf("[Peer: %d | RequestVote]: Candidate %d is of newer or equal term. Candidate's term: %d | My current term %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)

		if args.Term > rf.currentTerm {
			if rf.state == StateLeader {
				dprintf("[Peer: %d | RequestVote]: Recieved vote request from candiate of higher term, winding up my own leadership setup.\n", rf.me)
				if rf.leaderCancelFunc != nil {
					rf.leaderCancelFunc()
					rf.replicatorCond.Broadcast()
				}
			}

			rf.currentTerm = args.Term
			rf.state = StateFollower
			rf.votedFor = -1

			rf.persist(nil)
		}

		canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
		var currentLatestLogTerm int
		currentLatestLogIndex := len(rf.log) - 1

		if currentLatestLogIndex > 0 {
			currentLatestLogTerm = rf.log[currentLatestLogIndex].Term
		} else if rf.snapshotLastLogIndex > 0 {
			currentLatestLogTerm = rf.snapshotLastLogTerm
		}

		currentLatestLogIndex += rf.snapshotLastLogIndex

		isCandidateLogOlder := args.LastLogTerm < currentLatestLogTerm || (args.LastLogTerm == currentLatestLogTerm && args.LastLogIndex < currentLatestLogIndex)

		if canVote && !isCandidateLogOlder {
			dprintf("[Peer: %d | RequestVote]: Granted vote for term: %d, To candidate %d.\n", rf.me, args.Term, args.CandidateId)
			rf.votedFor = args.CandidateId
			rf.lastContactFromLeader = time.Now()

			reply.VoteGranted = true
			rf.persist(nil)
		} else {
			dprintf("[Peer: %d | RequestVote]: Candidate %d log is older than mine. Log(index/term): Candidate's: (%d, %d) | Mine: (%d, %d).\n", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, currentLatestLogIndex, currentLatestLogTerm)
			reply.VoteGranted = false
		}

		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	// Tigger election, send RequestVote RPC
	// Once you have voted for someone in a term the elction timeout should be reset
	// Reset election timer for self
	rf.lastContactFromLeader = time.Now()

	// Reset the election timeout with new value
	rf.electionTimeout = time.Duration(1500+(rand.Int63()%1500)) * time.Millisecond
	rf.currentTerm += 1 // increase term
	rf.state = StateCandidate
	peerCount := len(rf.peers)

	voteCount := 1 // self vote
	lastLogIndex := len(rf.log) - 1
	var lastLogTerm int

	done := make(chan struct{})

	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	} else if rf.snapshotLastLogIndex > 0 {
		lastLogTerm = rf.snapshotLastLogTerm
	}

	lastLogIndex += rf.snapshotLastLogIndex

	rf.persist(nil)

	dprintf("[Candidate: %d | Election Ticker]: Election timout! Initiating election for term %d, with lastLogIndex: %d & lastLogTerm: %d.\n", rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)
	dprintf("[Candidate: %d | Election Ticker]: Election timeout reset to: %v.\n", rf.me, rf.electionTimeout)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	requestVoteResponses := make(chan *RequestVoteReply)

	for peerIndex, peer := range rf.peers {
		if peerIndex != rf.me {
			go func(peer *labrpc.ClientEnd) {
				select {
				case <-done:
					// Either majority is achieved or candidate is stepping down as candidate
					// Dont wait for this peer's RequestVote RPC response and exit this goroutine
					// to prevent goroutine leak
					return
				default:
					reply := &RequestVoteReply{}
					dprintf("[Candidate: %d | Election Ticker]: Requesting vote from peer: %d.\n", rf.me, peerIndex)
					ok := peer.Call("Raft.RequestVote", args, reply)
					if ok {
						select {
						case requestVoteResponses <- reply:
						case <-done:
							return
						}
					}
				}
			}(peer)
		}
	}

	// Releasing the lock after making RPC calls
	// Each RPC call for RequestVote is in its own thread so its not blocking
	// We can release the lock after spawning RequestVote RPC thread for each peer
	// Before releasing the lock lets make copy of some state to verify sanity
	// After reacquiring the lock
	electionTerm := rf.currentTerm
	rf.mu.Unlock()

	majority := peerCount/2 + 1

	for i := 0; i < peerCount-1; i++ {
		select {
		case res := <-requestVoteResponses:
			if rf.killed() {
				dprintf("[Candidate: %d | Election Ticker]: Candidate killed while waiting for peer RequestVote response. Aborting election process.\n", rf.me)
				close(done) // Signal all other RequestVote goroutines to stop
				return
			}

			rf.mu.Lock()

			// State stale after RequestVote RPC
			if rf.currentTerm != electionTerm || rf.state != StateCandidate {
				rf.mu.Unlock()
				close(done)
				return
			}

			if res.Term > rf.currentTerm {
				// A follower voted for someone else
				// If they voted for same term then we can ignore
				// But if term number is higher than our current term then
				// we should step from candidate to follower and update our term as well
				dprintf("[Candidate: %d | Election Ticker]: Stepping down as Candidate, Recieved RequestVoteReply with term value %d > %d - my currentTerm.\n", rf.me, res.Term, rf.currentTerm)

				rf.currentTerm = res.Term
				rf.state = StateFollower
				rf.mu.Unlock()

				rf.persist(nil)
				close(done)
				return
			}

			if res.VoteGranted {
				voteCount++
				if voteCount >= majority {
					// Won election
					dprintf("[Candidate: %d | Election Ticker]: Election won with %d/%d majority! New Leader:%d.\n", rf.me, voteCount, peerCount, rf.me)
					rf.state = StateLeader

					rf.mu.Unlock()
					close(done)

					rf.setupLeader()
					return
				}
			}

			rf.mu.Unlock()

		case <-time.After(rf.electionTimeout):
			rf.mu.Lock()
			dprintf("[Candidate: %d | Election Ticker]: Election timeout! Wrapping up election for term: %d. Got %d votes. Current state = %d. Current set term: %d.\n", rf.me, electionTerm, voteCount, rf.state, rf.currentTerm)
			rf.mu.Unlock()

			close(done)
			return
		}

	}
}

func (rf *Raft) setupLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	rf.leaderCancelFunc = cancel

	for peerIndex := range rf.peers {
		if peerIndex != rf.me {
			rf.nextIndex[peerIndex] = len(rf.log) + rf.snapshotLastLogIndex
			go rf.replicate(peerIndex, ctx)
		}
	}

	go func() {
		for !rf.killed() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(HEARTBEAT_TIMEOUT):
				rf.replicatorCond.Broadcast()
			}
		}
	}()
}

func (rf *Raft) sendRPCWithTimeout(ctx context.Context, peer *labrpc.ClientEnd, peerIndex int, rpcType string, args any, reply any) bool {
	timeout := HEARTBEAT_TIMEOUT * 3

	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan bool, 1)

	go func() {
		var ok bool
		switch rpcType {
		case "InstallSnapshot":
			ok = peer.Call("Raft.InstallSnapshot", args, reply)
		case "AppendEntries":
			ok = peer.Call("Raft.AppendEntries", args, reply)
		}

		select {
		case done <- ok:
		case <-rpcCtx.Done():
			// Timeout occurred, abandon result
		}
	}()

	select {
	case success := <-done:
		return success
	case <-rpcCtx.Done():
		dprintf("[leader-replicate: %d | peer: %d]: %s RPC timed out after %v\n",
			rf.me, peerIndex, rpcType, timeout)
		return false
	}
}

func (rf *Raft) replicate(peerIndex int, ctx context.Context) {
	logMismatch := false
	for !rf.killed() {
		select {
		case <-ctx.Done():
			dprintf("[leader-replicate: %d | peer: %d]: Leader stepped down from leadership before initiating replicate.\n", rf.me, peerIndex)
			return
		default:
			rf.mu.Lock()

			if rf.state != StateLeader {
				dprintf("[leader-replicate: %d | peer: %d]: Not a leader anymore, winding up my leadership setup.\n", rf.me, peerIndex)
				if rf.leaderCancelFunc != nil {
					rf.leaderCancelFunc()
					rf.replicatorCond.Broadcast()
				}
				rf.mu.Unlock()
				return
			}

			// Only waiting when:
			// - There is no log to send - In this case the wait will be signalled by the heartbeat
			// - We are in a continuous loop to find correct nextIndex for this peer with retrial RPCs
			if !logMismatch && rf.nextIndex[peerIndex] >= len(rf.log)+rf.snapshotLastLogIndex {
				dprintf("[leader-replicate: %d | peer: %d]: Wating for next signal to replicate.\n", rf.me, peerIndex)
				rf.replicatorCond.Wait()
			}

			if rf.killed() {
				return
			}

			reply := &AppendEntriesReply{}
			logStartIndex := rf.nextIndex[peerIndex]
			var prevLogTerm int
			prevLogIndex := logStartIndex - 1
			peer := rf.peers[peerIndex]

			if prevLogIndex-rf.snapshotLastLogIndex > 0 {
				prevLogTerm = rf.log[prevLogIndex-rf.snapshotLastLogIndex].Term
			} else if prevLogIndex == rf.snapshotLastLogIndex {
				prevLogTerm = rf.snapshotLastLogTerm
			} else {
				// prevLogIndex < rf.snapshotLastLogIndex
				prevLogTerm = -1
			}

			if prevLogTerm == -1 {
				logMismatch = true
				// Leader does not have logs at `prevLogIndex` because of compaction
				// Leader needs to send snaphot to the peer as part of log repairing
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.snapshotLastLogIndex,
					LastIncludedTerm:  rf.snapshotLastLogTerm,
					Data:              rf.persister.ReadSnapshot(),
				}

				reply := &InstallSnapshotReply{}

				dprintf("[leader-install-snapshot: %d: peer: %d]: InstallSnapshot RPC with index: %d and term: %d sent.\n", rf.me, peerIndex, args.LastIncludedIndex, args.LastIncludedTerm)
				rf.mu.Unlock()

				// ok := peer.Call("Raft.InstallSnapshot", args, reply)
				ok := rf.sendRPCWithTimeout(ctx, peer, peerIndex, "InstallSnapshot", args, reply)

				rf.mu.Lock()

				if ok {
					if reply.Term > rf.currentTerm {
						dprintf("[leader-install-snapshot: %d: peer: %d]: Stepping down from leadership, Received InstallSnapshot reply from peer %d, with term %d > %d - my term\n", rf.me, peerIndex, peerIndex, reply.Term, rf.currentTerm)

						rf.state = StateFollower
						rf.currentTerm = reply.Term
						rf.lastContactFromLeader = time.Now()

						if rf.leaderCancelFunc != nil {
							rf.leaderCancelFunc()
							rf.replicatorCond.Broadcast()
						}
						rf.persist(nil)
						rf.mu.Unlock()
						return
					}

					dprintf("[leader-install-snapshot: %d: peer: %d]: Snapshot installed successfully\n", rf.me, peerIndex)
					rf.nextIndex[peerIndex] = rf.snapshotLastLogIndex + 1
					rf.mu.Unlock()
					continue
				} else {
					dprintf("[leader-install-snapshot: %d: peer: %d]: Snapshot installtion failed!\n", rf.me, peerIndex)
					rf.nextIndex[peerIndex] = rf.snapshotLastLogIndex
					rf.mu.Unlock()
					continue
				}
			} else {
				replicateTerm := rf.currentTerm

				logEndIndex := len(rf.log) + rf.snapshotLastLogIndex
				nLogs := logEndIndex - logStartIndex

				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: rf.commitIndex,
				}

				if nLogs > 0 {
					entriesToSend := rf.log[logStartIndex-rf.snapshotLastLogIndex:]
					args.Entries = make([]LogEntry, len(entriesToSend))
					copy(args.Entries, entriesToSend)
					dprintf("[leader-replicate: %d | peer: %d]: Sending AppendEntries RPC in term %d with log index range [%d, %d).\n", rf.me, peerIndex, replicateTerm, logStartIndex, logEndIndex)
				} else {
					dprintf("[leader-replicate: %d | peer: %d]: Sending AppendEntries Heartbeat RPC for term %d.\n", rf.me, peerIndex, replicateTerm)
				}

				rf.mu.Unlock()

				// ok := peer.Call("Raft.AppendEntries", args, reply)
				ok := rf.sendRPCWithTimeout(ctx, peer, peerIndex, "AppendEntries", args, reply)

				rf.mu.Lock()

				if ok {
					// dprintf("[Leader replicate: %d]: Sending AppendEntries RPC to peer %d successful.\n", rf.me, peerIndex)
					select {
					case <-ctx.Done():
						dprintf("[leader-replicate: %d | peer: %d]: Leader stepped down from leadership after sending AppendEntries RPC.\n", rf.me, peerIndex)
						rf.mu.Unlock()
						return
					default:
						// Check fot change in state during the RPC call
						if rf.currentTerm != replicateTerm || rf.state != StateLeader {
							// Leader already stepped down
							dprintf("[leader-replicate: %d | peer: %d]: Checked ladership state after getting AppendEntries Reply, Not a leader anymore, Winding up my leadership setup.\n", rf.me, peerIndex)
							if rf.leaderCancelFunc != nil {
								rf.leaderCancelFunc()
								rf.replicatorCond.Broadcast()
							}
							rf.mu.Unlock()
							return
						}

						// Handle Heartbeat response
						if !reply.Success {
							if reply.Term > rf.currentTerm {
								dprintf("[leader-replicate: %d: peer: %d]: Stepping down from leadership, Received ApppendEntries reply from peer %d, with term %d > %d - my term\n", rf.me, peerIndex, peerIndex, reply.Term, rf.currentTerm)

								rf.state = StateFollower
								rf.currentTerm = reply.Term
								rf.lastContactFromLeader = time.Now()

								if rf.leaderCancelFunc != nil {
									rf.leaderCancelFunc()
									rf.replicatorCond.Broadcast()
								}
								rf.persist(nil)
								rf.mu.Unlock()
								return
							}

							// Follower rejected the AppendEntries RPC beacuse of log conflict
							// Update the nextIndex for this follower
							logMismatch = true
							followersConflictTermPresent := false
							if reply.ConflictTerm != -1 {
								for i := prevLogIndex - rf.snapshotLastLogIndex; i > 0; i-- {
									if rf.log[i].Term == reply.ConflictTerm {
										rf.nextIndex[peerIndex] = i + 1 + rf.snapshotLastLogIndex
										followersConflictTermPresent = true
										break
									}

								}

								if !followersConflictTermPresent {
									rf.nextIndex[peerIndex] = reply.ConflictIndex
								}
							} else {
								rf.nextIndex[peerIndex] = reply.ConflictIndex
							}
							dprintf("[leader-replicate: %d | peer: %d]: Logmismatch - AppendEntries RPC with previous log index %d of previous log term %d failed. Retrying with log index:%d.\n", rf.me, peerIndex, prevLogIndex, prevLogTerm, rf.nextIndex[peerIndex])
							rf.mu.Unlock()
							continue
						} else {
							dprintf("[leader-replicate: %d | peer: %d]: responded success to AppendEntries RPC in term %d with log index range [%d, %d).\n", rf.me, peerIndex, replicateTerm, logStartIndex, logEndIndex)
							logMismatch = false

							if nLogs > 0 {
								// Log replication successful
								rf.nextIndex[peerIndex] = prevLogIndex + nLogs + 1
								rf.matchIndex[peerIndex] = prevLogIndex + nLogs

								// Need to track majority replication upto latest log index
								// - So that we can update commitIndex
								// - Apply logs upto commitIndex
								// Just an idea - maybe this needs to be done separately in a goroutine
								// Where we continuosly check lastApplied and commitIndex
								// Apply and lastApplied to commit index and if leader send the response to apply channel
								majority := len(rf.peers)/2 + 1

								for i := len(rf.log) - 1; i > rf.commitIndex-rf.snapshotLastLogIndex; i-- {
									matchedPeerCount := 1
									if rf.log[i].Term == rf.currentTerm {
										for pi := range rf.peers {
											if pi != rf.me && rf.matchIndex[pi] >= i+rf.snapshotLastLogIndex {
												matchedPeerCount++
											}
										}
									}

									// Largest possible log index greater the commitIndex replicated at majority of peers
									// update commitIndex
									if matchedPeerCount >= majority {
										rf.commitIndex = i + rf.snapshotLastLogIndex

										dprintf("[leader-replicate: %d | peer: %d]: Log index %d replicated to majority of peers.(%d/%d peers), updating commitIndex to : %d, current lastApplied value: %d.\n", rf.me, peerIndex, i+rf.snapshotLastLogIndex, matchedPeerCount, len(rf.peers), rf.commitIndex, rf.lastApplied)
										rf.applyCond.Signal()
										break
									}
								}
							}

							rf.mu.Unlock()
							continue
						}
					}
				}
				dprintf("[leader-replicate: %d | peer %d]: Sending AppendEntries RPC at leader's term: %d, failed. Payload prevLogIndex: %d | prevLogTerm: %d.\n", rf.me, peerIndex, replicateTerm, prevLogIndex, prevLogTerm)
				rf.mu.Unlock()
				continue
			}
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		if rf.killed() {
			return
		}

		i := rf.lastApplied + 1
		if i < rf.snapshotLastLogIndex+1 {
			i = rf.snapshotLastLogIndex + 1
		}

		messages := make([]raftapi.ApplyMsg, 0)
		for ; i <= rf.commitIndex; i++ {
			// Skip entries that are already in the snapshot
			if i <= rf.snapshotLastLogIndex {
				continue
			}
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.snapshotLastLogIndex].Command,
				CommandIndex: i,
			}
			messages = append(messages, msg)
		}
		commitIndex := rf.commitIndex
		rf.mu.Unlock()

		for _, msg := range messages {
			if rf.killed() {
				return
			}
			rf.applyCh <- msg
		}

		rf.mu.Lock()
		rf.lastApplied = commitIndex
		dprintf("[Peer: %d]: Applied logs till latest commit index: %d, lastApplied : %d.\n", rf.me, rf.lastApplied, rf.commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	// Handling heart beats from leader
	// When follower recieves heartbeat it should check for heartbeat validity

	// [Case: 1] if args.Term < rf.currentTerm then it means this follower is either a new leader
	// OR it voted for someone else after least heartbeat.
	// In such case follower should return reply.Success = false indicating it does not acknowledge the sender
	// as leader anymore and should set reply.Term = rf.currentTerm. (This indicated sender to step down from leadership)

	// [Case: 2] if args.Term >= rf.currentTerm the the current follower/candidate becomes follower again accepting current leader
	// In such case rf.currentTerm = args.Term and reply.Success = true with reply.Term = rf.currentTerm (same term as the sender)
	// In Case 2 we should reset the election timer since we have received the heartbeat from a genuine leader

	// In Case 1 since the previous leader is now left behind, there are 3 possibilities:
	// 		A. The current peer is a candidate now and an election was already started by it or even finished with it being the current leader
	// 		B. Some other peer is now a candidate with an election going on OR is a leader now
	// 		C. No election has taken place till now
	// In all the above cases we should not interrupt the election timeout or anything else

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) == 0 {
		// Heartbeat
		dprintf("[Peer: %d]: Recieved AppendEntries RPC as heartbeat from leader %d for term %d with commitIndex %d.\n", rf.me, args.LeaderId, args.Term, args.LeaderCommit)
	}

	if args.Term < rf.currentTerm {
		dprintf("[Peer: %d]: AppendEntries RPC from leader %d for term %d not acknowledged, Leader's term: %d is older than my current term: %d.\n", rf.me, args.LeaderId, args.Term, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm

		return
	} else {
		// Sent by current leader
		// Reset election timeout
		if rf.state == StateLeader {
			dprintf("[Peer: %d]: AppendEntries RPC recieved from current leader: %d, winding up my leadership setup.\n", rf.me, args.LeaderId)
			if rf.leaderCancelFunc != nil {
				rf.leaderCancelFunc()
				rf.replicatorCond.Broadcast()
			}
		}

		rf.lastContactFromLeader = time.Now()

		rf.currentTerm = args.Term
		rf.state = StateFollower

		rf.persist(nil)

		latestLogIndex := len(rf.log) - 1
		logTerm := 0

		if args.PrevLogIndex > rf.snapshotLastLogIndex && args.PrevLogIndex <= latestLogIndex+rf.snapshotLastLogIndex {
			logTerm = rf.log[args.PrevLogIndex-rf.snapshotLastLogIndex].Term
		} else if args.PrevLogIndex == rf.snapshotLastLogIndex {
			logTerm = rf.snapshotLastLogTerm
		} else if args.PrevLogIndex < rf.snapshotLastLogIndex {
			// This should trigger InstallSnapshot from leader
			reply.ConflictIndex = rf.snapshotLastLogIndex + 1
			reply.ConflictTerm = -1
		}

		if logTerm != args.PrevLogTerm {
			dprintf("[Peer: %d]: AppendEntries RPC from leader %d for term %d not acknowledged. Log terms do not match, (Leader term, Leader index): (%d, %d), peer's term for same log index: %d.\n", rf.me, args.LeaderId, args.Term, args.PrevLogTerm, args.PrevLogIndex, logTerm)
			reply.Success = false
			reply.Term = rf.currentTerm

			if args.PrevLogIndex <= latestLogIndex+rf.snapshotLastLogIndex {
				reply.ConflictTerm = logTerm
				i := args.PrevLogIndex - rf.snapshotLastLogIndex
				// Find fist index of `logTerm` in follower's log
				for ; i > 0; i-- {
					if rf.log[i].Term != logTerm {
						break
					}
				}

				reply.ConflictIndex = i + 1 + rf.snapshotLastLogIndex
			}

			return
		}

		reply.Success = true
		reply.Term = rf.currentTerm

		dprintf("[Peer: %d]: AppendEntries RPC from leader %d for term %d acknowledged.\n", rf.me, args.LeaderId, args.Term)

		// Todo: Handle addition of entires to the log according to following instructions as per paper:
		// 3. If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		// 4. Append any new entries not already in the log
		if len(args.Entries) > 0 {
			rf.reconcileLogs(args.Entries, args.PrevLogIndex)
		}

		if args.LeaderCommit > rf.commitIndex {
			// If leaderCommit > commitIndex, set commitIndex =
			// min(leaderCommit, index of last new entry)

			rf.commitIndex = args.LeaderCommit

			if args.LeaderCommit >= len(rf.log)+rf.snapshotLastLogIndex {
				rf.commitIndex = len(rf.log) - 1 + rf.snapshotLastLogIndex
			}

			dprintf("[Peer: %d]: Updated commmit index to %d.\n", rf.me, rf.commitIndex)

			rf.applyCond.Signal()
		}
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	latestLogIndex := len(rf.log) - 1 + rf.snapshotLastLogIndex

	if args.Term < rf.currentTerm {
		dprintf("[Peer: %d]: InstallSnapshot RPC from leader %d for term %d not acknowledged, Leader's term: %d is older than my current term: %d.\n", rf.me, args.LeaderId, args.Term, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm

		return
	} else {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.lastContactFromLeader = time.Now()
		rf.state = StateFollower

		if args.LastIncludedIndex > rf.snapshotLastLogIndex {
			// Leader's snaphot is bigger than peer's previous snapshot
			oldSnapshotIndex := rf.snapshotLastLogIndex
			rf.snapshotLastLogIndex = args.LastIncludedIndex
			rf.snapshotLastLogTerm = args.LastIncludedTerm

			var newLogs []LogEntry

			if latestLogIndex > args.LastIncludedIndex && rf.log[args.LastIncludedIndex-oldSnapshotIndex].Term == args.LastIncludedTerm {
				// Leader's snaphot is shorter then peer's and has no conflict
				newLogs = make([]LogEntry, 1, latestLogIndex-args.LastIncludedIndex+1)
				newLogs[0] = LogEntry{Term: 0}
				newLogs = append(newLogs, rf.log[args.LastIncludedIndex+1-oldSnapshotIndex:]...)
			} else {
				newLogs = make([]LogEntry, 1)
				newLogs[0] = LogEntry{Term: 0}
			}
			rf.log = newLogs

			rf.persist(args.Data)
			// if rf.commitIndex < args.LastIncludedIndex {
			// 	rf.commitIndex = args.LastIncludedIndex
			// }

			msg := raftapi.ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
				Snapshot:      args.Data,
			}

			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) reconcileLogs(leaderEntries []LogEntry, leaderPrevLogIndex int) {
	nextIndex := leaderPrevLogIndex + 1
	currentLogLength := len(rf.log) + rf.snapshotLastLogIndex
	leaderEntriesIndex := 0

	for nextIndex < currentLogLength && leaderEntriesIndex < len(leaderEntries) {
		if rf.log[nextIndex-rf.snapshotLastLogIndex].Term != leaderEntries[leaderEntriesIndex].Term {
			break
		}

		nextIndex++
		leaderEntriesIndex++
	}

	if leaderEntriesIndex < len(leaderEntries) {
		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		rf.log = rf.log[:nextIndex-rf.snapshotLastLogIndex]

		//  Append any new entries not already in the log
		rf.log = append(rf.log, leaderEntries[leaderEntriesIndex:]...)

		rf.persist(nil)
	}

	dprintf("[Peer %d]: Reconciled logs with leader from index %d, current logs length %d.\n", rf.me, leaderPrevLogIndex, len(rf.log)+rf.snapshotLastLogIndex)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		rf.mu.Lock()
		if rf.state != StateLeader && time.Since(rf.lastContactFromLeader) >= rf.electionTimeout {
			go rf.startElection()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		heartbeatMS := 50 + (rand.Int63() % 300) // [50, 350)ms time range
		time.Sleep(time.Duration(heartbeatMS) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	// Your initialization code here (3A, 3B, 3C).
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.log = make([]LogEntry, 1)
	rf.votedFor = -1
	rf.electionTimeout = time.Duration(1500+(rand.Int63()%1500)) * time.Millisecond
	rf.lastContactFromLeader = time.Now()
	rf.applyCh = applyCh
	rf.replicatorCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
