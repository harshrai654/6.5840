package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

const (
	Success HeartBeatStatus = iota
	LogMismatch
	RPCError
	LeaderSteppedDown
)

const HEARTBEAT_TIMEOUT = 150 * time.Millisecond

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persisted State
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader

	// Volatile state
	commitIndex           int           // index of highest log entry known to be committed
	lastApplied           int           // index of highest log entry applied to state machine
	state                 ServerState   // role of this server
	lastContactFromLeader time.Time     // Last timestamp at which leader sent heartbeat to current server
	electionTimeout       time.Duration // time duration since last recieved heartbeat after which election will be trigerred by this server
	applyCh               chan raftapi.ApplyMsg

	// Volatile leader state
	nextIndex  []int //	for each server, index of the next log entry to send to that server
	matchIndex []int //	for each server, index of highest log entry known to be replicated on server

	leaderCancelFunc context.CancelFunc
	replicatorCond   *sync.Cond
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
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[RequestVote: %d]: Candidate %d seeking vote for term: %d.\n", rf.me, args.CandidateId, args.Term)

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

		fmt.Printf("[RequestVote: %d]: Candidate %d is of older term. Candidate's term: %d | My current term %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)

		return
	} else {
		fmt.Printf("[RequestVote: %d]: Candidate %d is of newer or equal term. Candidate's term: %d | My current term %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)

		if args.Term > rf.currentTerm {
			if rf.state == StateLeader {
				if rf.leaderCancelFunc != nil {
					rf.leaderCancelFunc()
				}
			}

			rf.currentTerm = args.Term
			rf.state = StateFollower
			rf.votedFor = -1
		}

		canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
		var currentLatestLogTerm int
		currentLatestLogIndex := len(rf.log)

		if currentLatestLogIndex > 0 {
			currentLatestLogTerm = rf.log[currentLatestLogIndex-1].Term
		}

		isCandidateLogOlder := args.LastLogTerm < currentLatestLogTerm || (args.LastLogTerm == currentLatestLogTerm && args.LastLogIndex < currentLatestLogIndex)

		if canVote && !isCandidateLogOlder {
			fmt.Printf("[RequestVote: %d]: Granted vote for term: %d, To candidate %d.\n", rf.me, args.Term, args.CandidateId)
			rf.votedFor = args.CandidateId
			rf.lastContactFromLeader = time.Now()

			reply.VoteGranted = true
		} else {
			fmt.Printf("[RequestVote: %d]: Candidate %d log is older than me. Log(index/term): Candidate's: (%d, %d) | Mine: (%d, %d).\n", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, currentLatestLogIndex, currentLatestLogTerm)
			reply.VoteGranted = false
		}

		reply.Term = rf.currentTerm
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
		fmt.Printf("[AppendEntries: %d]: Recieved heartbeat from leader %d for term %d.\n", rf.me, args.LeaderId, args.Term)
	}

	if args.Term < rf.currentTerm {
		fmt.Printf("[AppendEntries: %d]:RPC from leader %d for term %d not acknowledged, Leader's term: %d is older than my current term: %d.\n", rf.me, args.LeaderId, args.Term, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm

		return
	} else {

		// Sent by current leader
		// Reset election timeout
		if rf.state == StateLeader {
			if rf.leaderCancelFunc != nil {
				rf.leaderCancelFunc()
			}
		}

		rf.lastContactFromLeader = time.Now()

		rf.currentTerm = args.Term
		rf.state = StateFollower

		latestLogIndex := len(rf.log) - 1
		logTerm := 0

		if args.PrevLogIndex <= latestLogIndex {
			logTerm = rf.log[args.PrevLogIndex].Term
		}

		if logTerm != args.PrevLogTerm {
			fmt.Printf("[AppendEntries: %d]: RPC from leader %d for term %d not acknowledged. Log terms do not match, (Leader term, Leader index): (%d, %d), peer's term for same log index: %d.\n", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, logTerm)
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}

		reply.Success = true
		reply.Term = rf.currentTerm

		fmt.Printf("[AppendEntries: %d]: RPC from leader %d for term %d acknowledged.\n", rf.me, args.LeaderId, args.Term)

		// Todo: Handle addition of entires to the log according to following instructions as per paper:
		// 3. If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		// 4. Append any new entries not already in the log
		if len(args.Entries) > 0 {
			rf.reconcileLogs(args.Entries, args.PrevLogIndex)
		}

		if args.LeaderCommit > rf.commitIndex {
			// rf.commitIndex =
		}
	}

}

func (rf *Raft) reconcileLogs(leaderEntries []LogEntry, leaderPrevLogIndex int) {
	nextIndex := leaderPrevLogIndex + 1
	currentLogLength := len(rf.log)
	leaderEntriesIndex := 0

	for nextIndex < currentLogLength && leaderEntriesIndex < len(leaderEntries) {
		if rf.log[nextIndex].Term != leaderEntries[leaderEntriesIndex].Term {
			break
		}

		nextIndex++
		leaderEntriesIndex++
	}

	if leaderEntriesIndex < len(leaderEntries) {
		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		rf.log = rf.log[:nextIndex]

		//  Append any new entries not already in the log
		rf.log = append(rf.log, leaderEntries[leaderEntriesIndex:]...)
	}
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
	rf.mu.Lock()

	index := -1
	term := -1
	isLeader := rf.state == StateLeader

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

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

	// 2. Signal all peer go-routines to send AppentEntries RPC
	// with entries according to their nextIndexes
	rf.replicatorCond.Broadcast()

	// Apply the log??
	// Waiting on the applyCh??

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
	fmt.Printf("[Raft Instance %d]: Killed.\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	lastLogIndex := len(rf.log)
	var lastLog LogEntry

	done := make(chan struct{})

	if lastLogIndex > 0 {
		lastLog = rf.log[lastLogIndex-1]
	}

	fmt.Printf("[Candidate: %d | Election Ticker]: Election timout! Initiating election for term %d.\n", rf.me, rf.currentTerm)
	fmt.Printf("[Candidate: %d | Election Ticker]: Election timeout reset to: %v.\n", rf.me, rf.electionTimeout)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLog.Term,
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
					fmt.Printf("[Candidate: %d | Election Ticker]: Requesting vote from peer: %d.\n", rf.me, peerIndex)
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
				fmt.Printf("[Candidate: %d | Election Ticker]: Candidate killed while waiting for peer RequestVote response. Aborting election process.\n", rf.me)
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
				fmt.Printf("[Candidate: %d | Election Ticker]: Stepping down as Candidate, Recieved RequestVoteReply with term value %d > %d - my currentTerm.\n", rf.me, res.Term, rf.currentTerm)

				rf.currentTerm = res.Term
				rf.state = StateFollower
				rf.mu.Unlock()

				close(done)
				return
			}

			if res.VoteGranted {
				voteCount++
				if voteCount >= majority {
					// Won election
					fmt.Printf("[Candidate: %d | Election Ticker]: Election won with %d/%d majority! New Leader:%d.\n", rf.me, voteCount, peerCount, rf.me)
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
			fmt.Printf("[Candidate: %d | Election Ticker]: Election timeout! Wrapping up election for term: %d. Got %d votes. Current state = %d. Current set term: %d.\n", rf.me, electionTerm, voteCount, rf.state, rf.currentTerm)
			rf.mu.Unlock()

			close(done)
			return
		}

	}
}

func (rf *Raft) sendHeartbeat(peerIndex int, ctx context.Context) HeartBeatStatus {
	rf.mu.Lock()

	if rf.state != StateLeader {
		if rf.leaderCancelFunc != nil {
			rf.leaderCancelFunc()
		}

		rf.mu.Unlock()
		return LeaderSteppedDown
	}

	reply := &AppendEntriesReply{}
	peerNextIndex := rf.nextIndex[peerIndex]
	prevLogIndex := peerNextIndex - 1
	prevLogTerm := rf.log[prevLogIndex].Term

	heartbeatTerm := rf.currentTerm
	peer := rf.peers[peerIndex]

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}

	fmt.Printf("[Leader heartbeat: %d]: Sending heartbeat to peer: %d.\n", rf.me, peerIndex)
	rf.mu.Unlock()

	ok := peer.Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()

	if ok {
		fmt.Printf("[Leader heartbeat: %d]: Sending heartbeat RPC to peer %d successful.\n", rf.me, peerIndex)
		select {
		case <-ctx.Done():
			return LeaderSteppedDown
		default:
			{
				// Check fot change in state during the RPC call
				if rf.currentTerm != heartbeatTerm || rf.state != StateLeader {
					// Leader already stepped down
					return LeaderSteppedDown
				}

				// Handle Heartbeat response
				if !reply.Success {
					if reply.Term > rf.currentTerm {
						fmt.Printf("[Leader heartbeat: %d]: Stepping down from leadership, Received heartbeat reply from peer %d, with term %d > %d - my term\n", rf.me, peerIndex, reply.Term, rf.currentTerm)

						rf.state = StateFollower
						rf.currentTerm = reply.Term
						rf.lastContactFromLeader = time.Now()

						if rf.leaderCancelFunc != nil {
							rf.leaderCancelFunc()
						}

						rf.mu.Unlock()
						return LeaderSteppedDown
					}

					// Follower rejected the AppendEntries RPC beacuse of log conflict
					// Update the nextIndex for this follower
					rf.nextIndex[peerIndex] = rf.nextIndex[peerIndex] - 1
					rf.mu.Unlock()
					return LogMismatch
				} else {
					fmt.Printf("[Leader heartbeat: %d]: Peer %d, responded success to heartbeat for term %d.\n", rf.me, peerIndex, rf.currentTerm)
					rf.mu.Unlock()

					return Success
					// rf.mu.Lock()
					// rf.nextIndex[peerIndex] = args.PrevLogIndex + 1
					// rf.mu.Unlock()
				}
			}
		}
	}
	fmt.Printf("[Leader heartbeat: %d]: Sending heartbeat RPC to peer %d failed.\n", rf.me, peerIndex)
	rf.mu.Unlock()
	return RPCError
}

func (rf *Raft) setupLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	rf.leaderCancelFunc = cancel

	for peerIndex := range rf.peers {
		if peerIndex != rf.me {
			rf.nextIndex[peerIndex] = len(rf.log)
			go rf.replicate(peerIndex, ctx)
		}
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(HEARTBEAT_TIMEOUT):
				rf.replicatorCond.Broadcast()
			}
		}
	}()
}

func (rf *Raft) replicate(peerIndex int, ctx context.Context) {
	// For now only setting up sending heartbeats
	// for !rf.killed() {
	// 	select {
	// 	case <-ctx.Done():
	// 		fmt.Printf("[Leader: %d]: Leader stepping down from leadership.\n", rf.me)
	// 		return
	// 	default:
	// 		status := rf.sendHeartbeat(peerIndex, ctx)

	// 		switch status {
	// 		case Success:
	// 			time.Sleep(HEARTBEAT_TIMEOUT)
	// 		case LeaderSteppedDown:
	// 			return
	// 		case LogMismatch:
	// 			continue
	// 		case RPCError:
	// 			heartbeatMS := 50 + (rand.Int63() % 300) // [50, 350)ms time range
	// 			time.Sleep(time.Duration(heartbeatMS) * time.Millisecond)
	// 		}
	// 	}

	// }
	logMismatch := false
	for !rf.killed() {
		select {
		case <-ctx.Done():
			fmt.Printf("[Leader: %d]: Leader stepped down from leadership.\n", rf.me)
			return
		default:
			rf.mu.Lock()

			// Only waiting when:
			// - There is no log to send - In this case the wait will be signalled by the heartbeat
			// - We are in a continuous loop to find correct nextIndex for this peer with retrial RPCs
			if !logMismatch && rf.nextIndex[peerIndex] >= len(rf.log) {
				rf.replicatorCond.Wait()
			}

			if rf.state != StateLeader {
				rf.mu.Unlock()
				return
			}

			reply := &AppendEntriesReply{}
			peerNextIndex := rf.nextIndex[peerIndex]
			prevLogIndex := peerNextIndex - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			replicateTerm := rf.currentTerm
			peer := rf.peers[peerIndex]

			logStartIndex := rf.nextIndex[peerIndex]
			logEndIndex := len(rf.log)
			nLogs := logEndIndex - logStartIndex

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
			}

			if nLogs > 0 {
				args.Entries = make([]LogEntry, nLogs)
				fmt.Printf("[Leader replicate: %d]: Sending AppendEntries RPC to peer: %d in term %d with log index range [%d, %d).\n", rf.me, peerIndex, replicateTerm, logStartIndex, logEndIndex)
			} else {
				fmt.Printf("[Leader replicate: %d]: Sending AppendEntries Heartbeat RPC to peer: %d for term %d.\n", rf.me, peerIndex, replicateTerm)
			}

			rf.mu.Unlock()

			ok := peer.Call("Raft.AppendEntries", args, reply)

			rf.mu.Lock()

			if ok {
				fmt.Printf("[Leader heartbeat: %d]: Sending heartbeat RPC to peer %d successful.\n", rf.me, peerIndex)
				select {
				case <-ctx.Done():
					return
				default:
					{
						// Check fot change in state during the RPC call
						if rf.currentTerm != replicateTerm || rf.state != StateLeader {
							// Leader already stepped down
							if rf.leaderCancelFunc != nil {
								rf.leaderCancelFunc()
							}
							rf.mu.Unlock()
							return
						}

						// Handle Heartbeat response
						if !reply.Success {
							if reply.Term > rf.currentTerm {
								fmt.Printf("[Leader heartbeat: %d]: Stepping down from leadership, Received heartbeat reply from peer %d, with term %d > %d - my term\n", rf.me, peerIndex, reply.Term, rf.currentTerm)

								rf.state = StateFollower
								rf.currentTerm = reply.Term
								rf.lastContactFromLeader = time.Now()

								if rf.leaderCancelFunc != nil {
									rf.leaderCancelFunc()
								}

								rf.mu.Unlock()
								return
							}

							// Follower rejected the AppendEntries RPC beacuse of log conflict
							// Update the nextIndex for this follower
							rf.nextIndex[peerIndex] = rf.nextIndex[peerIndex] - 1
							logMismatch = true
							rf.mu.Unlock()
							continue
						} else {
							fmt.Printf("[Leader heartbeat: %d]: Peer %d, responded success to heartbeat for term %d.\n", rf.me, peerIndex, rf.currentTerm)
							logMismatch = false

							if nLogs 
						}
					}
				}
			}
			fmt.Printf("[Leader heartbeat: %d]: Sending heartbeat RPC to peer %d failed.\n", rf.me, peerIndex)
			rf.mu.Unlock()
			return RPCError
		}
	}
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

// func (rf *Raft) heartbeats() {
// 	for !rf.killed() {
// 		rf.mu.Lock()
// 		done := make(chan struct{})

// 		if rf.state == StateLeader && time.Since(rf.lastHeartbeatSent) >= HEARTBEAT_TIMEOUT {
// 			for peerIndex, peer := range rf.peers {
// 				if peerIndex != rf.me {
// 					go rf.sendHeartbeat(peerIndex, peer, done)
// 				}
// 			}
// 			rf.lastHeartbeatSent = time.Now()
// 			rf.mu.Unlock()
// 		}

// 		select {
// 		case <-time.After(HEARTBEAT_TIMEOUT):
// 			close(done)
// 		case <-done:
// 			close(done)
// 		}
// 	}
// }

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

	// initialize from state persisted before a crash (3C)
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start heartbeat goroutine to send heartbeats
	// go rf.heartbeats()

	return rf
}
