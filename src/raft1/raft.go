package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
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

const (
	StateFollower ServerState = iota
	StateLeader
	StateCandidate
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
	lastHeartbeatSent     time.Time
	applyCh               chan raftapi.ApplyMsg

	// Volatile leader state
	nextIndex  []int //	for each server, index of the next log entry to send to that server
	matchIndex []int //	for each server, index of highest log entry known to be replicated on server
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

	fmt.Printf("[RequestVote: %d]: Candidate %d seeking vote.\n", rf.me, args.CandidateId)

	// Election voting restrictions for follower
	// - Candidate's term is older than follower from whom it is seeking vote
	// - Follower already voted
	// - Candidate's log is older then the follower
	// In all the above cases follower will not vote for the candidate and respond back with its current term
	// for the candidate to roll back to follower
	isCandidateOfOlderTerm := args.Term < rf.currentTerm
	followerAlreadyVoted := rf.votedFor != -1 && args.Term == rf.currentTerm // Specifically voted for someone in this term
	var currentLatestLogTerm int
	currentLatestLogIndex := len(rf.log)

	if currentLatestLogIndex > 0 {
		currentLatestLogTerm = rf.log[currentLatestLogIndex-1].Term
	}

	isCandidateLogOlder := args.LastLogTerm < currentLatestLogTerm || (args.LastLogTerm == currentLatestLogTerm && args.LastLogIndex < currentLatestLogIndex)

	if isCandidateOfOlderTerm || followerAlreadyVoted || isCandidateLogOlder {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		if isCandidateLogOlder {
			fmt.Printf("[RequestVote: %d]: Candidate %d log is older than me. Log(index/term): Candidate's: (%d, %d) | Mine: (%d, %d).\n", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, currentLatestLogIndex, currentLatestLogTerm)
		}

		if isCandidateOfOlderTerm {
			fmt.Printf("[RequestVote: %d]: Candidate %d is of older term. Candidate's term: %d | My current term %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		}

		if followerAlreadyVoted {
			fmt.Printf("[RequestVote: %d]: I already voted for %d for term %d.\n", rf.me, rf.votedFor, rf.currentTerm)
		}
		return
	}

	// Grant vote
	rf.currentTerm = args.Term
	rf.state = StateFollower
	rf.votedFor = args.CandidateId
	// resetting election timeout to prevent election by follower for atleast this term
	rf.lastContactFromLeader = time.Now()

	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	fmt.Printf("[RequestVote: %d]: Candidate %d,  granted vote for term: %d.\n", rf.me, args.CandidateId, args.Term)
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

	if len(args.Entries) > 0 {
		// Log append case by leader
		// TODO: replicate log entry 3B
	} else {
		// Heartbeat
		fmt.Printf("[AppendEntries: %d]: Recieved heartbeat from leader %d for term %d.\n", rf.me, args.LeaderId, args.Term)
		if args.Term > rf.currentTerm || args.Term == rf.currentTerm && rf.state != StateLeader {
			// Sent by current leader
			// Reset election timeout
			rf.lastContactFromLeader = time.Now()

			// Update peer's current term to received leader's term
			rf.currentTerm = args.Term
			rf.state = StateFollower
			reply.Success = true
			reply.Term = rf.currentTerm

			fmt.Printf("[AppendEntries: %d]: Heartbeat from leader %d for term %d acknowledged.\n", rf.me, args.LeaderId, args.Term)
			return
		}

		if args.Term < rf.currentTerm {
			fmt.Printf("[AppendEntries: %d]: Heartbeat from leader %d for term %d not acknowledged, Leader's term: %d is older than my current term: %d.\n", rf.me, args.LeaderId, args.Term, args.Term, rf.currentTerm)
		} else {
			fmt.Printf("[AppendEntries: %d]: Heartbeat from Peer %d for term %d not acknowledged, Peer's term: %d = my current term: %d and I am the leader of this term.\n", rf.me, args.LeaderId, args.Term, args.Term, rf.currentTerm)
		}

		reply.Term = rf.currentTerm
		reply.Success = false // Here false status indicates that the heartbeat was not acknowledged, telling sender to update it's current term and step down as leader
		return
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
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.state == StateLeader

	if !isLeader {
		return index, term, isLeader
	}

	// Your code here (3B).
	// Append the command to current logs.
	// We want to call AppendEntries RPC call to each peer and handle it response.
	// We want to block on `applyCh` so that the caller can wait for replication.
	// Once the replication is done on majority of peers we apply the command to state machine from last commitIndex to current logIndex
	// and updtae commit index and send the response back to client
	// Leader waits (and retires indefinetly) for AppendEntries response from all peers event if it responded to client

	// Append the command to own logs
	rf.log = append(rf.log, LogEntry{
		command: command,
		Term:    rf.currentTerm,
	})

	prevLogIndex := len(rf.log) - 1
	prevLog := rf.log[prevLogIndex]
	prevLogTerm := prevLog.Term

	appendEntriesResponse := make(chan AppendEntriesReply)

	// Send AppendEntries RPC to each peer in parallel
	for peerIndex, peer := range rf.peers {
		go func(peer *labrpc.ClientEnd) {
			// We have to send AppendEntries RPC to each peer in such a way that
			// the leader will send it indefinetly untill all peers have replicated till current
			// log index with correct term
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
				Entries: []LogEntry{
					Command: command,
					Term: rf.currentTerm
				}
			}
			reply := &AppendEntriesReply{}
			fmt.Printf("[Candidate Ticker: %d]: Requesting vote from peer: %d.\n", rf.me, peerIndex)
			ok := peer.Call("Raft.RequestVote", args, reply)
			if ok {
				select {
				case requestVoteResponses <- reply:
				case <-done:
					return
				}
			}
		}(peer)
	}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	// Tigger election, send RequestVote RPC
	// Once you have voted for someone in a term the elction timeout should be reset
	// Reset election timer for self
	rf.lastContactFromLeader = time.Now()
	// Reset the election timeout with new value
	rf.electionTimeout = time.Duration(1500+(rand.Int63()%1500)) * time.Millisecond

	voteCount := 1      // self vote
	rf.currentTerm += 1 // increase term
	rf.state = StateCandidate

	lastLogIndex := len(rf.log)
	var lastLog LogEntry
	done := make(chan struct{})
	peerCount := len(rf.peers)

	if lastLogIndex > 0 {
		lastLog = rf.log[lastLogIndex-1]
	}

	fmt.Printf("[Candidate Ticker: %d]: Election timout! Initiating election for term %d.\n", rf.me, rf.currentTerm)
	fmt.Printf("[Candidate Ticker: %d]: Election timeout reset to: %v.\n", rf.me, rf.electionTimeout)

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
					fmt.Printf("[Candidate Ticker: %d]: Requesting vote from peer: %d.\n", rf.me, peerIndex)
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

	isElgibleForLeader := true
	majority := peerCount/2 + 1

electionLoop:
	for i := 0; i < peerCount-1; i++ {
		select {
		case res := <-requestVoteResponses:
			if res.VoteGranted {
				voteCount++
				if voteCount >= majority && isElgibleForLeader {
					// Won election
					fmt.Printf("[Candidate Ticker: %d]: Election won with %d/%d majority! New Leader:%d.\n", rf.me, voteCount, peerCount, rf.me)

					rf.state = StateLeader
					close(done)        // Signal all other RequestVote goroutines to stop
					break electionLoop // Exit the switch case, not the function
				}
			} else {
				// A follower voted for someone else
				// If they voted for same term then we can ignore
				// But if term number is higher than our current term then
				// we should step from candidate to follower and update our term as well
				if res.Term > rf.currentTerm {
					fmt.Printf("[Candidate Ticker: %d]: Stepping down as Candidate, Recieved RequestVoteReply with term value %d > %d - my currentTerm.\n", rf.me, res.Term, rf.currentTerm)

					rf.currentTerm = res.Term
					rf.state = StateFollower
					isElgibleForLeader = false
					close(done)
					break electionLoop
				}
			}
		case <-time.After(rf.electionTimeout):
			close(done)
			fmt.Printf("[Candidate Ticker: %d]: Election timeout! Wrapping up election for term: %d, was not able to gain majority.\n", rf.me, rf.currentTerm)
			break electionLoop
		}

	}
}

func (rf *Raft) sendHeartbeats() {
	// Send heartbeats to all peers, send AppendEntries RPC
	heartbeatResponses := make(chan *AppendEntriesReply)
	done := make(chan struct{})
	peerCount := len(rf.peers)

	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	for peerIndex, peer := range rf.peers {
		if peerIndex != rf.me {
			go func(peer *labrpc.ClientEnd) {
				select {
				case <-done:
					return
				default:
					reply := &AppendEntriesReply{}
					fmt.Printf("[Leader Ticker: %d]: Sending heartbeat to peer: %d.\n", rf.me, peerIndex)
					ok := peer.Call("Raft.AppendEntries", args, reply)
					if ok {
						select {
						case <-done:
							return
						case heartbeatResponses <- reply:
						}
					}
				}
			}(peer)
		}
	}

	rf.lastHeartbeatSent = time.Now()

heartbeatCollectorLoop:
	for i := 0; i < peerCount-1; i++ {
		select {
		case res := <-heartbeatResponses:
			if !res.Success && res.Term != 0 {
				// Some follower did not acknowledged the heartbeat
				// There is another leader with term  > current leader's term
				// Current leader should go back to being a follower with updated term
				// Should leader wait for all responses?
				fmt.Printf("[Leader Ticker: %d]: Stepping down from leadership, Received heartbeat reply from a peer with term %d > %d - my current term\n", rf.me, res.Term, rf.currentTerm)

				rf.state = StateFollower
				rf.currentTerm = res.Term
				rf.lastContactFromLeader = time.Now()

				close(done)
				break heartbeatCollectorLoop
			} else {
				fmt.Printf("[Leader Ticker: %d]: A peer responded to heartbeat for term %d.\n", rf.me, rf.currentTerm)
			}
		case <-time.After(HEARTBEAT_TIMEOUT):
			close(done)
			fmt.Printf("[Leader Ticker: %d]: Heartbeat timeout. Not able to gather response for each heartbeat request, Recieved heartbeat from %d peers.\n", rf.me, i)
			break heartbeatCollectorLoop
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		switch rf.state {
		case StateFollower, StateCandidate:
			if time.Since(rf.lastContactFromLeader) >= rf.electionTimeout {
				rf.startElection()
			}
		case StateLeader:
			if time.Since(rf.lastHeartbeatSent) >= HEARTBEAT_TIMEOUT {
				rf.sendHeartbeats()
			}
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
	rf.log = make([]LogEntry, 1)
	rf.votedFor = -1
	rf.electionTimeout = time.Duration(1500+(rand.Int63()%1500)) * time.Millisecond
	rf.lastHeartbeatSent = time.Now()
	rf.lastContactFromLeader = time.Now()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash (3C)
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
