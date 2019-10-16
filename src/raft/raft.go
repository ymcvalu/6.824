package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/src/labrpc"
	"math/rand"
	"sync"
	"time"
	// "bytes"
	// "labgob"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type LogEntry struct {
	Term  int
	Index int
	Data  []byte
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type TickFunc func()

//
// A Go object implementing a single Raft peer.
//

type Role uint8

const (
	_ Role = iota
	RoleFollower
	RoleCandidate
	RoleLeader
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all instances
	currentTerm int
	voteFor     int
	logs        []LogEntry

	// volatile state on all instances
	votes       map[int]bool
	commitIndex int
	lastApplied int
	role        Role
	curLeader   int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	// timeout
	clock  *time.Ticker
	tickFn TickFunc

	electionElapsed    int
	electionTimeout    int
	minElectionTimeout int

	heartbeatElapsed int
	HeartbeatTimeout int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	isleader = rf.role == RoleLeader
	term = rf.currentTerm
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's Term
	CandidateId int
	//
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current Term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term, -1)
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = args.Term
	if rf.voteFor == -1 {
		// TODO: check logs
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm <= args.Term {
		if rf.curLeader != args.LeaderId {
			rf.becomeFollowerLocked(args.Term, args.LeaderId)
		} else {
			rf.electionElapsed = 0
		}
	} else {
		// the leader is expired
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// the peers isn't changed, so it's saf
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

func (r *Raft) kickoff() {
	for {
		<-r.clock.C
		func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.tickFn()
		}()
	}
}

func (r *Raft) tickElectionLocked() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.becomeCandidateLocked()

		voteReq := &RequestVoteArgs{
			Term:        r.currentTerm,
			CandidateId: r.me,
		}
		if len(r.logs) > 0 {
			voteReq.LastLogTerm = r.logs[len(r.logs)-1].Term
			voteReq.LastLogIndex = r.logs[len(r.logs)-1].Index
		}

		for pid := range r.peers {
			if pid == r.me {
				continue
			}
			// TODO: ugly!!!
			go func(pid int) {
				reply := &RequestVoteReply{}
				// TODO: retry when failed?
				ok := r.sendRequestVote(pid, voteReq, reply)
				if ok {
					r.handleVoteReply(pid, reply)
				}
			}(pid)
		}
	}
}

func (r *Raft) tickHeartbeatLocked() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.HeartbeatTimeout {
		r.heartbeatElapsed = 0
		r.broadcast()
	}
}

// send empty AppendMsg to all peers
func (r *Raft) broadcast() {
	for pid := range r.peers {
		if pid == r.me {
			continue
		}
		req := &AppendEntriesArgs{
			Term:     r.currentTerm,
			LeaderId: r.me,
		}
		go func(pid int) {
			reply := &AppendEntriesReply{}
			if ok := r.sendAppendEntries(pid, req, reply); ok {
				r.handleAppendEntriesReply(pid, reply)
			}
		}(pid)
	}
}

func (r *Raft) handleAppendEntriesReply(pid int, reply *AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term, -1)
	}
}

func (r *Raft) handleVoteReply(pid int, reply *RequestVoteReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// there's a bigger Term, turn to follower
	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term, -1)
	} else if r.role == RoleCandidate && reply.VoteGranted && reply.Term == r.currentTerm {
		r.votes[pid] = true
		// win!
		if len(r.votes) >= len(r.peers)/2+1 {
			r.becomeLeaderLocked()
			// send hb right now
			r.broadcast()
		}
	}
}

func (r *Raft) becomeFollowerLocked(term, lead int) {
	r.currentTerm = term
	r.curLeader = lead
	r.role = RoleFollower
	r.electionElapsed = 0
	r.electionTimeout = r.randomElectionTimeout() // random election timeout
	r.voteFor = -1
	r.votes = nil
	r.tickFn = r.tickElectionLocked
}

func (r *Raft) becomeLeaderLocked() {
	r.curLeader = r.me
	r.role = RoleLeader
	r.voteFor = -1
	r.votes = nil
	r.tickFn = r.tickHeartbeatLocked
}

func (r *Raft) becomeCandidateLocked() {
	r.role = RoleCandidate
	r.currentTerm += 1    // advance current Term
	r.electionElapsed = 0 // reset election elapsed
	r.electionTimeout = r.randomElectionTimeout()
	r.voteFor = r.me // vote for self
	r.votes = map[int]bool{
		r.me: true,
	}
}

// get a random election timeout
func (rf *Raft) randomElectionTimeout() int {
	return rf.minElectionTimeout + rand.Intn(5)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.minElectionTimeout = 15
	rf.HeartbeatTimeout = 5

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.becomeFollowerLocked(0, -1)

	rf.clock = time.NewTicker(time.Millisecond * 30)

	// daemon
	go rf.kickoff()

	return rf
}
