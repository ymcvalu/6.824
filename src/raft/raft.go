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
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"bytes"
	"fmt"
	"log"
	"math/rand"
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

type Role uint8

const (
	_ Role = iota
	RoleFollower
	RoleCandidate
	RoleLeader
)

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
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

	// reset when receiving AppendEntries **from current leader** or **granting** vote to candidate
	electionElapsed    int
	electionTimeout    int
	minElectionTimeout int

	heartbeatElapsed int
	HeartbeatTimeout int

	// channel
	msgCh   chan Message
	propCh  chan proposal
	stateCh chan state
}

type state struct {
	term     int
	isLeader bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	s := <-rf.stateCh
	return s.term, s.isLeader
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
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return command is the index that the command will appear at
// if it's ever committed. the second return command is the current
// Term. the third return command is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	var (
		index    int
		term     int
		isLeader bool
		wait     = make(chan struct{})
	)

	// propose
	rf.propCh <- proposal{
		command: command,
		cb: func(_term, _index int, _isLeader bool) {
			term, index, isLeader = _term, _index, _isLeader
			close(wait)
		},
	}

	<-wait

	return index, term, isLeader
}

func (rf *Raft) Send(msg Message, _ *Resp) {
	rf.msgCh <- msg // async communication
	return
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.clock.C:
			rf.tickFn()
		case msg := <-rf.msgCh:
			rf.step(msg)
		case p := <-rf.propCh:
			rf.propose(p)
		case rf.stateCh <- state{isLeader: rf.role == RoleLeader, term: rf.currentTerm}:
		}
	}
}

func (rf *Raft) step(msg Message) {
	if msg.To != rf.me {
		return
	}

	// handle msg from self
	if msg.From == rf.me {
		switch msg.Type {
		case MsgHup:
			rf.poll()
		case MsgBeat:
			rf.bcastHeartbeat()
		case MsgProp:
			rf.bcastAppend()
		}

		return
	}

	if msg.Term > rf.currentTerm {
		if msg.Type == MsgApp {
			rf.becomeFollower(msg.Term, msg.From)
		} else {
			rf.becomeFollower(msg.Term, -1)
		}
	}

	switch msg.Type {
	case MsgVote:
		rf.handleVoteReq(msg)
	case MsgVoteResp:
		rf.handleVoteResp(msg)
	case MsgApp:
		rf.handleAppendMsg(msg)
	case MsgAppResp:

	}

}

func (rf *Raft) propose(prop proposal) {
	isLeader := rf.role == RoleLeader
	if !isLeader {
		prop.cb(0, 0, false)
		log.Printf("term[%d]: peer[%d] isn't a leader, and failed to propose a new propoal", rf.currentTerm, rf.me)
		return
	}
	idx := rf.latestIndex() + 1
	msg := Message{
		Type: MsgProp,
		From: rf.me,
		To:   rf.me,
	}

	// append
	rf.logs = append(rf.logs, LogEntry{
		Term:  rf.currentTerm,
		Index: idx,
		Data:  encode(prop.command),
	})

	go prop.cb(rf.currentTerm, idx, true)

	rf.step(msg)
}

func (rf *Raft) poll() {
	// single-node
	if len(rf.peers) == 1 {
		rf.becomeLeader()
		return
	}

	pTerm := rf.latestTerm()
	pIndex := rf.latestIndex()
	msg := Message{
		Type:    MsgVote,
		Term:    rf.currentTerm,
		LogTerm: pTerm,
		Index:   pIndex,
	}
	rf.bcastMsgParallel(msg)
}

func (rf *Raft) handleVoteReq(req Message) {
	resp := Message{
		Type: MsgVoteResp,
	}
	// the req from a smaller term or we have voted to others
	if req.Term < rf.currentTerm || (rf.voteFor != -1 && rf.voteFor != req.From) {
		resp.Term = rf.currentTerm
		resp.Reject = true
	} else {
		pTerm := rf.latestTerm()
		pIndex := rf.latestIndex()
		resp.Term = req.Term

		resp.Reject = !(req.LogTerm > pTerm || (req.LogTerm == pTerm && req.Index >= pIndex))
	}

	if !resp.Reject {
		// reset electionElapsed when granting vote to a candidate
		rf.electionElapsed = 0
		if rf.voteFor == -1 {
			rf.voteFor = req.From
		}
	}

	rf.sendMsg(req.From, resp)
}

func (rf *Raft) handleVoteResp(msg Message) {
	if rf.role == RoleCandidate && !msg.Reject && msg.Term == rf.currentTerm {
		rf.votes[msg.From] = true
		// win!
		if len(rf.votes) >= len(rf.peers)/2+1 {
			rf.becomeLeader()
			// we have won! notify other peers right now
			rf.step(Message{
				Type: MsgBeat,
				From: rf.me,
				To:   rf.me,
			})
		}
	}
}

func (rf *Raft) bcastHeartbeat() {
	if rf.role != RoleLeader {
		panic(fmt.Sprintf("incorrectly state: a peer broadcastPallel heartbeat with role[%d]", rf.role))
	}

	pTerm := rf.latestTerm()
	pIndex := rf.latestIndex()

	msg := Message{
		Type:    MsgApp,
		Term:    rf.currentTerm,
		LogTerm: pTerm,
		Index:   pIndex,
		Commit:  rf.commitIndex,
	}
	rf.bcastMsgParallel(msg)
}

func (rf *Raft) tickElection() {
	rf.electionElapsed++
	if rf.electionElapsed >= rf.electionTimeout {
		log.Printf("term[%d]: peer[%d] election timeout", rf.currentTerm, rf.me)
		rf.becomeCandidate()
		rf.step(Message{
			Type: MsgHup,
			From: rf.me,
			To:   rf.me,
		})
	}
}

func (rf *Raft) tickHeartbeat() {
	rf.heartbeatElapsed++
	if rf.heartbeatElapsed >= rf.HeartbeatTimeout {
		rf.heartbeatElapsed = 0
		rf.step(Message{
			Type: MsgBeat,
			From: rf.me,
			To:   rf.me,
		})
	}
}

func (rf *Raft) bcastAppend() {
	for pid := range rf.peers {
		if pid == rf.me {
			continue
		}
		rf.sendAppend(pid)
	}
}

func (rf *Raft) sendAppend(pid int) {
	var (
		nextIdx = rf.nextIndex[pid]
		pl      LogEntry
	)

	if nextIdx > 0 {
		// TODO: may be compacted
		pl, _ = rf.logAt(nextIdx - 1) // previous log entry
	}

	// TODO: may be compacted
	logs, _ := rf.logSlice(nextIdx, -1) // logEntries to send

	msg := Message{
		Type:    MsgApp,
		Term:    rf.currentTerm,
		LogTerm: pl.Term,  // term of previous log entry
		Index:   pl.Index, // index of previous log entry
		Entries: logs,
		Commit:  rf.commitIndex,
	}

	rf.sendMsg(pid, msg)

	if len(logs) > 0 {
		rf.nextIndex[pid] = logs[len(logs)-1].Index
	}
}

func (rf *Raft) handleAppendMsg(msg Message) {
	if msg.Term < rf.currentTerm {
		// TODO: notify the old leader?
		return
	}

	// reset when receiving AppendEntries from current leader
	rf.electionElapsed = 0

	pIndex := rf.latestIndex()
	if msg.Index > pIndex {
		rf.sendMsg(msg.From, Message{
			Type:   MsgAppResp,
			Term:   msg.Term,
			Index:  pIndex,
			Reject: true,
		})
		return
	}

	if latest, ok := rf.maybeAppend(msg.LogTerm, msg.Index, msg.Commit, msg.Entries); ok {
		rf.sendMsg(msg.From, Message{
			Type:  MsgAppResp,
			Term:  msg.Term,
			Index: latest, // match index
		})
	} else {
		rf.sendMsg(msg.From, Message{
			Type:   MsgAppResp,
			Term:   msg.Term,
			Index:  msg.Index, // next index
			Reject: true,
		})
	}

	return
}

func (rf *Raft) maybeAppend(pTerm int, pIndex, cIndex int, logs []LogEntry) (int, bool) {
	at, _pTerm, _ := rf.logTerm(pIndex)
	if _pTerm != pTerm {
		return 0, false
	}

	// truncate
	rf.logs = rf.logs[:at+1]
	rf.logs = append(rf.logs, logs...)

	return rf.latestIndex(), true
}

func (rf *Raft) logTerm(idx int) (int, int, error) {
	for i := len(rf.logs) - 1; i >= 0; i-- {
		if rf.logs[i].Index == idx {
			return i, rf.logs[i].Term, nil
		}
	}
	return 0, 0, nil
}

// logAt
func (rf *Raft) logAt(at int) (LogEntry, bool) {
	return rf.logs[at], true
}

// return rf.logs[from: to)
func (rf *Raft) logSlice(from, to int) ([]LogEntry, bool) {
	if to == -1 {
		to = len(rf.logs)
	}

	return rf.logs[from:to:to], true
}

func (rf *Raft) latestTerm() int {
	if len(rf.logs) > 0 {
		return rf.logs[len(rf.logs)-1].Term
	}
	return 0
}

func (rf *Raft) latestIndex() int {
	if len(rf.logs) > 0 {
		return rf.logs[len(rf.logs)-1].Index
	}
	return 0
}

func (rf *Raft) becomeFollower(term, lead int) {
	log.Printf("term[%d]: peer[%d] becomd follower", rf.currentTerm, rf.me)

	rf.currentTerm = term
	rf.curLeader = lead
	rf.role = RoleFollower
	rf.electionElapsed = 0
	rf.electionTimeout = rf.randomElectionTimeout() // random election timeout
	rf.voteFor = -1
	rf.votes = nil
	rf.tickFn = rf.tickElection
}

func (rf *Raft) becomeLeader() {
	log.Printf("term[%d]: peer[%d] becomd leader", rf.currentTerm, rf.me)
	rf.curLeader = rf.me
	rf.role = RoleLeader
	rf.voteFor = -1
	rf.votes = nil
	rf.tickFn = rf.tickHeartbeat
	pIndex := rf.latestIndex()
	for pid := range rf.peers {
		rf.nextIndex[pid] = pIndex
	}

}

func (rf *Raft) becomeCandidate() {
	log.Printf("term[%d]: peer[%d] becomd candidate", rf.currentTerm, rf.me)

	rf.role = RoleCandidate
	rf.currentTerm += 1    // advance current Term
	rf.electionElapsed = 0 // reset election elapsed
	rf.electionTimeout = rf.randomElectionTimeout()
	rf.voteFor = rf.me // vote for self
	rf.votes = map[int]bool{
		rf.me: true,
	}
}

// get a random election timeout
func (rf *Raft) randomElectionTimeout() int {
	return rf.minElectionTimeout + rand.Intn(5)
}

func (rf *Raft) sendMsg(pid int, msg Message) {
	msg.From = rf.me
	msg.To = pid
	go rf.peers[pid].Call("Raft.Send", msg, &Resp{})
}

func (rf *Raft) bcastMsgParallel(msg Message) {
	for pid := range rf.peers {
		if pid == rf.me {
			continue
		}
		rf.sendMsg(pid, msg)
	}
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

	rf.clock = time.NewTicker(time.Millisecond * 30)

	rf.msgCh = make(chan Message, 1)
	rf.propCh = make(chan proposal)
	rf.stateCh = make(chan state)

	rf.becomeFollower(0, -1)

	// daemon
	go rf.run()

	return rf
}

/* async message */
type MsgType uint8

const (
	_ MsgType = iota
	MsgHup
	MsgBeat
	MsgProp
	MsgApp
	MsgAppResp
	MsgVote
	MsgVoteResp
)

type Message struct {
	Type    MsgType
	From    int
	To      int
	Term    int
	LogTerm int
	Index   int
	Entries []LogEntry
	Commit  int
	Reject  bool
}

type proposal struct {
	command interface{}
	cb      func(term, index int, isLeader bool) // callback
}

type Resp struct{}

func encode(v interface{}) []byte {
	labgob.Register(v)
	buf := bytes.NewBuffer(nil)
	labgob.NewEncoder(buf).Encode(v)
	return buf.Bytes()
}
