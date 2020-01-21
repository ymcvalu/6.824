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
	"sort"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const BatchLimit = 10

type LogEntry struct {
	Term  int
	Index int
	Data  interface{}
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

	storage storage

	// volatile state on all instances
	votes       map[int]bool
	lastApplied int
	role        Role
	curLeader   int

	shouldPersist bool

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.storage.offset)
	e.Encode(rf.storage.commit)
	e.Encode(rf.storage.logs)
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&rf.currentTerm); err != nil {
		panic(fmt.Errorf("failed to recover currentTerm: %w", err))
	}

	if err := d.Decode(&rf.voteFor); err != nil {
		panic(fmt.Errorf("failed to recover voteFor: %w", err))
	}

	if err := d.Decode(&rf.storage.offset); err != nil {
		panic(fmt.Errorf("failed to recover offset: %w", err))
	}

	if err := d.Decode(&rf.storage.commit); err != nil {
		panic(fmt.Errorf("fialed to recover commit index: %w", err))
	}

	if err := d.Decode(&rf.storage.logs); err != nil {
		panic(fmt.Errorf("failed to recover commited logs: %w", err))
	}

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
			rf.campaign()
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
		if rf.role != RoleLeader {
			rf.handleAppendMsg(msg)
		}
	case MsgAppResp:
		if rf.role == RoleLeader {
			rf.handleAppendResp(msg)
		}
	}
}

func (rf *Raft) propose(prop proposal) {
	isLeader := rf.role == RoleLeader
	if !isLeader {
		prop.cb(0, 0, false)
		log.Printf("term[%d]: peer[%d] isn't a leader, and failed to propose a new propoal", rf.currentTerm, rf.me)
		return
	}

	idx := rf.storage.latestIndex() + 1
	go prop.cb(rf.currentTerm, idx, true)
	rf._propse(idx, prop.command)
}

func (rf *Raft) _propse(idx int, cmd interface{}) {
	if rf.storage.append(rf.currentTerm, cmd) != idx {
		panic(fmt.Errorf("term[%d]: leader[%d] propose cmd with two diff index", rf.currentTerm, rf.me))
	}

	rf.persist()
	rf.shouldPersist = false

	rf.matchIndex[rf.me] = idx
	rf.step(Message{
		Type: MsgProp,
		From: rf.me,
		To:   rf.me,
	})
}

func (rf *Raft) campaign() {
	// single-node
	if len(rf.peers) == 1 {
		rf.becomeLeader()
		return
	}

	log.Printf("term[%d]: peer[%d] send votes", rf.currentTerm, rf.me)

	pTerm := rf.storage.latestTerm()
	pIndex := rf.storage.latestIndex()
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
		pTerm := rf.storage.latestTerm()
		pIndex := rf.storage.latestIndex()
		resp.Term = req.Term

		resp.Reject = !(req.LogTerm > pTerm || (req.LogTerm == pTerm && req.Index >= pIndex))
	}

	if !resp.Reject {
		// reset electionElapsed when granting vote to a candidate
		rf.electionElapsed = 0
		if rf.voteFor == -1 {
			rf.voteFor = req.From
			rf.shouldPersist = true
		}
	}

	if rf.shouldPersist {
		rf.shouldPersist = false
		rf.persist()
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

	pTerm := rf.storage.latestTerm()
	pIndex := rf.storage.latestIndex()

	msg := Message{
		Type:    MsgApp,
		Term:    rf.currentTerm,
		LogTerm: pTerm,
		Index:   pIndex,
		Commit:  rf.storage.commit,
		Ctx:     "beat",
	}
	rf.bcastMsgParallel(msg)
}

func (rf *Raft) tickElection() {
	rf.electionElapsed++
	if rf.electionElapsed >= rf.electionTimeout {
		log.Printf("term[%d]: peer[%d] election timeout", rf.currentTerm, rf.me)
		rf.becomeCandidate()
		rf.persist()
		rf.shouldPersist = false

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
		rf.sendAppend(pid, true)
	}
}

func (rf *Raft) sendAppend(pid int, allowEmpty bool) {
	var (
		nextIdx = rf.nextIndex[pid]
		pl      LogEntry
	)

	if nextIdx > 1 {
		pl = rf.storage.logAt(nextIdx - 1) // previous log entry
	}

	logs := rf.storage.slice(nextIdx, -1, BatchLimit) // logEntries to send

	if len(logs) == 0 && !allowEmpty {
		return
	}

	msg := Message{
		Type:    MsgApp,
		Term:    rf.currentTerm,
		LogTerm: pl.Term,  // term of previous log entry
		Index:   pl.Index, // index of previous log entry
		Entries: logs,
		Commit:  rf.storage.commit,
		Ctx:     "app",
	}

	rf.sendMsg(pid, msg)

	if len(logs) > 0 {
		rf.nextIndex[pid] = logs[len(logs)-1].Index + 1
	}
}

func (rf *Raft) Send(msg Message, _ *Resp) {
	rf.msgCh <- msg // async communication
	return
}

func (rf *Raft) handleAppendMsg(msg Message) {
	log.Printf("term[%d]: peer[%d] receive a %s msg from peer[%d], with %d logs", rf.currentTerm, rf.me, msg.Ctx, msg.From, len(msg.Entries))
	if msg.Term < rf.currentTerm {
		rf.sendMsg(msg.From, Message{
			Type:   MsgAppResp,
			Term:   rf.currentTerm,
			Reject: true,
		})
		return
	}

	if rf.role != RoleFollower {
		rf.becomeFollower(msg.Term, msg.From)
	}

	if rf.curLeader != msg.From {
		rf.curLeader = msg.From
	}

	// reset when receiving AppendEntries from current leader
	rf.electionElapsed = 0

	// the msg we have committed
	if len(msg.Entries) > 0 && msg.Entries[len(msg.Entries)-1].Index <= rf.storage.commit {
		rf.sendMsg(msg.From, Message{
			Type:  MsgAppResp,
			Term:  rf.currentTerm,
			Index: rf.storage.commit, // match index
		})
		return
	}

	pIndex := rf.storage.latestIndex()
	if msg.Index > pIndex {
		rf.sendMsg(msg.From, Message{
			Type:   MsgAppResp,
			Term:   rf.currentTerm,
			Index:  pIndex, // next index
			Reject: true,
		})
		return
	}

	var resp Message
	if latest, ok := rf.maybeAppend(msg.LogTerm, msg.Index, msg.Commit, msg.Entries); ok {
		resp = Message{
			Type:  MsgAppResp,
			Term:  rf.currentTerm,
			Index: latest, // match index
		}

	} else {
		next := rf.storage.latestIndexPreTerm(msg.Index)
		resp = Message{
			Type:   MsgAppResp,
			Term:   rf.currentTerm,
			Index:  next, // next index
			Reject: true,
		}
	}

	if rf.shouldPersist {
		rf.shouldPersist = false
		rf.persist()
	}
	rf.sendMsg(msg.From, resp)

	return
}

func (rf *Raft) handleAppendResp(msg Message) {
	if !msg.Reject {
		if rf.matchIndex[msg.From] < msg.Index {
			rf.matchIndex[msg.From] = msg.Index
		}
		if rf.nextIndex[msg.From] < msg.Index+1 {
			rf.nextIndex[msg.From] = msg.Index + 1
		}
		cIdx := rf.calcCommitIndex()
		// can't commit logs from previous term directly
		if rf.storage.logAt(cIdx).Term == rf.currentTerm {
			if rf.storage.commitAt(cIdx) {
				rf.shouldPersist = false
				rf.persist()
			}
		}
	} else {
		rf.nextIndex[msg.From] = msg.Index
	}

	rf.sendAppend(msg.From, false)
}

func (rf *Raft) maybeAppend(pTerm int, pIndex, cIndex int, logs []LogEntry) (int, bool) {

	_pTerm := rf.storage.termAt(pIndex)

	if _pTerm != pTerm {
		return 0, false
	}

	var last int
	if len(logs) > 0 {
		at := rf.storage.findConflict(logs)
		logs = logs[at:]

		rf.shouldPersist = true

		last = rf.storage.truncateAndAppend(logs)
	} else {
		last = rf.storage.latestIndex()
	}

	if rf.storage.commitAt(cIndex) {
		rf.shouldPersist = true
	}

	return last, true
}

func (rf *Raft) calcCommitIndex() int {
	mi := make([]int, len(rf.matchIndex))
	copy(mi, rf.matchIndex)
	sort.Ints(mi)

	cidx := mi[(len(rf.peers)-1)/2]
	if cidx > rf.storage.commit {
		return cidx
	}
	return rf.storage.commit
}

func (rf *Raft) becomeFollower(term, lead int) {
	log.Printf("term[%d]: peer[%d] becomd follower", rf.currentTerm, rf.me)
	if term != rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = -1
		rf.shouldPersist = true
	}
	rf.curLeader = lead
	rf.role = RoleFollower
	rf.electionElapsed = 0
	rf.electionTimeout = rf.randomElectionTimeout() // random election timeout

	rf.votes = nil
	rf.tickFn = rf.tickElection
}

func (rf *Raft) becomeLeader() {
	log.Printf("term[%d]: peer[%d] becomd leader", rf.currentTerm, rf.me)
	rf.curLeader = rf.me
	rf.role = RoleLeader

	rf.votes = nil
	rf.tickFn = rf.tickHeartbeat
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	pIndex := rf.storage.latestIndex()
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
	rf.shouldPersist = true
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
	rf.minElectionTimeout = 10
	rf.HeartbeatTimeout = 3

	// Your initialization code here (2A, 2B, 2C).

	rf.clock = time.NewTicker(time.Millisecond * 60)

	rf.msgCh = make(chan Message, 1)
	rf.propCh = make(chan proposal)
	rf.stateCh = make(chan state)

	rf.storage = storage{
		offset:  1,
		applyCh: applyCh,
	}

	rf.becomeFollower(0, -1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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
	Ctx     string
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

type storage struct {
	applyCh chan ApplyMsg
	logs    []LogEntry
	offset  int // index of first log engry in logs
	commit  int
}

func (s *storage) commitAt(cIdx int) bool {
	if s.latestIndex() < cIdx {
		cIdx = s.latestIndex()
	}

	if s.commit < cIdx {
		lastCommit := s.commit
		s.commit = cIdx
		lastIdx := lastCommit - s.offset
		curIdx := cIdx - s.offset
		for _, l := range s.logs[lastIdx+1 : curIdx+1] {
			s.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: l.Index,
				Command:      l.Data,
			}
		}
		return true
	}
	return false
}

func (s *storage) truncateAndAppend(logs []LogEntry) int {
	if len(logs) == 0 {
		return s.latestIndex()
	}

	fir := logs[0]
	switch lastIdx := s.latestIndex(); {
	case lastIdx+1 == fir.Index:
		s.logs = append(s.logs, logs...)
	case lastIdx >= fir.Index:
		for i := len(s.logs) - 1; i >= 0; i-- {
			if s.logs[i].Index == fir.Index {
				s.logs = append(s.logs[:i], logs...)
				break
			}
		}
	}

	return s.latestIndex()
}

func (s *storage) findConflict(logs []LogEntry) int {
	for i, l := range logs {
		if s.termAt(l.Index) != l.Term {
			return i
		}
	}
	return len(logs)
}

func (s *storage) slice(i, j, limit int) []LogEntry {
	var (
		off = i - s.offset
		l   = len(s.logs)
	)

	if j > i && j < s.latestIndex() {
		l = j - s.offset + 1
	}

	if l-off > limit {
		l = off + limit
	}

	if off >= 0 && len(s.logs) > off {
		return s.logs[off:l]
	}

	return nil
}

func (s *storage) termAt(idx int) int {
	off := idx - s.offset
	if off >= 0 && len(s.logs) > off {
		return s.logs[off].Term
	}
	return 0
}

func (s *storage) append(t int, cmd interface{}) int {
	idx := s.latestIndex() + 1
	s.logs = append(s.logs, LogEntry{
		Term:  t,
		Index: idx,
		Data:  cmd,
	})
	return idx
}

func (s *storage) latestTerm() int {
	if len(s.logs) == 0 { // TODO: 如果添加快照机制的话，offset需要更新
		return 0
	}
	return s.logs[len(s.logs)-1].Term
}

func (s *storage) latestIndex() int {
	if len(s.logs) == 0 {
		return s.offset - 1
	}
	return s.logs[len(s.logs)-1].Index
}

func (s *storage) logAt(idx int) LogEntry {
	_idx := idx - s.offset
	if _idx >= 0 && _idx < len(s.logs) {
		return s.logs[_idx]
	}
	return LogEntry{}
}

func (s *storage) latestIndexPreTerm(idx int) int {
	off := idx - s.offset

	term := s.logs[off].Term
	cOff := s.commit - s.offset
	for off > cOff && off >= 0 {
		if s.logs[off].Term != term {
			break
		}
		off--
	}

	if off == cOff {
		off++
	}

	if off >= 0 {
		return s.logs[off].Index
	} else {
		return s.offset
	}
}
