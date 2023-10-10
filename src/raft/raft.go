package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type RaftRole int

const (
	Leader RaftRole = iota
	Candidate
	Follower
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// peer's state -> guarded by mu
	leader int
	role   RaftRole

	currentTerm int
	votedFor    int
	logs        []LogEntry // logs[0]一定不为空，在初始时会设置一个哨兵日志，在生成快照时也一定会留下最后一个日志

	commitIndex int
	lastApplied int

	// 用 nextIndex[me]表示当前peer的日志的下一个index->这种方法不好，因为nextIndex不是持久化的数据，最好通过logs的数组长度获取index
	nextIndex  []int
	matchIndex []int

	lastElectionTime int64
	electionTimeouts int64
	votedNum         int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.role == Leader)
	rf.mu.Unlock()
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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 需要上锁后才能调用
	later := func(args *RequestVoteArgs) bool {
		lastIndex := rf.getLastLogIndex()
		lastTerm := rf.logs[lastIndex].Term
		return (args.LastLogTerm > lastTerm) ||
			(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
	}

	reply.VoteGranted = false
	rf.mu.Lock()

	if args.Term > rf.currentTerm {
		rf.UpdateTerm(args.Term)
	}
	// args.Term == rf.currentTerm / args.Term < rf.currentTerm(忽视)
	if args.Term == rf.currentTerm {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && later(args) {
			rf.currentTerm = args.Term // 把投过票的任期持久化保存起来
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			// reset election time
			rf.ResetElectionTime()
			// role exchange (Leader->Follower / Candidate->Follower)
			rf.role = Follower

			DPrintf(rf.role, rf.me, INFO, "vote for -> %d\n", args.CandidateId)
		}
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	rf.mu.Lock()
	// 包括：
	// 同term的其他candidate(==)
	// 给同term其他candidate的follower(==)
	// 给本term的leader投票的follwer(==)
	// 一些term落后的其他节点(>)
	if args.Term >= rf.currentTerm {
		rf.leader = args.LeaderId
		rf.UpdateTerm(args.Term)
		rf.ResetElectionTime()
		reply.Success = true
		if args.PrevLogIndex > rf.getLastLogIndex() {
			reply.Success = false
		} else if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
			reply.Success = false
		}
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	isLeader := true

	// Your code here (2B).

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

// 保证上锁时调用此函数
// 重启定时器
func (rf *Raft) ResetElectionTime() {
	rf.lastElectionTime = time.Now().UnixMilli()
	// 选取超时时间为500 - 800 ms
	rf.electionTimeouts = 500 + (rand.Int63() % 300)
}

// 保证上锁时调用此函数
func (rf *Raft) AppendEmptyEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := AppendEntriesArgs{}
			// rf.mu.Lock()
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			args.Entries = nil
			args.LeaderCommit = rf.commitIndex
			// rf.mu.Unlock()
			// 同ticker函数，应该提前把args的值设置好，通过传值的方式给rpc
			go func(i int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					DPrintf(Leader, args.LeaderId, DEBUG, "AppendEmptyEntries to %2d failed\n", i)
				} else {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.UpdateTerm(reply.Term)
					}
					rf.mu.Unlock()
				}
			}(i, args)
		}
	}
}

// 调用时需要上锁
// if args.term / reply.term > rf.currentTerm
func (rf *Raft) UpdateTerm(term int) {
	rf.currentTerm = term
	rf.role = Follower
	rf.votedFor = -1
	rf.votedNum = 0
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		currentTime := time.Now().UnixMilli()
		args := RequestVoteArgs{}
		rf.mu.Lock()
		if rf.role != Leader && currentTime >= rf.lastElectionTime+rf.electionTimeouts {
			DPrintf(rf.role, rf.me, INFO, "election timeout\n")
			rf.leader = -1
			rf.role = Candidate
			rf.currentTerm = rf.currentTerm + 1
			rf.votedFor = rf.me
			rf.votedNum = 1
			rf.ResetElectionTime()
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = rf.getLastLogIndex()
			args.LastLogTerm = rf.logs[args.LastLogIndex].Term
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					// 对每个rpc单开一个goroutine，防止一个rpc调用时间过长而阻塞其他,且保证上锁时交给其他goroutine调用rpc
					// 把rpc放在锁外，但条件是在rpc请求过程中不能使用rf直接用成员（上锁也不行），因为可能这段时间rf的变量的值被改变了
					// 必须通过传值参数的方式提前把rpc的参数args传过去，也可以保证给每个peer的rpc调用参数是相同的
					// 但在rpc返回时可以上锁并使用rf的成员，因为接受rpc返回值时，想获得的是rf的当前成员值
					go func(i int, args RequestVoteArgs) { // 必须传递值参数, 不能传递引用
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(i, &args, &reply)
						if !ok {
							DPrintf(Candidate, args.CandidateId, DEBUG, "sendRequestVote to %2d failed\n", i)
						} else {
							// 可以上锁并使用rf的值，因为想用的是当前值
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								rf.UpdateTerm(reply.Term)
							} else if reply.Term == rf.currentTerm && reply.VoteGranted {
								rf.votedNum = rf.votedNum + 1
								if rf.votedNum > len(rf.peers)/2 && rf.role != Leader {
									rf.role = Leader
									// 成为Leader时通知其他peer
									rf.AppendEmptyEntries()
									DPrintf(rf.role, rf.me, INFO, "become leader\n")
								}
							}
							rf.mu.Unlock()
						}
					}(i, args)
				}
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) HeartBeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == Leader {
			rf.AppendEmptyEntries()
		}
		rf.mu.Unlock()
		// 1s心跳10次
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leader = -1
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votedNum = 0
	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	// rf.logs[0] = LogEntry{term:0}, 在头部加一个哨兵结点
	rf.logs = append(rf.logs, LogEntry{nil, 0})
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.ResetElectionTime()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.HeartBeats()

	DPrintf(rf.role, rf.me, INFO, "initialize finished\n")

	return rf
}
