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

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	Index   int
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

	applyCh chan ApplyMsg
	cond    *sync.Cond

	snapshot      []byte
	snapshotTerm  int
	snapshotIndex int // 快照的部分是一定被apply的，也就是 commitIndex >= snapshotIndex.(注：lastApplied不一定>=snapshotIndex,因为对于follower来说，收到了leader的快照，可能还没来得及发送给上层去应用) 同理:Take care that these snapshots only advance the service's state, and don't cause it to move backwards. 快照不会向后回退状态，因为已经提交的数据不会回退
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	// 快照的元数据和快照一起存储
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil {
		DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "Decode failed in readPersist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm

		rf.snapshot = rf.persister.ReadSnapshot()

		// rf.lastApplied = rf.snapshotIndex
		// commitIndex >= snapshotIndex
		rf.commitIndex = rf.snapshotIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()

	DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "receive snapshot: {snapshot = %v, index = %v, term = %v}\n", rf.snapshot, rf.snapshotIndex, rf.snapshotTerm)

	// server层进行快照，那一定是已经applied的了
	if index < rf.lastApplied {
		DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "snapshot but not applied\n")
	}

	if index < rf.snapshotIndex {
		// DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "snapshot back out of range\n")
		// 对于follower来说，可能收到服务层对更小的索引进行快照的请求，忽视它
	} else if index > rf.getLastLogIndex() {
		DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "snapshot index out of range\n")
	} else if index > rf.snapshotIndex {
		term := rf.logs[rf.IndexInSlice(index)].Term
		var tmp []LogEntry
		tmp = append(tmp, rf.logs[rf.IndexInSlice(index+1):]...)
		rf.snapshotIndex = index
		rf.snapshotTerm = term
		rf.logs = tmp
		rf.snapshot = snapshot
		// commitIndex >= lastApplied >= snapshotIndex
		// index < lastApplied 必须已经保证了，所以不需要以下两条
		// rf.lastApplied = max(rf.lastApplied, index)
		// rf.commitIndex = max(rf.commitIndex, rf.lastApplied)
		rf.persist()
	}

	rf.mu.Unlock()
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
		lastTerm := rf.getLastLogTerm()
		return (args.LastLogTerm > lastTerm) ||
			(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
	}

	reply.VoteGranted = false
	rf.mu.Lock()

	// 不管给不给投票，发现更大的任期就要更新(为什么？)。所以任期相同不代表投过票了，要通过votedfor判断
	if args.Term > rf.currentTerm {
		rf.UpdateTerm(args.Term)
		rf.persist()
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

			rf.persist()

			DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "vote for -> %d\n", args.CandidateId)
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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	rf.mu.Lock()
	// 包括：
	// 同term的其他candidate(==)
	// 给同term其他candidate投票的follower(==)
	// 给本term的leader投票的follwer(==)
	// 一些term落后的其他节点(>)
	if args.Term >= rf.currentTerm {
		rf.leader = args.LeaderId
		rf.UpdateTerm(args.Term)
		rf.persist()
		rf.ResetElectionTime()
		reply.Success = true

		if args.PrevLogIndex < rf.snapshotIndex {
			// 把在snapshotIndex前面的那部分日志去掉(这部分日志一定是相同的，因为是已经commit并apply的了)，后面的日志正常添加
			if args.PrevLogIndex+len(args.Entries) >= rf.snapshotIndex {
				index := args.Entries[rf.snapshotIndex-args.PrevLogIndex-1].Index
				term := args.Entries[rf.snapshotIndex-args.PrevLogIndex-1].Term
				args.Entries = args.Entries[rf.snapshotIndex-args.PrevLogIndex:]
				args.PrevLogIndex = index
				args.PrevLogTerm = term
			}
		}

		// 快速恢复
		if args.PrevLogIndex > rf.getLastLogIndex() { // 本服务器在上一个日志的位置为空
			reply.XTerm = -1
			reply.XLen = args.PrevLogIndex - rf.getLastLogIndex()
			reply.Success = false
		} else if args.PrevLogIndex == rf.snapshotIndex && args.PrevLogTerm != rf.snapshotTerm {
			// 快照是以提交并应用的，所以一定不能冲突
			DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "snapshot not match: {index = %v, original term = %v, present term = %v}\n", rf.snapshotTerm, args.PrevLogTerm)
		} else if args.PrevLogIndex >= rf.snapshotIndex {
			if args.PrevLogIndex > rf.snapshotIndex && args.PrevLogTerm != rf.logs[rf.IndexInSlice(args.PrevLogIndex)].Term { // 上一个日志的任期不匹配
				if args.PrevLogIndex <= rf.commitIndex {
					DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "logs not matched but commited - Leader:%d\n", args.LeaderId)
				}
				reply.XTerm = rf.logs[rf.IndexInSlice(args.PrevLogIndex)].Term
				reply.XIndex = args.PrevLogIndex
				for i := rf.IndexInSlice(reply.XIndex) - 1; i >= 0; i-- {
					if rf.logs[i].Term == reply.XTerm {
						reply.XIndex = rf.logs[i].Index
					} else {
						break
					}
				}
				reply.Success = false
			} else { // 附加日志
				i := 0
				// 发现term不匹配，就把后面都删掉
				for ; i < len(args.Entries) && args.PrevLogIndex+1+i <= rf.getLastLogIndex(); i++ {
					if rf.logs[rf.IndexInSlice(args.PrevLogIndex+1+i)].Term != args.Entries[i].Term {
						// 不应该删除已提交的日志
						if args.PrevLogIndex+1+i <= rf.commitIndex {
							DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "Delete logs commited - Leader:%d\n", args.LeaderId)
						}
						rf.logs = rf.logs[:rf.IndexInSlice(args.PrevLogIndex+1+i)]
						break
					}
				}
				// 把剩余部分添加进来
				if i < len(args.Entries) {
					rf.logs = append(rf.logs, args.Entries[i:]...)
				}

				rf.persist()

				// 更新commitedIndex
				if args.LeaderCommit > rf.commitIndex {
					// index of last new entry 必须是"本任期"的最后一个新日志才行，防止把错误的日志提交
					lastNewEntry := rf.commitIndex
					// commitIndex >= snapshotIndex
					for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
						if rf.logs[rf.IndexInSlice(i)].Term == rf.currentTerm {
							lastNewEntry = i
						}
					}
					newCommitIndex := min(lastNewEntry, args.LeaderCommit)
					// 提交点不应该回退
					if newCommitIndex < rf.commitIndex {
						DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "CommitIndex Back\n")
					}
					rf.commitIndex = newCommitIndex
					rf.cond.Broadcast()
				}
			}
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
	rf.mu.Lock()
	if rf.role == Leader {
		index = rf.getLastLogIndex() + 1
		term = rf.currentTerm
		isLeader = true

		rf.logs = append(rf.logs, LogEntry{command, term, index})
		rf.persist()
		rf.matchIndex[rf.me] = index
		rf.LogReplication()
	} else {
		isLeader = false
	}
	DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "Start {Command = %v, index = %v, term = %v, isLeader = %v}\n", command, index, term, isLeader)
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
	// 这样写也有问题，如果正好ApplyLogs协程在判断完killed之后还没cond.Wait之前，kill函数把dead设置为1并调用 Broadcast，可能会导致ApplyLogs无法被唤醒 (根本原因在于kill是通过原子操作)
	rf.cond.Broadcast() // 防止ApplyLogs协程阻塞
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
// 发送空的心跳包，Entries为空
func (rf *Raft) AppendEmptyEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := AppendEntriesArgs{}
			// rf.mu.Lock()
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1

			if args.PrevLogIndex > rf.snapshotIndex {
				args.PrevLogTerm = rf.logs[rf.IndexInSlice(args.PrevLogIndex)].Term
			} else if args.PrevLogIndex == rf.snapshotIndex {
				args.PrevLogTerm = rf.snapshotTerm
			} else {
				args.PrevLogIndex = rf.snapshotIndex
				args.PrevLogTerm = rf.snapshotTerm
			}

			args.Entries = nil
			args.LeaderCommit = rf.commitIndex
			// rf.mu.Unlock()
			// 同ticker函数，应该提前把args的值设置好，通过传值的方式给rpc
			go func(i int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					// 空日志失败不需要重传
					DPrintf(Leader, args.LeaderId, args.Term, DEBUG, "AppendEmptyEntries to %2d failed\n", i)
				} else {
					rf.mu.Lock()
					DPrintf(rf.role, rf.me, rf.currentTerm, DEBUG, "AppendEmptyEntries args: {Term = %d, PrevLogIndex = %d, PrevLogTerm = %d} reply from S%d: {Term = %d, Success = %v, XTerm = %d, XIndex = %d, XLen = %d} current snapshot: {snapshotIndex = %v, snapshotTerm = %v}\n", args.Term, args.PrevLogIndex, args.PrevLogTerm, i, reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen, rf.snapshotIndex, rf.snapshotTerm)
					if reply.Term > rf.currentTerm {
						rf.UpdateTerm(reply.Term)
						rf.persist()
					} else if reply.Term == rf.currentTerm && rf.role == Leader {
						// 心跳日志不携带日志，尽管还有没发送的日志
						// TODO : 心跳日志也应该更新nextIndex这些
						if reply.Success == true {
							// 如果成功，心跳包什么都不需要做
						} else if reply.Success == false {
							// 和LogReplication一样，回退nextIndex
							rf.BackNextIndex(i, args, reply)
							// 空日志失败不需要重传
							// go sendLogs(server)
						}
					}
					rf.mu.Unlock()
				}
			}(i, args)
		}
	}
}

// 调用时需要上锁
// 有新的日志需要添加时调用(Start)
// AppendEntry失败时需要重新调用 (Call返回错误，或者reply中Success为false)
// RPC调用超时会怎么办？不需要计时重新调用？等下一次AppendEntries？
func (rf *Raft) LogReplication() {

	// 实际的rpc,每个rpc对应一个goroutine
	var sendLogs func(server int)
	sendLogs = func(server int) {
		// 每次重复调用的参数不同，选取当前的状态作为参数，因为上次rpc调用失败可能更新了nextIndex
		rf.mu.Lock()
		// 确保是leader才能发送（防止之前任期的leader在转为follwer后却因为之前的rpc重传而造成follwer发送日志）
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[server] - 1
		if args.PrevLogIndex > rf.snapshotIndex {
			args.PrevLogTerm = rf.logs[rf.IndexInSlice(args.PrevLogIndex)].Term
		} else if args.PrevLogIndex == rf.snapshotIndex {
			args.PrevLogTerm = rf.snapshotTerm
		} else {
			// TODO: 发送installsnapshot
			rf.SnapshotReplication(server)
			DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "send snapshot\n")
			rf.nextIndex[server] = rf.snapshotIndex + 1
			args.PrevLogIndex = rf.snapshotIndex
			args.PrevLogTerm = rf.snapshotTerm
		}

		if rf.nextIndex[server] <= rf.snapshotIndex {
			DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "index out of range: {rf.nextIndex[%d} = %d, rf.snapshotIndex = %d\n", server, rf.nextIndex[server], rf.snapshotIndex)
		}

		// 无论entries是否为空都要发送，因为nextIndex的值不一定精准，所以要一步步迭代
		args.Entries = nil
		if rf.nextIndex[server] <= rf.getLastLogIndex() {
			// 要深拷贝，因为不是立马发送数据
			s := rf.logs[rf.IndexInSlice(rf.nextIndex[server]):]
			args.Entries = make([]LogEntry, len(s))
			copy(args.Entries, s)
		}
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			// RPC出错就重传
			go sendLogs(server)
		} else {
			rf.mu.Lock()
			DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "AppendEntries args: {Term = %d, PrevLogIndex = %d, PrevLogTerm = %d} reply from S%d: {Term = %d, Success = %v, XTerm = %d, XIndex = %d, XLen = %d} current snapshot: {snapshotIndex = %v, snapshotTerm = %v}\n", args.Term, args.PrevLogIndex, args.PrevLogTerm, server, reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen, rf.snapshotIndex, rf.snapshotTerm)
			if reply.Term > rf.currentTerm {
				rf.UpdateTerm(reply.Term)
				rf.persist()
			} else if reply.Term == rf.currentTerm && rf.role == Leader { // 可能follower收到了之前的延迟rpc回复，且此端的term和对端的term都是最新term，就检测不出来了
				if reply.Success { // 日志成功添加
					if len(args.Entries) > 0 {
						newIndex := args.Entries[len(args.Entries)-1].Index
						// 日志添加成功 matchIndex和nextIndex 只会增加
						rf.matchIndex[server] = max(rf.matchIndex[server], newIndex)
						rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
						if rf.matchIndex[server] > rf.getLastLogIndex() {
							DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "matchIndex[%v] out of bound : %v\n", server, rf.matchIndex[server])
						}
					}
					rf.UpdateCommitIndex()
				} else { // 日志不匹配, 回退nextIndex
					rf.BackNextIndex(server, args, reply)
					go sendLogs(server)
				}
				DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "nextIndex[%d] = %d\n", server, rf.nextIndex[server])
			} else {
				// reply.Term < rf.currentTerm 不用管
			}
			rf.mu.Unlock()
		}
	}

	DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "nextIndex:%v\n", rf.nextIndex)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go sendLogs(i)
		}
	}
}

// 日志不匹配, 回退nextIndex
func (rf *Raft) BackNextIndex(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	// (应该用PrevLogIndex的值回退，而不是当前的nextIndex值，因为可能在这期间，nextIndex的值更改了)
	newNextIndex := -1
	if reply.XTerm == -1 {
		if reply.XLen <= 0 {
			DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "XTerm == -1 but XLen == %d\n", reply.XLen)
		}
		newNextIndex = args.PrevLogIndex - reply.XLen + 1
	} else if args.PrevLogIndex > rf.snapshotIndex {
		idx := rf.IndexInSlice(args.PrevLogIndex)
		for ; idx >= 0; idx-- {
			if rf.logs[idx].Term == reply.XTerm {
				newNextIndex = rf.logs[idx+1].Index
				break
			} else if rf.logs[idx].Term < reply.Term {
				newNextIndex = reply.XIndex
				break
			}
		}
		if idx < 0 || newNextIndex == -1 {
			// DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "nextIndex back over range\n")
			newNextIndex = rf.snapshotIndex
		}
	} else if args.PrevLogIndex == rf.snapshotIndex {
		newNextIndex = rf.snapshotIndex
	}

	// 如果返回false，才会进入此函数，所以要保证nextIndex一定是回退而不是增加（因为过去延迟的rpc可能导致nextIndex增加，造成死循环）
	// 且如果之前添加日志成功导致nextIndex和matchIndex增加，也要保证不会因为延迟的rpc回复而造成nextIndex的回退
	if newNextIndex < rf.nextIndex[server] && newNextIndex > rf.matchIndex[server] {
		rf.nextIndex[server] = newNextIndex
	}
}

// 调用时需要上锁
func (rf *Raft) UpdateCommitIndex() {
	sortMatch := make([]int, len(rf.matchIndex))
	copy(sortMatch, rf.matchIndex)
	sort.Ints(sortMatch)
	// 前len(sortMatch)+1个都是可提交的
	for i := 0; i <= len(sortMatch)/2; i++ {
		rf.commitIndex = max(rf.commitIndex, sortMatch[i])
	}
	rf.cond.Broadcast()
}

func (rf *Raft) ApplyLogs() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
			// 唤醒时检查此服务器是否已经被kill了
			if rf.killed() == true {
				rf.mu.Unlock()
				return
			}
		}
		for rf.lastApplied < rf.commitIndex && rf.killed() == false { // 防止在kill的时候该goroutine正在此循环sleep中，而不能正常退出
			msg := ApplyMsg{}

			if rf.lastApplied+1 <= rf.snapshotIndex {
				// DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "repeated apply {apply = %d, snapshotIndex = %d}\n", rf.lastApplied+1, rf.snapshotIndex)
				// 给上层传递快照
				msg.CommandValid = false
				msg.SnapshotValid = true
				msg.SnapshotIndex = rf.snapshotIndex
				msg.SnapshotTerm = rf.snapshotTerm
				// 深拷贝, 因为不是在上锁时发送数据
				msg.Snapshot = make([]byte, len(rf.snapshot))
				copy(msg.Snapshot, rf.snapshot)
			} else {
				log := rf.logs[rf.IndexInSlice(rf.lastApplied+1)]
				msg.CommandValid = true
				msg.CommandIndex = log.Index
				msg.Command = log.Command
				msg.SnapshotValid = false
			}

			// 也可以先把要发送的日志都放到临时数组中，然后在通道传递数据时不上锁，数据都传递成功之后再上锁修改rf状态

			// 设置成非阻塞管道, 防止上锁时通道阻塞造成死锁，如果通道满了，就等一段时间
			select {
			case rf.applyCh <- msg:
				if msg.CommandValid {
					rf.lastApplied++
					DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "Commit log[%v]: {Command = %v}\n", msg.CommandIndex, msg.Command)
				} else {
					rf.lastApplied = msg.SnapshotIndex
					DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "Commit snapshot: {snapshot = %v, index = %v, term = %v}\n", msg.Snapshot, msg.SnapshotIndex, msg.SnapshotTerm)
				}
			default:
				// 等待时必须解锁，上锁时睡眠就没意义了
				rf.mu.Unlock()
				// 也许睡眠间隔可以像server那样改成10ms
				time.Sleep(time.Millisecond * 50)
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()
	}
}

// 调用时需要上锁
// if args.term / reply.term > rf.currentTerm
func (rf *Raft) UpdateTerm(term int) {
	// 同一个任期内只能投票给一个人
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
	// 从Leader或Candidate变成leader要开启定时器
	if rf.role != Follower {
		rf.role = Follower
		rf.ResetElectionTime()
	}
	rf.votedNum = 0
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) < 1 {
		// DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "The lenght of logs is less than 1")
		return rf.snapshotIndex
	}

	if rf.IndexInSlice(rf.logs[len(rf.logs)-1].Index) != len(rf.logs)-1 {
		DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "index transform error\n")
	}

	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs) < 1 {
		return rf.snapshotTerm
	}

	if rf.logs[len(rf.logs)-1].Term != rf.logs[rf.IndexInSlice(rf.getLastLogIndex())].Term {
		DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "index transform error\n")
	}

	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) IndexInSlice(logIndex int) int {
	sliceIndex := logIndex - (rf.snapshotIndex + 1)
	if sliceIndex < 0 {
		DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "sliceIndex out of range : {logIndex = %d, snapshotIndex = %d, sliceIndex = %d}\n", logIndex, rf.snapshotIndex, sliceIndex)
	}
	return sliceIndex
}

func (rf *Raft) ticker() {
	// 请求投票的rpc，每个rpc对应一个gouroutine
	var sendVote func(i int, args RequestVoteArgs)
	sendVote = func(i int, args RequestVoteArgs) { // 必须传递值参数, 不能传递引用
		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(i, &args, &reply)
		if !ok {
			// 调用失败需不需要重传？
			DPrintf(Candidate, args.CandidateId, args.Term, DEBUG, "sendRequestVote to %2d failed\n", i)
			go sendVote(i, args) // 重传的参数和之前的完全一致
		} else {
			// 可以上锁并使用rf的值，因为想用的是当前值
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.UpdateTerm(reply.Term)
				rf.persist()
			} else if reply.Term == rf.currentTerm && reply.VoteGranted {
				rf.votedNum = rf.votedNum + 1
				if rf.votedNum > len(rf.peers)/2 && rf.role != Leader {
					rf.role = Leader
					rf.leader = rf.me
					// 当一个领导人刚获得权力的时候，他初始化所有的 nextIndex 值为自己的最后一条日志的index加1
					for j := 0; j < len(rf.peers); j++ {
						rf.nextIndex[j] = rf.getLastLogIndex() + 1
						// 重新初始化为0
						rf.matchIndex[j] = 0
					}
					rf.matchIndex[rf.me] = rf.getLastLogIndex()
					// 成为Leader时通知其他peer(通过心跳包)
					rf.AppendEmptyEntries()
					// 新的leader通过这个no-op日志来判断当前leader节点的哪些日志是已提交的 (8.客户端交互)
					// 因为leader不应该提交之前任期的日志，所以提交这个本任期的no-op日志 (5.4.2)
					// 不过测试代码的索引是从1开始，如果添加此功能会导致多一条日志，通不过测试。。。
					// rf.logs = append(rf.logs, LogEntry{nil, rf.currentTerm, rf.getLastLogIndex() + 1})
					// rf.LogReplication()
					DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "become leader - snapshot:{snapshot = %v, index = %v, term = %v} - logs:%v\n", rf.snapshot, rf.snapshotIndex, rf.snapshotTerm, rf.logs)
				}
			} else {
				// 这两种情况不需要管
				// reply.Term < rf.currentTerm
				// reply.Term == rf.currentTerm && reply.VoteGranted == false
			}
			rf.mu.Unlock()
		}
	}

	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		currentTime := time.Now().UnixMilli()
		args := RequestVoteArgs{}
		rf.mu.Lock()
		if rf.role != Leader && currentTime >= rf.lastElectionTime+rf.electionTimeouts {
			DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "election timeout\n")
			rf.leader = -1
			rf.role = Candidate
			rf.currentTerm = rf.currentTerm + 1
			rf.votedFor = rf.me
			rf.votedNum = 1
			rf.ResetElectionTime()

			rf.persist()

			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = rf.getLastLogIndex()
			args.LastLogTerm = rf.getLastLogTerm()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					// 对每个rpc单开一个goroutine，防止一个rpc调用时间过长而阻塞其他,且保证上锁时交给其他goroutine调用rpc
					// 把rpc放在锁外，但条件是在rpc请求过程中不能使用rf直接用成员（上锁也不行），因为可能这段时间rf的变量的值被改变了
					// 必须通过传值参数的方式提前把rpc的参数args传过去，也可以保证给每个peer的rpc调用参数是相同的
					// 但在rpc返回时可以上锁并使用rf的成员，因为接受rpc返回值时，想获得的是rf的当前成员值
					go sendVote(i, args)
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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term >= rf.currentTerm {
		rf.leader = args.LeaderId
		rf.UpdateTerm(args.Term)
		rf.persist()
		rf.ResetElectionTime()
		if args.LastIncludedIndex > rf.snapshotIndex {
			if args.LastIncludedIndex >= rf.getLastLogIndex() {
				rf.logs = make([]LogEntry, 0)
			} else {
				var tmp []LogEntry
				tmp = append(tmp, rf.logs[rf.IndexInSlice(args.LastIncludedIndex+1):]...)
				rf.logs = tmp
			}
			rf.snapshotIndex = args.LastIncludedIndex
			rf.snapshotTerm = args.LastIncludedTerm
			rf.snapshot = args.Data
			// rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex) 不能更新lastApplied, 通过判断lastApplied < snapshotIndex 来向上层发送快照
			// commitIndex >= napshotIndex
			rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
			rf.persist()
			// 更新commitIndex，通知发送快照给上层
			rf.cond.Broadcast()
		} else if args.LastIncludedIndex == rf.snapshotIndex && args.LastIncludedTerm != rf.snapshotTerm {
			DPrintf(rf.role, rf.me, rf.currentTerm, ERROR, "snapshot not match\n")
		}
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 调用时上锁
func (rf *Raft) SnapshotReplication(server int) {
	args := InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.snapshotIndex
	args.LastIncludedTerm = rf.snapshotTerm
	// args.Data = rf.snapshot
	// 要深拷贝，因为不是立刻发送数据
	args.Data = make([]byte, len(rf.snapshot))
	copy(args.Data, rf.snapshot)
	go func(args InstallSnapshotArgs) {
		reply := InstallSnapshotReply{}
		if ok := rf.sendInstallSnapshot(server, &args, &reply); ok {
			DPrintf(Leader, args.LeaderId, args.Term, DEBUG, "InstallSnapshot to %2d failed\n", server)
		} else {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.UpdateTerm(reply.Term)
				rf.persist()
			}
			rf.mu.Unlock()
		}
	}(args)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-`````````````````````````````````````````````````````````running work.
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
	// rf.logs[0] = LogEntry{Term:0, Index:0}, 在头部加一个哨兵结点 (在2D中由于有了snapshotIndex的存在，不需要这个哨兵节点了)
	// raft的索引是从1开始的，所以哨兵结点的索引为0
	// rf.logs = append(rf.logs, LogEntry{nil, 0, 0})
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)

	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.ResetElectionTime()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.HeartBeats()
	go rf.ApplyLogs()

	DPrintf(rf.role, rf.me, rf.currentTerm, INFO, "initialize finished\n")

	return rf
}
