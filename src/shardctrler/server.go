package shardctrler

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type DuplicateTableEntry struct {
	Seq    int64
	Err    Err
	Config Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	DuplicateTable map[int64]DuplicateTableEntry
	cond           *sync.Cond
	CurrentIndex   int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Operation string
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
	Seq       int64
	ClerkID   int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()

	if args.Seq == sc.DuplicateTable[args.ClerkID].Seq {
		reply.WrongLeader = false
		reply.Err = sc.DuplicateTable[args.ClerkID].Err
	} else if args.Seq > sc.DuplicateTable[args.ClerkID].Seq {
		op := Op{
			Operation: "Join",
			Servers:   args.Servers,
		}

		index, term, isLeader := sc.rf.Start(op)
		if isLeader {
			for {
				currentTerm, ok := sc.rf.GetState()
				if sc.CurrentIndex >= index || currentTerm != term || !ok {
					break
				}
				sc.cond.Wait()
			}

			if sc.DuplicateTable[args.ClerkID].Seq == args.Seq {
				reply.WrongLeader = false
				reply.Err = sc.DuplicateTable[args.ClerkID].Err
			} else if sc.DuplicateTable[args.ClerkID].Seq > args.Seq {
				reply.WrongLeader = true
			} else {
				reply.WrongLeader = true
			}
		} else {
			reply.WrongLeader = true
		}
	} else {

	}

	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()

	if args.Seq == sc.DuplicateTable[args.ClerkID].Seq {
		reply.WrongLeader = false
		reply.Err = sc.DuplicateTable[args.ClerkID].Err
	} else if args.Seq > sc.DuplicateTable[args.ClerkID].Seq {
		op := Op{
			Operation: "Leave",
			GIDs:      args.GIDs,
		}

		index, term, isLeader := sc.rf.Start(op)
		if isLeader {
			for {
				currentTerm, ok := sc.rf.GetState()
				if sc.CurrentIndex >= index || currentTerm != term || !ok {
					break
				}
				sc.cond.Wait()
			}

			if sc.DuplicateTable[args.ClerkID].Seq == args.Seq {
				reply.WrongLeader = false
				reply.Err = sc.DuplicateTable[args.ClerkID].Err
			} else if sc.DuplicateTable[args.ClerkID].Seq > args.Seq {
				reply.WrongLeader = true
			} else {
				reply.WrongLeader = true
			}
		} else {
			reply.WrongLeader = true
		}
	} else {

	}

	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()

	if args.Seq == sc.DuplicateTable[args.ClerkID].Seq {
		reply.WrongLeader = false
		reply.Err = sc.DuplicateTable[args.ClerkID].Err
	} else if args.Seq > sc.DuplicateTable[args.ClerkID].Seq {
		op := Op{
			Operation: "Move",
			Shard:     args.Shard,
			GID:       args.GID,
		}

		index, term, isLeader := sc.rf.Start(op)
		if isLeader {
			for {
				currentTerm, ok := sc.rf.GetState()
				if sc.CurrentIndex >= index || currentTerm != term || !ok {
					break
				}
				sc.cond.Wait()
			}

			if sc.DuplicateTable[args.ClerkID].Seq == args.Seq {
				reply.WrongLeader = false
				reply.Err = sc.DuplicateTable[args.ClerkID].Err
			} else if sc.DuplicateTable[args.ClerkID].Seq > args.Seq {
				reply.WrongLeader = true
			} else {
				reply.WrongLeader = true
			}
		} else {
			reply.WrongLeader = true
		}
	} else {

	}

	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()

	if args.Seq == sc.DuplicateTable[args.ClerkID].Seq {
		reply.WrongLeader = false
		reply.Err = sc.DuplicateTable[args.ClerkID].Err
		reply.Config = sc.DuplicateTable[args.ClerkID].Config
	} else if args.Seq > sc.DuplicateTable[args.ClerkID].Seq {
		op := Op{
			Operation: "Query",
			Num:       args.Num,
		}

		index, term, isLeader := sc.rf.Start(op)
		if isLeader {
			for {
				currentTerm, ok := sc.rf.GetState()
				if sc.CurrentIndex >= index || currentTerm != term || !ok {
					break
				}
				sc.cond.Wait()
			}

			if sc.DuplicateTable[args.ClerkID].Seq == args.Seq {
				reply.WrongLeader = false
				reply.Err = sc.DuplicateTable[args.ClerkID].Err
				reply.Config = sc.DuplicateTable[args.ClerkID].Config
			} else if sc.DuplicateTable[args.ClerkID].Seq > args.Seq {
				reply.WrongLeader = true
			} else {
				reply.WrongLeader = true
			}
		} else {
			reply.WrongLeader = true
		}
	} else {

	}

	sc.mu.Unlock()
}

func (sc *ShardCtrler) ApplyLogs() {
	for {
		select {
		case ch, ok := <-sc.applyCh:
			if !ok {
				return
			}

			sc.mu.Lock()

			if ch.CommandValid {
				if ch.CommandIndex != sc.CurrentIndex+1 {
					DPrintf(ServerRole, sc.me, ERROR, "applyCh index error, expect %v but %v\n", sc.CurrentIndex+1, ch.CommandIndex)
				}

				op, ok := ch.Command.(Op)
				if !ok {
					DPrintf(ServerRole, sc.me, ERROR, "type error\n")
				}

				if op.Seq < sc.DuplicateTable[op.ClerkID].Seq+1 {
					// 日志已apply nothing to do
				} else if op.Seq == sc.DuplicateTable[op.ClerkID].Seq+1 {

					sc.PerformOperation(op)

				} else {
					DPrintf(ServerRole, sc.me, ERROR, "op seq error, expect %v but %v\n", sc.DuplicateTable[op.ClerkID].Seq+1, op.Seq)
				}

				sc.CurrentIndex++
			} else if ch.SnapshotValid {
				DPrintf(ServerRole, sc.me, ERROR, "should not reach here\n")
			}

			sc.mu.Unlock()
		default:
			time.Sleep(time.Millisecond * 10)
		}

		sc.cond.Broadcast()
	}
}

func (sc *ShardCtrler) PerformOperation(op Op) {
	switch op.Operation {
	case "Join":
		{
			// 求平均每个组大概分多少个分片，然后分配
		}
	case "Leave":
		{

		}
	case "Move":
		{

		}
	case "Query":
		{

		}
	default:
		DPrintf(ServerRole, sc.me, ERROR, "invalid log type: %v\n", op.Operation)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	Debug = false
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg, 100)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.DuplicateTable = make(map[int64]DuplicateTableEntry)
	sc.cond = sync.NewCond(&sc.mu)
	sc.CurrentIndex = 0

	return sc
}
