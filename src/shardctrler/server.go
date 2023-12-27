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
			ClerkID:   args.ClerkID,
			Seq:       args.Seq,
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
			ClerkID:   args.ClerkID,
			Seq:       args.Seq,
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
			ClerkID:   args.ClerkID,
			Seq:       args.Seq,
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
			ClerkID:   args.ClerkID,
			Seq:       args.Seq,
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

// 使用类似于redis分布式集群的负载均衡方案。插槽的方式，16384个插槽改成10个插槽
func (sc *ShardCtrler) PerformOperation(op Op) {
	config := Config{}

	switch op.Operation {
	case "Join":
		{
			// 把已有组的分片分到新的组中
			// 求平均每个组大概分多少个分片，然后分配
			oldConfig := sc.configs[len(sc.configs)-1]

			// 仅考虑不重复的
			servers := map[int][]string{}
			for k, v := range op.Servers {
				if _, ok := oldConfig.Groups[k]; !ok {
					servers[k] = v
				}
			}

			gNum := len(oldConfig.Groups) + len(servers)
			if gNum > NShards {
				DPrintf(ServerRole, sc.me, ERROR, "too much groups, old: %v, add: %v", len(oldConfig.Groups), len(servers))
			}

			averLoad := NShards / gNum
			decNum := averLoad * len(servers)
			distributedShards := []int{}

			// GID -> set(Shard)
			ctlShards := map[int]map[int]struct{}{}
			for s, g := range oldConfig.Shards {
				if g == 0 {
					distributedShards = append(distributedShards, s)
					decNum -= len(distributedShards)
				} else {
					ctlShards[g][s] = struct{}{}
				}
			}

			// 把已有组中多余的分片先放到分发队列中
			critical := averLoad + 1 // 每个组最多包含的分片数量
			for decNum > 0 {
				for _, shards := range ctlShards {
					del := min(len(shards)-critical, decNum)

					for k := range shards {
						if del <= 0 {
							break
						}
						delete(shards, k)
						distributedShards = append(distributedShards, k)
						del--
					}

					if decNum <= 0 {
						break
					}
				}
				critical--
			}

			idx := 0
			for g := range servers {
				for i := 0; i < averLoad; i++ {
					ctlShards[g][distributedShards[idx]] = struct{}{}
					idx++
				}
			}

			if idx != len(distributedShards) {
				DPrintf(ServerRole, sc.me, ERROR, "the length of distributeShards is not correct: %v, idx: %v\n", len(distributedShards), idx)
			}

			newConfig := Config{}
			newConfig.Num = oldConfig.Num + 1
			for k, v := range oldConfig.Groups {
				if _, ok := newConfig.Groups[k]; ok {
					DPrintf(ServerRole, sc.me, ERROR, "repeated gid: %v\n", k)
				}
				newConfig.Groups[k] = v
			}

			for k, v := range servers {
				if _, ok := newConfig.Groups[k]; ok {
					DPrintf(ServerRole, sc.me, ERROR, "repeated gid: %v\n", k)
				}
				newConfig.Groups[k] = v
			}

			for g, shards := range ctlShards {
				for s := range shards {
					newConfig.Shards[s] = g
				}
			}

			sc.configs = append(sc.configs, newConfig)
		}
	case "Leave":
		{
			oldConfig := sc.configs[len(sc.configs)-1]

			// 找到存在在配置中的gid
			GIDs := []int{}
			for _, id := range op.GIDs {
				if _, ok := oldConfig.Groups[id]; ok {
					GIDs = append(GIDs, id)
				}
			}

			// 把要删除的gid对应的切片都拿出来
			ctlShards := map[int]map[int]struct{}{}
			distributedShards := []int{}

			for s, v := range oldConfig.Shards {
				removed := false
				for _, gid := range GIDs {
					if v == gid {
						distributedShards = append(distributedShards, s)
						removed = true
						break
					}
				}

				if !removed {
					ctlShards[v][s] = struct{}{}
				}
			}

			gNum := len(oldConfig.Groups) - len(GIDs)
			averLoad := NShards / gNum
			disIdx := 0

			// 把分发队列中的切片分到剩余组中
			critical := averLoad // 每个组最多包含的分片数量
			for disIdx < len(distributedShards) {
				for _, shards := range ctlShards {
					add := min(critical-len(shards), len(distributedShards)-disIdx)

					for add > 0 {
						shards[distributedShards[disIdx]] = struct{}{}
						disIdx++
						add--
					}

					if disIdx >= len(distributedShards) {
						break
					}
				}

				critical++
			}

			newConfig := Config{}
			newConfig.Num = oldConfig.Num + 1

			for g, shards := range ctlShards {
				newConfig.Groups[g] = oldConfig.Groups[g]
				for s := range shards {
					newConfig.Shards[s] = g
				}
			}

			sc.configs = append(sc.configs, newConfig)
		}
	case "Move":
		{
			oldConfig := sc.configs[len(sc.configs)-1]

			newConfig := Config{}
			newConfig.Num = oldConfig.Num + 1
			for i, v := range oldConfig.Shards {
				if i == op.Shard {
					newConfig.Shards[i] = op.GID
				} else {
					newConfig.Shards[i] = v
				}
			}

			for g, servers := range oldConfig.Groups {
				newConfig.Groups[g] = servers
			}
		}
	case "Query":
		{
			idx := op.Num
			if idx == -1 {
				idx = len(sc.configs) - 1
			}

			config = sc.configs[idx]
			config.Groups = make(map[int][]string)

			for g, server := range sc.configs[idx].Groups {
				config.Groups[g] = server
			}
		}
	default:
		DPrintf(ServerRole, sc.me, ERROR, "invalid log type: %v\n", op.Operation)
	}

	sc.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
		Seq:    op.Seq,
		Err:    OK,
		Config: config,
	}

	DPrintf(ServerRole, sc.me, INFO, "after op: %v, config: %v\n", op.Operation, sc.configs[len(sc.configs)-1])
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

	go sc.ApplyLogs()

	return sc
}
