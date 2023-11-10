package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

// const Debug = false

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClerkID   int64
	Seq       int64
}

type DuplicateTableEntry struct {
	seq   int64
	value string
	err   Err
}

// type RegisterEntry struct {
// 	index int
// 	err   Err
// 	value string
// }

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	subject map[string]string

	// clerkID -> {seq, value}
	duplicateTable map[int64]DuplicateTableEntry
	// 接收到raft指令 唤醒Get和PutAppend RPC
	cond *sync.Cond
	// clerkID -> 等待结果
	// 同一clerk可能有多个rpc在等待，因为可能有延迟的rpc
	// register map[int64]RegisterEntry

	// 只需要通过当前执行到的最大索引currentIndex和duplicateTable来判断该rpc是否正确返回
	currentIndex int

	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	if args.Seq == kv.duplicateTable[args.ClerkID].seq {
		reply.Err = OK
		reply.Value = kv.duplicateTable[args.ClerkID].value
	} else if args.Seq > kv.duplicateTable[args.ClerkID].seq {
		op := Op{
			Operation: "Get",
			Key:       args.Key,
			Value:     "",
			ClerkID:   args.ClerkID,
			Seq:       args.Seq,
		}
		index, term, isLeader := kv.rf.Start(op)
		if isLeader {
			for {
				currentTerm, ok := kv.rf.GetState()
				if kv.currentIndex >= index || currentTerm != term || !ok {
					break
				}
				kv.cond.Wait()
			}
			if kv.duplicateTable[args.ClerkID].seq == args.Seq {
				reply.Err = kv.duplicateTable[args.ClerkID].err
				reply.Value = kv.duplicateTable[args.ClerkID].value
			} else if kv.duplicateTable[args.ClerkID].seq > args.Seq {
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = ErrWrongLeader
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	} else {

	}

	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	if args.Seq == kv.duplicateTable[args.ClerkID].seq {
		reply.Err = OK
	} else if args.Seq > kv.duplicateTable[args.ClerkID].seq {
		// 这里args.seq > kv.duplicateTable[args.ClerkID].seq + 1也是可能的，因为本server可能是新leader，还没应用上一条日志，但client已经收到了之前的leader的结果，从而发送了下一条请求
		op := Op{
			Operation: args.Op,
			Key:       args.Key,
			Value:     args.Value,
			ClerkID:   args.ClerkID,
			Seq:       args.Seq,
		}
		index, term, isLeader := kv.rf.Start(op)
		if isLeader {
			for {
				// 注意：rpc要及时唤醒并返回，labrpc不负责检查超时，如果不返回，clerk端会一直等待
				currentTerm, ok := kv.rf.GetState()
				if kv.currentIndex >= index || currentTerm != term || !ok {
					break
				}
				kv.cond.Wait()
			}
			// 执行op的goroutine会设置好duplicateTable的seq和value，然后唤醒本gouroutine
			if kv.duplicateTable[args.ClerkID].seq == args.Seq {
				reply.Err = kv.duplicateTable[args.ClerkID].err
			} else if kv.duplicateTable[args.ClerkID].seq > args.Seq {
				// 此时说明本rpc对应的操作已经返回给client了，所以本rpc可能是延迟的rpc，不需要执行什么回复
				reply.Err = ErrWrongLeader // 稳妥一点，返回ErrWrongLeader，就算让client重试也不会出错
			} else {
				// 1. 如果currenIndex > index 说明该index对应的op是其他的clerk
				// 2. 否则一定是 currentTerm != term || !ok ，任期发生改变
				reply.Err = ErrWrongLeader
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	} else {
		// DPrintf(serverRole, kv.me, ERROR, "should not reach here, against the assumption: a client will make only one call into a Clerk at a time\n")
		// 不能abort，因为可能有延迟的rpc到达，无视就好
	}

	kv.mu.Unlock()
}

func (kv *KVServer) ApplyLogs() {
	for kv.killed() == false {

		select {
		case ch, ok := <-kv.applyCh:
			if !ok {
				return
			}
			kv.mu.Lock()
			if ch.CommandValid {
				if ch.CommandIndex != kv.currentIndex+1 {
					DPrintf(serverRole, kv.me, ERROR, "applyCh index error, expect %v but %v\n", kv.currentIndex+1, ch.CommandIndex)
				}

				op, ok := ch.Command.(Op)
				if !ok {
					DPrintf(serverRole, kv.me, ERROR, "type error\n")
				}

				if op.Seq < kv.duplicateTable[op.ClerkID].seq+1 {
					// 日志已apply nothing to do
				} else if op.Seq == kv.duplicateTable[op.ClerkID].seq+1 {
					if op.Operation == "Get" {
						value, ok := kv.subject[op.Key]
						if ok {
							kv.duplicateTable[op.ClerkID] = DuplicateTableEntry{seq: op.Seq, value: value, err: OK}
						} else {
							kv.duplicateTable[op.ClerkID] = DuplicateTableEntry{seq: op.Seq, value: "", err: ErrNoKey}
						}
					} else {
						value, ok := kv.subject[op.Key]
						if ok {
							if op.Operation == "Append" {
								kv.subject[op.Key] = value + op.Value
							} else {
								kv.subject[op.Key] = op.Value
							}
						} else {
							kv.subject[op.Key] = op.Value
						}
						kv.duplicateTable[op.ClerkID] = DuplicateTableEntry{seq: op.Seq, err: OK}
					}
					DPrintf(serverRole, kv.me, INFO, "apply logs, op: %v, state: %v\n", op, kv.subject)
				} else {
					DPrintf(serverRole, kv.me, ERROR, "op seq error, expect %v but %v\n", kv.duplicateTable[op.ClerkID].seq+1, op.Seq)
				}

				kv.currentIndex++
			}
			kv.mu.Unlock()
		default:
			// 这里休眠的间隔是多少会决定3A的Test:ops complete fast enough测试运行多长时间
			time.Sleep(time.Millisecond * 10)
		}
		// 必须保证一段时间唤醒一次，来判断当前任期是否改变，以此来返回rpc，不然rpc会一直等待（比如，客户端调用rpc后，leader改变，由于新的leader不会提交之前的日志，所以server层在channel中收不到日志，也感知不到leader的改变，无法唤醒rpc返回）
		kv.cond.Broadcast()
	}
}

func (kv *KVServer) CheckSnapshot() {
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.maxraftstate != -1 {

		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.cond.Broadcast()
	// 唤醒ApplyLogs gouroutine
	// 如果关闭管道，而raft层正好在往管道里写数据，就会触发panic，所以最好不要主动close channel
	// kv.applyCh <- raft.ApplyMsg{}
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.subject = make(map[string]string)
	kv.duplicateTable = make(map[int64]DuplicateTableEntry)
	kv.cond = sync.NewCond(&kv.mu)
	kv.currentIndex = 0

	kv.persister = persister

	// 对于无缓冲的channel的发送和输出端都用select会导致两方配合不到一起去，对于raft层，select是不必要的，所以还有优化空间，但是我现在不太想改raft
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.ApplyLogs()

	return kv
}
