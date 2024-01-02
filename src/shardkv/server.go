package shardkv

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// 关键： 同一时刻同一切片有且仅有一个组持有    对于新的配置，看某个切片如果分给了其他组，并且自己持有这个切片，就进行切片转移
// 在切片转移时，要先设置为自己不持有此切片，防止出现两个组持有同一个切片，产生脑裂问题
// 切片转移时，要始终保持不持有切片的状态，期间要阻止对于此切片的访问，直到下一个持有此切片的组产生

// 如果dp表里时err wrong group错误， 也要重新应用一遍日志， 因为 持有的切片信息属于server层状态机的一部分， group错误必须等到应用时候才能发现，所以需要填上dp表的值
// 而上次请求时err wrong group 不代表这次也是, 重复的请求seq是一样的，但是log的index不同

// 因为每一个组都通过raft来保证可用性和容错，所以假设每个组都是不会出错，不会分区，不会网络延迟的

// lab4 的一些假设：
// 1. 不会出现 只有leader网络出现分区，和某一节点（客户端，分片控制器）无法通信，但和其他raft的follower可以通信，造成整个raft无法连接到外部，却无法重新选举新leader
//    应该很少出现这样的分区，要是网络出问题，应该follower也通信不了，就会选举新leader
//    这种情况下raft的一致性不会有问题，但是可用性可能出问题了
// 2. 假设不会出现整个组集群全挂掉，不然那部分切片就没了
// 3. 任意两个组都可以正常通信，任意一个组都可以拿到最新分片配置信息
//    比如在配置10中，分片在组1中，配置11中，分片在组2，配置12中分片在组3。 如果组2拿不到最新配置，那配置就会留在组2。 如果组1无法发送给组2，那会一直重试，而不会发送给组3，
// 	  不然会出现一种情况，组1先发给组2，失败后再发给组3，但其实组2和组3都收到了，且组2拿不到最新配置，以为配置11是最新的，所以组2和3都以为自己持有这个切片，就会出问题。（You will need to ensure that at most one replica group is serving requests for each shard at any one time.）

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
	Seq   int64
	Value string
	Err   Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck *shardctrler.Clerk

	Subject      map[string]string
	masterShards map[int]struct{}

	DuplicateTable map[int64]DuplicateTableEntry
	cond           *sync.Cond
	CurrentIndex   int
	persister      *raft.Persister

	configNum int // 相当于raft的任期, 同一个configNum，同一个切片只能由一个组管理
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	// 注意: dup表中的err值只可能是ok, no key 或者wrong group, 如果是wrong leader，则不会更新dup表的seq
	if args.Seq == kv.DuplicateTable[args.ClerkID].Seq && kv.DuplicateTable[args.ClerkID].Err != ErrWrongGroup {
		reply.Err = kv.DuplicateTable[args.ClerkID].Err
		reply.Value = kv.DuplicateTable[args.ClerkID].Value
	} else if args.Seq > kv.DuplicateTable[args.ClerkID].Seq || (args.Seq == kv.DuplicateTable[args.ClerkID].Seq && kv.DuplicateTable[args.ClerkID].Err == ErrWrongGroup) {
		op := Op{
			Operation: "Get",
			Key:       args.Key,
			ClerkID:   args.ClerkID,
			Seq:       args.Seq,
		}

		index, term, isLeader := kv.rf.Start(op)
		if isLeader {
			for {
				currentTerm, ok := kv.rf.GetState()
				if kv.CurrentIndex >= index || currentTerm != term || !ok {
					break
				}
				kv.cond.Wait()
			}

			if kv.DuplicateTable[args.ClerkID].Seq == args.Seq {
				reply.Err = kv.DuplicateTable[args.ClerkID].Err
				reply.Value = kv.DuplicateTable[args.ClerkID].Value
			} else if kv.DuplicateTable[args.ClerkID].Seq > args.Seq {
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	if args.Seq == kv.DuplicateTable[args.ClerkID].Seq && kv.DuplicateTable[args.ClerkID].Err != ErrWrongGroup {
		reply.Err = kv.DuplicateTable[args.ClerkID].Err
	} else if args.Seq > kv.DuplicateTable[args.ClerkID].Seq || (args.Seq == kv.DuplicateTable[args.ClerkID].Seq && kv.DuplicateTable[args.ClerkID].Err == ErrWrongGroup) {
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
				currentTerm, ok := kv.rf.GetState()
				if kv.CurrentIndex >= index || currentTerm != term || !ok {
					break
				}
				kv.cond.Wait()
			}

			if kv.DuplicateTable[args.ClerkID].Seq == args.Seq {
				reply.Err = kv.DuplicateTable[args.ClerkID].Err
			} else if kv.DuplicateTable[args.ClerkID].Seq > args.Seq {
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

func (kv *ShardKV) SendShards(gid int, shards map[string]string) {

}

func (kv *ShardKV) RecvShards(args *ShardsArgs, reply *ShardsReply) {
	kv.mu.Lock()

	if int64(args.ConfigNum) == kv.DuplicateTable[int64(args.GID)].Seq && kv.DuplicateTable[int64(args.GID)].Err != ErrWrongGroup {
		reply.Err = kv.DuplicateTable[int64(args.GID)].Err
	} else if int64(args.ConfigNum) > kv.DuplicateTable[int64(args.GID)].Seq || (int64(args.ConfigNum) == kv.DuplicateTable[int64(args.GID)].Seq && kv.DuplicateTable[args.ClerkID].Err == ErrWrongGroup) {
		op := Op{
			Operation: "Shards",
			ClerkID:   int64(args.GID),
			Seq:       int64(args.ConfigNum),
		}

		index, term, isLeader := kv.rf.Start(op)
		if isLeader {
			for {
				currentTerm, ok := kv.rf.GetState()
				if kv.CurrentIndex >= index || currentTerm != term || !ok {
					break
				}
				kv.cond.Wait()
			}

			if kv.DuplicateTable[int64(args.GID)].Seq == int64(args.ConfigNum) {
				reply.Err = kv.DuplicateTable[int64(args.GID)].Err
			} else if kv.DuplicateTable[int64(args.GID)].Seq > int64(args.ConfigNum) {
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

func (kv *ShardKV) ApplyLogs() {
	for {
		select {
		case ch, ok := <-kv.applyCh:
			if !ok {
				return
			}

			kv.mu.Lock()

			if ch.CommandValid {
				if ch.CommandIndex != kv.CurrentIndex+1 {
					DPrintf(ServerRole, kv.me, ERROR, "applyCh index error, expect %v but %v\n", kv.CurrentIndex+1, ch.CommandIndex)
				}

				op, ok := ch.Command.(Op)
				if !ok {
					DPrintf(ServerRole, kv.me, ERROR, "type error\n")
				}

				if op.Seq == kv.DuplicateTable[op.ClerkID].Seq+1 || (op.Seq == kv.DuplicateTable[op.ClerkID].Seq && kv.DuplicateTable[op.ClerkID].Err == ErrWrongGroup) {
					kv.PerformOperation(op)
				} else if op.Seq < kv.DuplicateTable[op.ClerkID].Seq+1 {

				} else {
					DPrintf(ServerRole, kv.me, ERROR, "op seq error, expect %v but %v\n", kv.DuplicateTable[op.ClerkID].Seq+1, op.Seq)
				}

				kv.CurrentIndex++
			} else if ch.SnapshotValid {
				DPrintf(ServerRole, kv.me, ERROR, "should not reach here\n")
			}

			kv.mu.Unlock()
		default:
			time.Sleep(time.Millisecond * 10)
		}

		kv.cond.Broadcast()
	}
}

func (kv *ShardKV) PerformOperation(op Op) {

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.Subject = make(map[string]string)
	kv.masterShards = make(map[int]struct{})
	kv.DuplicateTable = make(map[int64]DuplicateTableEntry)
	kv.cond = sync.NewCond(&kv.mu)
	kv.CurrentIndex = 0

	kv.persister = persister

	go kv.ApplyLogs()

	return kv
}
