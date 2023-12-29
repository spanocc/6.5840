package shardkv

import (
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// 关键： 同一时刻同一切片有且仅有一个组持有    对于新的配置，看某个切片如果分给了其他组，并且自己持有这个切片，就进行切片转移
// 在切片转移时，要先设置为自己不持有此切片，防止出现两个组持有同一个切片，产生脑裂问题
// 切片转移时，要始终保持不持有切片的状态，期间要阻止对于此切片的访问，直到下一个持有此切片的组产生

// 如果dp表里时err wrong group错误， 也要重新应用一遍日志， 因为 持有的切片信息属于server层状态机的一部分， group错误必须等到应用时候才能发现，所以需要填上dp表的值

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
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
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *ShardKV) SendShards(gid int, shards map[string]string) {

}

func (kv *ShardKV) RecvShards(args *ShardsArgs, reply *ShardsReply) {

}

func (kv *ShardKV) ApplyLogs() {

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
