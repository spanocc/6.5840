package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID int64
	Seq     int64
}

type PutAppendReply struct {
	Err   Err
	Value string // 调试
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID int64
	Seq     int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardsArgs struct {
	ShardNum       int
	Shards         map[string]string
	ConfigNum      int
	DuplicateTable map[int64]DuplicateTableEntry // 如果两个组先后服务一个切片，那需要传递这个数据来去重
}

type ShardsReply struct {
	Err Err
}
