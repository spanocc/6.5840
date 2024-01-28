package shardkv

import (
	"bytes"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// 关键： 同一时刻同一切片有且仅有一个组持有(可以有多个组持有，但只能有一个组对外服务)    对于新的配置，看某个切片如果分给了其他组，并且自己持有这个切片，就进行切片转移
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

// 由于要保证在切片转移过程中仍能够提供服务给客户端，所以需要异步的rpc转移切片，不能等到切片转移完成后再执行下一条日志，所以在转移完成之前，currentIndex就会+1

// 必须要对最新的配置不断地进行raft层的日志应用，因为可能出现：
// 1. 因为快照，而导致异步的rpc转移在实例重启之后不会重新启动，所以至少有一个后台goroutine来不断查找有没有需要发送的切片，可以通过不断的获取最新配置并应用来达到这点 （由于这一点，所以无法确定reconfigure是否完成，所以等待前一个配置完成的方案就不成立了）
// 2. 在配置10中切片有a转移到b，在配置11中由b转移到c，但如果b实例获取到了最新配置11，但此时b还没有获取切片，如果此时认为b转移完成了，那等到a把切片转移给b时，b也不会转移给c，所以b必须一致获取新配置并查看有没有需要转移的切片

// 为什么同一组的每个raft实例都需要发送切片rpc？
// 因为每个实例发送完切片后都要删除本实例的切片并更新一些状态，所以每个实例都要做这些，但是放在日志里就太麻烦了 （必须放在日志里）

// 以上做法可能出现问题：
// 配置10中切片由a传到b，配置11中切片被组c管理  如果切片正在从a传到b的过程中，进行快照，并且实例宕机重启，重启后获取新配置11，想把切片发送给c，但是此时切片正处于发送到b的状态，所以有冲突。但是此时还不能阻塞等待配置10发布完成再发布配置11，因为在重配置的过程中还需要对客户端提供请求
// 所以解决办法是：在配置10完成之前，不会进行配置11的日志start。 Process re-configurations one at a time, in order.
// (这种情况，直接让b把配置传给c不就完了)

// 同时最好要保证所有更新状态的操作（发送完切片的更新状态）都放在日志里，所以在把切片发送完成后，必须应用一条日志，否则可能出现一个实例完成配置10，并且应用配置11的日志，其他实例还没有完成配置10，就已经收到了配置11的日志。
// 每个实例都会发送rpc切片，而不只是主实例，因为发送切片也属于日志的操作的一部分，但最后的更新状态只有主实例会start日志成功
// 因为上一个配置还没更新完 就可能出现下一个配置 所以需要保存还没完成的配置 （但只要保存一个正在做的配置就可以了，不需要保存一个完整的配置序列，某个组跳过某个配置也没啥问题）

// 众多问题的解决方案，用一个后台线程不断轮询发送切片，解决了快照并重启后仍有未发送完成的切片的问题
// 如果采用上面用的获取配置的方法，在获取配置11时，可能配置10还没弄完，就很麻烦，还不知道配置10何时弄完（前面也提到这点了）
// 同理，切片相关的且不需要回复发送端的 ，不能通过dup数组来判断是否完成过了，因为reconfigure不能判断是否完成了
// recvshards 需要回复发送端，所以流程和get，put一样

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClerkID   int64
	Seq       int64
	Config    shardctrler.Config
	Shards    map[string]string
	ShardNum  int
	ConfigNum int
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

	MasterShards map[int]struct {
		ConfigNum int      // 这个切片对应的配置号，如果在转移切片后，本切片的配置号仍然是旧的，则可以删除此切片 (防止出现这种情况: 在配置10把切片转移出去，在配置11切片又转移回来，此时才收到配置10的转移成功应答，但此时发现切片的配置号是11，所以不能删除此切片)
		Master    int      // master == gid 代表可以服务此切片 master == -1 表示不拥有此切片 master == 其他组gid 表示切片正在发送给这个组，所以切片不能发送给其他组了
		Servers   []string // 切片要发给的组对应的servers
		Subject   map[string]string
	}

	DuplicateTable map[int64]DuplicateTableEntry
	cond           *sync.Cond
	CurrentIndex   int
	persister      *raft.Persister

	FirstConfig bool
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
		reply.Value = kv.DuplicateTable[args.ClerkID].Value
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

func (kv *ShardKV) SendShards(servers []string, configNum int, shardNum int, shards map[string]string) {
	args := ShardsArgs{}
	args.ConfigNum = configNum
	args.ShardNum = shardNum
	args.Shards = shards

	DPrintf(ServerRole, kv.gid, kv.me, INFO, "send shards start, servers: %v, shards: %v", servers, shardNum)

Loop:
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply ShardsReply
			ok := srv.Call("ShardKV.RecvShards", &args, &reply)
			if ok && reply.Err == OK {
				break Loop
			}
			if ok && reply.Err == ErrWrongGroup {
				DPrintf(ServerRole, kv.gid, kv.me, ERROR, "should not reach here\n")
			}
			// ... not ok, or ErrWrongLeader
		}
	}

	DPrintf(ServerRole, kv.gid, kv.me, INFO, "send shards succ, servers: %v, shards: %v, subject: %v", servers, shardNum, shards)

	op := Op{
		Operation: "ShardsOver",
		Shards:    shards,
		ShardNum:  shardNum,
		ConfigNum: configNum,
	}

	kv.rf.Start(op)
}

func (kv *ShardKV) RecvShards(args *ShardsArgs, reply *ShardsReply) {
	kv.mu.Lock()

	// 发送切片不会出现wrong group错误，收到切片的组不管怎么样都要持有这个切片
	if int64(args.ConfigNum) == kv.DuplicateTable[int64(args.ShardNum)].Seq {
		reply.Err = kv.DuplicateTable[int64(args.ShardNum)].Err
	} else if int64(args.ConfigNum) > kv.DuplicateTable[int64(args.ShardNum)].Seq {
		op := Op{
			Operation: "RecvShards",
			ClerkID:   int64(args.ShardNum), // 以切片编号作为唯一客户端id， 因为同一个组在同一个config中 可能发送多个组切片， 但一个组切片在同一个confignum中只会被一个组提供服务
			Seq:       int64(args.ConfigNum),
			ConfigNum: args.ConfigNum,
			Shards:    args.Shards,
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

			if kv.DuplicateTable[int64(args.ShardNum)].Seq == int64(args.ConfigNum) {
				reply.Err = kv.DuplicateTable[int64(args.ShardNum)].Err
			} else if kv.DuplicateTable[int64(args.ShardNum)].Seq > int64(args.ConfigNum) {
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

func (kv *ShardKV) MonitorConfig() {
	for {

		kv.mu.Lock()
		ok := kv.FirstConfig
		kv.mu.Unlock()

		config := kv.mck.Query(-1)
		if config.Num > 1 && !ok {
			config = kv.mck.Query(1)
		}

		op := Op{
			Operation: "Reconfigure",
			Config:    config,
		}

		// 不需要结果, 因为每100ms会start一次
		kv.rf.Start(op)

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) TransferShards() {
	for {
		kv.mu.Lock()

		for shardNum, shards := range kv.MasterShards {
			if shards.Master != kv.gid && shards.Master != -1 {
				transferShards := map[string]string{}

				for k, v := range shards.Subject {
					transferShards[k] = v
				}

				go kv.SendShards(shards.Servers, shards.ConfigNum, shardNum, transferShards)
			}
		}

		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
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
					DPrintf(ServerRole, kv.gid, kv.me, ERROR, "applyCh index error, expect %v but %v\n", kv.CurrentIndex+1, ch.CommandIndex)
				}

				op, ok := ch.Command.(Op)
				if !ok {
					DPrintf(ServerRole, kv.gid, kv.me, ERROR, "type error\n")
				}

				if op.Operation == "Reconfigure" || op.Operation == "ShardsOver" || op.Operation == "RecvShards" {
					kv.PerformOperation(op)
				} else if op.Seq >= kv.DuplicateTable[op.ClerkID].Seq+1 || (op.Seq == kv.DuplicateTable[op.ClerkID].Seq && kv.DuplicateTable[op.ClerkID].Err == ErrWrongGroup) {
					kv.PerformOperation(op)
				} else if op.Seq < kv.DuplicateTable[op.ClerkID].Seq+1 {

				} else {
					// 对于这种分片分组情况，序列号不是以此加一递增的
					// DPrintf(ServerRole, kv.gid, kv.me, ERROR, "op: %v seq error, expect %v but %v\n", op.Operation, kv.DuplicateTable[op.ClerkID].Seq+1, op.Seq)
				}

				kv.CurrentIndex++
			} else if ch.SnapshotValid {
				r := bytes.NewBuffer(ch.Snapshot)
				d := labgob.NewDecoder(r)
				if d.Decode(&kv.MasterShards) != nil || d.Decode(&kv.DuplicateTable) != nil || d.Decode(&kv.CurrentIndex) != nil || d.Decode(&kv.FirstConfig) != nil {
					DPrintf(ServerRole, kv.gid, kv.me, ERROR, "Decode failed in ApplyLogs\n")
				} else {
					if kv.CurrentIndex != ch.SnapshotIndex {
						DPrintf(ServerRole, kv.gid, kv.me, ERROR, "snapshot index not match\n")
					}
				}
			}

			kv.mu.Unlock()
		default:
			time.Sleep(time.Millisecond * 10)
		}

		kv.cond.Broadcast()
	}
}

func (kv *ShardKV) PerformOperation(op Op) {
	switch op.Operation {
	case "Get":
		{
			shard := key2shard(op.Key)
			if kv.MasterShards[shard].Master != kv.gid {
				kv.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
					Seq: op.Seq,
					Err: ErrWrongGroup,
				}
				return
			}

			value, ok := kv.MasterShards[shard].Subject[op.Key]
			if !ok {
				kv.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
					Seq: op.Seq,
					Err: ErrNoKey,
				}
			} else {
				kv.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
					Seq:   op.Seq,
					Value: value,
					Err:   OK,
				}
			}
		}
	case "Append":
		{
			shard := key2shard(op.Key)
			if kv.MasterShards[shard].Master != kv.gid {
				kv.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
					Seq: op.Seq,
					Err: ErrWrongGroup,
				}
				return
			}

			value, ok := kv.MasterShards[shard].Subject[op.Key]
			if !ok {
				kv.MasterShards[shard].Subject[op.Key] = op.Value

				kv.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
					Seq:   op.Seq,
					Value: kv.MasterShards[shard].Subject[op.Key],
					Err:   OK,
				}
			} else {
				kv.MasterShards[shard].Subject[op.Key] = value + op.Value

				kv.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
					Seq:   op.Seq,
					Value: kv.MasterShards[shard].Subject[op.Key],
					Err:   OK,
				}
			}
		}
	case "Put":
		{
			shard := key2shard(op.Key)
			if kv.MasterShards[shard].Master != kv.gid {
				kv.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
					Seq: op.Seq,
					Err: ErrWrongGroup,
				}
				return
			}

			kv.MasterShards[shard].Subject[op.Key] = op.Value

			kv.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
				Seq:   op.Seq,
				Value: kv.MasterShards[shard].Subject[op.Key],
				Err:   OK,
			}
		}
	case "RecvShards":
		{
			shard := int(op.ClerkID)
			if op.ConfigNum <= kv.MasterShards[shard].ConfigNum {
				kv.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
					Seq: op.Seq,
					Err: OK,
				}
				return
			}

			kv.MasterShards[shard] = struct {
				ConfigNum int
				Master    int
				Servers   []string
				Subject   map[string]string
			}{
				ConfigNum: op.ConfigNum,
				Master:    kv.gid,
				Subject:   op.Shards,
			}

			kv.DuplicateTable[op.ClerkID] = DuplicateTableEntry{
				Seq: op.Seq,
				Err: OK,
			}
		}
	case "Reconfigure":
		{
			config := op.Config

			for shard, gid := range config.Shards {
				tmp := kv.MasterShards[shard]
				if tmp.ConfigNum < config.Num {
					if config.Num == 1 && gid == kv.gid {
						tmp.Master = gid
					} else if tmp.Master == kv.gid {
						if gid == kv.gid {
							tmp.ConfigNum = config.Num
						} else {
							tmp.Master = gid
							tmp.ConfigNum = config.Num
							tmp.Servers = config.Groups[gid]
						}
					}

					kv.MasterShards[shard] = tmp
				}
			}

			if config.Num >= 1 {
				kv.FirstConfig = true
			}

			DPrintf(ServerRole, kv.gid, kv.me, INFO, "Reconfig %v\n", op.Config.Num)
		}
	case "ShardsOver":
		{

			shard := op.ShardNum
			tmp := kv.MasterShards[shard]

			if op.ConfigNum == tmp.ConfigNum {
				tmp.Master = -1
				tmp.Subject = make(map[string]string)

				kv.MasterShards[shard] = tmp
			}
		}
	}
}

func (kv *ShardKV) CheckSnapshot() {
	for {
		kv.mu.Lock()
		if kv.maxraftstate != -1 {
			raftStateSize := kv.persister.RaftStateSize()
			if raftStateSize >= kv.maxraftstate-64 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.MasterShards)
				e.Encode(kv.DuplicateTable)
				e.Encode(kv.CurrentIndex)
				e.Encode(kv.FirstConfig)
				snapshot := w.Bytes()
				kv.rf.Snapshot(kv.CurrentIndex, snapshot)
			}
		}
		kv.mu.Unlock()

		time.Sleep(time.Millisecond * 100)
	}
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

	kv.MasterShards = make(map[int]struct {
		ConfigNum int
		Master    int
		Servers   []string
		Subject   map[string]string
	})

	for i := 0; i < shardctrler.NShards; i++ {
		kv.MasterShards[i] = struct {
			ConfigNum int
			Master    int
			Servers   []string
			Subject   map[string]string
		}{
			ConfigNum: -1,
			Master:    -1,
			Servers:   make([]string, 0),
			Subject:   make(map[string]string),
		}
	}

	kv.DuplicateTable = make(map[int64]DuplicateTableEntry)
	kv.cond = sync.NewCond(&kv.mu)
	kv.CurrentIndex = 0

	kv.persister = persister

	kv.FirstConfig = false

	go kv.ApplyLogs()
	go kv.MonitorConfig()
	go kv.CheckSnapshot()
	go kv.TransferShards()

	return kv
}
