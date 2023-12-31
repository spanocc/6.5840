package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkID int64
	seq     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clerkID = nrand()
	ck.seq = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClerkID = ck.clerkID
	args.Seq = ck.seq
	args.Num = num
	ck.seq++

	DPrintf(ClerkRole, int(ck.clerkID), INFO, "query rpc, args: %v\n", args)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf(ClerkRole, int(ck.clerkID), INFO, "query rpc return, args: %v, reply: %v\n", args, reply)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.ClerkID = ck.clerkID
	args.Seq = ck.seq
	args.Servers = servers
	ck.seq++

	DPrintf(ClerkRole, int(ck.clerkID), INFO, "join rpc, args: %v\n", args)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf(ClerkRole, int(ck.clerkID), INFO, "join rpc return, args: %v, reply: %v\n", args, reply)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.ClerkID = ck.clerkID
	args.Seq = ck.seq
	args.GIDs = gids
	ck.seq++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.ClerkID = ck.clerkID
	args.Seq = ck.seq
	args.Shard = shard
	args.GID = gid
	ck.seq++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
