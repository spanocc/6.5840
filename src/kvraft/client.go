package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkID    int64
	seq        int64
	lastLeader int
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
	// You'll have to add code here.
	ck.clerkID = nrand()
	ck.seq = 1
	ck.lastLeader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	value := ""
	for {
		args := GetArgs{}
		args.Key = key
		args.ClerkID = ck.clerkID
		args.seq = ck.seq
		ck.seq++
		reply := GetReply{}
		ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {

		} else if reply.Err == OK {
			value = reply.Value
			break
		} else {
			DPrintf("ErrNoKey: key = %v\n", key)
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
	}
	return value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	for {
		args := PutAppendArgs{}
		args.Key = key
		args.Value = value
		args.Op = op
		args.ClerkID = ck.clerkID
		args.seq = ck.seq
		ck.seq++
		reply := PutAppendReply{}
		ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {

		} else if reply.Err == OK {
			break
		} else { // ErrNoKey
			DPrintf("should not reach here\n")
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
