package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	MapType TaskType = iota
	ReduceType
	FinishedType
)

type Task struct {
	Type   TaskType
	Number int
}

type AssignTaskArgs struct {
}

type AssignTaskReply struct {
	Task      Task
	NMap      int
	NReduce   int
	Filenames []string // 对于map来说是输入文件的名字， 对于reduce来说是中间文件名字
}

type TaskDoneArgs struct {
	Task      Task
	Filenames []string
}

type TaskDoneReply struct {
	Over bool // 整个job是否已经结束 (其实有了FinishedType,就不需要这个over了,如果没有任务AssignTask会返回FinishedType)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
