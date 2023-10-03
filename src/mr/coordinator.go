package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	TaskWaiting TaskState = iota
	TaskRunning
	TaskFinished
)

type JobState int

const (
	MapJob    JobState = iota
	ReduceJob          // map任务都完成了，状态转移为reduce状态
	JobDone
)

type Coordinator struct {
	// Your definitions here.
	nMap      int
	nReduce   int
	files     []string
	taskQueue chan Task

	jobState  JobState // 当前job处于哪一状态
	stateLock sync.Mutex
	stateCond *sync.Cond

	MapProgress    []TaskState // 每个任务的状态。 如果任务的状态是TaskWaiting，则他一定在taskQueue队列中
	ReduceProgress []TaskState
	ProgressLock   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// rpc是并发的, 为每一个连接建立一个goroutine https://blog.csdn.net/benben_2015/article/details/90378698
// worker 申请派发任务
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	for {
		select {
		case task := <-c.taskQueue:
			flag := false

			c.ProgressLock.Lock()
			if task.Type == MapType {
				if c.MapProgress[task.Number] == TaskWaiting {
					c.MapProgress[task.Number] = TaskRunning
					flag = true
				}
			} else if task.Type == ReduceType {
				if c.ReduceProgress[task.Number] == TaskWaiting {
					c.ReduceProgress[task.Number] = TaskRunning
					flag = true
				}
			}
			c.ProgressLock.Unlock()

			if flag {
				reply.Task.Type = task.Type
				reply.Task.Number = task.Number
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				if task.Type == MapType {
					reply.Filename = c.files[task.Number]
				}

				go MonitorTask(c, task)

				return nil
			}
		default:
			c.stateLock.Lock()
			if c.jobState == JobDone {
				c.stateLock.Unlock()
				reply.Task.Type = FinishedType
				return nil
			}
			c.stateCond.Wait()
			c.stateLock.Unlock()
		}
	}
}

// worker 通知任务完成
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	reply.Over = false

	c.ProgressLock.Lock()
	if args.Task.Type == MapType {
		if c.MapProgress[args.Task.Number] != TaskFinished {
			c.MapProgress[args.Task.Number] = TaskFinished
			flag := true
			for _, taskType := range c.MapProgress {
				if taskType != TaskFinished {
					flag = false
					break
				}
			}
			// map任务结束，在队列中加入reduce任务
			if flag {
				for i := 0; i < c.nReduce; i++ {
					c.taskQueue <- Task{ReduceType, i}
				}
				c.stateLock.Lock()
				c.jobState = ReduceJob
				c.stateCond.Broadcast()
				c.stateLock.Unlock()
			}
		}
	} else if args.Task.Type == ReduceType {
		if c.ReduceProgress[args.Task.Number] != TaskFinished {
			c.ReduceProgress[args.Task.Number] = TaskFinished
			flag := true
			for _, taskType := range c.ReduceProgress {
				if taskType != TaskFinished {
					flag = false
					break
				}
			}
			if flag {
				c.stateLock.Lock()
				c.jobState = JobDone
				reply.Over = true
				c.stateCond.Broadcast()
				c.stateLock.Unlock()
			}
		}

	}
	c.ProgressLock.Unlock()
	return nil
}

// 检测每个任务10s后是否结束了
func MonitorTask(c *Coordinator, task Task) {
	time.Sleep(time.Second * 10)
	c.ProgressLock.Lock()
	if task.Type == MapType {
		if c.MapProgress[task.Number] == TaskRunning {
			c.MapProgress[task.Number] = TaskWaiting
			c.taskQueue <- task
		}
	} else if task.Type == ReduceType {
		if c.ReduceProgress[task.Number] == TaskRunning {
			c.ReduceProgress[task.Number] = TaskWaiting
			c.taskQueue <- task
		}
	}
	c.ProgressLock.Unlock()
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.stateLock.Lock()
	if c.jobState == JobDone {
		ret = true
	}
	c.stateLock.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.nReduce = nReduce
	c.nMap = len(files) // 让map任务编号和文件个数相同 (也可以一个map编号对应多个文件, 让c.files变成[][]string即可)
	c.files = files
	c.jobState = MapJob

	c.MapProgress = make([]TaskState, c.nMap)
	for i := 0; i < len(c.MapProgress); i++ {
		c.MapProgress[i] = TaskWaiting
	}
	c.ReduceProgress = make([]TaskState, c.nReduce)
	for i := 0; i < len(c.ReduceProgress); i++ {
		c.ReduceProgress[i] = TaskWaiting
	}
	// 对于每个任务最多在队列中出现一次, 因为只有在他处于TaskRunning状态时，才会重新加入队列
	c.taskQueue = make(chan Task, c.nMap+c.nReduce)

	for i := 0; i < c.nMap; i++ {
		c.taskQueue <- Task{MapType, i}
	}

	c.stateCond = sync.NewCond(&c.stateLock)

	c.server()
	return &c
}
