package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
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

	// 保存Map生成的中间文件的名字，由master来保存中间文件名，并传递给reduce)
	intermediateFiles [][]string
	// 由读写锁保证安全，map阶段写， reduce阶段只读
	rwlock sync.RWMutex

	jobState  JobState // 当前job处于哪一状态
	stateLock sync.Mutex
	stateCond *sync.Cond

	MapProgress    []TaskState // 每个任务的状态。 如果任务的状态是TaskWaiting，则他一定在taskQueue队列中
	ReduceProgress []TaskState
	ProgressLock   sync.Mutex // 同一个锁 锁两个队列
}

// Your code here -- RPC handlers for the worker to call.

// rpc是并发的, 为每一个连接建立一个goroutine https://blog.csdn.net/benben_2015/article/details/90378698
// worker 申请派发任务
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	for {
		select {
		case task := <-c.taskQueue:
			flag := false
			// 修改任务状态
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
			// 派发任务
			if flag {
				reply.Task.Type = task.Type
				reply.Task.Number = task.Number
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				if task.Type == MapType {
					// c.files一经初始化之后就是只读的，不需要上锁，而且可以直接用引用
					reply.Filenames = c.files[task.Number : task.Number+1]
				} else if task.Type == ReduceType {
					// 获取中间文件名需要上读锁，不能用引用，应该用深拷贝 （其实reduce阶段都是读，但为了安全，还是上锁吧）
					c.rwlock.RLock()
					reply.Filenames = make([]string, len(c.intermediateFiles[task.Number]))
					copy(reply.Filenames, c.intermediateFiles[reply.Task.Number])
					c.rwlock.RUnlock()
				}
				// 监视任务10s
				go MonitorTask(c, task)

				return nil
			}
		default:
			// 任务队列中没有任务，且job还没结束，那就在条件变量上阻塞，等通知
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
		// 只有任务状态第一次变为Finished时才要执行一些操作
		if c.MapProgress[args.Task.Number] != TaskFinished {
			c.MapProgress[args.Task.Number] = TaskFinished
			// 保存map产生的临时文件名
			c.rwlock.Lock()
			for _, filename := range args.Filenames {
				var numstr string
				for i := len(filename) - 1; i >= 0; i-- {
					if filename[i] >= '0' && filename[i] <= '9' {
						numstr = string(filename[i]) + numstr
					} else {
						break
					}
				}
				idx, err := strconv.Atoi(numstr)
				if err != nil {
					fmt.Printf("atoi failed %s %s\n", filename, numstr)
				}
				c.intermediateFiles[idx] = append(c.intermediateFiles[idx], filename)
			}
			c.rwlock.Unlock()
			// 判断map任务是否结束，如果结束，转至reduce状态
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
			fmt.Printf("Map %d timeout\n", task.Number)
			c.MapProgress[task.Number] = TaskWaiting
			c.taskQueue <- task     // 应当确保此时channel不会阻塞(通过在初始化时设置足够的channel容量)，以防在上锁时等待通道，发生死锁
			c.stateCond.Broadcast() // 唤醒其他goroutine处理task
		}
	} else if task.Type == ReduceType {
		if c.ReduceProgress[task.Number] == TaskRunning {
			fmt.Printf("Reduce %d timeout\n", task.Number)
			c.ReduceProgress[task.Number] = TaskWaiting
			c.taskQueue <- task
			c.stateCond.Broadcast() // 唤醒其他goroutine处理task
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

	c.intermediateFiles = make([][]string, c.nReduce)

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
