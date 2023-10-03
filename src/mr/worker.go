package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := AssignTaskReply{}
		ok := CallAssignTask(&reply)

		if !ok { // 出现错误
			return
		}
		finished := false

		if reply.Task.Type == MapType {
			MapWorker(mapf, &reply)
		} else if reply.Task.Type == ReduceType {
			finished = ReduceWorker(reducef, &reply)
		} else { // FinishedType
			finished = true
		}
		if finished {
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func MapWorker(mapf func(string, string) []KeyValue, reply *AssignTaskReply) {
	// 对传入的filename文件做map操作，结果放在intermediate中
	intermediate := []KeyValue{}
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()
	kva := mapf(reply.Filename, string(content))
	intermediate = append(intermediate, kva...)
	// 创建nReduce个文件, 设置为json格式
	fileList := make([]*os.File, reply.NReduce)
	jsonList := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", reply.Task.Number, i)
		fileList[i], err = os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		} else {
			jsonList[i] = json.NewEncoder(fileList[i])
			defer fileList[i].Close()
		}
	}
	// 把结果写到文件中
	for i := 0; i < len(intermediate); i++ {
		y := ihash(intermediate[i].Key) % reply.NReduce
		err := jsonList[y].Encode(intermediate[i])
		if err != nil {
			log.Fatalf("cannot encode %s", fmt.Sprintf("mr-%d-%d", reply.Task.Number, y))
		}
	}
	// 通知任务完成
	CallTaskDone(new(TaskDoneReply))

}

func ReduceWorker(reducef func(string, []string) string, reply *AssignTaskReply) bool {
	// 把对应于此reduce任务的中间文件加载出来
	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.Task.Number)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	// 排序
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.Task.Number)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	taskDoneReply := TaskDoneReply{}
	CallTaskDone(&taskDoneReply)
	return taskDoneReply.Over
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallAssignTask(reply *AssignTaskReply) bool {
	args := AssignTaskArgs{}

	ok := call("Coordinator.AssignTask", &args, reply)
	if ok {
		fmt.Printf("get the task : ")
		if reply.Task.Type == MapType {
			fmt.Printf("Map\n")
		} else if reply.Task.Type == ReduceType {
			fmt.Printf("Reduce\n")
		} else {
			fmt.Printf("all finished\n")
		}
	} else {
		fmt.Printf("call AssignTask failed！\n")
		return false
	}
	return true
}

func CallTaskDone(reply *TaskDoneReply) {
	args := TaskDoneArgs{}
	call("Coordinator.TaskDone", &args, reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
