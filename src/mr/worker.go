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
	for _, filename := range reply.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	// 创建nReduce个文件, 设置为json格式
	fileList := make([]*os.File, reply.NReduce) // 临时文件
	jsonList := make([]*json.Encoder, reply.NReduce)
	outFiles := []string{}    // 中间文件名
	pathname, _ := os.Getwd() // 获取当前路径（把文件输出到main路径下）
	for i := 0; i < reply.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", reply.Task.Number, i)
		outFiles = append(outFiles, oname)
		var err error
		fileList[i], err = ioutil.TempFile(pathname, oname) // 在main路径下生成临时文件
		if err != nil {
			log.Fatalf("cannot create tempfile %v", oname)
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

	// 原子的改名
	for i := 0; i < reply.NReduce; i++ {
		err := os.Rename(fileList[i].Name(), pathname+"/"+outFiles[i])
		if err != nil {
			fmt.Printf("rename failed! %s -> %s\n", fileList[i].Name(), pathname+"/"+outFiles[i])
			defer os.Remove(fileList[i].Name()) // 把改名失败的文件删除
		} else {
			// fmt.Printf("rename successfully! %s -> %s\n", fileList[i].Name(), pathname+"/"+outFiles[i])
		}
	}
	// 通知任务完成
	taskDoneArgs := TaskDoneArgs{reply.Task, outFiles}
	taskDoneReply := TaskDoneReply{}
	CallTaskDone(&taskDoneArgs, &taskDoneReply)

}

func ReduceWorker(reducef func(string, []string) string, reply *AssignTaskReply) bool {
	// 把对应于此reduce任务的中间文件加载出来
	intermediate := []KeyValue{}
	for _, filename := range reply.Filenames {
		// fmt.Printf("%d reduce: %s\n", reply.Task.Number, filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
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

	pathname, _ := os.Getwd() // 获取当前路径（把文件输出到main路径下）
	oname := fmt.Sprintf("mr-out-%d", reply.Task.Number)
	ofile, err := ioutil.TempFile(pathname, oname)
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

	// 原子的改名
	err = os.Rename(ofile.Name(), pathname+"/"+oname)
	if err != nil {
		fmt.Printf("rename failed! %s -> %s\n", ofile.Name(), pathname+"/"+oname)
		defer os.Remove(ofile.Name()) // 改名失败 删除临时文件
	} else {
		// fmt.Printf("rename successfully! %s -> %s\n", ofile.Name(), pathname+"/"+oname)
	}

	taskDoneArgs := TaskDoneArgs{reply.Task, []string{}}
	taskDoneReply := TaskDoneReply{}
	CallTaskDone(&taskDoneArgs, &taskDoneReply)
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
		var s string
		if reply.Task.Type == MapType {
			s = "Map"
		} else if reply.Task.Type == ReduceType {
			s = "Reduce"
		} else {
			s = "all finished"
		}
		fmt.Printf("get the task : %s %d\n", s, reply.Task.Number)
	} else {
		fmt.Printf("call AssignTask failed！\n")
		return false
	}
	return true
}

func CallTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) {
	call("Coordinator.TaskDone", args, reply)

	var s string
	if args.Task.Type == MapType {
		s = "Map"
	} else if args.Task.Type == ReduceType {
		s = "Reduce"
	}
	fmt.Printf("TaskDone : %s %d\n", s, args.Task.Number)
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
