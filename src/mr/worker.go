package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"

var globalWorkerId int

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for i:=0;i<5;i++ {
		registArgs := RegistArgs{}
		registReply := RegistReply{}
		ok := call("Coordinator.Register", &registArgs, &registReply)
		if ok == true && registReply.OK==true {
			globalWorkerId = registReply.WorkerId
			fmt.Println("register success ,workerId:", globalWorkerId )
			break
		} else {
			fmt.Println("register failed")
			time.Sleep(time.Second * 2)
		}
	}
	time.Sleep(time.Second*10)

	// fmt.Println("====================regist finish=================")

	//请求map任务 
	//初始化请求rpc中的workerid
	args := RequestTaskArgs{WorkerId: globalWorkerId}
	reply := []WorkerRequestTask{}
	//请求coordinator分配任务
	ok:= call("Coordinator.WorkerRequestTask", &args, &reply)
	if ok == false {
		fmt.Printf("RequestTask() call failed\n")
	}else{
		fmt.Printf("%v",reply)
		// fmt.Printf("worker get task %s ,taskID %d ,map num %s, reduce Num %s", reply.TaskId,reply.FileName,reply.NMap,reply.NReduce)	
	}

	// todo 执行map任务


	// 报告任务完成状态



	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}


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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
