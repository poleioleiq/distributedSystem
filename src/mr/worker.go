package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "sort"
import "strconv"
import "sync"
import "path/filepath"

var globalWorkerId int
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	time.Sleep(time.Second*3)

	// fmt.Println("====================regist finish=================")
	ch := make(chan []KeyValue, 100000) // 使用缓冲channel避免阻塞

	//请求map任务 ，初始化请求rpc中的workerid
	args := RequestTaskArgs{WorkerId: globalWorkerId}
	reply := []WorkerRequestTask{}
	//请求coordinator分配任务
	ok := call("Coordinator.WorkerRequestTask", &args, &reply)
	if ok == false {
		fmt.Printf("RequestTask() call failed\n")
	} else {
		fmt.Printf("%v", reply)
	}
	//启动多个协程，读取所有文件，每个协程计算每个文件中的kv，最后再聚合
	var wg sync.WaitGroup
	for i := 0; i < len(reply); i++ {
		wg.Add(1)
		go func(task WorkerRequestTask) {
			defer wg.Done()
			fmt.Printf("workerId = %v,task = %v\n",globalWorkerId,task)
			compute(task, mapf, reducef, ch)
		}(reply[i])
	}
	
	wg.Wait()
	close(ch)


	// time.Sleep(time.Second * 10)

	// 收集所有中间结果
	var intermediate []KeyValue
	for kv := range ch {
		intermediate = append(intermediate, kv...)
		// fmt.Printf("%v",intermediate)
	}
	
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(globalWorkerId)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Cannot create output file %s: %v", oname, err)
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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	// 所有任务完成，不需要额外睡眠






	// todo 执行map任务


	// 报告任务完成状态


}
func compute(R WorkerRequestTask,mapf func(string, string) []KeyValue,reducef func(string, []string) string, ch chan<- []KeyValue){
	intermediate := []KeyValue{}
	// for _, R := range reply {
	file, err := os.Open(R.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", R.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", R.FileName)
	}
	file.Close()
	kva := mapf(R.FileName, string(content))
	intermediate = append(intermediate, kva...)
	// }

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//
	//创建一个channel传递intermediate到一个合并goruntine进行sort

	sort.Sort(ByKey(intermediate))
	baseFileName := filepath.Base(R.FileName)
	oname := "mrr-out-"+strconv.Itoa(globalWorkerId)+ "-" +  baseFileName
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Cannot create output file %s: %v", oname, err)
	}

	ch <- intermediate

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		//创建一个channel传递intermediate[i].Key/output到一个合并goruntine进行sort
		// ch <- KeyValue{intermediate[i].Key, output}


		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
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
