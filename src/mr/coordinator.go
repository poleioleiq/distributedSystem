package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync/atomic"
import "fmt"

type TaskStatus int

const (
	MapTaskStatus_Idle TaskStatus = iota
	MapTaskStatus_Running
	MapTaskStatus_Finished
	MapTaskStatus_failed
)

const (
	ReduceTaskStatus_Idle TaskStatus = iota
	ReduceTaskStatus_Running
	ReduceTaskStatus_Finished
	ReduceTaskStatus_failed
)

type Coordinator struct {
	// Your definitions here.

	// map task
	mapTask []MapTask
	mapTaskNum int
	reduceTask []ReduceTask
	reduceTaskNum int

	//原子类型
	remainMapTaskNum int32 
	remainReduceTaskNum int32
	
}

type MapTask struct {
	FileName string
	TaskId int
	Status TaskStatus
}

type ReduceTask struct {
	FileName string
	TaskId int
	Status TaskStatus
}

// worker方法，用于创建处理任务的协程
func (c *Coordinator) worker(workerId int) {
	// 根据workerId分配任务
	// 如果workerId小于mapTaskNum，则处理对应的map任务
	if workerId < len(c.mapTask) {
		// 这里应该实现具体的map任务处理逻辑
		// 例如：读取文件、执行map函数、生成中间文件等
		// 由于这是一个简化实现，我们只打印信息
		fmt.Printf("Worker %d processing map task %d: %s\n", workerId, workerId, c.mapTask[workerId].FileName)
		// 处理完成后更新任务状态
		c.mapTask[workerId].Status = MapTaskStatus_Finished
		atomic.AddInt32(&c.remainMapTaskNum, -1)
	} else if workerId < len(c.mapTask)+len(c.reduceTask) {
		// 处理reduce任务
		reduceId := workerId - len(c.mapTask)
		fmt.Printf("Worker %d processing reduce task %d\n", workerId, reduceId)
		// 处理完成后更新任务状态
		c.reduceTask[reduceId].Status = ReduceTaskStatus_Finished
		atomic.AddInt32(&c.remainReduceTaskNum, -1)
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// 需要两个状态，map任务结束，reduce任务结束，
	// 即map结束任务数量等于map任务数量，reduce结束任务数量等于reduce任务数量
	//原子检查remainMapTasknum和remainReduceTasknum是否都为零，如果都为零则返回true
	//否则返回false
	ret = atomic.CompareAndSwapInt32(&c.remainMapTaskNum, 0, 0) && atomic.CompareAndSwapInt32(&c.remainReduceTaskNum, 0, 0)
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 初始化任务分片数目，初始化剩余map数、map任务状态、
	// 剩余reduce数、reduce任务状态
	c.mapTaskNum = len(files)
	c.reduceTaskNum=nReduce
	c.remainMapTaskNum=int32(len(files))
	c.remainReduceTaskNum=int32(nReduce)
	c.mapTask = make([]MapTask,len(files))
	c.reduceTask = make([]ReduceTask,nReduce)

	for i:=0;i<len(files);i++{
		c.mapTask[i].FileName=files[i]
		c.mapTask[i].TaskId=i
		c.mapTask[i].Status=MapTaskStatus_Idle
	}
	
	for i:=0;i<nReduce;i++{
		c.reduceTask[i].FileName=""
		c.reduceTask[i].TaskId=i
		c.reduceTask[i].Status=ReduceTaskStatus_Idle
	}
	
	//创建nredeuce+len(files)个worker协程
	for i:=0;i<nReduce+len(files);i++{
		go c.worker(i)
	}


	c.server()
	return &c
}