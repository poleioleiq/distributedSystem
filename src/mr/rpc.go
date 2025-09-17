package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type RegistArgs struct{
	WorkerId int
}
type RegistReply struct{
	OK bool
	WorkerId int
}

type RequestTaskArgs struct {
	WorkerId int
}

type WorkerRequestTask struct {
	TaskType   string // "map", "reduce", "wait", "done"
	TaskId     int
	FileName   string
}

// RPC报告任务完成的参数和回复结构体
type ReportTaskDoneArgs struct {
	WorkerId   int
	TaskType   string // "map" or "reduce"
	TaskId     int
}

type ReportTaskDoneReply struct {
	OK bool
}


type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
