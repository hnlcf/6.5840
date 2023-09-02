package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

const (
	TaskTypeMap    = 0
	TaskTypeReduce = 1
)

const (
	WorkerStateIdle   = 0
	WorkerStateMap    = 1
	WorkerStateReduce = 2
)

const (
	ExecStatusSuccess = 0
	ExecStatusFail    = 1
)

type Task struct {
	TaskType  int
	Index     int
	InputFile string
}

type TaskRequest struct {
	WokerId     int
	WorkerState int
}

type TaskReply struct {
	Task        Task
	TaskId      string
	ServerStage int
}

type TaskResult struct {
	WokerId int

	TaskId    string
	TaskIndex int
	TaskType  int

	ExecStatus int
	Output     string
}

// Add your RPC definitions here.
func CallAskTask() (TaskReply, bool) {
	args := TaskRequest{
		WokerId:     0,
		WorkerState: WorkerStateIdle,
	}
	reply := TaskReply{}

	is_ok := call("Coordinator.AskTask", &args, &reply)

	return reply, is_ok
}

func CallReportTaskResult(res TaskResult) bool {
	args := res
	reply := 0

	is_ok := call("Coordinator.ReportTaskResult", &args, &reply)

	return is_ok
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	socketName := coordinatorSock()

	c, err := rpc.DialHTTP("unix", socketName)
	if err != nil {
		log.Fatal("[worker]: dialing:", err)
	}

	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	} else {
		fmt.Println(err)
		return false
	}
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
