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
	ExecStatusSuccess = 0
	ExecStatusFail    = 1
)

const (
	TaskStateWait   = 0
	TaskStateMap    = 1
	TaskStateReduce = 2
	TaskStateEnd    = 3
)

type InitRequest struct {
}

type InitReply struct {
	WokerId int
}

type QuitRequest struct {
	WokerId int
	AskQuit bool
}

type QuitReply struct {
	IsQuit bool
}

type Task struct {
	TaskType  int
	Index     int
	Id        string
	InputFile string
}

type TaskRequest struct {
	WokerId int
}

type TaskReply struct {
	Task      Task
	TaskId    string
	TaskState int
	NReduce   int
	NMap      int
}

type TaskResult struct {
	WokerId    int
	WorkTask   Task
	ExecStatus int
	Output     []string
}

// Add your RPC definitions here.
func CallInitWorker() (bool, InitReply) {
	args := InitRequest{}
	reply := InitReply{}

	is_ok := call("Coordinator.InitWorker", &args, &reply)

	return is_ok, reply
}

func CallAskTask(wokerId int) TaskReply {
	args := TaskRequest{
		WokerId: wokerId,
	}
	reply := TaskReply{
		TaskState: TaskStateWait,
	}

	call("Coordinator.AskTask", &args, &reply)

	return reply
}

func CallReportTaskResult(res TaskResult) bool {
	args := res
	reply := 0

	is_ok := call("Coordinator.ReportTaskResult", &args, &reply)

	return is_ok
}
func CallAskQuit(workerId int) (bool, QuitReply) {
	args := QuitRequest{
		WokerId: workerId,
		AskQuit: true,
	}
	reply := QuitReply{
		IsQuit: false,
	}

	is_ok := call("Coordinator.AskQuit", &args, &reply)

	return is_ok, reply
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
