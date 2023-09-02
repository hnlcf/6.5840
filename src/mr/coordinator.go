package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	RunStageReady  = 0
	RunStageMap    = 1
	RunStageReduce = 2
	RunStageDone   = 3
)

type Coordinator struct {
	lock          sync.Mutex
	runStage      int
	nMap          int
	nReduce       int
	tasks         map[string]Task
	availableTask chan Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskMapTask(args *TaskRequest, reply *TaskReply) error {
	if args.WorkerState == WorkerStateIdle {
		t := <-c.availableTask
		id := generateTaskId(t.InputFile, t.Index)
		reply.Task = t
		reply.TaskId = id

		log.Printf("[server]: Pass task %s to worker %d", id, args.WokerId)
	} else {
		log.Fatalf("[server]: worker %d is busy with %d", args.WokerId, args.WorkerState)
	}

	return nil
}

func (c *Coordinator) ReportTaskResult(args *TaskResult, reply *TaskReply) error {
	if args.ExecStatus == ExecStatusSuccess {
		log.Printf("[worker %d]: Task %s is already processed.", args.WokerId, args.TaskId)
	} else {
		log.Fatalf("[worker %d]: Failed to process task %s", args.WokerId, args.TaskId)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("[server]: listen error:", e)
	}

	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// TODO

	return ret
}

// MakeCoordinator
//
// Create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	channelLen := int(math.Max(float64(len(files)), float64(nReduce)))
	c := Coordinator{
		lock:          sync.Mutex{},
		runStage:      RunStageReady,
		nMap:          len(files),
		nReduce:       nReduce,
		tasks:         make(map[string]Task),
		availableTask: make(chan Task, channelLen),
	}

	c.runStage = RunStageMap
	for i, file := range files {
		task := Task{
			TaskType:  TaskTypeMap,
			Index:     i,
			InputFile: file,
		}
		c.tasks[generateTaskId(file, i)] = task
		c.availableTask <- task
	}

	log.Printf("[server]: ===Coordiantor start===\n")
	c.server()

	return &c
}

func generateTaskId(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}
