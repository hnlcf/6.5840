package mr

import (
	"fmt"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	RunStageReady   = 0
	RunStageProcess = 1
	RunStageDone    = 2
)

type Coordinator struct {
	lock          sync.Mutex
	RunStage      int
	nMap          int
	nReduce       int
	tasks         map[string]Task
	availableTask chan Task
}

var logger = GetLogger()

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(args *TaskRequest, reply *TaskReply) error {
	if len(c.tasks) == 0 {
		c.RunStage = RunStageDone
	} else {
		if args.WorkerState == WorkerStateIdle {
			t := <-c.availableTask
			id := generateTaskId(t.InputFile, t.Index)
			reply.Task = t
			reply.TaskId = id

			logger.Infof("[server]: Pass task %s to worker %d.", id, args.WokerId)
		} else {
			logger.Warnf("[server]: worker %d is busy with %d.", args.WokerId, args.WorkerState)
		}
	}

	reply.ServerStage = c.RunStage

	return nil
}

func (c *Coordinator) ReportTaskResult(args *TaskResult, reply *int) error {
	if args.ExecStatus == ExecStatusSuccess {
		c.lock.Lock()
		delete(c.tasks, args.TaskId)
		c.lock.Unlock()

		logger.Infof("[server]: Task %s is already processed by worker %d.", args.TaskId, args.WokerId)
	} else {
		c.availableTask <- c.tasks[args.TaskId]

		logger.Warnf("[server]: Worker %d failed to process task %s.", args.WokerId, args.TaskId)
	}

	if args.TaskType == TaskTypeMap {
		reduceTask := Task{
			TaskType:  TaskTypeReduce,
			Index:     args.TaskIndex,
			InputFile: args.Output,
		}
		c.lock.Lock()
		c.tasks[generateTaskId(reduceTask.InputFile, reduceTask.Index)] = reduceTask
		c.lock.Unlock()

		c.availableTask <- reduceTask
	}

	if len(c.tasks) == 0 {
		*reply = RunStageDone
	} else {
		*reply = RunStageProcess
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
		logger.Error("[server]: listen error:", e)
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
		RunStage:      RunStageReady,
		nMap:          len(files),
		nReduce:       nReduce,
		tasks:         make(map[string]Task),
		availableTask: make(chan Task, channelLen),
	}

	c.RunStage = RunStageProcess
	for i, file := range files {
		task := Task{
			TaskType:  TaskTypeMap,
			Index:     i,
			InputFile: file,
		}
		c.tasks[generateTaskId(file, i)] = task
		c.availableTask <- task
	}

	logger.Infof("[server]: ===Coordiantor start===")
	c.server()

	return &c
}

func generateTaskId(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}
