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

type Set map[int]bool

func (s Set) Add(item int) {
	s[item] = true
}

func (s Set) Remove(item int) {
	delete(s, item)
}

func (s Set) Contains(item int) bool {
	return s[item]
}

func (s Set) IsEmpty() bool {
	return len(s) == 0
}

type Coordinator struct {
	RunStage int

	lock          sync.Mutex
	nMap          int
	nReduce       int
	workers       Set
	tasks         map[string]Task
	availableTask chan Task
}

var logger = GetLogger()

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) InitWorker(args *InitRequest, reply *InitReply) error {
	id := len(c.workers)

	reply.WokerId = id
	c.workers[id] = true

	logger.Infof("[server]: Register a new worker %d.", id)

	return nil
}

func (c *Coordinator) AskTask(args *TaskRequest, reply *TaskReply) error {
	logger.Debugf("[server]: Get a new ask from worker %d.", args.WokerId)

	if len(c.tasks) == 0 {
		logger.Debugf("[server]: All tasks done.")

		c.RunStage = RunStageDone

		reply.TaskState = TaskStateEnd
		return nil
	} else {
		select {
		case t := <-c.availableTask:
			reply.Task = t
			reply.TaskId = t.Id
			if t.TaskType == TaskTypeMap {
				reply.TaskState = TaskStateMap
			} else {
				reply.TaskState = TaskStateReduce
			}

			c.lock.Lock()
			delete(c.tasks, t.Id)
			c.lock.Unlock()

			logger.Infof("[server]: Pass task %s to worker %d.", t.Id, args.WokerId)

			c.RunStage = RunStageProcess

		default:
			logger.Warnf("[server]: Chanel is empty now, please retry.")

			c.RunStage = RunStageProcess

			reply.TaskState = TaskStateWait
		}

		logger.Debugf("[server]: %d tasks left.", len(c.tasks))

		return nil
	}

}

func (c *Coordinator) ReportTaskResult(args *TaskResult, reply *int) error {
	if args.ExecStatus == ExecStatusSuccess {
		logger.Infof("[server]: Task %s is already processed by worker %d.", args.WorkTask.Id, args.WokerId)

		if args.WorkTask.TaskType == TaskTypeMap {
			index := len(c.tasks)
			id := generateTaskId("reduce", index)
			reduceTask := Task{
				TaskType:  TaskTypeReduce,
				Index:     index,
				Id:        id,
				InputFile: args.Output,
			}

			c.lock.Lock()
			c.tasks[id] = reduceTask
			c.lock.Unlock()

			c.availableTask <- reduceTask

			logger.Infof("[server]: Create new reduce task %s based on map task %s.", generateTaskId(reduceTask.InputFile, reduceTask.Index), args.WorkTask.Id)
		}
	} else {
		c.lock.Lock()
		c.tasks[args.WorkTask.Id] = args.WorkTask
		c.lock.Unlock()

		c.availableTask <- c.tasks[args.WorkTask.Id]

		logger.Warnf("[server]: Worker %d failed to process task %s.", args.WokerId, args.WorkTask.Id)
	}

	if len(c.tasks) == 0 {
		*reply = RunStageDone
	} else {
		*reply = RunStageProcess
	}

	return nil
}

func (c *Coordinator) AskQuit(args *QuitRequest, reply *QuitReply) error {
	if c.RunStage == RunStageDone && args.AskQuit {
		reply.IsQuit = true

		c.lock.Lock()
		c.workers.Remove(args.WokerId)
		c.lock.Unlock()

		logger.Infof("[server]: worker %d is quit now.", args.WokerId)
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
	logger.Debug("[server]: Coordinaltor work loop.")

	ret := false

	// Your code here.
	if len(c.tasks) == 0 {
		ret = true
		logger.Debug("[server]: Coordinaltor Done!")
	}

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
		workers:       make(Set),
		tasks:         make(map[string]Task),
		availableTask: make(chan Task, channelLen),
	}

	c.RunStage = RunStageProcess
	for i, file := range files {
		taskId := generateTaskId("map", i)
		task := Task{
			TaskType:  TaskTypeMap,
			Index:     i,
			Id:        taskId,
			InputFile: file,
		}
		c.tasks[taskId] = task
		c.availableTask <- task
	}

	logger.Infof("[server]: ===Coordiantor start===")
	c.server()

	return &c
}

func generateTaskId(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}
