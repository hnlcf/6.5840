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
	RunStageReady  = 0
	RunStageMap    = 1
	RunStageReduce = 2
	RunStageDone   = 3
)

type WorkerSet struct {
	Lock sync.Mutex
	Set  map[int]bool
}

func (s *WorkerSet) Add(item int) {
	s.Lock.Lock()
	s.Set[item] = true
	s.Lock.Unlock()
}

func (s *WorkerSet) Remove(item int) {
	s.Lock.Lock()
	delete(s.Set, item)
	s.Lock.Unlock()
}

func (s *WorkerSet) Contains(item int) bool {
	return s.Set[item]
}

func (s *WorkerSet) Size() int {
	return len(s.Set)
}

func (s *WorkerSet) IsEmpty() bool {
	return len(s.Set) == 0
}

type TaskManager struct {
	Lock sync.Mutex

	Table       map[string]Task
	WorkRecords map[string]Task
	WaitQueue   chan Task
}

// Return a Task for ask request
func (m *TaskManager) PopTask() (Task, bool) {
	t := Task{}
	is_ok := false

	select {
	case t = <-m.WaitQueue:
		m.Lock.Lock()
		delete(m.Table, t.Id)
		m.WorkRecords[t.Id] = t
		m.Lock.Unlock()

		is_ok = true
	default:
		logger.Warnf("[server]: Chanel is empty now, please retry.")
	}

	return t, is_ok
}

// Confirm delete task record based on report
func (m *TaskManager) ConfirmPop(id string) {
	m.Lock.Lock()
	delete(m.WorkRecords, id)
	m.Lock.Unlock()
}

// Push(or recover) a new task
func (m *TaskManager) PushTask(t Task) {
	m.WaitQueue <- t

	m.Lock.Lock()
	m.Table[t.Id] = t
	delete(m.WorkRecords, t.Id)
	m.Lock.Unlock()
}

func (m *TaskManager) IsEmpty() bool {
	return len(m.Table) == 0 && len(m.WorkRecords) == 0
}

func (m *TaskManager) Size() int {
	return len(m.Table)
}

type Coordinator struct {
	RunStage int

	nMap    int
	nReduce int
	workers WorkerSet

	mapTasks    TaskManager
	reduceTasks TaskManager
}

var logger = GetLogger()

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) InitWorker(args *InitRequest, reply *InitReply) error {
	id := c.workers.Size()

	reply.WokerId = id
	c.workers.Add(id)

	logger.Infof("[server]: Register a new worker %d.", id)

	return nil
}

func (c *Coordinator) AskTask(args *TaskRequest, reply *TaskReply) error {
	logger.Debugf("[server]: Get a new ask from worker %d.", args.WokerId)

	// Finish reduce stage, over.
	if c.mapTasks.IsEmpty() && c.reduceTasks.IsEmpty() && (c.RunStage == RunStageReduce || c.RunStage == RunStageDone) {
		logger.Debugf("[server]: All tasks done.")

		c.RunStage = RunStageDone
		reply.TaskState = TaskStateEnd

		return nil
	}

	// Finish map stage, switch to init reduce stage.
	if c.mapTasks.IsEmpty() && c.reduceTasks.IsEmpty() && c.RunStage == RunStageMap {
		logger.Debugf("[server]: All map tasks done, start reduce stage")

		initReduceStage(c)
		reply.TaskState = TaskStateWait

		return nil
	}

	switch c.RunStage {
	case RunStageMap:
		t, is_ok := c.mapTasks.PopTask()
		if is_ok {
			reply.Task = t
			reply.TaskId = t.Id
			reply.TaskState = TaskStateMap

			logger.Debugf("[server]: Pass map task %s to worker %d.", t.Id, args.WokerId)
		} else {
			reply.TaskState = TaskStateWait
		}

		logger.Debugf("[server]: %d map tasks left.", c.mapTasks.Size())
	case RunStageReduce:
		t, is_ok := c.reduceTasks.PopTask()
		if is_ok {
			reply.Task = t
			reply.TaskId = t.Id
			reply.TaskState = TaskStateReduce

			logger.Debugf("[server]: Pass reduce task %s to worker %d.", t.Id, args.WokerId)
		} else {
			reply.TaskState = TaskStateWait
		}

		logger.Debugf("[server]: %d reduce tasks left.", c.reduceTasks.Size())
	default:
		reply.TaskState = TaskStateEnd

		logger.Warnf("[server]: Illegal running state.")
	}

	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	return nil
}

func (c *Coordinator) ReportTaskResult(args *TaskResult, reply *int) error {
	if args.WorkTask.TaskType == TaskTypeMap {
		if args.ExecStatus == ExecStatusSuccess {
			c.mapTasks.ConfirmPop(args.WorkTask.Id)

			logger.Infof("[server]: Map task %s is already processed by worker %d.", args.WorkTask.Id, args.WokerId)
		} else {
			c.mapTasks.PushTask(args.WorkTask)

			logger.Warnf("[server]: Worker %d failed to process map task %s.", args.WokerId, args.WorkTask.Id)
		}
	}

	if args.WorkTask.TaskType == TaskTypeReduce {
		if args.ExecStatus == ExecStatusSuccess {
			c.reduceTasks.ConfirmPop(args.WorkTask.Id)

			logger.Infof("[server]: Reduce task %s is already processed by worker %d.", args.WorkTask.Id, args.WokerId)
		} else {
			c.reduceTasks.PushTask(args.WorkTask)

			logger.Warnf("[server]: Worker %d failed to process reduce task %s.", args.WokerId, args.WorkTask.Id)
		}
	}

	return nil
}

func (c *Coordinator) AskQuit(args *QuitRequest, reply *QuitReply) error {
	if c.RunStage == RunStageDone && args.AskQuit {
		reply.IsQuit = true

		c.workers.Remove(args.WokerId)

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
	if c.RunStage == RunStageDone && c.workers.IsEmpty() {
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
		RunStage: RunStageReady,
		nMap:     len(files),
		nReduce:  nReduce,
		workers: WorkerSet{
			Lock: sync.Mutex{},
			Set:  make(map[int]bool),
		},
		mapTasks: TaskManager{
			Lock:        sync.Mutex{},
			Table:       make(map[string]Task),
			WorkRecords: make(map[string]Task),
			WaitQueue:   make(chan Task, channelLen),
		},
		reduceTasks: TaskManager{
			Lock:        sync.Mutex{},
			Table:       make(map[string]Task),
			WorkRecords: make(map[string]Task),
			WaitQueue:   make(chan Task, channelLen),
		},
	}

	initMapStage(&c, files)

	logger.Infof("[server]: ===Coordiantor start===")
	c.server()

	return &c
}

func initMapStage(c *Coordinator, files []string) {
	for i, file := range files {
		taskId := generateTaskId("map", i)
		task := Task{
			TaskType:  TaskTypeMap,
			Index:     i,
			Id:        taskId,
			InputFile: file,
		}
		c.mapTasks.PushTask(task)
	}

	c.RunStage = RunStageMap
}

func initReduceStage(c *Coordinator) {
	for i := 0; i < c.nReduce; i++ {
		taskId := generateTaskId("reduce", i)
		task := Task{
			TaskType:  TaskTypeReduce,
			Index:     i,
			Id:        taskId,
			InputFile: "none",
		}
		c.reduceTasks.PushTask(task)
	}

	c.RunStage = RunStageReduce
}

func generateTaskId(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}
