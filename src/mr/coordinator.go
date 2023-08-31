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

const (
	MapTask    = 0
	ReduceTask = 1
)

type Task struct {
	taskType  int
	index     int
	inputFile string
}

type Coordinator struct {
	lock          sync.Mutex
	stage         int
	nMap          int
	nReduce       int
	tasks         map[string]Task
	availableTask chan Task
}

// Your code here -- RPC handlers for the worker to call.
// TODO

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
		log.Fatal("listen error:", e)
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
		stage:         RunStageReady,
		nMap:          len(files),
		nReduce:       nReduce,
		tasks:         make(map[string]Task),
		availableTask: make(chan Task, channelLen),
	}

	c.stage = RunStageMap
	for i, file := range files {
		task := Task{
			taskType:  MapTask,
			index:     i,
			inputFile: file,
		}
		c.tasks[generateTaskId(file, i)] = task
		c.availableTask <- task
	}

	log.Printf("===Coordiantor start===\n")
	c.server()

	return &c
}

func generateTaskId(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}
