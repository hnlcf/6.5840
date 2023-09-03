package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var workerId = 0

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getFileContents(task Task) (string, string) {
	inputFile := task.InputFile

	file, err := os.Open(inputFile)
	if err != nil {
		logger.Warnf("[worker %d]: Cannot open %v.", workerId, inputFile)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		logger.Warnf("[worker %d]: Cannot read %v.", workerId, inputFile)
	}
	file.Close()

	logger.Debugf("[worker %d]: Read file contents from %s.", workerId, inputFile)
	return inputFile, string(content)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	is_init, initReply := CallInitWorker()
	if is_init {
		workerId = initReply.WokerId
	}

WorkLoop:
	for {
		taskReply := CallAskTask(workerId)

		switch taskReply.TaskState {
		case TaskStateWait:
			time.Sleep(time.Duration(time.Second * 5))
		case TaskStateMap:
			logger.Debugf("[worker %d]: Get a new task %s with stage %d.", workerId, taskReply.TaskId, taskReply.TaskState)

			taskResult := processMapTask(taskReply.TaskId, taskReply.Task, mapf)
			CallReportTaskResult(taskResult)
		case TaskStateReduce:
			logger.Debugf("[worker %d]: Get a new task %s with stage %d.", workerId, taskReply.TaskId, taskReply.TaskState)

			taskResult := processReduceTask(taskReply.TaskId, taskReply.Task, reducef)
			CallReportTaskResult(taskResult)
		case TaskStateEnd:
			logger.Infof("[worker %d]: Recieve exit signal from server.", workerId)

			break WorkLoop
		default:
			logger.Errorf("[worker %d]: Unknown task state %d", workerId, taskReply.TaskState)

			panic("Panic Now!")
		}
	}

	CallAskQuit(workerId)
}

func processMapTask(taskId string, task Task, mapf func(string, string) []KeyValue) TaskResult {
	file, content := getFileContents(task)
	kva := mapf(file, content)

	output := fmt.Sprintf("mr-tmp-%d", task.Index)
	outputFile, _ := os.Create(output)
	for i := 0; i < len(kva); i++ {
		kv := kva[i]
		fmt.Fprintf(outputFile, "%s,%s\n", kv.Key, kv.Value)
	}
	outputFile.Close()

	res := TaskResult{
		WokerId:    workerId,
		WorkTask:   task,
		ExecStatus: ExecStatusSuccess,
		Output:     output,
	}
	return res
}

func processReduceTask(taskId string, task Task, reducef func(string, []string) string) TaskResult {
	_, content := getFileContents(task)

	lines := strings.Split(content, "\n")

	intermediate := []KeyValue{}
	for _, l := range lines {
		if len(l) != 0 {
			parts := strings.Split(l, ",")

			kv := KeyValue{Key: parts[0], Value: parts[1]}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	output := fmt.Sprintf("mr-out-%d", task.Index)
	outputFile, _ := os.Create(output)

	result := make(map[string]string)

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
		reduceVal := reducef(intermediate[i].Key, values)

		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, reduceVal)

		result[intermediate[i].Key] = reduceVal

		i = j
	}

	outputFile.Close()

	res := TaskResult{
		WokerId:    workerId,
		WorkTask:   task,
		ExecStatus: ExecStatusSuccess,
		Output:     output,
	}

	return res
}
