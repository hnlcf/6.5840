package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"strings"
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
		logger.Warnf("cannot open %v", inputFile)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		logger.Warnf("cannot read %v", inputFile)
	}
	file.Close()

	logger.Debugf("Read file contents from %s", inputFile)
	return inputFile, string(content)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// TODO
	// 1. get MapTask from task channel
	// 2. read file content
	// 3. call `mapf` and write result to tmp file
	// 4. create ReduceTask and read tmp file
	// 5. call `reducef` and write result to output file

	for {
		is_get_task := false
		reply := TaskReply{}
		for !is_get_task {
			reply, is_get_task = CallAskTask()
		}

		if reply.ServerStage == RunStageDone {
			logger.Infof("[worker %d]: recieve exit signal from server", workerId)
			break
		}

		logger.Infof("[worker %d]: get task %s from server", workerId, reply.TaskId)

		is_report := false
		if reply.Task.TaskType == TaskTypeMap {
			taskResult := processMapTask(reply.TaskId, reply.Task, mapf)
			is_report = CallReportTaskResult(taskResult)
		}
		if reply.Task.TaskType == TaskTypeReduce {
			taskResult := processReduceTask(reply.TaskId, reply.Task, reducef)
			is_report = CallReportTaskResult(taskResult)
		}

		if is_report {
			logger.Infof("[worker %d]: report result of task %s to server", workerId, reply.TaskId)
		} else {
			logger.Warnf("[worker %d]: failed to report result of task %s to server", workerId, reply.TaskId)
		}

	}
}

func processMapTask(taskId string, task Task, mapf func(string, string) []KeyValue) TaskResult {
	file, content := getFileContents(task)
	kva := mapf(file, content)

	output := fmt.Sprintf("middle-%s", taskId)
	outputFile, _ := os.Create(output)
	for i := 0; i < len(kva); i++ {
		kv := kva[i]
		fmt.Fprintf(outputFile, "%s,%s\n", kv.Key, kv.Value)
	}
	outputFile.Close()

	res := TaskResult{
		WokerId:    workerId,
		TaskId:     taskId,
		TaskIndex:  task.Index,
		TaskType:   task.TaskType,
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

			logger.Debugf("Parse KV: %s,%s", parts[0], parts[1])

			kv := KeyValue{Key: parts[0], Value: parts[1]}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	output := fmt.Sprintf("output-%s", taskId)
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
		TaskId:     taskId,
		TaskIndex:  task.Index,
		TaskType:   task.TaskType,
		ExecStatus: ExecStatusSuccess,
		Output:     output,
	}

	return res
}
