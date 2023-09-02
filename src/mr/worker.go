package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	return inputFile, string(content)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId := 0
	// TODO
	// 1. get MapTask from task channel
	// 2. read file content
	// 3. call `mapf` and write result to tmp file
	// 4. create ReduceTask and read tmp file
	// 5. call `reducef` and write result to output file

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	is_get_task := false
	reply := TaskReply{}
	for !is_get_task {
		reply, is_get_task = CallAskMapTask()
	}

	file, content := getFileContents(reply.Task)
	kva := mapf(file, content)

	outputFile, _ := os.Create("output.txt")
	for i := 0; i < len(kva); i++ {
		kv := kva[i]
		fmt.Fprintf(outputFile, "%s, %s\n", kv.Key, kv.Value)
	}
	outputFile.Close()

	logger.Infof("[worker %d]: write task %s output to file %s", workerId, reply.TaskId, "output.txt")

	is_report := CallReportTaskResult(workerId, reply.TaskId, reply.Task.TaskType, ExecStatusSuccess)
	if is_report {
		logger.Infof("[worker %d]: report result of task %s to server", workerId, reply.TaskId)
	} else {
		logger.Warnf("[worker %d]: failed to report result of task %s to server", workerId, reply.TaskId)
	}
}
