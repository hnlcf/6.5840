package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
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

func getFileContents(inputFile string) (string, string) {
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
			logger.Infof("[worker %d]: Get a new map task %s with stage %d.", workerId, taskReply.TaskId, taskReply.TaskState)

			taskResult := processMapTask(taskReply.NReduce, taskReply.Task, mapf)
			CallReportTaskResult(taskResult)
		case TaskStateReduce:
			logger.Infof("[worker %d]: Get a new reduce task %s with stage %d.", workerId, taskReply.TaskId, taskReply.TaskState)

			taskResult := processReduceTask(taskReply.NMap, taskReply.Task, reducef)
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

func processMapTask(nReduce int, task Task, mapf func(string, string) []KeyValue) TaskResult {
	file, content := getFileContents(task.InputFile)
	kva := mapf(file, content)

	tmpNames := make([]string, nReduce)
	outputNames := make([]string, nReduce)
	outputFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		tmpNames[i] = fmt.Sprintf("mr-tmp-%d-%d", task.Index, i)
		outputNames[i] = fmt.Sprintf("mr-%d-%d", task.Index, i)
		outputFiles[i], _ = os.Create(tmpNames[i])
	}

	for i := 0; i < len(kva); i++ {
		kv := kva[i]
		outputIndex := ihash(kv.Key) % nReduce

		fmt.Fprintf(outputFiles[outputIndex], "%s,%s\n", kv.Key, kv.Value)
	}

	for i, f := range outputFiles {
		f.Close()

		oldpath := filepath.Join(f.Name())
		os.Rename(oldpath, outputNames[i])
	}

	res := TaskResult{
		WokerId:    workerId,
		WorkTask:   task,
		ExecStatus: ExecStatusSuccess,
		Output:     outputNames,
	}
	return res
}

func processReduceTask(nMap int, task Task, reducef func(string, []string) string) TaskResult {
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, task.Index)
		_, content := getFileContents(fileName)
		lines := strings.Split(content, "\n")

		for _, l := range lines {
			if len(l) != 0 {
				parts := strings.Split(l, ",")

				kv := KeyValue{Key: parts[0], Value: parts[1]}
				intermediate = append(intermediate, kv)
			}
		}
	}

	sort.Sort(ByKey(intermediate))

	output := make([]string, 10)
	tmpName := fmt.Sprintf("mr-tmp-%d", task.Index)
	outputName := fmt.Sprintf("mr-out-%d", task.Index)
	outputFile, _ := os.Create(tmpName)

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
	oldpath := filepath.Join(outputFile.Name())
	os.Rename(oldpath, outputName)

	output = append(output, outputName)

	res := TaskResult{
		WokerId:    workerId,
		WorkTask:   task,
		ExecStatus: ExecStatusSuccess,
		Output:     output,
	}

	return res
}
