package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var workerId WorkerId = WorkerId(os.Getpid())
var dirName string = fmt.Sprintf("w-%d", workerId)

const WORKER_SLEEP_DURATION = time.Second * 2

func Log(msg string) {
	fmt.Printf("[Worker: %d]: %s\n", workerId, msg)
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	Log("Started")

	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		err := os.Mkdir(dirName, 0755)
		if err != nil {
			Log(fmt.Sprintf("Failed to create directory %s: %v", dirName, err))
			os.Exit(1)
		}
	}

	getTaskargs := GetTaskArgs{
		WorkerId: WorkerId(workerId),
	}

	for {
		getTaskReply := GetTaskReply{}

		Log("Fetching task from coordinator...")
		ok := call("Coordinator.GetTask", &getTaskargs, &getTaskReply)

		if ok {
			task := &getTaskReply.Task

			switch task.Type {
			case MapType:
				{
					Log(fmt.Sprintf("Assigned map job with task id: %s", task.Id))

					processMapTask(task, mapf)

					reportTaskArgs := ReportTaskArgs{
						Task: *task,
					}
					reportTaskReply := ReportTaskReply{}
					ok = call("Coordinator.ReportTask", &reportTaskArgs, &reportTaskReply)

					if !ok {
						Log("Failed to call 'Coordinator.ReportTask' ! Cooridnator not found or exited, closing worker")
						return
					}
				}
			case ReduceType:
				{

					Log(fmt.Sprintf("Assigned reduce job with task id: %s", task.Id))
					intermediateFiles := getTaskReply.IntermediateFiles
					processReduceTask(task, intermediateFiles, reducef)
				}
			default:
				Log("Invalid task recieved")
			}

			Log("Waiting for sometime before requesting next task or retrying in case invalid task was recieved...")
			time.Sleep(WORKER_SLEEP_DURATION)
		} else {
			Log("Failed to call 'Coordinator.GetTask' ! Cooridnator not found or exited, closing worker")
			return
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// Processes map task by fetching `Filename` from Task
// Calls provided mapf function and stores intermediate files after
// paritioninng them based on `ihash` function
func processMapTask(task *Task, mapf func(string, string) []KeyValue) error {
	Log(fmt.Sprintf("Processing map task with id %s and file: %s\n", task.Id, task.Filename))

	file, err := os.Open(task.Filename)

	if err != nil {
		task.Status = StatusError
		Log(fmt.Sprintf("Error: %s\n", err))
		return err
	}

	// TODO
	// Possible optimisation, read in buffer size
	// Flush periodically to a file
	content, err := io.ReadAll(file)
	if err != nil {
		task.Status = StatusError
		Log(fmt.Sprintf("Error: %s\n", err))
		return err
	}
	file.Close()
	intermediate := mapf(task.Filename, string(content))

	buckets := make(map[int][]KeyValue)

	for _, kv := range intermediate {
		partition := ihash(kv.Key)
		buckets[partition] = append(buckets[partition], kv)
	}

	for partition, kva := range buckets {
		sort.Sort(ByKey(kva))
		file, err := os.CreateTemp(dirName, "mwt-*")

		if err != nil {
			task.Status = StatusError
			Log(fmt.Sprintf("Error: %s\n", err))
			return err
		}

		defer file.Close()

		enc := json.NewEncoder(file)

		for _, kv := range kva {
			err := enc.Encode(&kv)

			if err != nil {
				task.Status = StatusError
				Log(fmt.Sprintf("Error: %s\n", err))
				return err
			}
		}

		intermediateFilename := filepath.Join(dirName, fmt.Sprintf("mr-%s-%d", task.Id, partition))

		// Renaming to final intermediate file name
		err = os.Rename(file.Name(), intermediateFilename)

		if err != nil {
			task.Status = StatusError
			Log(fmt.Sprintf("Error: %s\n", err))
			return err
		}

		task.Output = append(task.Output, intermediateFilename)
	}

	task.Status = StatusSuccess
	return nil

}

func processReduceTask(task *Task, intermediateFiles map[WorkerId]string, reducef func(string, []string) string) error {
	Log(fmt.Sprintf("Processing reduce task with id %s\n", task.Id))

	tempReduceFile, err := os.CreateTemp(dirName, "mwt-*")
	if err != nil {
		task.Status = StatusError
		Log(fmt.Sprintf("Error: %s\n", err))
		return err
	}
	defer tempReduceFile.Close()

	for workerId, filename := range intermediateFiles {
		Log(fmt.Sprintf("Processing intermediate file %s from worker %d\n", filename, workerId))

		intermediateFile, err := os.Open(filename)

		if err != nil {
			task.Status = StatusError
			Log(fmt.Sprintf("Error: %s\n", err))
			return err
		}

		dec := json.NewDecoder(intermediateFile)
		var kva []KeyValue

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tempReduceFile, "%v %v\n", kva[i].Key, output)

			i = j
		}
	}

	reduceFileName := fmt.Sprintf("mr-out-%s", task.Filename)

	// Renaming to final intermediate file name
	err = os.Rename(tempReduceFile.Name(), reduceFileName)

	if err != nil {
		task.Status = StatusError
		Log(fmt.Sprintf("Error: %s\n", err))
		return err
	}

	task.Output = []string{reduceFileName}

	return nil
}
