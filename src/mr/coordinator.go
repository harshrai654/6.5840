package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type TaskType int
type TaskId string
type TaskStatus int
type WorkerId int
type IntermediateFileMap map[string]map[WorkerId]string
type TaskOutput struct {
	filenames []string
	duration  time.Duration
}
type WorkerMetdata struct {
	lastHeartBeat   time.Time
	runningTask     TaskId
	successfulTasks map[TaskId]*TaskOutput
}

type TaskList struct {
	list *list.List
}

const (
	StatusError TaskStatus = iota
	StatusSuccess
	StatusReady
	StatusRunning
)

const (
	MapType TaskType = iota
	ReduceType
	InvalidType = -1
)

const WORKER_TIMEOUT_SECONDS = time.Second * 10

func NewTaskList() *TaskList {
	return &TaskList{
		list: list.New(),
	}
}

func (tl *TaskList) RemoveTask() *Task {
	element := tl.list.Front()
	if element == nil {
		return nil
	}

	return tl.list.Remove(element).(*Task)
}

func (tl *TaskList) AddTask(t *Task) {
	tl.list.PushBack(t)
}

func (tl *TaskList) GetTaskCount() int {
	return tl.list.Len()
}

type Task struct {
	Filename  string
	Worker    WorkerId
	Status    TaskStatus
	Type      TaskType
	Id        TaskId
	StartTime time.Time
	Output    []string
}

type RunningTask struct {
	task    *Task
	workers []WorkerId
}

type Coordinator struct {
	mu                sync.Mutex
	readyTasks        TaskList                // Contains both ready tasks and failed tasks
	runningTasks      map[TaskId]*RunningTask // Contains all in-progress tasks, a single task may be processed by multiple workers
	successTasks      map[TaskId]*Task        // Contains all completed tasks
	nReduce           int
	nMap              int
	intermediateFiles IntermediateFileMap // contains partition key -> file name mappings
	workers           map[WorkerId]*WorkerMetdata
	finished          bool
	done              chan struct{}
}

// An RPC handler to find next available task (map or reduce)
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerMetadata, ok := c.workers[args.WorkerId]

	// Requesting worker already processing a task
	// Skip task assignment
	if ok && workerMetadata.runningTask != "" {
		fmt.Printf("[GetTask]: Worker %d already processing task %s, rejecting task assignment request.\n", args.WorkerId, workerMetadata.runningTask)
		return nil
	}

	if c.readyTasks.GetTaskCount() == 0 {
		// No tasks available
		// map reduce is complete if we also have len(runningTasks) == 0
		// Sending InvalidType task in such cases to worker
		reply.Task = Task{
			Type: InvalidType,
		}
		return nil
	}

	task := c.readyTasks.RemoveTask()

	// Skipping tasks that are possible retrials with an instance already completed and part of success set
	// It is possible that a task here already has a status of `StatusRunning` we are not skipping such tasks in ready queue
	// This will result in multiple instances of same task execution, This case is possible if previous worker processing the task
	// failed/crashed (timeout of not reporting reached) and we added another instance of the same task.
	// Even if two workers report completion of same task only one of them will remove the task from running queue and add it to
	// success set, Reporting by slower worker will be skipped.
	for c.readyTasks.GetTaskCount() > 0 && task.Status == StatusSuccess {
		task = c.readyTasks.RemoveTask()
	}

	// Either all tasks are completed (if len(runningTasks) == 0)
	// OR all tasks are currently being processed by some workers
	if task == nil {
		reply.Task = Task{
			Type: InvalidType,
		}
		fmt.Printf("[GetTask]: No task to assign to worker %d, # Tasks Running : %d, # Tasks Completed: %d\n", workerId, len(c.runningTasks), len(c.successTasks))
		return nil
	}

	fmt.Printf("[GetTask]: Found a task with id %s for worker %d. Current Task Status: %v\n", task.Id, task.Worker, task.Status)

	// Found a task with Status as either `StatusError` or `StatusReady` or `StatusRunning`
	// If task's status is: `StatusError`` -> Retrying failed task again
	// If task's status is `StatusReady` -> First Attempt of processing of task
	// If task's status is `StatusRunning` -> Retrying already running task due to delay from previous assigned worker
	task.Worker = args.WorkerId
	task.StartTime = time.Now()
	task.Status = StatusRunning
	reply.Task = *task

	if task.Type == ReduceType {
		// Add intermediate file locations collected from various map executions
		reply.IntermediateFiles = c.intermediateFiles[task.Filename]
	}

	// Update list of workers currently processing a taskId
	rt, ok := c.runningTasks[task.Id]

	if !ok || rt == nil {
		rt = &RunningTask{
			task: task,
		}
		c.runningTasks[task.Id] = rt
	} else {
		c.runningTasks[task.Id].task = task
	}

	c.runningTasks[task.Id].workers = append(c.runningTasks[task.Id].workers, args.WorkerId)

	if ok {
		workerMetadata.lastHeartBeat = time.Now()
		workerMetadata.runningTask = task.Id
	} else {
		c.workers[args.WorkerId] = &WorkerMetdata{
			lastHeartBeat:   time.Now(),
			runningTask:     task.Id,
			successfulTasks: make(map[TaskId]*TaskOutput),
		}
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskSuccessInstance := c.successTasks[args.Id]
	taskRunningInstances := c.runningTasks[args.Id].workers

	workerMetadata := c.workers[args.Worker]
	workerMetadata.lastHeartBeat = time.Now()
	// Reported task is already in success set.
	// Possibly retried after timeout by another worker
	// One of the worker complted the task.
	if taskSuccessInstance != nil {
		fmt.Printf("[ReportTask]: Task %s has already been completed by worker %d\n", taskSuccessInstance.Id, taskSuccessInstance.Worker)
		workerMetadata.runningTask = ""
		return nil
	}

	// This case is possible if the worker failed to send heartbeat before timeout
	// In that case we move all the runningTasks from worker's metdata back to ready queue
	// We also delete the workerId from global runningTasks map corresponding to given taskId.
	// This results in worker loosing shared shared ownership for given taskId
	// This can be termed as late reporting
	if !isWorkerOwnerOfTask(args, &taskRunningInstances) {
		fmt.Println("[ReportTask]: Worker %d was never assigned the task %s.\n", args.Worker, args.Id)
		// Remove the runningTask from workerMetdata
		workerMetadata.runningTask = ""
		return nil
	}

	// Task reported with error status
	// If this was the only worker processing the task then we need to add it back to ready queue
	// with status `StatusReady`, Otherwise wait for other worker's report.
	// coordinator's runningTasks list will gurantee that it consists of workers for a taskid which haven't timedout
	// And actually processing the task
	if args.Status == StatusError {
		fmt.Printf("[ReportTask]: Task %s reported with status %v by worker %d\n", args.Id, args.Status, args.Worker)

		// Update global state of coordinator
		disOwnWorkerFromTask(args, &taskRunningInstances)

		// Update worker metdata to reflect no task is currently processed by the worker
		workerMetadata.runningTask = ""

		// No other worker processing the same task
		// Adding task back to ready queue with `StatusReady`
		if len(c.runningTasks[args.Id].workers) == 0 {
			task := args.Task
			// Adding the task back to ready queue only if it is of map type
			// A error in reduce task means possible loog
			if task.Type == MapType {

			}

			// Resetting details of task before adding it back to ready queue
			task.Worker = 0
			task.StartTime = time.Time{}
			task.Status = StatusReady

			c.readyTasks.AddTask(&task)
		} else {
			// It is possible there are other workers still processng the task
			// In this case we will not do anything with failed task report by current worker
			// And wait for other running worker's status for this task
			fmt.Printf("[ReportTask]: Task %s is still processed by %d other workers. Skipping retrial of the task. Waiting for report by other workers.\n", args.Id, len(c.runningTasks[args.Id].workers))
		}

		return nil
	}

	// Task reported successfully, This will be first success reporting of a task
	// If another task already reported with success it will be already present in `successTasks`
	// and will be skipped
	if args.Status == StatusSuccess {
		// Depending on the type of task getting completed we need to handle this
		// accordingly and update args.Task

		switch args.Type {
		case MapType:
			{
				// 1. Extract hash from the intermediate file names
				// 	  File name structure: mr-<taskId>-<hash>
				// 2. Save the hash->filename pair in `IntermediateFileMap`
				// 3. Update worker's metadata
				// 4. Remove completed task from runningTasks and add it to successfulTasks

				intermediateFiles := args.Output

				fmt.Printf("[ReportTask]: Mapper Task %s completed successfully, produced following intermediate files: %v\n", args.Id, intermediateFiles)

				for _, filename := range intermediateFiles {
					partitionKey := strings.Split(filename, "-")[2]
					c.intermediateFiles[partitionKey][args.Worker] = filename
				}

				workerMetadata.lastHeartBeat = time.Now()
				workerMetadata.successfulTasks[args.Id] = &TaskOutput{
					filenames: intermediateFiles,
					duration:  time.Now().Sub(args.StartTime),
				}

				// Deletion of task from running instance map will automatically disown
				// other workers processing the same task
				delete(c.runningTasks, args.Id)
				c.successTasks[args.Id] = &args.Task

				// Check if this was the last Map Task
				// If Yes then we need to add R reduce tasks to the ready queue
				// TODO: find a way to not to re-add the reduce task if this is done earlier, case possible
				// If mapper task is being retired after reduce tasks have been added -> This is possible if
				// A worker excuting a reduce task crashed, since in that case we re-execute all `successful tasks` of that worker
				if c.readyTasks.GetTaskCount() == 0 && len(c.runningTasks) == 0 && len(c.successTasks) == c.nMap {
					fmt.Printf("\nAll map task ran successfully. Tasks Run Details: \n %v \n", c.successTasks)
					fmt.Printf("Map task completed: %d | File input count: %d\n", len(c.successTasks), c.nMap)
					c.addReduceTasks()
				}
			}

		case ReduceType:
			{
				reduceOutput := args.Output

				fmt.Printf("[ReportTask]: Reduce Task %s completed successfully, produced output files: %v\n", args.Id, reduceOutput)

				workerMetadata.lastHeartBeat = time.Now()
				workerMetadata.successfulTasks[args.Id] = &TaskOutput{
					filenames: reduceOutput,
					duration:  time.Since(args.StartTime),
				}

				// Deletion of task from running instance map will automatically disown
				// other workers processing the same task
				delete(c.runningTasks, args.Id)
				c.successTasks[args.Id] = &args.Task

				if len(c.successTasks) == c.nMap+c.nReduce {
					fmt.Printf("\nAll reduce tasks ran successfully. Tasks Run Details: \n %v \n", c.successTasks)
					fmt.Printf("Overall map+reduce task completed count: %d\n", len(c.successTasks))
					c.finished = true

					// Coordinator will close after this, there may be some running tasks which will not be
					// able to report back their results since coordinator will be down, in such case
					// worker will exist itself as well.
				}

			}
		default:
			fmt.Printf("[ReportTask]: Worker %d reported a task of unknown type with status success, ignoring...\n", args.Worker)
		}

	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Printf("Listening for HTTP data on socket: %s\n", sockname)
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.finished {
		close(c.done)
	}

	return c.finished
}

func (c *Coordinator) checkWorkerStatus() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for workerId, metadata := range c.workers {
		// For workers with active running task (metadata.runningTask != ""):
		//
		// Worker last heart beat was older than timout, consider worker has crashed
		// What to do when a worker crashes
		// - Remove the worker from global running task list this will disown the worker from the task and
		//   if later worker reports the task it will be skipped and worker's local metdata will be updated accordingly
		// - Re-add all successful mapper tasks (and reduce task iff the worker never completed any mapper task) of this worker, remove them from successful set
		lastHeartBeatDuration := time.Now().Sub(metadata.lastHeartBeat)

		if metadata.runningTask != "" && lastHeartBeatDuration >= WORKER_SLEEP_DURATION {
			fmt.Printf("Worker %d have not reported in last %s\n", lastHeartBeatDuration)
			taskToRetry := make([]*Task, 0)

			// Adding successful map tasks of this worker for retrial
			for taskId, _ := range metadata.successfulTasks {
				if c.successTasks[taskId] != nil && c.successTasks[taskId].Type == MapType {
					taskToRetry = append(taskToRetry, c.successTasks[taskId])
					delete(c.successTasks, taskId)
				}
			}

			// Tombstoning metdata of intermediate files produced by this worker
			// From global state so that downstream reduce workers get to know about the failure
			// This will result in reduce workers reporting error and getting re-added to the queue
			// These redcue workers should always be added after the successful map task of current worker
			// So that in next retry these reducers have the upsrream map's intermediate files
			for _, v := range c.intermediateFiles {
				// new line signifies that the intermediate file produces by this worker is now invalid
				v[workerId] = "\n"
			}

			// Adding current running task for retrial based on task type
			runningTask := c.runningTasks[metadata.runningTask]

			taskToRetry = append(taskToRetry, runningTask.task)

			runningTask.workers = slices.DeleteFunc(c.runningTasks[metadata.runningTask].workers, func(w WorkerId) bool {
				return w == workerId
			})

			if len(taskToRetry) > 0 {
				for _, task := range taskToRetry {
					fmt.Printf("[checkWorkerStatus]: Adding task %s of type %d with status %d back to the ready queue.\n", task.Id, task.Type, task.Status)
					task.Status = StatusReady
					task.Worker = 0
					task.Output = make([]string, 0)

					c.readyTasks.AddTask(task)

				}
			} else {
				fmt.Printf("No successful or running task executed by worker %d, nothing to add back to ready queue\n", workerId)
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	readyTaskList := NewTaskList()

	for index, file := range files {
		readyTaskList.AddTask(&Task{
			Filename: file,
			Status:   StatusReady,
			Type:     MapType,
			Id:       TaskId(fmt.Sprintf("m-%d", index)),
		})
	}

	c := Coordinator{
		readyTasks:        *readyTaskList,
		runningTasks:      make(map[TaskId]*RunningTask),
		successTasks:      make(map[TaskId]*Task),
		nReduce:           nReduce,
		nMap:              len(files),
		intermediateFiles: make(map[string]map[WorkerId]string),
		done:              make(chan struct{}),
		finished:          false,
	}

	fmt.Printf("Initialised ready tasklist of %d tasks\n", len(files))

	c.server()

	// Thread to check worker's health in background.
	// A channel here is needed to signal the background thread to exit
	// as soon as the parent thread signals it to do so via repeatitive calls to c.Done()
	go func() {
		for {
			select {
			case <-c.done:
				return
			default:
				c.checkWorkerStatus()
				time.Sleep(WORKER_TIMEOUT_SECONDS / 2)
			}
		}
	}()

	return &c
}

func isWorkerOwnerOfTask(args *ReportTaskArgs, taskRunningInstances *[]WorkerId) bool {
	// Check the worker list for the task reported just to be double sure that the currently
	// reporting worker is present in that task's list
	for _, item := range *taskRunningInstances {
		if item == args.Worker {
			return true
		}
	}
	return false
}
func disOwnWorkerFromTask(args *ReportTaskArgs, taskRunningInstances *[]WorkerId) {
	*taskRunningInstances = slices.DeleteFunc(*taskRunningInstances, func(w WorkerId) bool {
		return args.Worker == w
	})
}

func (c *Coordinator) addReduceTasks() {
	index := 0

	for partition, v := range c.intermediateFiles {
		task := Task{
			Status:   StatusReady,
			Type:     ReduceType,
			Id:       TaskId(fmt.Sprintf("r-%d", index)),
			Filename: partition,
		}
		c.readyTasks.AddTask(&task)

		fmt.Printf("Reduce Task with Id %s Added to ready queue (Intermediate partition %s with %d files)\n", task.Id, partition, len(v))
		index++
	}

}
