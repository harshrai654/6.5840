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
type IntermediateFileMap map[string]map[WorkerId][]string
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
	wg                sync.WaitGroup
	shutdownSignaled  bool
	allGoroutinesDone bool
	pendingMappers    int
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

	// Only assing a reduce task when we are sure there is no pending map task left
	// Since then reduce task will surely fail because of unavailabiltiy of intermeidate fiel data
	for task != nil {
		if task.Status == StatusSuccess || (task.Type == ReduceType && c.pendingMappers > 0) {
			if task.Status == StatusSuccess {
				fmt.Printf("[GetTask]: Skipping ready task %s since it is already successfully completed\n", task.Id)
			} else {
				fmt.Printf("[GetTask]: Skipping reduce task %s since there are %d pending mappers\n", task.Id, c.pendingMappers)
			}
			task = c.readyTasks.RemoveTask()
		} else {
			break
		}
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

	fmt.Printf("[GetTask]: Found a task with id %s for worker %d. Current Task Status: %v\n", task.Id, args.WorkerId, task.Status)

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

	reply.NR = c.nReduce

	// Update list of workers currently processing a taskId
	rt := c.runningTasks[task.Id]

	if rt == nil {
		c.runningTasks[task.Id] = &RunningTask{}
	}
	c.runningTasks[task.Id].task = task

	c.runningTasks[task.Id].workers = append(c.runningTasks[task.Id].workers, args.WorkerId)

	if workerMetadata != nil {
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

	reply.Status = true

	taskSuccessInstance := c.successTasks[args.Task.Id]
	v := c.runningTasks[args.Task.Id]

	if v == nil {
		c.runningTasks[args.Task.Id] = &RunningTask{
			workers: make([]WorkerId, 0),
		}
	}

	taskRunningInstances := c.runningTasks[args.Task.Id].workers

	workerMetadata, ok := c.workers[args.Task.Worker]

	if !ok {
		reply.Status = false
		fmt.Printf("[ReportTask]: Something went wrong! WorkerMetadata for worker %d does not exists! While assigning task to worker it should have been created for the worker.\n", args.Task.Worker)
		return nil
	}

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
	// This results in worker loosing shared ownership for given taskId
	// This can be termed as late reporting
	if !isWorkerOwnerOfTask(args, &taskRunningInstances) {
		fmt.Printf("[ReportTask]: Worker %d lost ownership of the task %s.\n", args.Task.Worker, args.Task.Id)
		// Remove the runningTask from workerMetdata
		workerMetadata.runningTask = ""
		return nil
	}

	// Task reported with error status
	// If this was the only worker processing the task then we need to add it back to ready queue
	// with status `StatusReady`, Otherwise wait for other worker's report.
	// coordinator's runningTasks list will gurantee that it consists of workers for a taskid which haven't timedout
	// And actually processing the task
	if args.Task.Status == StatusError {
		fmt.Printf("[ReportTask]: Task %s reported with status %v by worker %d\n", args.Task.Id, args.Task.Status, args.Task.Worker)

		// Update global state of coordinator
		disOwnWorkerFromTask(args, &taskRunningInstances)

		// Update worker metdata to reflect no task is currently processed by the worker
		workerMetadata.runningTask = ""

		// No other worker processing the same task
		// Adding task back to ready queue with `StatusReady`
		if len(c.runningTasks[args.Task.Id].workers) == 0 {
			task := args.Task

			// Resetting details of task before adding it back to ready queue
			task.Worker = 0
			task.StartTime = time.Time{}
			task.Status = StatusReady

			c.readyTasks.AddTask(&task)
		} else {
			// It is possible there are other workers still processng the task
			// In this case we will not do anything with failed task report by current worker
			// And wait for other running worker's status for this task
			fmt.Printf("[ReportTask]: Task %s is still processed by %d other workers. Skipping retrial of the task. Waiting for report by other workers.\n", args.Task.Id, len(c.runningTasks[args.Task.Id].workers))
		}

		return nil
	}

	// Task reported successfully, This will be first success reporting of a task
	// If another task already reported with success it will be already present in `successTasks`
	// and will be skipped
	if args.Task.Status == StatusSuccess {
		// Depending on the type of task getting completed we need to handle this
		// accordingly and update args.Task

		switch args.Task.Type {
		case MapType:
			{
				// 1. Extract hash from the intermediate file names
				// 	  File name structure: w-<workerId>/mr-m-<taskId>-<hash>
				// 2. Save the hash->filename pair in `IntermediateFileMap`
				// 3. Update worker's metadata
				// 4. Remove completed task from runningTasks and add it to successfulTasks

				intermediateFiles := args.Task.Output

				fmt.Printf("[ReportTask]: Mapper Task %s completed successfully by worker %d, produced following intermediate files: %v\n", args.Task.Id, args.Task.Worker, intermediateFiles)

				for _, filename := range intermediateFiles {
					partitionKey := strings.Split(filename, "-")[4]
					paritionFiles, ok := c.intermediateFiles[partitionKey]

					if !ok || paritionFiles == nil {
						paritionFiles = make(map[WorkerId][]string)
					}
					paritionFiles[args.Task.Worker] = append(paritionFiles[args.Task.Worker], filename)
					c.intermediateFiles[partitionKey] = paritionFiles
				}

				workerMetadata.lastHeartBeat = time.Now()
				workerMetadata.successfulTasks[args.Task.Id] = &TaskOutput{
					filenames: intermediateFiles,
					duration:  time.Since(args.Task.StartTime),
				}
				workerMetadata.runningTask = ""

				// Deletion of task from running instance map will automatically disown
				// other workers processing the same task
				delete(c.runningTasks, args.Task.Id)
				c.successTasks[args.Task.Id] = &args.Task

				c.pendingMappers--

				// Check if this was the last Map Task
				// If Yes then we need to add R reduce tasks to the ready queue
				// TODO: find a way to not to re-add the reduce task if this is done earlier, case possible
				// If mapper task is being retired after reduce tasks have been added -> This is possible if
				// A worker excuting a reduce task crashed, since in that case we re-execute all `successful tasks` of that worker
				// if c.readyTasks.GetTaskCount() == 0 && len(c.runningTasks) == 0 && len(c.successTasks) == c.nMap {
				if c.pendingMappers == 0 {
					fmt.Printf("\nAll map task ran successfully. Tasks Run Details: \n %v \n", c.successTasks)
					fmt.Printf("Map task completed: %d | File input count: %d\n", len(c.successTasks), c.nMap)
					c.addReduceTasks()
				}
			}

		case ReduceType:
			{
				reduceOutput := args.Task.Output

				fmt.Printf("[ReportTask]: Reduce Task %s completed successfully by worker %d, produced output files: %v\n", args.Task.Id, args.Task.Worker, reduceOutput)

				workerMetadata.lastHeartBeat = time.Now()
				workerMetadata.successfulTasks[args.Task.Id] = &TaskOutput{
					filenames: reduceOutput,
					duration:  time.Since(args.Task.StartTime),
				}
				workerMetadata.runningTask = ""

				// Deletion of task from running instance map will automatically disown
				// other workers processing the same task
				delete(c.runningTasks, args.Task.Id)
				c.successTasks[args.Task.Id] = &args.Task

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
			fmt.Printf("[ReportTask]: Worker %d reported a task of unknown type with status success, ignoring...\n", args.Task.Worker)
		}

		fmt.Printf("[ReportTask]: Pending map tasks: %d\n", c.pendingMappers)
		fmt.Printf("[ReportTask]: Total success task count: %d\n", len(c.successTasks))

	}

	return nil
}

// func (c *Coordinator) CheckWorkersStatus(args *CheckWorkerStatusArgs, reply *CheckWorkerStatusReply) error {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()

// 	partition := args.Partition

// 	reply.IntermediateFiles = c.intermediateFiles[partition]
// 	reply.NM = c.nMap

// 	return nil
// }

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

	// If the job is marked as finished and we haven't started the shutdown sequence for goroutines yet
	if c.finished && !c.shutdownSignaled {
		fmt.Printf("[Coordinator Shutdown]: MR workflow completed. Signaling internal goroutines to stop.\n")
		close(c.done)             // Signal all listening goroutines
		c.shutdownSignaled = true // Mark that we've signaled them
	}

	// If we have signaled for shutdown, but haven't yet confirmed all goroutines are done
	if c.shutdownSignaled && !c.allGoroutinesDone {
		// Unlock before c.wg.Wait() to allow other goroutines that might need c.mu
		// (e.g., for their final operations before calling c.wg.Done()) to proceed.
		// The health check goroutine primarily needs c.mu for c.checkWorkerStatus.
		// Its exit path via <-c.done does not require c.mu.
		c.mu.Unlock()
		c.wg.Wait() // Wait for all goroutines (like the health checker) to call c.wg.Done()
		c.mu.Lock() // Re-acquire the lock
		c.allGoroutinesDone = true
		fmt.Printf("[Coordinator Shutdown]: All internal goroutines have completed.\n")
	}

	// The coordinator is truly "Done" only if the main job is finished AND all its goroutines have cleaned up.
	isCompletelyDone := c.finished && c.allGoroutinesDone
	c.mu.Unlock()
	return isCompletelyDone
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
		lastHeartBeatDuration := time.Since(metadata.lastHeartBeat)

		if metadata.runningTask != "" && lastHeartBeatDuration >= WORKER_TIMEOUT_SECONDS {
			fmt.Printf("Worker %d have not reported in last %s\n", workerId, lastHeartBeatDuration)
			taskToRetry := make([]*Task, 0)

			// Adding successful map tasks of this worker for retrial
			// for taskId, _ := range metadata.successfulTasks {
			// 	if c.successTasks[taskId] != nil && c.successTasks[taskId].Type == MapType {
			// 		taskToRetry = append(taskToRetry, c.successTasks[taskId])
			// 		delete(c.successTasks, taskId)
			// 	}
			// }
			// fmt.Printf("DEBUG: pendingMappers: %d\n", c.pendingMappers)

			// Tombstoning metdata of intermediate files produced by this worker
			// From global state so that downstream reduce workers get to know about the failure
			// This will result in reduce workers reporting error and getting re-added to the queue
			// These redcue workers should always be added after the successful map task of current worker
			// So that in next retry these reducers have the upsrream map's intermediate files
			// for _, v := range c.intermediateFiles {
			// 	// new line signifies that the intermediate file produces by this worker is now invalid
			// 	v[workerId] = nil
			// }

			// Adding current running task for retrial based on task type
			runningTask := c.runningTasks[metadata.runningTask]

			if runningTask == nil {
				fmt.Printf("[checkWorkerStatus]: Local worker state shows worker %d running rask %s whereas global running tasks state does not show any worker for the same task.\n", workerId, metadata.runningTask)

				return
			}

			taskToRetry = append(taskToRetry, runningTask.task)
			metadata.runningTask = ""

			runningTask.workers = slices.DeleteFunc(runningTask.workers, func(w WorkerId) bool {
				return w == workerId
			})

			if len(taskToRetry) > 0 {
				for _, task := range taskToRetry {
					fmt.Printf("[checkWorkerStatus]: Adding task %s of type %d with status %d back to the ready queue.\n", task.Id, task.Type, task.Status)

					// if task.Type == MapType && task.Status == StatusSuccess {
					// 	c.pendingMappers++
					// }

					task.Status = StatusReady
					task.Worker = 0
					task.Output = make([]string, 0)

					c.readyTasks.AddTask(task)

				}
			} else {
				fmt.Printf("[checkWorkerStatus]: No successful or running task executed by worker %d, nothing to add back to ready queue\n", workerId)
			}
		} else {
			if metadata.runningTask == "" {
				fmt.Printf("[checkWorkerStatus]: Worker %d is not executing any task right now, skipping health check\n", workerId)
			} else {
				fmt.Printf("[checkWorkerStatus]: Worker %d is currently active with last heartbeat recorded %s later. Executing task %s\n", workerId, lastHeartBeatDuration, metadata.runningTask)
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
		intermediateFiles: make(IntermediateFileMap),
		done:              make(chan struct{}),
		finished:          false,
		workers:           make(map[WorkerId]*WorkerMetdata),
		shutdownSignaled:  false,
		allGoroutinesDone: false,
		pendingMappers:    len(files),
	}

	fmt.Printf("Initialised ready tasklist of %d tasks\n", len(files))

	c.server()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.done:
				fmt.Printf("[Coordinator Shutdown]: Closing worker health check background thread.\n")
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
		if item == args.Task.Worker {
			return true
		}
	}
	return false
}
func disOwnWorkerFromTask(args *ReportTaskArgs, taskRunningInstances *[]WorkerId) {
	*taskRunningInstances = slices.DeleteFunc(*taskRunningInstances, func(w WorkerId) bool {
		return args.Task.Worker == w
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

		if c.successTasks[task.Id] == nil {
			c.readyTasks.AddTask(&task)

			fmt.Printf("Reduce Task with Id %s Added to ready queue (Intermediate partition %s with %d files)\n", task.Id, partition, len(v))
		}

		index++
	}

	c.nReduce = index

}
