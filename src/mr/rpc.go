package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskArgs struct {
	WorkerId WorkerId
}

type GetTaskReply struct {
	Task Task
	NR   int
}

type ReportTaskArgs struct {
	Task Task
}

type ReportTaskReply struct {
	Status bool
}

type GetIntermediateFileLocationArgs struct {
	Partition string
	WorkerId  WorkerId
}

type GetIntermediateFileLocationReply struct {
	IntermediateFiles map[WorkerId][]string
	NM                int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
