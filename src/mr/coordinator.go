package mr

import (
	"log"
	net "net"
	http "net/http"
	"net/rpc"
	atomic "sync/atomic"
)

type Coordinator struct {
	// Your definitions here.
	nReduce    int
	fileNames  []string
	DoneAmount atomic.Int32
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) CallDone(args *interface{}, reply *interface{}) error {
	c.DoneAmount.Add(1)
	return nil
}

func (c *Coordinator) InitWorker(ignore *interface{}, reply *InitWorkerReply) error {
	reply.WorkerAmount = c.nReduce
	reply.FileNames = c.fileNames
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":12345")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.nReduce == int(c.DoneAmount.Load())

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.fileNames = files

	c.server()
	return &c
}
