package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type mapTask struct {
	Filename string
	state    string // "pending", "in-progress", "done"
	jobId    int
}

type reduceTask struct {
	state string
	jobId int
}

type Coordinator struct {
	// Your definitions here.
	maptasks    []mapTask
	reducetasks []reduceTask
	nReduce     int // number of reduce tasks
	mu          sync.Mutex

	done1 int           // number of completed map tasks
	done2 int           // number of completed reduce tasks
	ch    chan struct{} // channel to signal completion
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *ExampleArgs, reply *Reply) error {

	for c.done1 < len(c.maptasks) {
		for i := range c.maptasks {
			t := &c.maptasks[i]
			if t.state == "pending" {
				c.mu.Lock()
				if t.state != "pending" {
					c.mu.Unlock()
					continue
				}
				t.state = "in-progress"
				c.mu.Unlock()
				reply.Id = t.jobId
				reply.NReduce = c.nReduce
				reply.NMap = 0 // Not used in map tasks
				reply.Filename = t.Filename
				reply.TaskType = "map"
				go c.startTimer("map", t.jobId)
				return nil
			}
		}
	}

	for c.done2 < c.nReduce {
		for i := range c.reducetasks {
			t := &c.reducetasks[i]
			if t.state == "pending" {
				c.mu.Lock()
				if t.state != "pending" {
					c.mu.Unlock()
					continue
				}
				t.state = "in-progress"
				c.mu.Unlock()
				reply.Id = t.jobId
				reply.NReduce = 0 // Not used in reduce tasks
				reply.NMap = len(c.maptasks)
				reply.Filename = "" // Not used in reduce tasks
				reply.TaskType = "reduce"
				go c.startTimer("reduce", t.jobId)
				return nil
			}
		}
	}

	reply.TaskType = "none"
	reply.Id = -1
	reply.NReduce = c.nReduce
	reply.Filename = ""
	return nil
}

func (c *Coordinator) CompleteTask(args *DoneArgs, Reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case "map":
		if args.TaskID < len(c.maptasks) && c.maptasks[args.TaskID].state == "in-progress" {
			c.maptasks[args.TaskID].state = "done"
			c.done1++
			// log.Printf("Map task %d completed\n", args.TaskID)
		} else {
			log.Printf("Map task %d already completed or invalid\n", args.TaskID)
		}
	case "reduce":
		if args.TaskID < len(c.reducetasks) && c.reducetasks[args.TaskID].state == "in-progress" {
			c.reducetasks[args.TaskID].state = "done"
			c.done2++
			// log.Printf("Reduce task %d completed\n", args.TaskID)
		} else {
			log.Printf("Reduce task %d already completed or invalid\n", args.TaskID)
		}
	}

	return nil
}

func (c *Coordinator) startTimer(taskType string, jobId int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	switch taskType {
	case "map":
		if c.maptasks[jobId].state == "in-progress" {
			c.maptasks[jobId].state = "pending"
			log.Printf("Map task %d timed out, resetting state to pending\n", jobId)
		}
	case "reduce":
		if c.reducetasks[jobId].state == "in-progress" {
			c.reducetasks[jobId].state = "pending"
			log.Printf("Reduce task %d timed out, resetting state to pending\n", jobId)
		}
	}
}

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
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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
	if c.done2 == c.nReduce {
		os.RemoveAll("../temp") // clean up temporary files
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.maptasks = make([]mapTask, 0)
	c.reducetasks = make([]reduceTask, nReduce)
	c.done1 = 0
	c.done2 = 0
	c.ch = make(chan struct{}, 1) // buffered channel to signal task completion
	for i, file := range files {
		c.maptasks = append(c.maptasks, mapTask{
			Filename: file, state: "pending", jobId: i})
	}
	for i := range nReduce {
		c.reducetasks[i] = reduceTask{state: "pending", jobId: i}
	}

	c.server()
	return &c
}
