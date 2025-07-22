package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := ExampleArgs{}
		reply := Reply{}
		// ask the coordinator for a task.
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			if reply.TaskType == "none" {
				// println("All tasks completed.")
				break
			}
			if reply.TaskType == "map" {
				handleMapTask(reply, mapf)
				doneArgs := DoneArgs{TaskType: "map", TaskID: reply.Id}
				doneReply := ExampleReply{}
				ok := call("Coordinator.CompleteTask", &doneArgs, &doneReply)
				if !ok {
					log.Fatalf("Failed to complete map task %d", reply.Id)
				}
				// println("Completed map task:", reply.Id)
			}
			if reply.TaskType == "reduce" {
				handleReduceTask(reply, reducef)
				doneArgs := DoneArgs{TaskType: "reduce", TaskID: reply.Id}
				doneReply := ExampleReply{}
				ok := call("Coordinator.CompleteTask", &doneArgs, &doneReply)
				if !ok {
					log.Fatalf("Failed to complete reduce task %d", reply.Id)
				}
				// println("Completed reduce task:", reply.Id)
			}
		} else {
			time.Sleep(time.Second) // wait before retrying
		}
	}
}

func handleMapTask(reply Reply, mapf func(string, string) []KeyValue) {

	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// write the intermediate key-value pairs to files.

	files := make([]*os.File, reply.NReduce)
	bufs := make([]*bufio.Writer, reply.NReduce)
	encs := make([]*json.Encoder, reply.NReduce)

	tempDir := "../temp"
	os.Mkdir(tempDir, 0755)
	for i := 0; i < reply.NReduce; i++ {
		tempfile := fmt.Sprintf("%s/mr-%d-%d-t", tempDir, reply.Id, i)
		f, err := os.Create(tempfile)
		if err != nil {
			log.Fatalf("cannot create %v", tempfile)
		}

		files[i] = f
		buf := bufio.NewWriter(f)
		bufs[i] = buf
		enc := json.NewEncoder(buf)
		encs[i] = enc
	}

	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.NReduce
		err := encs[idx].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}

	for i := range reply.NReduce {
		err := bufs[i].Flush()
		if err != nil {
			log.Fatalf("cannot flush %v", files[i].Name())
		}
		err = files[i].Close()
		if err != nil {
			log.Fatalf("cannot close %v", files[i].Name())
		}
		err = os.Rename(files[i].Name(), fmt.Sprintf("%s/mr-%d-%d", tempDir, reply.Id, i))
		if err != nil {
			log.Fatalf("cannot rename %v to %s/mr-%d-%d", files[i].Name(), tempDir, reply.Id, i)
		}
	}
}

func handleReduceTask(reply Reply, reducef func(string, []string) string) {
	tempDir := "../temp"
	kva := make([]KeyValue, 0)

	for i := 0; i < reply.NMap; i++ {
		filename := fmt.Sprintf("%s/mr-%d-%d", tempDir, i, reply.Id)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("cannot open", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d-t", reply.Id)

	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	os.Rename(oname, fmt.Sprintf("mr-out-%d", reply.Id))
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
