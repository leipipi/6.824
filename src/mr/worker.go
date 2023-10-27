package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

func generateWorkerId() string {
	timestamp := time.Now().UnixNano() / 1000000
	Id := fmt.Sprintf("%06d", timestamp%1000000) //方便观察，取后六位作id
	return Id
}

func TaskIsDone(task *Task) {
	req := &TaskIsDoneRequest{}
	req.ReqTask = task
	res := &TaskIsDoneReponse{}
	call("Coordinator.TaskIsDone", req, res)
}

func RequireTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *Task {
	req := &DistributeTaskRequest{}
	res := DistributeTaskResponse{
		ResTask: &Task{
			TaskType: WaitingType,
		},
	}
	call("Coordinator.DistributeTask", req, &res)
	return res.ResTask
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerid := generateWorkerId()
	fmt.Println("generate new worker, id = ", workerid)
	alive := true
	attempt := 0

	for alive {
		attempt++
		fmt.Println("worker ", workerid, " attempt : ", attempt)
		task := RequireTask(mapf, reducef)
		switch task.TaskType {
		case MapType:
			DoMapTask(mapf, task)
			fmt.Println("worker ", workerid, "do map task...")
			TaskIsDone(task)
		case ReduceType:
			taskid, _ := strconv.Atoi(task.TaskId)
			if taskid >= 8 {
				DoReduceTask(reducef, task)
				fmt.Println("worker ", workerid, "do reduce task...")
				TaskIsDone(task)
			}
		case WaitingType:
			fmt.Println("worker ", workerid, "waiting 1 second...")
			time.Sleep(time.Second)
		case KillType:
			time.Sleep(time.Second)
			fmt.Println("worker ", workerid, "terminated...")
		}
		time.Sleep(time.Second)
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

func readFromLocalFile(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
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
	return kva
}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue
	filename := response.InputFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))
	rn := response.ReduceNum
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		tmpfilename := "mr-tmp-" + response.TaskId + "-" + strconv.Itoa(i) + ".json"
		// file, err = os.OpenFile(tmpfilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		file, err = os.OpenFile(tmpfilename, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("create maptmpfile failed %v", tmpfilename)
		}
		enc := json.NewEncoder(file)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("convert to json failed")
			}
		}
		file.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, res *Task) {
	reduceFileNum := res.TaskId
	intermediate := readFromLocalFile(res.InputFiles)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	tempfile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("failed to create temp file", err)
	}
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
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempfile.Close()
	oname := fmt.Sprintf("mr-out-%s", reduceFileNum)
	os.Rename(tempfile.Name(), oname)
}
