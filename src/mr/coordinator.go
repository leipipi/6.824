package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type TaskType string
type TaskCondition string
type CoordinatorPhase string

var mu sync.Mutex

const (
	MapType     TaskType = "maptype"
	ReduceType  TaskType = "reducetype"
	WaitingType TaskType = "waitingtype"
	KillType    TaskType = "killtype"
)

const (
	Waiting TaskCondition = "waiting"
	Running TaskCondition = "running"
	Finish  TaskCondition = "finish"
)

const (
	MapPhase    CoordinatorPhase = "mapphase"
	ReducePhase CoordinatorPhase = "reducephase"
	DonePhase   CoordinatorPhase = "donephase"
)

type Task struct {
	TaskType   TaskType
	InputFiles []string
	TaskId     string
	ReduceNum  int
}

type Coordinator struct {
	TaskChanMap          chan *Task
	TaskChanReduce       chan *Task
	CoordinatorCondition CoordinatorPhase
	MetaHolder           TaskMetaHolder
	ReduceNum            int
}

type TaskMetaInfo struct {
	taskcondition TaskCondition
	StartTime     time.Time
	TaskPtr       *Task
}

type TaskMetaHolder struct {
	MetaMap map[string]*TaskMetaInfo
}

var counter int64

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) generateTaskId() string {
	id := atomic.AddInt64(&counter, 1)
	return strconv.FormatInt(id, 10)
}

func (c *Coordinator) generateMapTask(files []string, nReduce int) {
	for _, v := range files {
		mapTask := Task{
			TaskType:   MapType,
			InputFiles: []string{v},
			TaskId:     c.generateTaskId(),
			ReduceNum:  nReduce,
		}
		taskmeta := TaskMetaInfo{
			taskcondition: Waiting,
			TaskPtr:       &mapTask,
		}
		c.TaskChanMap <- &mapTask
		c.MetaHolder.putTask(&taskmeta)
		fmt.Println("generate a map task, task id=", mapTask.TaskId)
	}
	// c.MetaHolder.checkMapTaskFinish()
	// c.MetaHolder.checkReduceTaskFinish()
}

func (c *Coordinator) generateReduceTask() {
	for i := 0; i < c.ReduceNum; i++ {
		reduceTask := Task{
			TaskType:   ReduceType,
			TaskId:     c.generateTaskId(),
			InputFiles: getTmpFile(i, "main/mr-tmp"),
		}
		taskmeta := TaskMetaInfo{
			taskcondition: Waiting,
			TaskPtr:       &reduceTask,
			StartTime:     time.Now(),
		}
		c.TaskChanReduce <- &reduceTask
		c.MetaHolder.putTask(&taskmeta)
		fmt.Println("generate a reduce task, id=", reduceTask.TaskId)
	}
}

func getTmpFile(ReduceNum int, FilePath string) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path)
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(ReduceNum)+".json") {
			res = append(res, fi.Name())
		}
	}
	return res
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) nextPhase() {
	if c.CoordinatorCondition == MapPhase {
		fmt.Println("From map phase to reduce phase")
		c.generateReduceTask()
		c.CoordinatorCondition = ReducePhase
	} else if c.CoordinatorCondition == ReducePhase {
		fmt.Println("From reduce phase to done phase")
		c.CoordinatorCondition = DonePhase
	}
}

func (c *Coordinator) DistributeTask(req *DistributeTaskRequest, res *DistributeTaskResponse) error {
	mu.Lock()
	defer mu.Unlock()
	// fmt.Println((*req.MapF)("edae.txt", "3"))
	if c.CoordinatorCondition == MapPhase {
		if len(c.TaskChanMap) > 0 {
			res.ResTask = <-c.TaskChanMap
			c.MetaHolder.emitTask(res.ResTask.TaskId)
		} else {
			// res.ResTask.TaskType = WaitingType
			if c.MetaHolder.checkMapTaskFinish() {
				c.nextPhase()
			}
			return nil
		}
	} else if c.CoordinatorCondition == ReducePhase {
		if len(c.TaskChanReduce) > 0 {
			res.ResTask = <-c.TaskChanReduce
			c.MetaHolder.emitTask(res.ResTask.TaskId)
		} else {
			// res.ResTask.TaskType = WaitingType
			if c.MetaHolder.checkReduceTaskFinish() {
				c.nextPhase()
			}
			return nil
		}
	} else {
		task := Task{
			TaskType: KillType,
		}
		res.ResTask = &task
	}
	return nil
}

func (c *Coordinator) TaskIsDone(req *TaskIsDoneRequest, res *TaskIsDoneReponse) error {
	mu.Lock()
	defer mu.Unlock()
	switch req.ReqTask.TaskType {
	case MapType:
		meta := c.MetaHolder.getTaskMetaById(req.ReqTask.TaskId)
		if meta.taskcondition == Running {
			meta.taskcondition = Finish
			fmt.Printf("map task %s is finished\n", req.ReqTask.TaskId)
		} else {
			fmt.Println("[duplicated] map job done, id = ", req.ReqTask.TaskId)
		}
		break
	case ReduceType:
		meta := c.MetaHolder.getTaskMetaById(req.ReqTask.TaskId)
		if meta.taskcondition == Running {
			meta.taskcondition = Finish
			fmt.Printf("reduce task %s is finished\n", req.ReqTask.TaskId)
		} else {
			fmt.Println("[duplicated] reduce job done, id = ", req.ReqTask.TaskId)
		}
		break
	default:
		panic("error job done")
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := c.CoordinatorCondition == DonePhase
	return ret
}

func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.CoordinatorCondition == DonePhase {
			mu.Unlock()
			return
		}

		for _, v := range c.MetaHolder.MetaMap {
			if v.taskcondition == Waiting {
				fmt.Println("[crash check]: task", v.TaskPtr.TaskId, "is waiting")
			}
			if v.taskcondition == Running && time.Now().Sub(v.StartTime) > 5*time.Second && time.Now().Sub(v.StartTime) < 30*time.Second {
				fmt.Println("[crash check]: detect crash task ", v.TaskPtr.TaskId)
				switch v.TaskPtr.TaskType {
				case MapType:
					if c.CoordinatorCondition == MapPhase {
						fmt.Println("[crash check]: re-put a map task")
						c.TaskChanMap <- v.TaskPtr
						v.taskcondition = Waiting
					}
				case ReduceType:
					if c.CoordinatorCondition == ReducePhase {
						fmt.Println("[crash check]: re-put a reduce task")
						c.TaskChanReduce <- v.TaskPtr
						v.taskcondition = Waiting
					}
				}
			}
			// if v.taskcondition == Running && time.Now().Sub(v.StartTime) > 15*time.Second && time.Now().Sub(v.StartTime) < 30*time.Second {
			// 	Worker(*c.MapF, *c.ReduceF)
			// }
		}
		mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskChanMap:          make(chan *Task, len(files)),
		TaskChanReduce:       make(chan *Task, nReduce),
		ReduceNum:            nReduce,
		CoordinatorCondition: MapPhase,
		MetaHolder: TaskMetaHolder{
			MetaMap: make(map[string]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	c.server()
	c.generateMapTask(files, nReduce)
	go c.CrashHandler()
	return &c
}

func (t *TaskMetaHolder) putTask(taskInfo *TaskMetaInfo) bool {
	taskId := taskInfo.TaskPtr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("task meta already contains this task, id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = taskInfo
	}
	return true
}

func (t *TaskMetaHolder) getTaskMetaById(taskId string) *TaskMetaInfo {
	meta, ok := t.MetaMap[taskId]
	if !ok {
		log.Fatal("task meta not exist, id = ", taskId)
		return nil
	}
	return meta
}

func (t *TaskMetaHolder) emitTask(taskId string) bool {
	meta := t.getTaskMetaById(taskId)
	if meta == nil || meta.taskcondition != Waiting {
		fmt.Println("task can not emit, id = ", taskId)
		return false
	}
	meta.taskcondition = Running
	(*meta).StartTime = time.Now()
	return true
}

func (t *TaskMetaHolder) checkMapTaskFinish() bool {
	mapfinish := 0
	mapunfinish := 0
	for _, v := range t.MetaMap {
		if v.TaskPtr.TaskType == MapType {
			if v.taskcondition == Finish {
				mapfinish += 1
			} else {
				mapunfinish += 1
			}
		}
	}
	fmt.Printf("check map task, finish = %d, unfinish = %d\n", mapfinish, mapunfinish)
	return mapfinish > 0 && mapunfinish == 0
}

func (t *TaskMetaHolder) checkReduceTaskFinish() bool {
	reducefinish := 0
	reduceunfinish := 0
	for _, v := range t.MetaMap {
		if v.TaskPtr.TaskType == ReduceType {
			if v.taskcondition == Finish {
				reducefinish += 1
			} else {
				reduceunfinish += 1
			}
		}
	}
	fmt.Printf("check reduce task, finish = %d, unfinish = %d\n", reducefinish, reduceunfinish)
	return reducefinish > 0 && reduceunfinish == 0
}
