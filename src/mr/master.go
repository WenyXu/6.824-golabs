package mr

import (
	"github.com/kataras/iris/core/errors"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"



type Master struct {
	// Your definitions here.
	Initialized bool
	activeWorkers map[string]*worker
	todoTasks []task
	mapWorkers int
	reduceWorkers int
	mapTasks int
	reduceTasks int
	nReduce int
	mu sync.RWMutex
}

// Your code here -- RPC handlers for the Worker to call.
func (m *Master) Init(files []string, nReduce int) {
	//Initialize variables
	m.activeWorkers = map[string]*worker{}
	m.todoTasks=[]task{}
	m.nReduce=nReduce
	m.InitStatus()
	m.CreateLocalFiles(files,nReduce)
	m.Initialized=true
}
func (m *Master) AddTask2TodoByStrings(action string,files ...string) {
	var tasks []task
	for _,file:=range files {
		tasks=append(tasks,task{
			Action: action,
			File:   file,
		})
	}
	log.Printf("add tasks %s",tasks)
	//Add tasks to todoTasks
	m.mu.Lock()
	switch action {
		case "map":
			m.todoTasks=append(tasks,m.todoTasks...)
		break
		case "reduce":
			m.todoTasks=append(m.todoTasks,tasks...)
		break
	}
	m.mu.Unlock()
	log.Printf("tasks: %s",m.todoTasks)

	// Todo : any other good solutions
	m.UpdateStatus()
}
func (m *Master) UpdateStatus(){
	go func() {
		m.InitStatus()
		m.mu.Lock()
		for _,w:=range m.activeWorkers{
			if w.Task !=nil{
				switch w.Task.Action {
				case "map":
					m.mapWorkers+=1
					break
				case "reduce":
					m.reduceWorkers+=1
					break
				}
			}
		}
		for _,t:=range m.todoTasks{
			switch t.Action {
			case "map":
				m.mapTasks+=1
				break
			case "reduce":
				m.reduceTasks+=1
				break
			}
		}
		m.mu.Unlock()
		//log.Printf("[Master] Map Workers %d,Map Todo Tasks %d,Reduce Workers %d,Reduce Todo Tasks %d",
		//	m.mapWorkers,m.mapTasks,m.reduceWorkers,m.reduceTasks)
	}()
}
func (m *Master) InitStatus()  {
	m.mu.Lock()
	m.mapTasks=0
	m.mapWorkers=0
	m.reduceTasks=0
	m.reduceWorkers=0
	m.mu.Unlock()
}
func (m *Master) CreateLocalFiles(files []string, nReduce int)  {
	var tmpMaps []string
	for i := 0;i != nReduce; i++ {
		output :="mr-out-"+strconv.Itoa(i+1)
		tmpMap :="mr-tmp-map-"+strconv.Itoa(i+1)
		tmpMaps=append(tmpMaps,tmpMap)
		// Create File
		if _,e:=os.Create(output);e!=nil{
			log.Printf("[Master] Create [%s] File Success",output)
		}
		if _,e:=os.Create(tmpMap);e!=nil{
			log.Printf("[Master] Create [%s] File Success",tmpMap)
		}
	}
	// Add tasks to TodoTask
	m.AddTask2TodoByStrings("map",files...)
	m.AddTask2TodoByStrings("reduce",tmpMaps...)

}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Auth(worker *worker) error  {
	if time.Now().After(worker.TaskTimeout) {
		return errors.New("[Master] timeout ")
	}
	return  nil
}
func (m *Master) RemoveWorker(worker *worker) {
	m.mu.Lock()
	if _,ok:=m.activeWorkers[worker.UUID];ok{
		delete(m.activeWorkers,worker.UUID)
	}
	m.mu.Unlock()
	m.UpdateStatus()

}
func (m *Master) RemoveTask(task *task) {
	if task ==nil {
		return
	}
	m.mu.Lock()
	for i,t:=range m.todoTasks{
		if t.File==task.File &&t.Action==task.Action{
			log.Printf("[Master] Remove finished task")
			m.todoTasks=append(m.todoTasks[0:i],m.todoTasks[i+1:]...)
		}
	}
	m.mu.Unlock()
	m.UpdateStatus()
}
func (m *Master) AddActiveWorkerAndAssignATask(currentWorker *worker) (NextWorker *worker) {
	NextWorker=nil
	m.mu.Lock()
	if len(m.todoTasks)>0{
		t:=m.todoTasks[0]
		m.todoTasks=m.todoTasks[1:]
		w:=worker{
			UUID:        currentWorker.UUID,
			Status:      "processing",
			Task:        &t,
			TaskTimeout: time.Now().Add(time.Second*10),
		}
		m.activeWorkers[currentWorker.UUID]=&w
		NextWorker=&w
	}
	m.mu.Unlock()
	//update Status here
	m.UpdateStatus()
	return
}
func (m *Master) Sync(args *Args,reply *Reply) error{

	switch args.Worker.Status {
		case "idle":
			//assign a mission to the Worker
		reply.NextWorker=
			m.AddActiveWorkerAndAssignATask(args.Worker)
		break
		case "processing":
			//sync the Status only
			if e:=m.Auth(args.Worker) ;e!=nil{
				return e
			}
		break
		case "done":
			m.RemoveWorker(args.Worker)
			m.RemoveTask(args.Worker.Task)
			reply.NextWorker=&worker{
				UUID:        args.Worker.UUID,
				Status:      "idle",
				TaskTimeout: time.Time{},
				Task:        nil,
			}
		break
	}
	//Sync the Status
	reply.NReduce=m.nReduce
	reply.IsMapFinished=false
	reply.IsAllFinished=false
	m.mu.Lock()
	if m.mapTasks==0&&m.mapWorkers==0&&m.Initialized{
		reply.IsMapFinished=true
	}
	if m.mapWorkers==0&&m.mapTasks==0&&m.reduceTasks==0&&m.reduceWorkers==0&&m.Initialized{
		reply.IsAllFinished=true
	}
	m.mu.Unlock()
	return nil
}
func (m *Master) Checker (){
	go func() {
		for{
			log.Printf("[Master] [Checker] Map Workers %d,Map Todo Tasks %d,Reduce Workers %d,Reduce Todo Tasks %d",
				m.mapWorkers,m.mapTasks,m.reduceWorkers,m.reduceTasks)
			//find the timeout workers and remove them
			m.mu.Lock()
			for _,w:=range m.activeWorkers{
				if time.Now().After(w.TaskTimeout){
					log.Printf("[Master] remove timeout worker:%v",w)
					go m.RemoveWorker(w)
					go m.AddTask2TodoByStrings(w.Task.Action,w.Task.File)
				}
			}
			m.mu.Unlock()
			time.Sleep(time.Second)
		}

	}()
}

//
// start a thread that listens for RPCs from Worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if m.mapWorkers==0&&m.mapTasks==0&&m.reduceTasks==0&&m.reduceWorkers==0&&m.Initialized{
		ret=true
	}
	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.Init(files,nReduce)
	m.Checker()
	// Your code here.


	m.server()
	return &m
}
