package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type IJ struct {
	I int
	J int
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
var (
	wg sync.WaitGroup
	s =&service{}
)

type service struct {
	currentWorker *worker
	w chan *worker
	NReduce int
	IsMapFinished bool
	IsAllFinished bool
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}
func (s *service) Sync(){
	if s.currentWorker==nil{
		s.Reset()
		return
	}
	args:=Args{Worker: s.currentWorker}
	reply:=Reply{}
	if !call("Master.Sync",&args,&reply) {
		//s.Init(s.mapf,s.reducef)
		os.Exit(0)
	}
	//Sync here
	if reply.NextWorker==nil{
		//log.Printf("Received a nil NextWorker")
	}else {
		s.currentWorker=reply.NextWorker
		// a new mission here
		s.w<-reply.NextWorker
	}
	s.NReduce=reply.NReduce
	s.IsAllFinished=reply.IsAllFinished
	s.IsMapFinished=reply.IsMapFinished
	if reply.IsAllFinished{
		wg.Done()
	}
}
func (s *service) Reset()  {
	s.currentWorker.Status="idle"
	s.currentWorker.Task=nil
	s.currentWorker.TaskTimeout=time.Time{}
}
func (s *service) Init(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string)  {
	s.w=make(chan *worker)
	s.currentWorker =&worker{
		UUID:   UUIDgen(),
		Status: "idle",
		Task:   nil,
	}
	s.mapf=mapf
	s.reducef=reducef
}
func (s *service) Reduce(){
	t:=s.currentWorker.Task

	var intermediate []KeyValue
	intermediateFile,_:=os.Open(t.File)
	dec := json.NewDecoder(intermediateFile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	sort.Sort(ByKey(intermediate))

	// read the i.File
	localCache:="[inter-tmp]"+t.File
	file,err:=os.OpenFile(localCache,os.O_WRONLY|os.O_TRUNC, os.ModeAppend)
	defer func(){ _ = file.Close() }()
	ij:=IJ{}
	if err!=nil && os.IsNotExist(err) {
		file, _ = os.Create(localCache)
		ij.I=0
		ij.J=0
	} else {
		dec := json.NewDecoder(file)
		_ = dec.Decode(&ij)
	}
	for ij.I<len(intermediate) {
		enc:=json.NewEncoder(file)
		_ = enc.Encode(&ij)

		ij.J=ij.I+1
		for ij.J < len(intermediate) && intermediate[ij.J].Key == intermediate[ij.I].Key {
			ij.J++
		}
		var values []string
		for k := ij.I; k < ij.J; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := s.reducef(intermediate[ij.I].Key, values)

		s:="mr-out-"+strconv.Itoa(ihash(intermediate[ij.I].Key)%s.NReduce+1)
		outputFile, err := os.OpenFile(s,os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err!=nil{
			log.Print(err)
		}
		// this is the correct format for each line of Reduce Output.
		_, e := fmt.Fprintf(outputFile, "%v %v\n", intermediate[ij.I].Key, output)
		if e!=nil{
			log.Print(e)
		}
		_ = outputFile.Close()
		ij.I = ij.J
	}
}
func (s *service) Map(){
	t:=s.currentWorker.Task
	file, err := os.Open(t.File)
	if err != nil {
		log.Fatalf("cannot open %v", file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	_ = file.Close()
	// if not crash here , everything will be ok
	kva:=s.mapf(t.File,string(content))
	// Todo: maybe there are some problems here,sometime will I/O caused the worker timeout
	// Todo: you need to tell the master which intermediate files are success
	// e.g mr-[worker-uuid]-[part-num]
	// then the reduce func will read all files which has same [part-num]
	for _, kv := range kva {
		s:="mr-tmp-map-"+strconv.Itoa(ihash(kv.Key)% s.NReduce+1)
		outputFile, err := os.OpenFile(s,os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err!=nil{
			log.Print(err)
		}
		enc:=json.NewEncoder(outputFile)
		e:= enc.Encode(&kv)
		if e!=nil{
			log.Print(e)
		}
		_ = outputFile.Close()
	}
}
func (s *service) Schedule(){
	go func() {
		for {
			s.Sync()
			// deal with the Task
			time.Sleep(time.Second)
		}

	}()
}

func UUIDgen() (uuid string){
	// generate 32 bits timestamp
	unix32bits := uint32(time.Now().UTC().Unix())

	buff := make([]byte, 12)

	numRead, err := rand.Read(buff)

	if numRead != len(buff) || err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x-%x\n", unix32bits, buff[0:2], buff[2:4], buff[4:6], buff[6:8], buff[8:])
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your Worker implementation here.
	s.Init(mapf,reducef)
	wg.Add(1)

	s.Schedule()
	go func() {
		for worker:=range s.w{
			wg.Add(1)
			if t:=worker.Task;t!=nil {

				log.Printf("[Worker] Working with a %s Task using %s File",t.Action,t.File)
				switch s.currentWorker.Task.Action {
				case "map":
					s.Map()
					break
				case "reduce":
					for {
						if !s.IsMapFinished {
							log.Printf("[Worker] Waiting for map workers done")
							time.Sleep(time.Second)
						}else{
							break
						}
					}
					s.Reduce()
					break
				}
				log.Printf("[Worker] Finished %s Task using %s File",t.Action,t.File)
				s.currentWorker.Status="done"
			}
			wg.Done()
		}
	}()
	wg.Wait()
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
