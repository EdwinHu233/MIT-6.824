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

type TaskStatus int

const (
	Idle    TaskStatus = iota
	Working            = iota
	Done               = iota
)

const MaxWaitTime = 10 * time.Second

type Master struct {
	sync.Mutex
	MapStatus    []TaskStatus
	ReduceStatus []TaskStatus
	InputFiles   []string
	AllDone      bool
}

// Your code here -- RPC handlers for the worker to call.

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.Lock()
	defer m.Unlock()
	return m.AllDone
}

func (m *Master) ReadConfig(args *Empty, reply *ReadConfigReply) error {
	reply.NumMap = len(m.MapStatus)
	reply.NumReduce = len(m.ReduceStatus)
	reply.InputFiles = m.InputFiles
	return nil
}

func (m *Master) waitMap(taskID int) {
	time.Sleep(MaxWaitTime)
	m.Lock()
	if m.MapStatus[taskID] == Working {
		m.MapStatus[taskID] = Idle
		log.Printf("master: map task %v timeout\n", taskID)
	}
	m.Unlock()
}

func (m *Master) waitReduce(taskID int) {
	time.Sleep(MaxWaitTime)
	m.Lock()
	if m.ReduceStatus[taskID] == Working {
		m.ReduceStatus[taskID] = Idle
		log.Printf("master: reduce task %v timeout\n", taskID)
	}
	m.Unlock()
}

func (m *Master) TaskRequest(args *Empty, reply *Task) error {
	m.Lock()
	defer m.Unlock()

	for i, s := range m.MapStatus {
		if s == Idle {
			*reply = Task(i)
			m.MapStatus[i] = Working
			log.Printf("master: handout map %v\n", i)
			go m.waitMap(i)
			return nil
		}
	}

	for i, s := range m.ReduceStatus {
		if s == Idle {
			*reply = Task(i + len(m.MapStatus))
			m.ReduceStatus[i] = Working
			log.Printf("master: handout reduce %v\n", i)
			go m.waitReduce(i)
			return nil
		}
	}

	m.AllDone = true
	*reply = -1
	return nil
}

func (m *Master) TaskDone(args *Task, reply *Empty) error {
	m.Lock()
	defer m.Unlock()
	n := int(*args)
	if n < 0 || n >= len(m.MapStatus)+len(m.ReduceStatus) {
		return nil
	}
	if n < len(m.MapStatus) {
		m.MapStatus[n] = Done
		log.Printf("master: map task %v done\n", n)
	} else {
		n -= len(m.MapStatus)
		m.ReduceStatus[n] = Done
		log.Printf("master: reduce task %v done\n", n)
	}
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.MapStatus = make([]TaskStatus, len(files))
	m.ReduceStatus = make([]TaskStatus, nReduce)
	m.InputFiles = files

	m.server()
	return &m
}

// start a thread that listens for RPCs from worker.go
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
