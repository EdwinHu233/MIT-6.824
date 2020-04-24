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
	MapStatus        []TaskStatus
	ReduceStatus     []TaskStatus
	InputFiles       []string
	NumIdleMap       int
	NumWorkingMap    int
	NumIdleReduce    int
	NumWorkingReduce int
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
	return m.NumIdleMap == 0 &&
		m.NumWorkingMap == 0 &&
		m.NumIdleReduce == 0 &&
		m.NumWorkingReduce == 0
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
		m.NumIdleMap += 1
		m.NumWorkingMap -= 1
		log.Printf("master: map task %v timeout\n", taskID)
	}
	m.Unlock()
}

func (m *Master) waitReduce(taskID int) {
	time.Sleep(MaxWaitTime)
	m.Lock()
	if m.ReduceStatus[taskID] == Working {
		m.ReduceStatus[taskID] = Idle
		m.NumIdleReduce += 1
		m.NumWorkingReduce -= 1
		log.Printf("master: reduce task %v timeout\n", taskID)
	}
	m.Unlock()
}

func (m *Master) TaskRequest(args *Empty, reply *Task) error {
	m.Lock()
	defer m.Unlock()
	if m.NumIdleMap != 0 {
		for i, s := range m.MapStatus {
			if s == Idle {
				*reply = Task(i)
				m.NumIdleMap -= 1
				m.NumWorkingMap += 1
				m.MapStatus[i] = Working
				go m.waitMap(i)
				log.Printf("master: assign map task %v\n", i)
				return nil
			}
		}
	} else if m.NumWorkingMap == 0 && m.NumIdleReduce != 0 {
		for i, s := range m.ReduceStatus {
			if s == Idle {
				*reply = Task(i + len(m.MapStatus))
				m.NumIdleReduce -= 1
				m.NumWorkingReduce += 1
				m.ReduceStatus[i] = Working
				go m.waitReduce(i)
				log.Printf("master: assign reduce task %v\n", i)
				return nil
			}
		}
	}
	*reply = Task(-1)
	// log.Printf("master: suspend worker\n")
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
		log.Printf("master: finished map %v\n", n)
		s := m.MapStatus[n]
		if s == Idle {
			m.NumIdleMap -= 1
		} else if s == Working {
			m.NumWorkingMap -= 1
		}
		m.MapStatus[n] = Done
	} else {
		n -= len(m.MapStatus)
		log.Printf("master: finished reduce %v\n", n)
		s := m.ReduceStatus[n]
		if s == Idle {
			m.NumIdleReduce -= 1
		} else if s == Working {
			m.NumWorkingReduce -= 1
		}
		m.ReduceStatus[n] = Done
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
	m.NumIdleMap = len(files)
	m.NumIdleReduce = nReduce
	log.Println("created master:", m.MapStatus, m.ReduceStatus, m.NumIdleMap, m.NumWorkingMap, m.NumIdleReduce, m.NumWorkingReduce)
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
