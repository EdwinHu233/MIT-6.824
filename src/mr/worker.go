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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func intermediateFile(mapID, reduceID int) string {
	return fmt.Sprintf("mr-%v-%v", mapID, reduceID)
}

func finalFile(reduceID int) string {
	return fmt.Sprintf("mr-out-%v", reduceID)
}

func readConfig() ReadConfigReply {
	args := Empty{}
	reply := ReadConfigReply{}
	call("Master.ReadConfig", &args, &reply)
	return reply
}

func doMapTask(mapf func(string, string) []KeyValue,
	id int, inputFile string, numReduce int) {
	// open and read the file
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("worker: cannot open %v", inputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("worker: cannot read %v", inputFile)
	}
	file.Close()
	// get results and split them to buckets with size of 'numReduce'
	kva := mapf(inputFile, string(content))
	output := make([][]KeyValue, numReduce)
	for _, kv := range kva {
		x := ihash(kv.Key) % numReduce
		output[x] = append(output[x], kv)
	}
	// write each bucket to file
	for i := 0; i < numReduce; i += 1 {
		file, err := ioutil.TempFile(".", ".tmp-intermediate-*.txt")
		if err != nil {
			log.Fatalf("worker: cannot open temporary file %s\n", file.Name())
		}
		enc := json.NewEncoder(file)
		for _, kv := range output[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("worker: cannot encode %v to json\n", kv)
			}
		}
		file.Close()
		os.Rename(file.Name(), intermediateFile(id, i))
	}
}

func readIntermediateFile(mapID int, reduceID int, kva *[]KeyValue) bool {
	fileName := intermediateFile(mapID, reduceID)
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return false
	}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("worker: cannot open intermediate file %v\n", fileName)
	}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		*kva = append(*kva, kv)
	}
	file.Close()
	return true
}

func readAllIntermediateFiles(reduceID int, numMap int) []KeyValue {
	kva := []KeyValue{}
	numRead := 0
	read := make([]bool, numMap)
	for numRead < numMap {
		for i := 0; i < numMap; i += 1 {
			if read[i] {
				continue
			}
			if readIntermediateFile(i, reduceID, &kva) {
				read[i] = true
				numRead += 1
			}
		}
		time.Sleep(time.Microsecond * 50)
	}
	return kva
}

func doReduceTask(reducef func(string, []string) string,
	id int, numMap int) {
	kva := readAllIntermediateFiles(id, numMap)
	sort.Sort(ByKey(kva))
	file, err := ioutil.TempFile(".", "tmp-final-*.txt")
	if err != nil {
		log.Fatalf("worker: cannot create temporary file\n")
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}
	file.Close()
	os.Rename(file.Name(), finalFile(id))
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	config := readConfig()
	// Your worker implementation here.
	var args Empty
	var reply Task
	for call("Master.TaskRequest", &args, &reply) {
		n := int(reply)
		if n < 0 {
			return
		}
		if n < config.NumMap {
			doMapTask(mapf, n, config.InputFiles[n], config.NumReduce)
		} else if n < config.NumMap+config.NumReduce {
			n -= config.NumMap
			doReduceTask(reducef, n, config.NumMap)
		} else {
			log.Fatalf("worker: invalid task %v\n", n)
		}
	}
}

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
