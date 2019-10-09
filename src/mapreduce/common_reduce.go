package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var (
		kvsM = make(map[string]*KeyValues)
		kvsS = make([]*KeyValues, 0)
		kv   = new(KeyValue)
	)
	for i := 0; i < nMap; i++ {
		fname := reduceName(jobName, i, reduceTask)
		fd, err := os.Open(fname)
		if err != nil {
			log.Fatalf("failed to open file %s: %v", fname, err)
		}
		defer fd.Close()
		decoder := json.NewDecoder(fd)
		for decoder.More() {
			decoder.Decode(kv)
			kvs, ok := kvsM[kv.Key]
			if !ok {
				kvs = new(KeyValues)
				kvs.Key = kv.Key
				kvsM[kv.Key] = kvs
				kvsS = append(kvsS, kvs)
			}
			kvs.Values = append(kvs.Values, kv.Value)
			kv.Key = ""
			kv.Value = ""
		}
	}
	sort.Slice(kvsS, func(i, j int) bool {
		return kvsS[i].Key < kvsS[j].Key
	})

	resultFile, err := os.OpenFile(outFile,os.O_CREATE|os.O_RDWR|os.O_TRUNC,0777)
	if err != nil {
		log.Printf("failed to create or open file: %v", err)
		return
	}
	defer resultFile.Close()
	encoder := json.NewEncoder(resultFile)
	for _, kvs := range kvsS {
		ret := reduceF(kvs.Key, kvs.Values)
		if err := encoder.Encode(&KeyValue{Key: kvs.Key, Value: ret}); err != nil {
			log.Printf("failed to encode and write reduce result: %v", err)
		}
	}

}

type KeyValues struct {
	Key    string
	Values []string
}
