package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	freeWkChan := make(chan string) // 用户获取空闲的worker
	cancel := make(chan struct{})

	go func() {
	outer1:
		for {
			select {
			case wk, ok := <-registerChan:
				if !ok {
					break outer1
				}
				freeWkChan <- wk
			case <-cancel:
				break outer1
			}

		}
	}()
	wg := sync.WaitGroup{}
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		go func(i int) {
			args := DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[i],
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: n_other,
			}

			for {
				svc := <-freeWkChan
				if !call(svc, "Worker.DoTask", args, nil) {
					log.Printf("worker %s failed to handle task %d, re-assign...", svc, i)
					continue
				}
				wg.Done()
				select {
				case freeWkChan <- svc:
				case <-cancel:
				}
				break
			}
		}(i)
	}
	// wait for all task completing
	wg.Wait()
	close(cancel)

	fmt.Printf("Schedule: %v done\n", phase)
}
