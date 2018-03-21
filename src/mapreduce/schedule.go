package mapreduce

import (
	"fmt"
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
	var wg sync.WaitGroup
	// Split task to worker
	for taskNum := 0; taskNum < ntasks; taskNum++ {
		taskArgs := DoTaskArgs{jobName, mapFiles[taskNum], phase, taskNum, n_other}
		wg.Add(1)

		go func() {
			defer wg.Done()
			// part_4
		CallDoTask:
			worker := <-registerChan
			// Do work
			response := call(worker, "Worker.DoTask", taskArgs, new(struct{}))
			if response == false {
				fmt.Printf("Worker: RPC %s DoTask error\n", worker)
				// Handling fallover for part_4
				go func() { registerChan <- worker }()
				goto CallDoTask
			}
			// Give worker back to registerChan
			go func() { registerChan <- worker }()
		}()
	}
	// Wait untill all task finished
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
