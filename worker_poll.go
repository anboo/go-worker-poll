package main

import (
	"sync"
)

type WorkerPool struct {
	wg       *sync.WaitGroup
	taskChan chan func()
}

// NewWorkerPool create new WorkerPoll
// and create maxConcurrentlyWorkers goroutines which wait for new tasks
func NewWorkerPool(maxConcurrentlyWorkers int) *WorkerPool {
	wg := &WorkerPool{
		wg:       &sync.WaitGroup{},
		taskChan: make(chan func()),
	}

	for j := 0; j < maxConcurrentlyWorkers; j++ {
		wg.RunWorker()
	}

	return wg
}

// RunWorker run goroutine and fetch all tasks
func (wp *WorkerPool) RunWorker() {
	go func(wg *sync.WaitGroup) {
		for task := range wp.taskChan {
			func() {
				defer wg.Done()

				// if task will throw panic
				// we do wg.Done() anyway
				defer func() {
					if recover() != nil {
					}
				}()

				task()
			}()
		}
	}(wp.wg)
}

// AddTask delay execution function
func (wp *WorkerPool) AddTask(f func()) {
	wp.wg.Add(1)
	wp.taskChan <- f
}

// StopAndWait lock current process while free all workers
func (wp *WorkerPool) StopAndWait() {
	wp.wg.Wait()
}
