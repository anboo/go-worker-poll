package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type WorkerPool struct {
	wg       *sync.WaitGroup
	taskChan chan func(ctx context.Context)
	ctx      context.Context
	duration *time.Duration

	mDuration *sync.RWMutex
}

func (wp *WorkerPool) Timeout(duration time.Duration) {
	wp.mDuration.Lock()
	wp.duration = &duration
	wp.mDuration.Unlock()
}

// NewWorkerPool create new WorkerPoll
// and create maxConcurrentlyWorkers goroutines which wait for new tasks
// if you use timeout functional worker pool will need run +2 goroutine for checking it (timeout goroutine and context.WithCancel goroutine)
func NewWorkerPool(maxConcurrentlyWorkers int, ctx context.Context) *WorkerPool {
	wp := &WorkerPool{
		wg:        &sync.WaitGroup{},
		taskChan:  make(chan func(ctx context.Context)),
		ctx:       ctx,
		mDuration: &sync.RWMutex{},
	}

	for j := 0; j < maxConcurrentlyWorkers; j++ {
		wp.RunWorker()
	}

	return wp
}

// RunWorker run goroutine and fetch all tasks
func (wp *WorkerPool) RunWorker() {
	go func() {
		for task := range wp.taskChan {
			job := func(ctx context.Context, t func(ctx context.Context)) {
				defer wp.wg.Done()

				// if task will throw panic
				// we do wg.Done() anyway
				defer func() {
					r := recover()

					if r != nil {
						fmt.Println("panic from task worker poll ", r)
					}
				}()

				t(ctx)
			}

			wp.mDuration.RLock()
			if wp.duration != nil {
				res := make(chan bool, 1)

				lCtx, cancel := context.WithCancel(wp.ctx)

				go func(task func(ctx context.Context)) {
					job(lCtx, task)
					res <- true
					close(res)
				}(task)

				select {
				case <-time.After(*wp.duration):
					wp.mDuration.RUnlock()
					fmt.Println("worker poll error timeout")
					cancel()
					continue
				case <-res:
					wp.mDuration.RUnlock()
					cancel()
					close(res) //Fix context leak
				}
			} else {
				wp.mDuration.RUnlock()
				job(wp.ctx, task)
			}
		}
	}()
}

// AddTask delay execution function
func (wp *WorkerPool) AddTask(f func(ctx context.Context)) {
	wp.wg.Add(1)
	wp.taskChan <- f
}

// StopAndWait lock current process while free all workers
func (wp *WorkerPool) StopAndWait() {
	wp.wg.Wait()
}
