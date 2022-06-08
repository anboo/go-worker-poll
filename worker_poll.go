package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type task struct {
	f func(ctx context.Context)
	d *time.Duration
}

type worker struct {
	processed uint64
	lastUsage uint64
	localCtx  *context.Context
}

type WorkerPool struct {
	wg       *sync.WaitGroup
	taskChan chan task
	ctx      context.Context

	workers []*worker

	maxConcurrentlyWorkers int
	mWorkers               *sync.RWMutex
}

// NewWorkerPool create new WorkerPoll
// and create maxConcurrentlyWorkers goroutines which wait for new tasks
// if you use timeout functional worker pool will need run +2 goroutine for checking it (work goroutine and context.WithCancel goroutine)
func NewWorkerPool(maxConcurrentlyWorkers int, ctx context.Context) *WorkerPool {
	wp := &WorkerPool{
		wg:       &sync.WaitGroup{},
		taskChan: make(chan task),
		ctx:      ctx,

		maxConcurrentlyWorkers: maxConcurrentlyWorkers,
		mWorkers:               &sync.RWMutex{},
	}

	for j := 0; j < maxConcurrentlyWorkers; j++ {
		wp.RunWorker()
	}

	return wp
}

func (wp *WorkerPool) createJobFuncWrapper() func(ctx context.Context, t func(ctx context.Context)) {
	return func(ctx context.Context, t func(ctx context.Context)) {
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
}

func (wp *WorkerPool) allocateNewWorkerGoroutine(ctx context.Context) {
	go func(ctx context.Context) {
		for {
			select {
			case t := <-wp.taskChan:
				job := wp.createJobFuncWrapper()

				if t.d != nil {
					res := make(chan bool, 1)

					c, cancel := context.WithTimeout(wp.ctx, *t.d)

					go func(f func(ctx context.Context)) {
						job(c, f)
						res <- true
					}(t.f)

					select {
					case <-c.Done():
						fmt.Println("worker poll error timeout " + c.Err().Error())
						cancel()
					case <-res:
						cancel() //Fix context goroutine leak
						close(res)
					}
				} else {
					job(wp.ctx, t.f)
				}
				break
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
}

// RunWorker run goroutine and fetch all tasks
func (wp *WorkerPool) RunWorker() {
	wp.mWorkers.Lock()
	defer wp.mWorkers.Unlock()

	if len(wp.workers) < wp.maxConcurrentlyWorkers {
		ctx := context.Background()
		wp.workers = append(wp.workers, &worker{localCtx: &ctx})
		wp.allocateNewWorkerGoroutine(ctx)
	}
}

// AddTask delay execution function
func (wp *WorkerPool) AddTask(f func(ctx context.Context), duration *time.Duration) {
	wp.wg.Add(1)
	wp.taskChan <- task{d: duration, f: f}
}

// StopAndWait lock current process while free all workers
func (wp *WorkerPool) StopAndWait() {
	wp.wg.Wait()
}
