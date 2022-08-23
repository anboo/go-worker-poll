package github.com/anboo/go-worker-poll

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type task struct {
	jobFunc func(ctx context.Context) error
	timeout *time.Duration
}

type worker struct {
	ctx context.Context //timeout context or os context
}

//WorkerPool вынести в отдельный репозиторий?
type WorkerPool struct {
	name string

	wg       *sync.WaitGroup
	taskChan chan *task
	ctx      context.Context

	logger *zerolog.Logger

	workers []*worker

	maxWorkers int
	mutex      *sync.RWMutex
}

// NewWorkerPool create new WorkerPoll
// and create maxWorkers goroutines which wait for new tasks
func NewWorkerPool(ctx context.Context, name string, maxWorkers int, logger *zerolog.Logger) *WorkerPool {
	wp := &WorkerPool{
		name:     name,
		wg:       &sync.WaitGroup{},
		taskChan: make(chan *task, maxWorkers),
		ctx:      ctx,

		maxWorkers: maxWorkers,
		mutex:      &sync.RWMutex{},

		logger: logger,
	}

	for j := 0; j < maxWorkers; j++ {
		wp.RunWorker()
	}

	return wp
}

// RunWorker run goroutine and fetch all tasks
func (wp *WorkerPool) RunWorker() {
	wp.mutex.Lock()
	defer wp.mutex.Unlock()

	if len(wp.workers) < wp.maxWorkers {
		wp.workers = append(wp.workers, &worker{ctx: wp.ctx})
		wp.allocateNewWorkerGoroutine(wp.ctx)
	}
}

// AddTask delay execution function
func (wp *WorkerPool) AddTask(f func(ctx context.Context) error) {
	wp.addTask(&task{jobFunc: f})
}

// AddTaskWithTimeout delay execution function
func (wp *WorkerPool) AddTaskWithTimeout(f func(ctx context.Context) error, timeout time.Duration) {
	wp.addTask(&task{timeout: &timeout, jobFunc: f})
}

// AddTask delay execution function
func (wp *WorkerPool) addTask(t *task) {
	wp.wg.Add(1)
	wp.taskChan <- t
}

// Wait lock current process while free all workers
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

func (wp *WorkerPool) createJobFuncWrapper() func(ctx context.Context, job func(ctx context.Context) error) {
	return func(ctx context.Context, job func(ctx context.Context) error) {
		defer wp.wg.Done()

		err := job(ctx)
		if err != nil {
			wp.logger.
				Err(err).
				Str("worker", wp.name).
				Msg("worker pool error")
		}
	}
}

func (wp *WorkerPool) allocateNewWorkerGoroutine(ctx context.Context) {
	go func(ctx context.Context) {
		for {
			select {
			case t := <-wp.taskChan:
				job := wp.createJobFuncWrapper()

				if t.timeout != nil {
					res := make(chan struct{}, 1)

					timeoutCtx, cancel := context.WithTimeout(wp.ctx, *t.timeout)

					go func(f func(ctx context.Context) error) {
						job(timeoutCtx, f)
						res <- struct{}{}
					}(t.jobFunc)

					select {
					case <-timeoutCtx.Done():
						wp.logger.
							Err(timeoutCtx.Err()).
							Str("worker", wp.name).
							Str("timeout", t.timeout.String()).
							Msg("worker pool timeout")

						cancel()
					case <-res:
						cancel() //Fix context goroutine leak
						close(res)
					}
				} else {
					job(wp.ctx, t.jobFunc)
				}
				break
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
}
