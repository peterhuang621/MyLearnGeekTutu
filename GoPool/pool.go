package gopool

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Job struct {
	Exec   JobFunc
	Params any
	Ctx    context.Context
	Cancel context.CancelFunc
	NameID string
}

type PoolConfig struct {
	WorkerNum       int
	JobQueueLen     int
	JobTimeout      time.Duration
	ErrorBufferSize int
	EnableTimeout   bool
	ParentContext   context.Context
}

type PoolState struct {
	NumJobs     int64
	NumFinished int64
	NumActive   int64
}

type Pool struct {
	jobs          chan Job
	quit          chan bool
	wg            sync.WaitGroup
	workerNum     int
	jobTimeout    time.Duration
	errors        chan error
	enableTimeout bool
	parentCtx     context.Context
	numJobs       int64
	numFinished   int64
}

type JobFunc func(context.Context, any) error

func NewPool(cfg *PoolConfig) *Pool {
	return &Pool{
		jobs:          make(chan Job, cfg.JobQueueLen),
		quit:          make(chan bool),
		wg:            sync.WaitGroup{},
		workerNum:     cfg.WorkerNum,
		jobTimeout:    cfg.JobTimeout,
		errors:        make(chan error, cfg.ErrorBufferSize),
		enableTimeout: cfg.EnableTimeout,
		parentCtx:     cfg.ParentContext,
		numJobs:       0,
		numFinished:   0,
	}
}

func (p *Pool) Start() {
	p.wg.Add(p.workerNum)
	for i := 0; i < p.workerNum; i++ {
		go func() {
			defer p.wg.Done()
			for {
				select {
				case job := <-p.jobs:
					done := make(chan error, 1)
					go func() {
						done <- job.Exec(job.Ctx, job.Params)
						defer close(done)
					}()

					if p.enableTimeout {
						select {
						case err := <-done:
							log.Printf("job %v done\n", job.NameID)
							if err != nil {
								p.errors <- err
							}
							atomic.AddInt64(&p.numFinished, 1)
						case <-job.Ctx.Done():
							log.Printf("job %v \x1b[31;1mexceeds time limit!\x1b[0m\n", job.NameID)
						}
					} else {
						err := <-done
						if err != nil {
							p.errors <- err
						}
						atomic.AddInt64(&p.numFinished, 1)
					}
					job.Cancel()

				case <-p.quit:
					return
				}
			}
		}()
	}
}

func (p *Pool) Stop() {
	for i := 0; i < p.workerNum; i++ {
		p.quit <- true
	}
	close(p.jobs)
	close(p.errors)
	p.wg.Wait()
}

func (p *Pool) AddJob(jobFunc JobFunc, params any, nameID string) {
	ctx, cancel := context.WithTimeout(p.parentCtx, p.jobTimeout)
	if len(nameID) == 0 {
		nameID = "<anonymous task>"
	}
	p.jobs <- Job{
		Exec:   jobFunc,
		Params: params,
		Ctx:    ctx,
		Cancel: cancel,
		NameID: nameID,
	}
	atomic.AddInt64(&p.numJobs, 1)
}

func (p *Pool) GetErrors() <-chan error {
	return p.errors
}

func (p *Pool) State() PoolState {
	return PoolState{
		NumJobs:     atomic.LoadInt64(&p.numJobs),
		NumFinished: atomic.LoadInt64(&p.numFinished),
		NumActive:   atomic.LoadInt64(&p.numJobs) - atomic.LoadInt64(&p.numFinished),
	}
}
