package main

import (
	"context"
	"log"
	"sync"
	"time"
)

type Job struct {
	Exec   func(context.Context, any) error
	Params any
	Ctx    context.Context
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

type Pool struct {
	jobs          chan Job
	quit          chan bool
	wg            sync.WaitGroup
	workerNum     int
	jobTimeout    time.Duration
	errors        chan error
	enableTimeout bool
	parentCtx     context.Context
}

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
	}
}

func (p *Pool) Start() {
	p.wg.Add(p.workerNum)
	for i := 0; i < p.workerNum; i++ {
		go func() {
			for {
				select {
				case job := <-p.jobs:
					done := make(chan error, 1)
					go func() {
						done <- job.Exec(job.Ctx, job.Params)
					}()

					select {
					case err := <-done:
						log.Printf("job %v done with err: %s\n", job.NameID, err.Error())
						if err != nil {
							p.errors <- err
						}
					case <-job.Ctx.Done():
						log.Printf("job %v exceeds time limit!\n", job.NameID)
					}

				case <-p.quit:
					p.wg.Done()
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
	p.wg.Wait()
}

func (p *Pool) AddJob(jobFunc func(context.Context, any) error, params any, nameID string) {
	ctx, cancel := context.WithTimeout(context.Background(), p.jobTimeout)
	defer cancel()
	if len(nameID) == 0 {
		nameID = "<anonymous task>"
	}
	p.jobs <- Job{
		Exec:   jobFunc,
		Params: params,
		Ctx:    ctx,
		NameID: nameID,
	}
}

func (p *Pool) GetErrors() <-chan error {
	return p.errors
}
