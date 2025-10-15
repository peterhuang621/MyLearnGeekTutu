package main

import (
	"context"
	"fmt"
	. "gopool"
	"log"
	"math/rand"
	"time"
)

var pool *Pool

func main() {
	cfg := &PoolConfig{
		WorkerNum:       8,
		JobQueueLen:     20,
		JobTimeout:      time.Second * 3,
		ErrorBufferSize: 10,
		EnableTimeout:   true,
		ParentContext:   context.Background(),
	}
	pool = NewPool(cfg)

	var joba JobFunc = func(ctx context.Context, foradding any) error {
		if x, ok := foradding.(int); ok {
			for i := 0; i < x; i++ {
				log.Printf("increasing jobs from %d", i)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(30)))
			}
		}
		return nil
	}
	var jobb JobFunc = func(ctx context.Context, foradding any) error {
		if x, ok := foradding.(int); ok {
			for i := x - 1; i >= 0; i-- {
				log.Printf("decreasing jobs from %d", i)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(20)))
			}
		}
		return nil
	}
	num := 8
	pool.AddJob(joba, num, fmt.Sprintf("doing joba for %d times!!! - %s", num, time.Now().Local().String()))
	num = 10
	pool.AddJob(jobb, num, fmt.Sprintf("doing jobb for %d times!!! - %s", num, time.Now().Local().String()))
	pool.Start()
	for i := 0; i < 10; i++ {
		sta := pool.State()
		log.Printf("current state at %d second: %v", i, sta)
		if sta.NumActive == 0 {
			log.Printf("\x1b[32;1;4mall tasks have finished already, test program out \x1b[0m")
			break
		}
		time.Sleep(time.Second)
	}

	go func() {
		for err := range pool.GetErrors() {
			log.Printf("\x1b[31;1m;ERROR: %v\x1b[0m\n", err)
		}
	}()

	pool.Stop()

	log.Printf("\x1b[32;1;4mpool stopped! final state is: %v\x1b[0m\n", pool.State())
}
