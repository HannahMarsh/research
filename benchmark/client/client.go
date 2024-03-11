package client

import (
	bconfig "benchmark/config"
	"benchmark/db"
	"benchmark/workload"
	"context"
	"sync"
	"time"
)

type Client struct {
	p        *bconfig.Config
	workload *workload.Workload
	db       db.DB
	cache    *CacheWrapper
}

// NewClient returns a client with the given workload and DB.
// The workload and db can't be nil.
func NewClient(p *bconfig.Config, workload *workload.Workload, db db.DB, cache_ *CacheWrapper) *Client {
	return &Client{p: p, workload: workload, db: db, cache: cache_}
}

// Run runs the workload to the target DB, and blocks until all workers end.
func (c *Client) Run(ctx context.Context) {
	var wg sync.WaitGroup

	var totalOpCount = int64((c.p.Workload.TargetExecutionTime.Value + c.p.Measurements.WarmUpTime.Value) * c.p.Workload.TargetOperationsPerSec.Value)
	threadCount := int(float64(totalOpCount) / 100.0)

	wg.Add(threadCount)
	measureCtx, measureCancel := context.WithCancel(ctx)
	measureCh := make(chan struct{}, 1)
	//start := time.Now()
	go func() {
		defer func() {
			measureCh <- struct{}{}
		}()
		if c.p.Logging.LogInterval.Value > 0 {
			t := time.NewTicker(time.Duration(c.p.Logging.LogInterval.Value) * time.Second)
			defer t.Stop()

			for {
				select {
				case <-t.C:
					//metrics.PlotMetrics(start, time.Now(), "data/")
				case <-measureCtx.Done():
					return
				}
			}
		}
	}()

	for i := 0; i < threadCount; i++ {
		go func(threadId int) {
			defer wg.Done()

			w := workload.NewWorker(c.p, threadId, c.workload, c.db, c.cache, threadCount, totalOpCount)
			ctx := c.workload.InitThread(ctx, threadId, threadCount)
			ctx = c.db.InitThread(ctx, threadId, threadCount)
			w.Run(ctx)
			c.db.CleanupThread(ctx)
		}(i)
	}

	wg.Wait()
	measureCancel()
	<-measureCh
}

func (c *Client) Load(ctx context.Context) {
	var wg sync.WaitGroup

	var totalOpCount = int64((c.p.Workload.TargetExecutionTime.Value + c.p.Measurements.WarmUpTime.Value) * c.p.Workload.TargetOperationsPerSec.Value)
	threadCount := int(float64(totalOpCount) / 500.0)

	wg.Add(threadCount)
	measureCtx, measureCancel := context.WithCancel(ctx)
	measureCh := make(chan struct{}, 1)
	//start := time.Now()
	go func() {
		defer func() {
			measureCh <- struct{}{}
		}()
		if c.p.Logging.LogInterval.Value > 0 {
			t := time.NewTicker(time.Duration(c.p.Logging.LogInterval.Value) * time.Second)
			defer t.Stop()

			for {
				select {
				case <-t.C:
					//metrics.PlotMetrics(start, time.Now(), "data/")
				case <-measureCtx.Done():
					return
				}
			}
		}
	}()

	for i := 0; i < threadCount; i++ {
		go func(threadId int) {
			defer wg.Done()

			w := workload.NewWorker(c.p, threadId, c.workload, c.db, nil, threadCount, totalOpCount)
			ctx := c.workload.InitThread(ctx, threadId, threadCount)
			ctx = c.db.InitThread(ctx, threadId, threadCount)
			w.Load(ctx)
			c.db.CleanupThread(ctx)
		}(i)
	}

	wg.Wait()
	measureCancel()
	<-measureCh
}
