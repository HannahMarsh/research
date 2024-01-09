package client

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	"benchmark/db"
	"benchmark/measurement"
	"benchmark/workload"
	"context"
	"sync"
	"time"
)

type Client struct {
	p        *bconfig.Config
	workload *workload.Workload
	db       db.DB
	cache    *cache.Cache
}

// NewClient returns a client with the given workload and DB.
// The workload and db can't be nil.
func NewClient(p *bconfig.Config, workload *workload.Workload, db db.DB, cache_ *cache.Cache) *Client {
	return &Client{p: p, workload: workload, db: db, cache: cache_}
}

// Run runs the workload to the target DB, and blocks until all workers end.
func (c *Client) Run(ctx context.Context) {
	var wg sync.WaitGroup
	threadCount := c.p.ThreadCount

	wg.Add(int(threadCount))
	measureCtx, measureCancel := context.WithCancel(ctx)
	measureCh := make(chan struct{}, 1)
	go func() {
		defer func() {
			measureCh <- struct{}{}
		}()
		// load stage no need to warm up
		if c.p.DoTransactions {
			dur := c.p.WarmUpTime
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(dur) * time.Second):
			}
		}
		// finish warming up
		measurement.EnableWarmUp(false)

		dur := c.p.LogInterval
		t := time.NewTicker(time.Duration(dur) * time.Second)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				measurement.Summary()
			case <-measureCtx.Done():
				return
			}
		}
	}()

	for i := 0; i < int(threadCount); i++ {
		go func(threadId int) {
			defer wg.Done()

			w := workload.NewWorker(c.p, threadId, int(threadCount), c.workload, c.db, c.cache)
			ctx := c.workload.InitThread(ctx, threadId, int(threadCount))
			ctx = c.db.InitThread(ctx, threadId, int(threadCount))
			w.Run(ctx)
			c.db.CleanupThread(ctx)
		}(i)
	}

	wg.Wait()
	if !c.p.DoTransactions {
		// when loading is finished, try to analyze table if possible.
		if analyzeDB, ok := c.db.(db.AnalyzeDB); ok {
			err := analyzeDB.Analyze(ctx, c.p.TableName)
			if err != nil {
				panic(err)
			}
		}
	}
	measureCancel()
	<-measureCh
}
