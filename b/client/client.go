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

	wg.Add(c.p.Performance.ThreadCount.Value)
	measureCtx, measureCancel := context.WithCancel(ctx)
	measureCh := make(chan struct{}, 1)
	//start := time.Now()
	go func() {
		defer func() {
			measureCh <- struct{}{}
		}()
		// load stage no need to warm up
		if c.p.Workload.DoTransactions.Value {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(c.p.Measurements.WarmUpTime.Value) * time.Second):
			}
		}
		// finish warming up
		//measurement.EnableWarmUp(false)

		//dur := c.p.Logging.LogInterval.Value
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

	for i := 0; i < c.p.Performance.ThreadCount.Value; i++ {
		go func(threadId int) {
			defer wg.Done()

			w := workload.NewWorker(c.p, threadId, c.p.Performance.ThreadCount.Value, c.workload, c.db, c.cache)
			ctx := c.workload.InitThread(ctx, threadId, c.p.Performance.ThreadCount.Value)
			ctx = c.db.InitThread(ctx, threadId, c.p.Performance.ThreadCount.Value)
			w.Run(ctx)
			c.db.CleanupThread(ctx)
		}(i)
	}

	wg.Wait()
	if !c.p.Workload.DoTransactions.Value {
		// when loading is finished, try to analyze table if possible.
		if analyzeDB, ok := c.db.(db.AnalyzeDB); ok {
			err := analyzeDB.Analyze(ctx, c.p.Database.CassandraTableName.Value)
			if err != nil {
				panic(err)
			}
		}
	}
	measureCancel()
	<-measureCh
}
