package client

import (
	bconfig "benchmark/config"
	"benchmark/db"
	"benchmark/measurement"
	"benchmark/workload"
	"context"
	"github.com/magiconair/properties"
	"sync"
	"time"
)

type Client struct {
	p        *properties.Properties
	workload *workload.Workload
	db       db.DB
}

// NewClient returns a client with the given workload and DB.
// The workload and db can't be nil.
func NewClient(p *properties.Properties, workload *workload.Workload, db db.DB) *Client {
	return &Client{p: p, workload: workload, db: db}
}

// Run runs the workload to the target DB, and blocks until all workers end.
func (c *Client) Run(ctx context.Context) {
	var wg sync.WaitGroup
	threadCount := c.p.GetInt(bconfig.ThreadCount, 1)

	wg.Add(threadCount)
	measureCtx, measureCancel := context.WithCancel(ctx)
	measureCh := make(chan struct{}, 1)
	go func() {
		defer func() {
			measureCh <- struct{}{}
		}()
		// load stage no need to warm up
		if c.p.GetBool(bconfig.DoTransactions, true) {
			dur := c.p.GetInt64(bconfig.WarmUpTime, 0)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(dur) * time.Second):
			}
		}
		// finish warming up
		measurement.EnableWarmUp(false)

		dur := c.p.GetInt64(bconfig.LogInterval, 10)
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

	for i := 0; i < threadCount; i++ {
		go func(threadId int) {
			defer wg.Done()

			w := workload.NewWorker(c.p, threadId, threadCount, c.workload, c.db)
			ctx := c.workload.InitThread(ctx, threadId, threadCount)
			ctx = c.db.InitThread(ctx, threadId, threadCount)
			w.Run(ctx)
			c.db.CleanupThread(ctx)
		}(i)
	}

	wg.Wait()
	if !c.p.GetBool(bconfig.DoTransactions, true) {
		// when loading is finished, try to analyze table if possible.
		if analyzeDB, ok := c.db.(db.AnalyzeDB); ok {
			err := analyzeDB.Analyze(ctx, c.p.GetString(bconfig.TableName, bconfig.TableNameDefault))
			if err != nil {
				panic(err)
			}
		}
	}
	measureCancel()
	<-measureCh
}
