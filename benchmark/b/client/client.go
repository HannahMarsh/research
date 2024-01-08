package client

import (
	"benchmark/b/measurement"
	"benchmark/b/workload"
	"context"
	"sync"
	"time"
)

type Client struct {
	workload *workload.Workload
}

func NewClient(workload *workload.Workload) *Client {
	return &Client{workload: workload}
}

// Run runs the workload to the target DB, and blocks until all workers end.
func (c *Client) Run(ctx context.Context, threadCount int, logInterval int64) {
	var wg sync.WaitGroup

	wg.Add(threadCount)
	measureCtx, measureCancel := context.WithCancel(ctx)
	measureCh := make(chan struct{}, 1)
	go func() {
		defer func() {
			measureCh <- struct{}{}
		}()

		dur := logInterval
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

			// doTransactions bool, batchSize int, target int64, totalOpCount int64, threadID int, threadCount int, workload *Workload
			var batchSize int
			var target int64
			var totalOpCount int64
			w := workload.NewWorker(true, batchSize, target, totalOpCount, threadId, threadCount, c.workload)
			ctx := c.workload.InitThread(ctx, threadId, threadCount)
			w.Run(ctx)
		}(i)
	}

	wg.Wait()
	measureCancel()
	<-measureCh
}
