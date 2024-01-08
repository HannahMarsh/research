package workload

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

type worker struct {
	workload        *Workload
	doTransactions  bool
	doBatch         bool
	batchSize       int
	opCount         int64
	targetOpsPerMs  float64
	threadID        int
	targetOpsTickNs int64
	opsDone         int64
}

func NewWorker(doTransactions bool, batchSize int, target int64, totalOpCount int64, threadID int, threadCount int, workload *Workload) *worker {
	w := new(worker)
	w.doTransactions = doTransactions
	w.batchSize = batchSize
	if w.batchSize > 1 {
		w.doBatch = true
	}
	w.threadID = threadID
	w.workload = workload

	if totalOpCount < int64(threadCount) {
		fmt.Printf("totalOpCount(%s): %d should be bigger than threadCount: %d",
			totalOpCount,
			threadCount)
		os.Exit(-1)
	}

	w.opCount = totalOpCount / int64(threadCount)
	if threadID < int(totalOpCount%int64(threadCount)) {
		w.opCount++
	}

	targetPerThreadPerms := float64(-1)
	if v := target; v > 0 {
		targetPerThread := float64(v) / float64(threadCount)
		targetPerThreadPerms = targetPerThread / 1000.0
	}

	if targetPerThreadPerms > 0 {
		w.targetOpsPerMs = targetPerThreadPerms
		w.targetOpsTickNs = int64(1000000.0 / w.targetOpsPerMs)
	}

	return w
}

func (w *worker) throttle(ctx context.Context, startTime time.Time) {
	if w.targetOpsPerMs <= 0 {
		return
	}

	d := time.Duration(w.opsDone * w.targetOpsTickNs)
	d = startTime.Add(d).Sub(time.Now())
	if d < 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (w *worker) Run(ctx context.Context) {
	// spread the thread operation out so they don't all hit the DB at the same time
	if w.targetOpsPerMs > 0.0 && w.targetOpsPerMs <= 1.0 {
		time.Sleep(time.Duration(rand.Int63n(w.targetOpsTickNs)))
	}

	startTime := time.Now()

	for w.opCount == 0 || w.opsDone < w.opCount {
		var err error
		opsCount := 1
		if w.doTransactions {
			if w.doBatch {
				err = w.workload.DoBatchTransaction(ctx, w.batchSize)
				opsCount = w.batchSize
			} else {
				err = w.workload.DoTransaction(ctx)
			}
		} else {
			if w.doBatch {
				err = w.workload.DoBatchInsert(ctx, w.batchSize)
				opsCount = w.batchSize
			} else {
				err = w.workload.DoInsert(ctx)
			}
		}

		if err != nil {
			log.Panic(err)
		}

		w.opsDone += int64(opsCount)
		w.throttle(ctx, startTime)

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
