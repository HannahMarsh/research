package workload

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	"benchmark/db"
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"
)

type Worker struct {
	p               *bconfig.Config
	workDB          db.DB
	cache           cache.Cache
	workload        *Workload
	doBatch         bool
	opCount         int64
	targetOpsPerMs  float64
	threadID        int
	targetOpsTickNs int64
	opsDone         int64
}

func NewWorker(p *bconfig.Config, threadID int, threadCount int, workload *Workload, db db.DB, cache cache.Cache) *Worker {
	w := new(Worker)
	w.p = p
	if w.p.Performance.BatchSize.Value > 1 {
		w.doBatch = true
	}
	w.threadID = threadID
	w.workload = workload
	w.workDB = db
	w.cache = cache

	var totalOpCount int64
	if w.p.Workload.DoTransactions.Value {
		totalOpCount = int64(p.Performance.OperationCount.Value)
	} else {
		if p.Performance.InsertCount.Value > 0 {
			totalOpCount = int64(p.Performance.InsertCount.Value)
		} else {
			totalOpCount = int64(p.Performance.RecordCount.Value)
		}
	}

	if totalOpCount < int64(threadCount) {
		fmt.Printf("totalOpCount(%s/%s/%s): %d should be bigger than threadCount: %d",
			p.Performance.OperationCount.Value,
			p.Performance.InsertCount.Value,
			p.Performance.RecordCount.Value,
			totalOpCount,
			threadCount)

		os.Exit(-1)
	}

	w.opCount = totalOpCount / int64(threadCount)
	if threadID < int(totalOpCount%int64(threadCount)) {
		w.opCount++
	}

	targetPerThreadPerms := float64(-1)
	if v := p.Performance.TargetOperationsPerSec.Value; v > 0 {
		targetPerThread := float64(v) / float64(threadCount)
		targetPerThreadPerms = targetPerThread / 1000.0
	}

	if targetPerThreadPerms > 0 {
		w.targetOpsPerMs = targetPerThreadPerms
		w.targetOpsTickNs = int64(1000000.0 / w.targetOpsPerMs)
	}

	return w
}

func (w *Worker) throttle(ctx context.Context, startTime time.Time) {
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

func (w *Worker) Run(ctx context.Context) {
	// spread the thread operation out so they don't all hit the DB at the same time
	if w.targetOpsPerMs > 0.0 && w.targetOpsPerMs <= 1.0 {
		time.Sleep(time.Duration(rand.Int63n(w.targetOpsTickNs)))
	}

	startTime := time.Now()

	for w.opCount == 0 || w.opsDone < w.opCount {
		var err error
		opsCount := 1
		if w.p.Workload.DoTransactions.Value {
			if w.doBatch {
				err, _ = w.workload.DoBatchTransaction(ctx, w.p.Performance.BatchSize.Value, w.workDB, w.cache)
				opsCount = w.p.Performance.BatchSize.Value
			} else {
				err, _ = w.workload.DoTransaction(ctx, w.workDB, w.cache)
			}
		} else {
			if w.doBatch {
				err = w.workload.DoBatchInsert(ctx, w.p.Performance.BatchSize.Value, w.workDB, w.cache)
				opsCount = w.p.Performance.BatchSize.Value
			} else {
				err = w.workload.DoInsert(ctx, w.workDB, w.cache)
			}
		}

		if err != nil {
			//log.Panic(err)
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
