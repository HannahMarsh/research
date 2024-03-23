package workload

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	"benchmark/db"
	"context"
	"math/rand"
	"time"
)

type Worker struct {
	p               *bconfig.Config
	workDB          db.DB
	cache           cache.Cache
	workload        *Workload
	opCount         int64
	targetOpsPerMs  float64
	threadID        int
	targetOpsTickNs int64
	opsDone         int64
}

func NewWorker(p *bconfig.Config, threadID int, workload *Workload, db db.DB, cache cache.Cache, threadCount int, totalOpCount int64) *Worker {
	w := new(Worker)
	w.p = p
	w.threadID = threadID
	w.workload = workload
	w.workDB = db
	w.cache = cache

	w.opCount = totalOpCount / int64(threadCount)
	if threadID < int(totalOpCount%int64(threadCount)) {
		w.opCount++
	}

	targetPerThreadPerms := float64(-1)
	if v := p.Workload.TargetOperationsPerSec.Value; v > 0 {
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
	// spread the thread operation out
	if w.targetOpsPerMs > 0.0 && w.targetOpsPerMs <= 1.0 {
		time.Sleep(time.Duration(rand.Int63n(w.targetOpsTickNs)))
	}

	for startTime := time.Now(); w.opCount == 0 || w.opsDone < w.opCount; w.opsDone++ {
		w.workload.DoTransaction(ctx, w.workDB, w.cache)
		w.throttle(ctx, startTime)

		select {
		case <-ctx.Done():
			//time.Sleep(100 * time.Millisecond)
			return
		default:
		}
	}
}

func (w *Worker) Load(ctx context.Context) {
	// spread the thread operation out
	if w.targetOpsPerMs > 0.0 && w.targetOpsPerMs <= 1.0 {
		time.Sleep(time.Duration(rand.Int63n(w.targetOpsTickNs)))
	}

	for startTime := time.Now(); w.opCount == 0 || w.opsDone < w.opCount; w.opsDone++ {
		go w.workload.DoInsertion(ctx, w.workDB, nil, nil, nil)
		w.throttle(ctx, startTime)

		select {
		case <-ctx.Done():
			return
		default:

		}
	}
}
