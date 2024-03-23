package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type Job struct {
	Method  string
	URL     string
	Payload []byte
	Ctx     context.Context    // Add context to track cancellation
	Cancel  context.CancelFunc // Cancel function to invoke on timeout
	Result  chan<- JobResult
}

type JobResult struct {
	Response string
	Status   int
	Error    error
}

type WorkerPool struct {
	JobQueue   chan Job
	MaxWorkers int
	wg         sync.WaitGroup
}

func NewWorkerPool(maxWorkers int) *WorkerPool {
	pool := &WorkerPool{
		JobQueue:   make(chan Job, maxWorkers*2), // Buffer to prevent blocking
		MaxWorkers: maxWorkers,
	}

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		pool.wg.Add(1)
		go pool.worker(i)
	}

	return pool
}

//func (wp *WorkerPool) worker(id int) {
//	defer wp.wg.Done()
//
//	for job := range wp.JobQueue {
//		job.Result <- wp.processJob(job)
//	}
//}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for job := range wp.JobQueue {
		select {
		case <-job.Ctx.Done():
			// Job was cancelled, don't process it
			continue // Skip to the next job
		default:
			// Proceed with processing
			job.Result <- wp.processJob(job)
		}
	}
}

func (wp *WorkerPool) processJob(job Job) JobResult {
	// Here, you integrate your existing HTTP request logic from sendRequest
	// For simplicity, let's assume a simplified version
	client := httpClient
	var reader io.Reader = nil
	if job.Payload != nil {
		reader = bytes.NewBuffer(job.Payload)
	}

	req, err := http.NewRequestWithContext(job.Ctx, job.Method, job.URL, reader)
	if err != nil {
		return JobResult{Error: err}
	}

	resp, err := client.Do(req)
	if err != nil {
		return JobResult{Error: err}
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %v\n", err)
		}
	}(resp.Body)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return JobResult{Error: err}
	}

	if job.Payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return JobResult{Response: string(body), Status: resp.StatusCode}
}

//func (wp *WorkerPool) SubmitJob(job Job) <-chan JobResult {
//	resultChan := make(chan JobResult, 1) // Buffer to prevent blocking
//	job.Result = resultChan
//	wp.JobQueue <- job
//	return resultChan
//}

func (wp *WorkerPool) SubmitJob(job Job, timeout time.Duration) <-chan JobResult {
	resultChan := make(chan JobResult, 1) // Buffer to prevent blocking
	job.Result = resultChan
	globalCtx := job.Ctx

	// Create a context with timeout for the whole job processing, not just the HTTP request
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	job.Ctx = ctx
	job.Cancel = cancel

	go func() {
		select {
		case wp.JobQueue <- job:
		case <-globalCtx.Done():
			job.Result <- JobResult{Error: fmt.Errorf("done")}
			job.Cancel()
		case <-ctx.Done():
			// Timeout occurred before we could even queue the job
			job.Result <- JobResult{Error: fmt.Errorf("job queueing timed out")}
			job.Cancel()
		}
	}()

	return resultChan
}

func (wp *WorkerPool) Shutdown() {
	close(wp.JobQueue)
	wp.wg.Wait()
}
