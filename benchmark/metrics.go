package main

import (
	"sort"
	"sync"
	"time"
)

type Metrics struct {
	config           config_
	start            time.Time
	end              time.Time
	nodeFailures     []ThreadSafeSortedIntervals
	databaseRequests ThreadSafeSortedMetrics
	allRequests      ThreadSafeSortedMetrics
	cacheHits        ThreadSafeSortedMetrics
	latency          ThreadSafeSortedMetrics
}

func NewMetrics(start time.Time, end time.Time, config config_) *Metrics {
	return &Metrics{
		config:           config,
		start:            start,
		end:              end,
		nodeFailures:     make([]ThreadSafeSortedIntervals, len(config.nodeConfigs)),
		databaseRequests: ThreadSafeSortedMetrics{},
		cacheHits:        ThreadSafeSortedMetrics{},
		latency:          ThreadSafeSortedMetrics{},
	}
}

type ThreadSafeSortedIntervals struct {
	mu        sync.Mutex
	intervals []MetricInterval
}

type ThreadSafeSortedMetrics struct {
	mu      sync.Mutex
	metrics []Metric
}

type Metric struct {
	metricType string
	timestamp  time.Time
	label      string
	value      float64
}

type MetricInterval struct {
	metricType string
	start      time.Time
	end        time.Time
	label      string
	value      float64
}

func (m *Metrics) AddDatabaseRequest(timestamp time.Time) {
	go m.databaseRequests.InsertTimestampWithLabel(timestamp, "databaseRequest", "", 0.0)
}

func (m *Metrics) AddCacheHit(timestamp time.Time, nodeIndex int) {
	go m.cacheHits.InsertTimestampWithLabel(timestamp, "cacheHit", "node index", float64(nodeIndex))
}

func (m *Metrics) AddLatency(timestamp time.Time, latency time.Duration) {
	go m.latency.InsertTimestampWithLabel(timestamp, "latency", "latency in seconds", latency.Seconds())
}

func (m *Metrics) GetLatency() []Metric {
	return m.latency.GetMetrics()
}

func (m *Metrics) GetCacheHits() []Metric {
	return m.cacheHits.GetMetrics()
}

func (m *Metrics) GetDatabaseRequests() []Metric {
	return m.databaseRequests.GetMetrics()
}

func (m *Metrics) AddRequest(timestamp time.Time, operationType string, nodeIndex int) {
	go m.databaseRequests.InsertTimestampWithLabel(timestamp, "request", operationType, float64(nodeIndex))
}

func (m *Metrics) GetAllRequests() []Metric {
	return m.databaseRequests.GetMetrics()
}

func (m *Metrics) GetFailureIntervals() [][]MetricInterval {
	intervals := make([][]MetricInterval, len(m.nodeFailures))

	for i := range m.nodeFailures {
		t := m.nodeFailures[i].GetIntervals()
		for _, interval := range t {
			intervals[i] = append(intervals[i], interval)
		}
	}
	return intervals
}

func (m *Metrics) AddNodeFailureInterval(nodeIndex int, start time.Time, end time.Time) {
	go m.nodeFailures[nodeIndex].InsertFailureInterval(start, end)
}

// InsertTimestampWithLabel safely inserts a new timestamp into the slice in sorted order
func (ts *ThreadSafeSortedMetrics) InsertTimestampWithLabel(newTimestamp time.Time, name string, label string, value float64) {
	ts.mu.Lock()         // Lock the mutex to ensure exclusive access to the slice
	defer ts.mu.Unlock() // Unlock the mutex when the function returns

	// Append and sort - todo not the most efficient for large slices
	ts.metrics = append(ts.metrics, Metric{timestamp: newTimestamp, metricType: name, label: label, value: value})
	sort.Slice(ts.metrics, func(i, j int) bool {
		return ts.metrics[i].timestamp.Before(ts.metrics[j].timestamp)
	})
}

func (tsi *ThreadSafeSortedIntervals) InsertFailureInterval(start time.Time, end time.Time) {
	go func() {
		tsi.mu.Lock()         // Lock the mutex to ensure exclusive access to the slice
		defer tsi.mu.Unlock() // Unlock the mutex when the function returns

		// Append and sort - todo not the most efficient for large slices
		tsi.intervals = append(tsi.intervals, MetricInterval{start: start, end: end})
		sort.Slice(tsi.intervals, func(i, j int) bool {
			return tsi.intervals[i].start.Before(tsi.intervals[j].start)
		})
	}()
}

// GetMetrics safely returns a copy of the list of metrics
func (ts *ThreadSafeSortedMetrics) GetMetrics() []Metric {
	ts.mu.Lock()         // Lock the mutex to ensure exclusive access to the slice
	defer ts.mu.Unlock() // Unlock the mutex when the function returns
	// Return a copy of the metrics slice to avoid race conditions
	copiedTimestamps := make([]Metric, len(ts.metrics))
	copy(copiedTimestamps, ts.metrics)
	return copiedTimestamps
}

func (tsi *ThreadSafeSortedIntervals) GetIntervals() []MetricInterval {
	tsi.mu.Lock()         // Lock the mutex to ensure exclusive access to the slice
	defer tsi.mu.Unlock() // Unlock the mutex when the function returns

	// Return a copy of the metrics slice to avoid race conditions
	copiedIntervals := make([]MetricInterval, len(tsi.intervals))
	copy(copiedIntervals, tsi.intervals)
	return copiedIntervals
}
