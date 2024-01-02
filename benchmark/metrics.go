package main

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

type Metrics struct {
	config           config_
	start            time.Time
	end              time.Time
	nodeFailures     ThreadSafeSortedMetrics
	databaseRequests ThreadSafeSortedMetrics
	allRequests      ThreadSafeSortedMetrics
	cacheHits        ThreadSafeSortedMetrics
	latency          ThreadSafeSortedMetrics
	keyspacePop      ThreadSafeSortedMetrics
}

func NewMetrics(start time.Time, end time.Time, config config_) *Metrics {
	return &Metrics{
		config:           config,
		start:            start,
		end:              end,
		nodeFailures:     ThreadSafeSortedMetrics{},
		databaseRequests: ThreadSafeSortedMetrics{},
		cacheHits:        ThreadSafeSortedMetrics{},
		latency:          ThreadSafeSortedMetrics{},
		keyspacePop:      ThreadSafeSortedMetrics{},
	}
}

type ThreadSafeSortedMetrics struct {
	mu      sync.Mutex
	metrics []Metric
}

type Metric struct {
	metricType   string
	timestamp    time.Time
	stringValues map[string]string
	floatValues  map[string]float64
}

func (m *Metrics) AddDatabaseRequest(timestamp time.Time, successful bool) {
	stringValues := map[string]string{
		"successful": strconv.FormatBool(successful),
	}
	go m.databaseRequests.InsertTimestampWithLabel(timestamp, "databaseRequest", stringValues, nil)
}

func (m *Metrics) AddCacheHit(timestamp time.Time, key string, nodeIndex int) {
	stringValues := map[string]string{
		"key": key,
	}
	floatValues := map[string]float64{
		"nodeIndex": float64(nodeIndex),
	}
	go m.cacheHits.InsertTimestampWithLabel(timestamp, "cacheHit", stringValues, floatValues)
}

func (m *Metrics) AddLatency(timestamp time.Time, latency time.Duration) {
	floatValues := map[string]float64{
		"latency": latency.Seconds(),
	}
	go m.latency.InsertTimestampWithLabel(timestamp, "latency", nil, floatValues)
}

func (m *Metrics) AddRequest(timestamp time.Time, operationType string, nodeIndex int, successful bool) {
	stringValues := map[string]string{
		"operationType": operationType,
		"successful":    strconv.FormatBool(successful),
	}
	floatValues := map[string]float64{
		"nodeIndex": float64(nodeIndex),
	}
	go m.allRequests.InsertTimestampWithLabel(timestamp, "request", stringValues, floatValues)
}

func (m *Metrics) AddNodeFailureInterval(nodeIndex int, start time.Time, end time.Time) {
	floatValues := map[string]float64{
		"nodeIndex": float64(nodeIndex),
		"duration":  end.Sub(start).Seconds(),
	}
	go m.nodeFailures.InsertTimestampWithLabel(start, "nodeFailureInterval", nil, floatValues)
}

func (m *Metrics) AddKeyspaceRequest(keyspace int, timestamp time.Time) {
	floatValues := map[string]float64{
		"keyspace": float64(keyspace),
	}
	go m.keyspacePop.InsertTimestampWithLabel(timestamp, "keyspacePop", nil, floatValues)
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

func (m *Metrics) GetAllRequests() []Metric {
	return m.allRequests.GetMetrics()
}

func (m *Metrics) GetFailureIntervals() []Metric {
	return m.nodeFailures.GetMetrics()
}

func (m *Metrics) GetKeyspacePopularities() []Metric {
	return m.keyspacePop.GetMetrics()
}

// InsertTimestampWithLabel safely inserts a new timestamp into the slice in sorted order
func (ts *ThreadSafeSortedMetrics) InsertTimestampWithLabel(newTimestamp time.Time, name string, stringValues map[string]string, floatValues map[string]float64) {
	ts.mu.Lock()         // Lock the mutex to ensure exclusive access to the slice
	defer ts.mu.Unlock() // Unlock the mutex when the function returns

	// Append and sort - todo not the most efficient for large slices
	ts.metrics = append(ts.metrics, Metric{timestamp: newTimestamp, metricType: name, stringValues: stringValues, floatValues: floatValues})
	sort.Slice(ts.metrics, func(i, j int) bool {
		return ts.metrics[i].timestamp.Before(ts.metrics[j].timestamp)
	})
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
