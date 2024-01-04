package main

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

// Metrics holds different metric data for the benchmark
type Metrics struct {
	config           benchmark               // configuration for the benchmark
	start            time.Time               // start time of simulation
	end              time.Time               // end time of simulation
	nodeFailures     ThreadSafeSortedMetrics // metric type for node failures
	databaseRequests ThreadSafeSortedMetrics // metric type for database requests
	allRequests      ThreadSafeSortedMetrics // metric type for all types of requests
	cacheHits        ThreadSafeSortedMetrics // metric type for cache hits
	latency          ThreadSafeSortedMetrics // metric type for latency
	keyspacePop      ThreadSafeSortedMetrics // metric type for keyspace popularity
	cacheSize        ThreadSafeSortedMetrics // metric type for cache sizes
}

// NewMetrics initializes a new Metrics struct and returns a pointer to it
func NewMetrics(start time.Time, end time.Time, config benchmark) *Metrics {
	return &Metrics{
		config:           config,
		start:            start,
		end:              end,
		nodeFailures:     ThreadSafeSortedMetrics{},
		databaseRequests: ThreadSafeSortedMetrics{},
		cacheHits:        ThreadSafeSortedMetrics{},
		latency:          ThreadSafeSortedMetrics{},
		keyspacePop:      ThreadSafeSortedMetrics{},
		cacheSize:        ThreadSafeSortedMetrics{},
	}
}

// ThreadSafeSortedMetrics encapsulates a slice of metrics for a single metric type and with concurrent access protection.
type ThreadSafeSortedMetrics struct {
	mu      sync.Mutex
	metrics []Metric // slice of metrics
}

type Metric struct {
	metricType   string
	timestamp    time.Time
	stringValues map[string]string
	floatValues  map[string]float64
}

func (m *Metrics) AddCacheSize(timestamp time.Time, nodeIndex int, size int64) {
	floatValues := map[string]float64{
		"nodeIndex": float64(nodeIndex),
		"size":      float64(size),
	}
	go m.cacheSize.InsertTimestampWithLabel(timestamp, "cacheSize", nil, floatValues)
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

func (m *Metrics) GetCacheSizes() []Metric {
	return m.cacheSize.GetMetrics()
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
	ts.mu.Lock() // maintain exclusive access to the slice
	defer ts.mu.Unlock()

	// Append and sort - todo sorting isn't efficient for large slices
	ts.metrics = append(ts.metrics, Metric{timestamp: newTimestamp, metricType: name, stringValues: stringValues, floatValues: floatValues})
	sort.Slice(ts.metrics, func(i, j int) bool {
		return ts.metrics[i].timestamp.Before(ts.metrics[j].timestamp)
	})
}

// GetMetrics safely returns a copy of the list of metrics
func (ts *ThreadSafeSortedMetrics) GetMetrics() []Metric {
	ts.mu.Lock() // maintain exclusive access to the slice
	defer ts.mu.Unlock()
	// return a copy of the metrics slice (to avoid race conditions)
	copiedTimestamps := make([]Metric, len(ts.metrics))
	copy(copiedTimestamps, ts.metrics)
	return copiedTimestamps
}
