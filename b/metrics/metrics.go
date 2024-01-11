package metrics

import (
	bconfig "benchmark/config"
	"sort"
	"sync"
	"time"
)

var globalMetrics *metrics

var (
	DATABASE_OPERATION = "DATABASE_OPERATION"
	CACHE_OPERATION    = "CACHE_OPERATION"
)

type metrics struct {
	config     *bconfig.Config                     // configuration for the benchmark
	start      time.Time                           // start time of simulation
	end        time.Time                           // end time of simulation
	allMetrics map[string]*threadSafeSortedMetrics // dynamically hold different metric types
}

func Init(config *bconfig.Config) {
	globalMetrics = newMetrics(time.Now(), config)
}

func newMetrics(start time.Time, config *bconfig.Config) *metrics {
	return &metrics{
		config:     config,
		start:      start,
		allMetrics: make(map[string]*threadSafeSortedMetrics),
	}
}

type threadSafeSortedMetrics struct {
	mu      sync.Mutex
	metrics []Metric
}

type Metric struct {
	metricType   string
	timestamp    time.Time
	stringValues map[string]string
	floatValues  map[string]float64
}

func (mtrc *Metric) TestStringValue(str string, test func(string) bool) bool {
	if val, exists := mtrc.stringValues[str]; exists {
		return test(val)
	}
	return false
}

func (mtrc *Metric) TestFloatValue(str string, test func(float64) bool) bool {
	if val, exists := mtrc.floatValues[str]; exists {
		return test(val)
	}
	return false
}

func (mtrc *Metric) TestTimestamp(test func(time2 time.Time) bool) bool {
	return test(mtrc.timestamp)
}

func (mtrc *Metric) IsType(type_ string) bool {
	return mtrc.metricType == type_
}

func AddMeasurement(metricType string, timestamp time.Time, stringValues map[string]string, floatValues map[string]float64) {
	globalMetrics.addMetric(metricType, timestamp, stringValues, floatValues)
}

func (m *metrics) addMetric(metricType string, timestamp time.Time, stringValues map[string]string, floatValues map[string]float64) {
	// insert the metric concurrently
	go func() {
		tsMetrics, exists := m.allMetrics[metricType]
		if !exists {
			tsMetrics = &threadSafeSortedMetrics{}
			m.allMetrics[metricType] = tsMetrics
		}
		tsMetrics.insertTimestampWithLabel(timestamp, metricType, stringValues, floatValues)
	}()
}

// insertTimestampWithLabel safely inserts a new timestamp into the slice in sorted order
func (ts *threadSafeSortedMetrics) insertTimestampWithLabel(newTimestamp time.Time, name string, stringValues map[string]string, floatValues map[string]float64) {
	ts.mu.Lock() // maintain exclusive access to the slice
	defer ts.mu.Unlock()

	// Append and sort - todo sorting isn't efficient for large slices
	ts.metrics = append(ts.metrics, Metric{timestamp: newTimestamp, metricType: name, stringValues: stringValues, floatValues: floatValues})
	sort.Slice(ts.metrics, func(i, j int) bool {
		return ts.metrics[i].timestamp.Before(ts.metrics[j].timestamp)
	})
}

type MetricSlice []Metric

func GetMetricsByType(metricType string) MetricSlice {
	if tsMetrics, exists := globalMetrics.allMetrics[metricType]; exists {
		return tsMetrics.getMetrics()
	}
	return nil
}

// getMetrics safely returns a copy of the list of metrics
func (ts *threadSafeSortedMetrics) getMetrics() MetricSlice {
	ts.mu.Lock() // maintain exclusive access to the slice
	defer ts.mu.Unlock()
	// return a copy of the metrics slice (to avoid race conditions)
	copiedTimestamps := make([]Metric, len(ts.metrics))
	copy(copiedTimestamps, ts.metrics)
	return copiedTimestamps
}

func allTests(m Metric, tests ...func(Metric) bool) bool {
	for _, test := range tests {
		if !test(m) {
			return false
		}
	}
	return true
}

func (ms MetricSlice) Filter(tests ...func(Metric) bool) MetricSlice {
	var result MetricSlice
	for _, m := range ms {
		if allTests(m, tests...) {
			result = append(result, m)
		}
	}
	return result
}

func (ms MetricSlice) FilterByStringValue(label string, test func(string) bool) MetricSlice {
	return ms.Filter(func(m Metric) bool {
		return m.TestStringValue(label, test)
	})
}

func (ms MetricSlice) FilterByFloatValue(label string, test func(float64) bool) MetricSlice {
	return ms.Filter(func(m Metric) bool {
		return m.TestFloatValue(label, test)
	})
}

func (ms MetricSlice) FilterByTimestamp(test func(time2 time.Time) bool) MetricSlice {
	return ms.Filter(func(m Metric) bool {
		return m.TestTimestamp(test)
	})
}

func (ms MetricSlice) CountFrom(start time.Time, end time.Time) int {
	return len(ms.FilterByTimestamp(func(time2 time.Time) bool {
		return time2.Equal(start) || (time2.After(start) && time2.Before(end))
	}))
}
