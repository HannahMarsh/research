package metrics

import (
	bconfig "benchmark/config"
	"sync"
	"time"
)

// var globalMetrics *metrics

var globalAllMetrics MetricSlice
var mu sync.Mutex
var globalConfig *bconfig.Config

var (
	TAG = "TAG"
	// metric types
	DATABASE_OPERATION = "DATABASE_OPERATION"
	CACHE_OPERATION    = "CACHE_OPERATION"
	TRANSACTION        = "TRANSACTION"
	// labels
	SUCCESSFUL = "SUCCESSFUL"
	OPERATION  = "OPERATION"
	ERROR      = "ERROR"
	LATENCY    = "LATENCY"
	NODE_INDEX = "NODE_INDEX"
	DATABASE   = "DATABASE"
	SIZE       = "SIZE"

	// string values
	BATCH_READ        = "BATCH_READ"
	BATCH_INSERT      = "BATCH_INSERT"
	BATCH_UPDATE      = "BATCH_UPDATE"
	READ              = "READ"
	UPDATE            = "UPDATE"
	SCAN              = "SCAN"
	INSERT            = "INSERT"
	READ_MODIFY_WRITE = "READ_MODIFY_WRITE"
)

func Init(config *bconfig.Config) {
	globalConfig = config
}

type Metric struct {
	metricType string
	timestamp  time.Time
	tags       map[string]interface{}
}

func (mtrc *Metric) TestTimestamp(test func(time2 time.Time) bool) bool {
	return test(mtrc.timestamp)
}

func (mtrc *Metric) IsType(type_ string) bool {
	return mtrc.metricType == type_
}

func AddMeasurement(name string, newTimestamp time.Time, values map[string]interface{}) {
	go func() {
		mu.Lock() // maintain exclusive access to the slice
		defer mu.Unlock()

		values[TAG] = name
		globalAllMetrics = append(globalAllMetrics, Metric{timestamp: newTimestamp, metricType: name, tags: values})
	}()
}

type MetricSlice []Metric

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

func Filter(tests ...func(Metric) bool) MetricSlice {
	var mtrcs MetricSlice
	for _, m := range globalAllMetrics {
		if allTests(m, tests...) {
			mtrcs = append(mtrcs, m)
		}
	}
	return mtrcs
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
