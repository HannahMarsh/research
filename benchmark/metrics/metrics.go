package metrics

import (
	bconfig "benchmark/config"
	"fmt"
	"sync"
	"time"
)

// var globalMetrics *metrics

var globalAllMetrics MetricSlice
var mu sync.Mutex
var globalConfig *bconfig.Config
var warmUptime time.Duration
var globalStartTime time.Time

var (
	TAG = "TAG"
	// metric types
	DATABASE_OPERATION = "DATABASE_OPERATION"
	CACHE_OPERATION    = "CACHE_OPERATION"
	TRANSACTION        = "TRANSACTION"
	WORKLOAD           = "WORKLOAD"
	// labels
	SUCCESSFUL                = "SUCCESSFUL"
	OPERATION                 = "OPERATION"
	ERROR                     = "ERROR"
	LATENCY                   = "LATENCY"
	NODE_INDEX                = "NODE_INDEX"
	DATABASE                  = "DATABASE"
	SIZE                      = "SIZE"
	NODE_FAILURE              = "NODE_FAILURE"
	KEY                       = "KEY"
	CLIENT_FAILURE_DETECTION  = "CLIENT_FAILURE_DETECTION"
	CLIENT_RECOVERY_DETECTION = "CLIENT_RECOVERY_DETECTION"

	// string values
	BATCH_READ        = "BATCH_READ"
	BATCH_INSERT      = "BATCH_INSERT"
	BATCH_UPDATE      = "BATCH_UPDATE"
	READ              = "READ"
	UPDATE            = "UPDATE"
	SCAN              = "SCAN"
	INSERT            = "INSERT"
	READ_MODIFY_WRITE = "READ_MODIFY_WRITE"
	INTERVAL          = "INTERVAL"
	START             = "START"
	END               = "END"
	HOTTEST           = "HOTTEST"
	KEYS              []map[string]int64
)

func Init(config *bconfig.Config) {
	globalConfig = config
	globalStartTime = time.Now()
	warmUptime = time.Duration(config.Measurements.WarmUpTime.Value) * time.Second

	for _, _ = range config.Cache.Nodes {
		KEYS = append(KEYS, make(map[string]int64))
	}
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
	//go func() {
	now := time.Now()
	if now.Before(globalStartTime.Add(warmUptime)) {
		return
	}
	//if now.After(start.Add(estimatedRunningTime)) {
	//	return
	//}
	mu.Lock() // maintain exclusive access to the slice
	defer mu.Unlock()

	values[TAG] = name
	if values[KEY] != nil && values[NODE_INDEX] != nil {
		if key, isString := values[KEY].(string); isString {
			if nodeIndex, isInt := values[NODE_INDEX].(int); isInt {
				if _, ok := KEYS[nodeIndex][key]; !ok {
					KEYS[nodeIndex][key] = 0
				}
				KEYS[nodeIndex][key] += 1
			} else {
				panic(fmt.Errorf("node index is not an int: %v", values[NODE_INDEX]))
			}
		} else {
			panic(fmt.Errorf("key is not a string: %v", values[KEY]))
		}
	}
	globalAllMetrics = append(globalAllMetrics, Metric{timestamp: newTimestamp, metricType: name, tags: values})
	//}()
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
