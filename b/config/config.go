package config

import (
	"gopkg.in/yaml.v3"
	"os"
)

type DatabaseConfig struct {
	CassandraCluster      string `yaml:"CassandraCluster"`
	CassandraConnections  int    `yaml:"CassandraConnections"`
	CassandraKeyspace     string `yaml:"CassandraKeyspace"`
	CassandraTableName    string `yaml:"CassandraTableName"`
	CassandraPassword     string `yaml:"CassandraPassword"`
	CassandraUsername     string `yaml:"CassandraUsername"`
	DropData              bool   `yaml:"DropData"`
	PasswordAuthenticator bool   `yaml:"PasswordAuthenticator"`
}

type PerformanceConfig struct {
	BatchSize               int    `yaml:"BatchSize"`
	DataIntegrity           bool   `yaml:"DataIntegrity"`
	FieldCount              int64  `yaml:"FieldCount"`
	FieldLength             int64  `yaml:"FieldLength"`
	FieldLengthDistribution string `yaml:"FieldLengthDistribution"`
	InsertCount             int64  `yaml:"InsertCount"`
	InsertionRetryInterval  int64  `yaml:"InsertionRetryInterval"`
	InsertionRetryLimit     int64  `yaml:"InsertionRetryLimit"`
	MaxExecutionTime        int64  `yaml:"MaxExecutionTime"`
	MaxScanLength           int64  `yaml:"MaxScanLength"`
	MinScanLength           int64  `yaml:"MinScanLength"`
	OperationCount          int64  `yaml:"OperationCount"`
	RecordCount             int64  `yaml:"RecordCount"`
	ThreadCount             int64  `yaml:"ThreadCount"`
	WarmUpTime              int64  `yaml:"WarmUpTime"`
	VirtualNodes            int64  `yaml:"VirtualNodes"`
}

type WorkloadConfig struct {
	Workload                  string  `yaml:"Workload"`
	Command                   string  `yaml:"Command"`
	DoTransactions            bool    `yaml:"DoTransactions"`
	ExponentialFrac           float64 `yaml:"ExponentialFrac"`
	ExponentialPercentile     float64 `yaml:"ExponentialPercentile"`
	HotspotDataFraction       float64 `yaml:"HotspotDataFraction"`
	HotspotOpnFraction        float64 `yaml:"HotspotOpnFraction"`
	InsertOrder               string  `yaml:"InsertOrder"`
	InsertProportion          float64 `yaml:"InsertProportion"`
	InsertStart               int64   `yaml:"InsertStart"`
	KeyPrefix                 string  `yaml:"KeyPrefix"`
	ReadAllFields             bool    `yaml:"ReadAllFields"`
	ReadModifyWriteProportion float64 `yaml:"ReadModifyWriteProportion"`
	ReadProportion            float64 `yaml:"ReadProportion"`
	RequestDistribution       string  `yaml:"RequestDistribution"`
	ScanLengthDistribution    string  `yaml:"ScanLengthDistribution"`
	ScanProportion            float64 `yaml:"ScanProportion"`
	UpdateProportion          float64 `yaml:"UpdateProportion"`
	WriteAllFields            bool    `yaml:"WriteAllFields"`
}

type MeasurementsConfig struct {
	MeasurementType                    string `yaml:"MeasurementType"`
	MeasurementRawOutputFile           string `yaml:"MeasurementRawOutputFile"`
	HistogramPercentilesExport         bool   `yaml:"HistogramPercentilesExport"`
	HistogramPercentilesExportFilepath string `yaml:"HistogramPercentilesExportFilepath"`
	FieldLengthHistogramFile           string `yaml:"FieldLengthHistogramFile"`
	TargetOperationsPerSec             int64  `yaml:"TargetOperationsPerSec"`
	ZeroPadding                        int64  `yaml:"ZeroPadding"`
}

type LoggingConfig struct {
	DebugPprof  string `yaml:"DebugPprof"`
	Label       string `yaml:"Label"`
	LogInterval int64  `yaml:"LogInterval"`
	OutputStyle string `yaml:"OutputStyle"`
	Silence     bool   `yaml:"Silence"`
	Status      string `yaml:"Status"`
	Verbose     bool   `yaml:"Verbose"`
}

type Config struct {
	Database     DatabaseConfig     `yaml:"Database"`
	Performance  PerformanceConfig  `yaml:"Performance"`
	Workload     WorkloadConfig     `yaml:"Workload"`
	Measurements MeasurementsConfig `yaml:"Measurements"`
	Logging      LoggingConfig      `yaml:"Logging"`
}

//type Config struct {
//	BatchSize                                    int     `yaml:"BatchSize"`
//	CassandraCluster                             string  `yaml:"CassandraCluster"`
//	CassandraConnections                         int     `yaml:"CassandraConnections"`
//	CassandraKeyspace                            string  `yaml:"CassandraKeyspace"`
//	CassandraPassword                            string  `yaml:"CassandraPassword"`
//	CassandraUsername                            string  `yaml:"CassandraUsername"`
//	Command                                      string  `yaml:"Command"`
//	DataIntegrity                                bool    `yaml:"DataIntegrity"`
//	DebugPprof                                   string  `yaml:"DebugPprof"`
//	DoTransactions                               bool    `yaml:"DoTransactions"`
//	DropData                                     bool    `yaml:"DropData"`
//	ExponentialFrac                              float64 `yaml:"ExponentialFrac"`
//	ExponentialPercentile                        float64 `yaml:"ExponentialPercentile"`
//	FieldCount                                   int64   `yaml:"FieldCount"`
//	FieldLength                                  int64   `yaml:"FieldLength"`
//	FieldLengthDistribution                      string  `yaml:"FieldLengthDistribution"`
//	FieldLengthHistogramFile                     string  `yaml:"FieldLengthHistogramFile"`
//	HotspotDataFraction                          float64 `yaml:"HotspotDataFraction"`
//	HotspotOpnFraction                           float64 `yaml:"HotspotOpnFraction"`
//	InsertCount                                  int64   `yaml:"InsertCount"`
//	InsertionRetryInterval                       int64   `yaml:"InsertionRetryInterval"`
//	InsertionRetryLimit                          int64   `yaml:"InsertionRetryLimit"`
//	InsertOrder                                  string  `yaml:"InsertOrder"`
//	InsertProportion                             float64 `yaml:"InsertProportion"`
//	InsertStart                                  int64   `yaml:"InsertStart"`
//	KeyPrefix                                    string  `yaml:"KeyPrefix"`
//	Label                                        string  `yaml:"Label"`
//	LogInterval                                  int64   `yaml:"LogInterval"`
//	MaxExecutionTime                             int64   `yaml:"MaxExecutionTime"`
//	MaxScanLength                                int64   `yaml:"MaxScanLength"`
//	MeasurementHistogramPercentileExport         bool    `yaml:"HistogramPercentilesExport"`
//	MeasurementHistogramPercentileExportFilepath string  `yaml:"HistogramPercentilesExportFilepath"`
//	MeasurementRawOutputFile                     string  `yaml:"MeasurementRawOutputFile"`
//	MeasurementType                              string  `yaml:"MeasurementType"`
//	MinScanLength                                int64   `yaml:"MinScanLength"`
//	OperationCount                               int64   `yaml:"OperationCount"`
//	OutputStyle                                  string  `yaml:"OutputStyle"`
//	PasswordAuthenticator                        bool    `yaml:"PasswordAuthenticator"`
//	ReadAllFields                                bool    `yaml:"ReadAllFields"`
//	ReadModifyWriteProportion                    float64 `yaml:"ReadModifyWriteProportion"`
//	ReadProportion                               float64 `yaml:"ReadProportion"`
//	RecordCount                                  int64   `yaml:"RecordCount"`
//	RequestDistribution                          string  `yaml:"RequestDistribution"`
//	ScanLengthDistribution                       string  `yaml:"ScanLengthDistribution"`
//	ScanProportion                               float64 `yaml:"ScanProportion"`
//	Silence                                      bool    `yaml:"Silence"`
//	Status                                       string  `yaml:"Status"`
//	CassandraTableName                           string  `yaml:"CassandraTableName"`
//	TargetOperationsPerSec                       int64   `yaml:"TargetOperationsPerSec"`
//	ThreadCount                                  int64   `yaml:"ThreadCount"`
//	UpdateProportion                             float64 `yaml:"UpdateProportion"`
//	Verbose                                      bool    `yaml:"Verbose"`
//	VirtualNodes                                 int64   `yaml:"VirtualNodes"`
//	WarmUpTime                                   int64   `yaml:"WarmUpTime"`
//	Workload                                     string  `yaml:"Workload"`
//	WriteAllFields                               bool    `yaml:"WriteAllFields"`
//	ZeroPadding                                  int64   `yaml:"ZeroPadding"`
//}

// NewConfig creates a new Config instance, populating it with values
// from a YAML file or using default values if not present.
func NewConfig(yamlFileName string) (*Config, error) {
	// initialize with default values
	defaultConfig := Config{
		Database: DatabaseConfig{
			CassandraCluster:      "127.0.0.1:9042",
			CassandraConnections:  2,
			CassandraKeyspace:     "test",
			CassandraTableName:    "usertable",
			CassandraPassword:     "",
			CassandraUsername:     "",
			DropData:              false,
			PasswordAuthenticator: false,
		},
		Performance: PerformanceConfig{
			BatchSize:               1,
			DataIntegrity:           false,
			FieldCount:              10,
			FieldLength:             100,
			FieldLengthDistribution: "constant",
			InsertCount:             10000,
			InsertionRetryInterval:  3,
			InsertionRetryLimit:     0,
			MaxExecutionTime:        0,
			MaxScanLength:           1000,
			MinScanLength:           1,
			OperationCount:          10000,
			ThreadCount:             200,
			WarmUpTime:              0,
			VirtualNodes:            500,
		},
		Workload: WorkloadConfig{
			Workload:                  "",
			Command:                   "",
			DoTransactions:            false,
			ExponentialFrac:           0.8571428571,
			ExponentialPercentile:     95.0,
			HotspotDataFraction:       0.2,
			HotspotOpnFraction:        0.8,
			InsertOrder:               "hashed",
			InsertProportion:          0.0,
			InsertStart:               10000,
			KeyPrefix:                 "user",
			ReadAllFields:             true,
			ReadModifyWriteProportion: 0.0,
			ReadProportion:            0.95,
			RequestDistribution:       "uniform",
			ScanLengthDistribution:    "uniform",
			ScanProportion:            0.0,
			UpdateProportion:          0.05,
			WriteAllFields:            false,
		},
		Measurements: MeasurementsConfig{
			MeasurementType:                    "histogram",
			MeasurementRawOutputFile:           "",
			HistogramPercentilesExport:         false,
			HistogramPercentilesExportFilepath: "./",
			FieldLengthHistogramFile:           "hist.txt",
			TargetOperationsPerSec:             500,
			ZeroPadding:                        1,
		},
		Logging: LoggingConfig{
			DebugPprof:  ":6060",
			Label:       "",
			LogInterval: 10,
			OutputStyle: "",
			Silence:     true,
			Status:      "",
			Verbose:     false,
		},
	}

	// Read YAML file
	yamlFile, err := os.ReadFile(yamlFileName)
	if err != nil {
		return &defaultConfig, err
	}

	// unmarshal the YAML file
	err = yaml.Unmarshal(yamlFile, &defaultConfig)
	if err != nil {
		return &defaultConfig, err
	}

	return &defaultConfig, nil
}
