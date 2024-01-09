package config

import (
	"gopkg.in/yaml.v3"
	"os"
)

type Config struct {
	InsertStart                                  int64   `yaml:"InsertStart"`
	InsertCount                                  int64   `yaml:"InsertCount"`
	OperationCount                               int64   `yaml:"OperationCount"`
	RecordCount                                  int64   `yaml:"RecordCount"`
	Workload                                     string  `yaml:"Workload"`
	ThreadCount                                  int64   `yaml:"ThreadCount"`
	Target                                       int64   `yaml:"Target"` // target operations per second
	MaxExecutionTime                             int64   `yaml:"MaxExecutionTime"`
	WarmUpTime                                   int64   `yaml:"WarmUpTime"`
	DoTransactions                               bool    `yaml:"DoTransactions"`
	Status                                       string  `yaml:"Status"`
	Label                                        string  `yaml:"Label"`
	BatchSize                                    int     `yaml:"BatchSize"`
	TableName                                    string  `yaml:"TableName"`
	FieldCount                                   int64   `yaml:"FieldCount"`
	FieldLengthDistribution                      string  `yaml:"FieldLengthDistribution"`
	FieldLength                                  int64   `yaml:"FieldLength"`
	FieldLengthHistogramFile                     string  `yaml:"FieldLengthHistogramFile"`
	ReadAllFields                                bool    `yaml:"ReadAllFields"`
	WriteAllFields                               bool    `yaml:"WriteAllFields"`
	DataIntegrity                                bool    `yaml:"DataIntegrity"`
	ReadProportion                               float64 `yaml:"ReadProportion"`
	UpdateProportion                             float64 `yaml:"UpdateProportion"`
	InsertProportion                             float64 `yaml:"InsertProportion"`
	ScanProportion                               float64 `yaml:"ScanProportion"`
	ReadModifyWriteProportion                    float64 `yaml:"ReadModifyWriteProportion"`
	RequestDistribution                          string  `yaml:"RequestDistribution"`
	ZeroPadding                                  int64   `yaml:"ZeroPadding"`
	MinScanLength                                int64   `yaml:"MinScanLength"`
	MaxScanLength                                int64   `yaml:"MaxScanLength"`
	ScanLengthDistribution                       string  `yaml:"ScanLengthDistribution"`
	InsertOrder                                  string  `yaml:"InsertOrder"`
	HotspotDataFraction                          float64 `yaml:"HotspotDataFraction"`
	HotspotOpnFraction                           float64 `yaml:"HotspotOpnFraction"`
	InsertionRetryLimit                          int64   `yaml:"InsertionRetryLimit"`
	InsertionRetryInterval                       int64   `yaml:"InsertionRetryInterval"`
	ExponentialPercentile                        float64 `yaml:"ExponentialPercentile"`
	ExponentialFrac                              float64 `yaml:"ExponentialFrac"`
	DebugPprof                                   string  `yaml:"DebugPprof"`
	Verbose                                      bool    `yaml:"Verbose"`
	DropData                                     bool    `yaml:"DropData"`
	Silence                                      bool    `yaml:"Silence"`
	KeyPrefix                                    string  `yaml:"KeyPrefix"`
	LogInterval                                  int64   `yaml:"LogInterval"`
	MeasurementType                              string  `yaml:"MeasurementType"`
	MeasurementRawOutputFile                     string  `yaml:"MeasurementRawOutputFile"`
	Command                                      string  `yaml:"Command"`
	OutputStyle                                  string  `yaml:"OutputStyle"`
	MeasurementHistogramPercentileExport         bool    `yaml:"HistogramPercentilesExport"`
	MeasurementHistogramPercentileExportFilepath string  `yaml:"HistogramPercentilesExportFilepath"`
	CassandraCluster                             string  `yaml:"CassandraCluster"`
	CassandraKeyspace                            string  `yaml:"CassandraKeyspace"`
	CassandraConnections                         int     `yaml:"CassandraConnections"`
	CassandraUsername                            string  `yaml:"CassandraUsername"`
	CassandraPassword                            string  `yaml:"CassandraPassword"`
}

// NewConfig creates a new Config instance, populating it with values
// from a YAML file or using default values if not present.
func NewConfig(yamlFileName string) (*Config, error) {
	// initialize with default values
	defaultConfig := Config{
		InsertStart:                          0,
		InsertCount:                          0,
		OperationCount:                       0,
		RecordCount:                          0,
		Workload:                             "",
		ThreadCount:                          200,
		Target:                               500,
		MaxExecutionTime:                     0,
		WarmUpTime:                           0,
		DoTransactions:                       false,
		Status:                               "",
		Label:                                "",
		BatchSize:                            1,
		TableName:                            "usertable",
		FieldCount:                           10,
		FieldLengthDistribution:              "constant",
		FieldLength:                          100,
		FieldLengthHistogramFile:             "hist.txt",
		ReadAllFields:                        true,
		WriteAllFields:                       false,
		DataIntegrity:                        false,
		ReadProportion:                       0.95,
		UpdateProportion:                     0.05,
		InsertProportion:                     0.0,
		ScanProportion:                       0.0,
		ReadModifyWriteProportion:            0.0,
		RequestDistribution:                  "uniform",
		ZeroPadding:                          1,
		MinScanLength:                        1,
		MaxScanLength:                        1000,
		ScanLengthDistribution:               "uniform",
		InsertOrder:                          "hashed",
		HotspotDataFraction:                  0.2,
		HotspotOpnFraction:                   0.8,
		InsertionRetryLimit:                  0,
		InsertionRetryInterval:               3,
		ExponentialPercentile:                95.0,
		ExponentialFrac:                      0.8571428571,
		DebugPprof:                           ":6060",
		Verbose:                              false,
		DropData:                             false,
		Silence:                              true,
		KeyPrefix:                            "user",
		LogInterval:                          0,
		MeasurementType:                      "histogram",
		MeasurementRawOutputFile:             "",
		Command:                              "",
		OutputStyle:                          "plain",
		MeasurementHistogramPercentileExport: false,
		MeasurementHistogramPercentileExportFilepath: "./",
		CassandraCluster:     "127.0.0.1:9042",
		CassandraKeyspace:    "test",
		CassandraConnections: 2,
		CassandraUsername:    "cassandra",
		CassandraPassword:    "cassandra",
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
