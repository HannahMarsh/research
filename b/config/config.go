package config

import (
	"gopkg.in/yaml.v3"
	"os"
)

type Config struct {
	BatchSize                                    int     `yaml:"BatchSize"`
	CassandraCluster                             string  `yaml:"CassandraCluster"`
	CassandraConnections                         int     `yaml:"CassandraConnections"`
	CassandraKeyspace                            string  `yaml:"CassandraKeyspace"`
	CassandraPassword                            string  `yaml:"CassandraPassword"`
	CassandraUsername                            string  `yaml:"CassandraUsername"`
	Command                                      string  `yaml:"Command"`
	DataIntegrity                                bool    `yaml:"DataIntegrity"`
	DebugPprof                                   string  `yaml:"DebugPprof"`
	DoTransactions                               bool    `yaml:"DoTransactions"`
	DropData                                     bool    `yaml:"DropData"`
	ExponentialFrac                              float64 `yaml:"ExponentialFrac"`
	ExponentialPercentile                        float64 `yaml:"ExponentialPercentile"`
	FieldCount                                   int64   `yaml:"FieldCount"`
	FieldLength                                  int64   `yaml:"FieldLength"`
	FieldLengthDistribution                      string  `yaml:"FieldLengthDistribution"`
	FieldLengthHistogramFile                     string  `yaml:"FieldLengthHistogramFile"`
	HotspotDataFraction                          float64 `yaml:"HotspotDataFraction"`
	HotspotOpnFraction                           float64 `yaml:"HotspotOpnFraction"`
	InsertCount                                  int64   `yaml:"InsertCount"`
	InsertionRetryInterval                       int64   `yaml:"InsertionRetryInterval"`
	InsertionRetryLimit                          int64   `yaml:"InsertionRetryLimit"`
	InsertOrder                                  string  `yaml:"InsertOrder"`
	InsertProportion                             float64 `yaml:"InsertProportion"`
	InsertStart                                  int64   `yaml:"InsertStart"`
	KeyPrefix                                    string  `yaml:"KeyPrefix"`
	Label                                        string  `yaml:"Label"`
	LogInterval                                  int64   `yaml:"LogInterval"`
	MaxExecutionTime                             int64   `yaml:"MaxExecutionTime"`
	MaxScanLength                                int64   `yaml:"MaxScanLength"`
	MeasurementHistogramPercentileExport         bool    `yaml:"HistogramPercentilesExport"`
	MeasurementHistogramPercentileExportFilepath string  `yaml:"HistogramPercentilesExportFilepath"`
	MeasurementRawOutputFile                     string  `yaml:"MeasurementRawOutputFile"`
	MeasurementType                              string  `yaml:"MeasurementType"`
	MinScanLength                                int64   `yaml:"MinScanLength"`
	OperationCount                               int64   `yaml:"OperationCount"`
	OutputStyle                                  string  `yaml:"OutputStyle"`
	PasswordAuthenticator                        bool    `yaml:"PasswordAuthenticator"`
	ReadAllFields                                bool    `yaml:"ReadAllFields"`
	ReadModifyWriteProportion                    float64 `yaml:"ReadModifyWriteProportion"`
	ReadProportion                               float64 `yaml:"ReadProportion"`
	RecordCount                                  int64   `yaml:"RecordCount"`
	RequestDistribution                          string  `yaml:"RequestDistribution"`
	ScanLengthDistribution                       string  `yaml:"ScanLengthDistribution"`
	ScanProportion                               float64 `yaml:"ScanProportion"`
	Silence                                      bool    `yaml:"Silence"`
	Status                                       string  `yaml:"Status"`
	TableName                                    string  `yaml:"TableName"`
	TargetOperationsPerSec                       int64   `yaml:"TargetOperationsPerSec"` // target operations per second
	ThreadCount                                  int64   `yaml:"ThreadCount"`
	UpdateProportion                             float64 `yaml:"UpdateProportion"`
	Verbose                                      bool    `yaml:"Verbose"`
	VirtualNodes                                 int64   `yaml:"VirtualNodes"`
	WarmUpTime                                   int64   `yaml:"WarmUpTime"`
	Workload                                     string  `yaml:"Workload"`
	WriteAllFields                               bool    `yaml:"WriteAllFields"`
	ZeroPadding                                  int64   `yaml:"ZeroPadding"`
}

// NewConfig creates a new Config instance, populating it with values
// from a YAML file or using default values if not present.
func NewConfig(yamlFileName string) (*Config, error) {
	// initialize with default values
	defaultConfig := Config{
		BatchSize:                            1,
		CassandraCluster:                     "127.0.0.1:9042",
		CassandraConnections:                 2,
		CassandraKeyspace:                    "test",
		CassandraPassword:                    "",
		CassandraUsername:                    "",
		Command:                              "",
		DataIntegrity:                        false,
		DebugPprof:                           ":6060",
		DoTransactions:                       false,
		DropData:                             false,
		ExponentialFrac:                      0.8571428571,
		ExponentialPercentile:                95.0,
		FieldCount:                           10,
		FieldLength:                          100,
		FieldLengthDistribution:              "constant",
		FieldLengthHistogramFile:             "hist.txt",
		HotspotDataFraction:                  0.2,
		HotspotOpnFraction:                   0.8,
		InsertCount:                          0,
		InsertionRetryInterval:               3,
		InsertionRetryLimit:                  0,
		InsertOrder:                          "hashed",
		InsertProportion:                     0.0,
		InsertStart:                          0,
		KeyPrefix:                            "user",
		Label:                                "",
		LogInterval:                          0,
		MaxExecutionTime:                     0,
		MaxScanLength:                        1000,
		MeasurementHistogramPercentileExport: false,
		MeasurementHistogramPercentileExportFilepath: "./",
		MeasurementRawOutputFile:                     "",
		MeasurementType:                              "histogram",
		MinScanLength:                                1,
		OperationCount:                               0,
		OutputStyle:                                  "plain",
		PasswordAuthenticator:                        false,
		ReadAllFields:                                true,
		ReadModifyWriteProportion:                    0.0,
		ReadProportion:                               0.95,
		RecordCount:                                  0,
		RequestDistribution:                          "uniform",
		ScanLengthDistribution:                       "uniform",
		ScanProportion:                               0.0,
		Silence:                                      true,
		Status:                                       "",
		TableName:                                    "usertable",
		TargetOperationsPerSec:                       500,
		ThreadCount:                                  200,
		UpdateProportion:                             0.05,
		Verbose:                                      false,
		VirtualNodes:                                 500,
		WarmUpTime:                                   0,
		Workload:                                     "",
		WriteAllFields:                               false,
		ZeroPadding:                                  1,
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
