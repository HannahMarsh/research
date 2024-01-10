package config

import (
	"gopkg.in/yaml.v3"
	"os"
)

type IntProperty struct {
	Value       int    `yaml:"Value"`
	Description string `yaml:"Description"`
}

type FloatProperty struct {
	Value       float64 `yaml:"Value"`
	Description string  `yaml:"Description"`
}

type StringProperty struct {
	Value       string `yaml:"Value"`
	Description string `yaml:"Description"`
}

type BoolProperty struct {
	Value       bool   `yaml:"Value"`
	Description string `yaml:"Description"`
}

type ArrayProperty struct {
	Value       int    `yaml:"Value"`
	Description string `yaml:"Description"`
}

type DatabaseConfig struct {
	CassandraCluster      StringProperty `yaml:"CassandraCluster"`
	CassandraConnections  IntProperty    `yaml:"CassandraConnections"`
	CassandraKeyspace     StringProperty `yaml:"CassandraKeyspace"`
	CassandraTableName    StringProperty `yaml:"CassandraTableName"`
	CassandraPassword     StringProperty `yaml:"CassandraPassword"`
	CassandraUsername     StringProperty `yaml:"CassandraUsername"`
	DropData              BoolProperty   `yaml:"DropData"`
	PasswordAuthenticator BoolProperty   `yaml:"PasswordAuthenticator"`
}

type PerformanceConfig struct {
	BatchSize               IntProperty    `yaml:"BatchSize"`
	DataIntegrity           BoolProperty   `yaml:"DataIntegrity"`
	FieldCount              IntProperty    `yaml:"FieldCount"`
	FieldLength             IntProperty    `yaml:"FieldLength"`
	FieldLengthDistribution StringProperty `yaml:"FieldLengthDistribution"`
	InsertCount             IntProperty    `yaml:"InsertCount"`
	InsertionRetryInterval  IntProperty    `yaml:"InsertionRetryInterval"`
	InsertionRetryLimit     IntProperty    `yaml:"InsertionRetryLimit"`
	MaxExecutionTime        IntProperty    `yaml:"MaxExecutionTime"`
	MaxScanLength           IntProperty    `yaml:"MaxScanLength"`
	MinScanLength           IntProperty    `yaml:"MinScanLength"`
	OperationCount          IntProperty    `yaml:"OperationCount"`
	RecordCount             IntProperty    `yaml:"RecordCount"`
	ThreadCount             IntProperty    `yaml:"ThreadCount"`
	WarmUpTime              IntProperty    `yaml:"WarmUpTime"`
	VirtualNodes            IntProperty    `yaml:"VirtualNodes"`
}

type WorkloadConfig struct {
	Workload                  StringProperty `yaml:"Workload"`
	Command                   StringProperty `yaml:"Command"`
	DoTransactions            BoolProperty   `yaml:"DoTransactions"`
	ExponentialFrac           FloatProperty  `yaml:"ExponentialFrac"`
	ExponentialPercentile     FloatProperty  `yaml:"ExponentialPercentile"`
	HotspotDataFraction       FloatProperty  `yaml:"HotspotDataFraction"`
	HotspotOpnFraction        FloatProperty  `yaml:"HotspotOpnFraction"`
	InsertOrder               StringProperty `yaml:"InsertOrder"`
	InsertProportion          FloatProperty  `yaml:"InsertProportion"`
	InsertStart               IntProperty    `yaml:"InsertStart"`
	KeyPrefix                 StringProperty `yaml:"KeyPrefix"`
	ReadAllFields             BoolProperty   `yaml:"ReadAllFields"`
	ReadModifyWriteProportion FloatProperty  `yaml:"ReadModifyWriteProportion"`
	ReadProportion            FloatProperty  `yaml:"ReadProportion"`
	RequestDistribution       StringProperty `yaml:"RequestDistribution"`
	ScanLengthDistribution    StringProperty `yaml:"ScanLengthDistribution"`
	ScanProportion            FloatProperty  `yaml:"ScanProportion"`
	UpdateProportion          FloatProperty  `yaml:"UpdateProportion"`
	WriteAllFields            BoolProperty   `yaml:"WriteAllFields"`
}

type MeasurementsConfig struct {
	MeasurementType                    StringProperty `yaml:"MeasurementType"`
	MeasurementRawOutputFile           StringProperty `yaml:"MeasurementRawOutputFile"`
	HistogramPercentilesExport         BoolProperty   `yaml:"HistogramPercentilesExport"`
	HistogramPercentilesExportFilepath StringProperty `yaml:"HistogramPercentilesExportFilepath"`
	FieldLengthHistogramFile           StringProperty `yaml:"FieldLengthHistogramFile"`
	TargetOperationsPerSec             IntProperty    `yaml:"TargetOperationsPerSec"`
	ZeroPadding                        IntProperty    `yaml:"ZeroPadding"`
}

type LoggingConfig struct {
	DebugPprof  StringProperty `yaml:"DebugPprof"`
	Label       StringProperty `yaml:"Label"`
	LogInterval IntProperty    `yaml:"LogInterval"`
	OutputStyle StringProperty `yaml:"OutputStyle"`
	Silence     BoolProperty   `yaml:"Silence"`
	Status      StringProperty `yaml:"Status"`
	Verbose     BoolProperty   `yaml:"Verbose"`
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
			CassandraCluster: StringProperty{
				Value:       "127.0.0.1:9042",
				Description: "",
			},
			CassandraConnections: IntProperty{
				Value:       2,
				Description: "",
			},
			CassandraKeyspace: StringProperty{
				Value:       "test",
				Description: "",
			},
			CassandraTableName: StringProperty{
				Value:       "usertable",
				Description: "",
			},
			CassandraPassword: StringProperty{
				Value:       "",
				Description: "",
			},
			CassandraUsername: StringProperty{
				Value:       "",
				Description: "",
			},
			DropData: BoolProperty{
				Value:       false,
				Description: "",
			},
			PasswordAuthenticator: BoolProperty{
				Value:       false,
				Description: "",
			},
		},
		Performance: PerformanceConfig{
			BatchSize: IntProperty{
				Value:       1,
				Description: "",
			},
			DataIntegrity: BoolProperty{
				Value:       false,
				Description: "",
			},
			FieldCount: IntProperty{
				Value:       10,
				Description: "",
			},
			FieldLength: IntProperty{
				Value:       100,
				Description: "",
			},
			FieldLengthDistribution: StringProperty{
				Value:       "constant",
				Description: "",
			},
			InsertCount: IntProperty{
				Value:       10000,
				Description: "",
			},
			InsertionRetryInterval: IntProperty{
				Value:       3,
				Description: "",
			},
			InsertionRetryLimit: IntProperty{
				Value:       0,
				Description: "",
			},
			MaxExecutionTime: IntProperty{
				Value:       0,
				Description: "",
			},
			MaxScanLength: IntProperty{
				Value:       1000,
				Description: "",
			},
			MinScanLength: IntProperty{
				Value:       1,
				Description: "",
			},
			OperationCount: IntProperty{
				Value:       10000,
				Description: "",
			},
			ThreadCount: IntProperty{
				Value:       200,
				Description: "",
			},
			WarmUpTime: IntProperty{
				Value:       0,
				Description: "",
			},
			VirtualNodes: IntProperty{
				Value:       500,
				Description: "",
			},
		},
		Workload: WorkloadConfig{
			Workload: StringProperty{
				Value:       "",
				Description: "",
			},
			Command: StringProperty{
				Value:       "",
				Description: "",
			},
			DoTransactions: BoolProperty{
				Value:       false,
				Description: "",
			},
			ExponentialFrac: FloatProperty{
				Value:       0.8571428571,
				Description: "",
			},
			ExponentialPercentile: FloatProperty{
				Value:       95.0,
				Description: "",
			},
			HotspotDataFraction: FloatProperty{
				Value:       0.2,
				Description: "",
			},
			HotspotOpnFraction: FloatProperty{
				Value:       0.8,
				Description: "",
			},
			InsertOrder: StringProperty{
				Value:       "hashed",
				Description: "",
			},
			InsertProportion: FloatProperty{
				Value:       0.0,
				Description: "",
			},
			InsertStart: IntProperty{
				Value:       10000,
				Description: "",
			},
			KeyPrefix: StringProperty{
				Value:       "user",
				Description: "",
			},
			ReadAllFields: BoolProperty{
				Value:       true,
				Description: "",
			},
			ReadModifyWriteProportion: FloatProperty{
				Value:       0.0,
				Description: "",
			},
			ReadProportion: FloatProperty{
				Value:       0.95,
				Description: "",
			},
			RequestDistribution: StringProperty{
				Value:       "uniform",
				Description: "",
			},
			ScanLengthDistribution: StringProperty{
				Value:       "uniform",
				Description: "",
			},
			ScanProportion: FloatProperty{
				Value:       0.0,
				Description: "",
			},
			UpdateProportion: FloatProperty{
				Value:       0.05,
				Description: "",
			},
			WriteAllFields: BoolProperty{
				Value:       false,
				Description: "",
			},
		},
		Measurements: MeasurementsConfig{
			MeasurementType: StringProperty{
				Value:       "histogram",
				Description: "",
			},
			MeasurementRawOutputFile: StringProperty{
				Value:       "",
				Description: "",
			},
			HistogramPercentilesExport: BoolProperty{
				Value:       false,
				Description: "",
			},
			HistogramPercentilesExportFilepath: StringProperty{
				Value:       "./",
				Description: "",
			},
			FieldLengthHistogramFile: StringProperty{
				Value:       "hist.txt",
				Description: "",
			},
			TargetOperationsPerSec: IntProperty{
				Value:       500,
				Description: "",
			},
			ZeroPadding: IntProperty{
				Value:       1,
				Description: "",
			},
		},
		Logging: LoggingConfig{
			DebugPprof: StringProperty{
				Value:       ":6060",
				Description: "",
			},
			Label: StringProperty{
				Value:       "",
				Description: "",
			},
			LogInterval: IntProperty{
				Value:       10,
				Description: "",
			},
			OutputStyle: StringProperty{
				Value:       "",
				Description: "",
			},
			Silence: BoolProperty{
				Value:       true,
				Description: "",
			},
			Status: StringProperty{
				Value:       "",
				Description: "",
			},
			Verbose: BoolProperty{
				Value:       false,
				Description: "",
			},
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
