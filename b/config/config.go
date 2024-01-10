package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

type IntProperty struct {
	Description string `yaml:"Description"`
	Value       int    `yaml:"Value"`
}

type FloatProperty struct {
	Description string  `yaml:"Description"`
	Value       float64 `yaml:"Value"`
}

type StringProperty struct {
	Description string `yaml:"Description"`
	Value       string `yaml:"Value"`
}

type BoolProperty struct {
	Description string `yaml:"Description"`
	Value       bool   `yaml:"Value"`
}

type ArrayProperty struct {
	Description string `yaml:"Description"`
	Value       int    `yaml:"Value"`
}

type DatabaseConfig struct {
	CassandraCluster      StringProperty `yaml:"CassandraCluster"`
	CassandraConnections  IntProperty    `yaml:"CassandraConnections"`
	CassandraKeyspace     StringProperty `yaml:"CassandraKeyspace"`
	CassandraTableName    StringProperty `yaml:"CassandraTableName"`
	CassandraPassword     StringProperty `yaml:"CassandraPassword"`
	CassandraUsername     StringProperty `yaml:"CassandraUsername"`
	PasswordAuthenticator BoolProperty   `yaml:"PasswordAuthenticator"`
}

type PerformanceConfig struct {
	BatchSize                  IntProperty    `yaml:"BatchSize"`
	PerformDataIntegrityChecks BoolProperty   `yaml:"PerformDataIntegrityChecks"`
	EnableDroppingDataOnStart  BoolProperty   `yaml:"EnableDroppingDataOnStart"`
	MaxFields                  IntProperty    `yaml:"MaxFields"`
	AvFieldSizeBytes           IntProperty    `yaml:"AvFieldSizeBytes"`
	FieldSizeDistribution      StringProperty `yaml:"FieldSizeDistribution"`
	InsertCount                IntProperty    `yaml:"InsertCount"`
	InsertionRetryInterval     IntProperty    `yaml:"InsertionRetryInterval"`
	InsertionRetryLimit        IntProperty    `yaml:"InsertionRetryLimit"`
	MaxExecutionTime           IntProperty    `yaml:"MaxExecutionTime"`
	MaxScanLength              IntProperty    `yaml:"MaxScanLength"`
	MinScanLength              IntProperty    `yaml:"MinScanLength"`
	OperationCount             IntProperty    `yaml:"OperationCount"`
	RecordCount                IntProperty    `yaml:"RecordCount"`
	TargetOperationsPerSec     IntProperty    `yaml:"TargetOperationsPerSec"`
	ThreadCount                IntProperty    `yaml:"ThreadCount"`
	VirtualNodes               IntProperty    `yaml:"VirtualNodes"`
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
	MeasurementType            StringProperty `yaml:"MeasurementType"`
	RawOutputDir               StringProperty `yaml:"RawOutputDir"`
	HistogramPercentilesExport BoolProperty   `yaml:"HistogramPercentilesExport"`
	HistogramOutputDir         StringProperty `yaml:"HistogramOutputDir"`
	FieldLengthHistogramFile   StringProperty `yaml:"FieldLengthHistogramFile"`
	OutputStyle                StringProperty `yaml:"OutputStyle"`
	WarmUpTime                 IntProperty    `yaml:"WarmUpTime"`
	ZeroPadding                IntProperty    `yaml:"ZeroPadding"`
}

type LoggingConfig struct {
	DebugPprof  StringProperty `yaml:"DebugPprof"`
	Label       StringProperty `yaml:"Label"`
	LogInterval IntProperty    `yaml:"LogInterval"`
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

var defaultConfig_ = Config{
	Database: DatabaseConfig{
		CassandraCluster: StringProperty{
			Value:       "127.0.0.1:9042",
			Description: "The host and port of the Cassandra cluster.",
		},
		CassandraConnections: IntProperty{
			Value:       2,
			Description: "The number of connections to the Cassandra cluster.",
		},
		CassandraKeyspace: StringProperty{
			Value:       "test",
			Description: "Keyspace to use within the Cassandra database.",
		},
		CassandraTableName: StringProperty{
			Value:       "usertable",
			Description: "Name of the table to use within the Cassandra keyspace.",
		},
		CassandraPassword: StringProperty{
			Value:       "",
			Description: "The password for authenticating with Cassandra, if required.",
		},
		CassandraUsername: StringProperty{
			Value:       "",
			Description: "The username for authenticating with Cassandra, if required.",
		},
		PasswordAuthenticator: BoolProperty{
			Value:       false,
			Description: "Enables the use of Cassandra's PasswordAuthenticator for client connections. If this is true,\nthen the `CassandraUsername` and `CassandraPassword` properties must be non-empty and valid.",
		},
	},
	Performance: PerformanceConfig{
		BatchSize: IntProperty{
			Value:       1,
			Description: "The number of operations to batch together in a single transaction. If this value is 1, then batching is disabled.",
		},
		PerformDataIntegrityChecks: BoolProperty{
			Value:       false,
			Description: "Enables performing data integrity checks during operations. If enabled, `FieldSizeDistribution` must be constant.",
		},
		EnableDroppingDataOnStart: BoolProperty{
			Value:       false,
			Description: "Enables dropping any pre-existing database data upon startup.",
		},
		MaxFields: IntProperty{
			Value:       10,
			Description: "The maximum number of fields (columns) to include in the database table.",
		},
		AvFieldSizeBytes: IntProperty{
			Value:       100,
			Description: "The average size (in bytes) of each field stored in the database.",
		},
		FieldSizeDistribution: StringProperty{
			Value:       "constant",
			Description: "The type of distribution used to vary the length of fields in data records. Options are 'constant', 'unfiorm', 'zipfian', and 'histogram'",
		},
		InsertCount: IntProperty{
			Value:       10000,
			Description: "The total number of records to insert during the workload execution.",
		},
		InsertionRetryInterval: IntProperty{
			Value:       3,
			Description: "The time in seconds to wait before retrying a failed insert operation. This controls the\nback-off strategy for handling write failures.",
		},
		InsertionRetryLimit: IntProperty{
			Value:       0,
			Description: "The maximum number of times to retry a failed insert operation.",
		},
		MaxExecutionTime: IntProperty{
			Value:       30,
			Description: "The maximum allowed time for the benchmark to run before it is forcibly stopped.",
		},
		MaxScanLength: IntProperty{
			Value:       1000,
			Description: "The maximum number of records to scan in a single operation.",
		},
		MinScanLength: IntProperty{
			Value:       1,
			Description: "The minimum number of records to scan in a single operation.",
		},
		OperationCount: IntProperty{
			Value:       10000,
			Description: "The total number of operations to perform during the workload execution.",
		},
		TargetOperationsPerSec: IntProperty{
			Value:       500,
			Description: "The target number of operations per second that the workload should aim to achieve.",
		},
		ThreadCount: IntProperty{
			Value:       200,
			Description: "The number of concurrent threads to use when executing the workload.",
		},
		VirtualNodes: IntProperty{
			Value:       500,
			Description: "The number of virtual nodes to simulate or use in the execution of the workload.",
		},
	},
	Workload: WorkloadConfig{
		Workload: StringProperty{
			Value:       "workload1",
			Description: "The name of the workload to be executed.",
		},
		Command: StringProperty{
			Value:       "<>",
			Description: "The specific command to run as part of the workload. This is set automatically by the program.",
		},
		DoTransactions: BoolProperty{
			Value:       false,
			Description: "Indicates whether transactions should be executed.",
		},
		ExponentialFrac: FloatProperty{
			Value:       0.8571428571,
			Description: "The fraction of the exponential function used for generating workload distributions.",
		},
		ExponentialPercentile: FloatProperty{
			Value:       95.0,
			Description: "The target percentile for the exponential distribution when generating workloads.",
		},
		HotspotDataFraction: FloatProperty{
			Value:       0.2,
			Description: "The fraction of data that will be considered 'hot' for generating hotspots in the workload.",
		},
		HotspotOpnFraction: FloatProperty{
			Value:       0.8,
			Description: "The fraction of operations that will be focused on the 'hot' data.",
		},
		InsertOrder: StringProperty{
			Value:       "hashed",
			Description: "The order in which records are inserted, which can be 'hashed' or another specified order.",
		},
		InsertProportion: FloatProperty{
			Value:       0.0,
			Description: "The proportion of insert operations in the workload.",
		},
		InsertStart: IntProperty{
			Value:       10000,
			Description: "The starting point for insert operations in the workload.",
		},
		KeyPrefix: StringProperty{
			Value:       "user",
			Description: "The prefix to be used for keys in the workload.",
		},
		ReadAllFields: BoolProperty{
			Value:       true,
			Description: "Indicates whether all fields should be read in read operations.",
		},
		ReadModifyWriteProportion: FloatProperty{
			Value:       0.0,
			Description: "The proportion of read-modify-write operations in the workload.",
		},
		ReadProportion: FloatProperty{
			Value:       0.95,
			Description: "The proportion of read operations in the workload.",
		},
		RequestDistribution: StringProperty{
			Value:       "uniform",
			Description: "The distribution of request types in the workload (to simulate different access patterns on the dataset).\nOptions are 'uniform', 'sequential', 'zipfian', 'latest', 'hotspot', and 'exponential'.",
		},
		ScanLengthDistribution: StringProperty{
			Value:       "uniform",
			Description: "The distribution for the number of records to scan during scan operations (to simulate\ndifferent data access spreads). Options are 'uniform' and 'zipfian'.",
		},
		ScanProportion: FloatProperty{
			Value:       0.0,
			Description: "The proportion (from 0.0 to 1.0) of scan operations in the workload. If the value is 0.0, then scanning is disabled.",
		},
		UpdateProportion: FloatProperty{
			Value:       0.05,
			Description: "The proportion (from 0.0 to 1.0) of update operations in the workload. If the value is 0.0, then updating is disabled.",
		},
		WriteAllFields: BoolProperty{
			Value:       false,
			Description: "Indicates whether all fields should be written in write operations (as opposed to updating).",
		},
	},
	Measurements: MeasurementsConfig{
		MeasurementType: StringProperty{
			Value:       "histogram",
			Description: "Specifies the type of measurement for performance metrics. Valid values are 'histogram', 'raw', and 'csv'.",
		},
		RawOutputDir: StringProperty{
			Value:       "data/raw/",
			Description: "The directory to output raw measurement data, if any.",
		},
		HistogramPercentilesExport: BoolProperty{
			Value:       false,
			Description: "Enables exporting percentile data to the directory given by `HistogramOutputDir`.",
		},
		HistogramOutputDir: StringProperty{
			Value:       "data/histogram/percentiles/",
			Description: "The directory where histogram percentile data files are to be saved.",
		},
		FieldLengthHistogramFile: StringProperty{
			Value:       "data/histogram/field-lengths.txt",
			Description: "The file path of the histogram that has the generated field length distribution.",
		},
		OutputStyle: StringProperty{
			Value:       "table",
			Description: "Defines the formatting style for outputting measurement data. Valid values are 'plain' 'table', and 'json'.",
		},
		WarmUpTime: IntProperty{
			Value:       2,
			Description: "The duration in seconds before metrics collection starts (allows the system to reach a steady operational state).",
		},
		ZeroPadding: IntProperty{
			Value:       1,
			Description: "The amount of zero-padding for numeric fields (for a fixed width representation).",
		},
	},
	Logging: LoggingConfig{
		DebugPprof: StringProperty{
			Value:       ":6060",
			Description: "The address to bind the pprof debugging server to, for profiling and debugging purposes.",
		},
		Label: StringProperty{
			Value:       "benchmark_log",
			Description: "A label to tag log entries for easier filtering.",
		},
		LogInterval: IntProperty{
			Value:       3,
			Description: "The interval, in seconds, at which log entries should be written to the log output.",
		},
		Silence: BoolProperty{
			Value:       true,
			Description: "If set to true, suppresses the output of logs to the console or log files.",
		},
		Status: StringProperty{
			Value:       "",
			Description: "A field to log the current status of the application.",
		},
		Verbose: BoolProperty{
			Value:       false,
			Description: "Enables verbose logging for debugging purposes.",
		},
	},
}

func GetDefaultConfig() Config {
	defaultConfig := defaultConfig_
	yamlData, err := yaml.Marshal(&defaultConfig)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	yamlFilePath := filepath.Join("config", "default.yaml")
	err = os.WriteFile(yamlFilePath, yamlData, 0644)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = os.WriteFile("/Users/hanma/cloud computing research/research/b/tool/property_files/0.yaml", yamlData, 0644)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return defaultConfig
}

// NewConfig creates a new Config instance, populating it with values
// from a YAML file or using default values if not present.
func NewConfig(yamlFileName string) (*Config, error) {
	// initialize with default values
	defaultConfig := GetDefaultConfig()

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
