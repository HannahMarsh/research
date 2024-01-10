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

func GetDefaultConfig() Config {
	return Config{
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
			DropData: BoolProperty{
				Value:       false,
				Description: "Indicates whether to drop any pre-existing database data upon startup.",
			},
			PasswordAuthenticator: BoolProperty{
				Value:       false,
				Description: "Indicates if Cassandra's PasswordAuthenticator is used for client connections.",
			},
		},
		Performance: PerformanceConfig{
			BatchSize: IntProperty{
				Value:       1,
				Description: "The number of operations to batch together in a single transaction or request.",
			},
			DataIntegrity: BoolProperty{
				Value:       false,
				Description: "Whether to perform data integrity checks during operations.",
			},
			FieldCount: IntProperty{
				Value:       10,
				Description: "The number of fields (columns) to include in a database table or a data model.",
			},
			FieldLength: IntProperty{
				Value:       100,
				Description: "The fixed length of data to store in each field of the database.",
			},
			FieldLengthDistribution: StringProperty{
				Value:       "constant",
				Description: "The type of distribution used to vary the length of fields in data records.",
			},
			InsertCount: IntProperty{
				Value:       10000,
				Description: "The total number of records to insert during the workload execution.",
			},
			InsertionRetryInterval: IntProperty{
				Value:       3,
				Description: "The time in seconds to wait before retrying a failed insert operation.",
			},
			InsertionRetryLimit: IntProperty{
				Value:       0,
				Description: "The maximum number of retry attempts for a failed insert operation.",
			},
			MaxExecutionTime: IntProperty{
				Value:       0,
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
			ThreadCount: IntProperty{
				Value:       200,
				Description: "The number of concurrent threads to use when executing the workload.",
			},
			WarmUpTime: IntProperty{
				Value:       0,
				Description: "The duration in seconds to run the workload before measurement begins.",
			},
			VirtualNodes: IntProperty{
				Value:       500,
				Description: "The number of virtual nodes to simulate or use in the execution of the workload.",
			},
		},
		Workload: WorkloadConfig{
			Workload: StringProperty{
				Value:       "",
				Description: "The name of the workload to be executed.",
			},
			Command: StringProperty{
				Value:       "",
				Description: "The specific command to run as part of the workload.",
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
				Description: "The distribution of request types in the workload, such as 'uniform'.",
			},
			ScanLengthDistribution: StringProperty{
				Value:       "uniform",
				Description: "The distribution used to determine scan lengths in scan operations.",
			},
			ScanProportion: FloatProperty{
				Value:       0.0,
				Description: "The proportion of scan operations in the workload.",
			},
			UpdateProportion: FloatProperty{
				Value:       0.05,
				Description: "The proportion of update operations in the workload.",
			},
			WriteAllFields: BoolProperty{
				Value:       false,
				Description: "Indicates whether all fields should be written in write operations.",
			},
		},
		Measurements: MeasurementsConfig{
			MeasurementType: StringProperty{
				Value:       "histogram",
				Description: "Specifies the type of measurement for performance metrics, e.g., 'histogram'.",
			},
			MeasurementRawOutputFile: StringProperty{
				Value:       "",
				Description: "The file path where raw measurement data will be output, if any.",
			},
			HistogramPercentilesExport: BoolProperty{
				Value:       false,
				Description: "Indicates whether percentile data should be exported from histogram measurements.",
			},
			HistogramPercentilesExportFilepath: StringProperty{
				Value:       "./",
				Description: "The directory path where histogram percentile data files are to be saved.",
			},
			FieldLengthHistogramFile: StringProperty{
				Value:       "hist.txt",
				Description: "The file path of the histogram file that defines the distribution of field lengths.",
			},
			TargetOperationsPerSec: IntProperty{
				Value:       500,
				Description: "The target number of operations per second that the workload should aim to achieve.",
			},
			ZeroPadding: IntProperty{
				Value:       1,
				Description: "The amount of zero-padding for numeric fields, ensuring a fixed width representation.",
			},
		},
		Logging: LoggingConfig{
			DebugPprof: StringProperty{
				Value:       ":6060",
				Description: "The address to bind the pprof debugging server to, for profiling and debugging purposes.",
			},
			Label: StringProperty{
				Value:       "",
				Description: "A label used to tag log entries for easier filtering and identification.",
			},
			LogInterval: IntProperty{
				Value:       10,
				Description: "The interval, in seconds, at which log entries should be written to the log output.",
			},
			OutputStyle: StringProperty{
				Value:       "",
				Description: "Defines the formatting style for the log output, such as 'json', 'plain', etc.",
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
