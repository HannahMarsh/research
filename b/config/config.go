package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

type FailureInterval struct {
	Start float64 `yaml:"Start"`
	End   float64 `yaml:"End"`
}

type DatabaseConfig struct {
	CassandraCluster      StringProperty `yaml:"CassandraCluster"`
	CassandraConnections  IntProperty    `yaml:"CassandraConnections"`
	CassandraKeyspace     StringProperty `yaml:"CassandraKeyspace"`
	CassandraTableName    StringProperty `yaml:"CassandraTableName"`
	CassandraPassword     StringProperty `yaml:"CassandraPassword"`
	CassandraUsername     StringProperty `yaml:"CassandraUsername"`
	PasswordAuthenticator BoolProperty   `yaml:"PasswordAuthenticator"`
	ReplicationStrategy   StringProperty `yaml:"ReplicationStrategy"`
	ReplicationFactor     IntProperty    `yaml:"ReplicationFactor"`
}

type NodeConfig struct {
	NodeId             IntProperty       `yaml:"NodeId"`
	Address            StringProperty    `yaml:"Address"`
	FailureIntervals   []FailureInterval `yaml:"FailureIntervals"`
	MaxSize            IntProperty       `yaml:"MaxSize"`
	UseDefaultDatabase BoolProperty      `yaml:"UseDefaultDatabase"`
}

type CacheConfig struct {
	VirtualNodes IntProperty  `yaml:"VirtualNodes"`
	Nodes        []NodeConfig `yaml:"Nodes"`
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
}

type WorkloadConfig struct {
	WorkloadIdentifier        StringProperty `yaml:"WorkloadIdentifier"`
	Command                   StringProperty `yaml:"Command"`
	DoTransactions            BoolProperty   `yaml:"DoTransactions"`
	ExponentialFrac           FloatProperty  `yaml:"ExponentialFrac"`
	ExponentialPercentile     FloatProperty  `yaml:"ExponentialPercentile"`
	HotspotDataFraction       FloatProperty  `yaml:"HotspotDataFraction"`
	HotspotOpnFraction        FloatProperty  `yaml:"HotspotOpnFraction"`
	HashInsertOrder           BoolProperty   `yaml:"HashInsertOrder"`
	InsertProportion          FloatProperty  `yaml:"InsertProportion"`
	KeyRangeLowerBound        IntProperty    `yaml:"KeyRangeLowerBound"`
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
	MetricsOutputDir           StringProperty `yaml:"MetricsOutputDir"`
	MeasurementType            StringProperty `yaml:"MeasurementType"`
	RawOutputDir               StringProperty `yaml:"RawOutputDir"`
	HistogramPercentilesExport BoolProperty   `yaml:"HistogramPercentilesExport"`
	HistogramOutputDir         StringProperty `yaml:"HistogramOutputDir"`
	FieldLengthHistogramFile   StringProperty `yaml:"FieldLengthHistogramFile"`
	OutputStyle                StringProperty `yaml:"OutputStyle"`
	WarmUpTime                 IntProperty    `yaml:"WarmUpTime"`
	ZeroPadding                IntProperty    `yaml:"ZeroPadding"`
	CassandraCluster           StringProperty `yaml:"CassandraCluster"`
	CassandraKeyspace          StringProperty `yaml:"CassandraKeyspace"`
	CassandraTableName         StringProperty `yaml:"CassandraTableName"`
	ReplicationStrategy        StringProperty `yaml:"ReplicationStrategy"`
	ReplicationFactor          IntProperty    `yaml:"ReplicationFactor"`
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
	Cache        CacheConfig        `yaml:"Cache"`
	Performance  PerformanceConfig  `yaml:"Performance"`
	Workload     WorkloadConfig     `yaml:"WorkloadIdentifier"`
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
			Description: "The number of concurrent connections to establish with the Cassandra cluster.",
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
			Description: "The password for authenticating with Cassandra, if PasswordAuthenticator is enabled.",
		},
		CassandraUsername: StringProperty{
			Value:       "",
			Description: "The username for authenticating with Cassandra, if PasswordAuthenticator is enabled.",
		},
		PasswordAuthenticator: BoolProperty{
			Value:       false,
			Description: "Enables the use of Cassandra's PasswordAuthenticator for client connections. If this is true,\nthen the `CassandraUsername` and `CassandraPassword` properties must be non-empty and valid.",
		},
		ReplicationStrategy: StringProperty{
			Description: "Replication strategy to use for the Cassandra keyspace.",
			Value:       "SimpleStrategy",
		},
		ReplicationFactor: IntProperty{
			Description: "Replication factor to use for the Cassandra keyspace.",
			Value:       1,
		},
	},
	Cache: CacheConfig{
		Nodes: []NodeConfig{
			{
				NodeId: IntProperty{
					Value:       1,
					Description: "The ID of the node.",
				},
				Address: StringProperty{
					Description: "Address and port of redis server",
					Value:       "0.0.0.0:6379",
				},
				FailureIntervals: []FailureInterval{
					{
						Start: 0.3,
						End:   0.6,
					},
				},
				MaxSize: IntProperty{
					Value:       1000000,
					Description: "The maximum number of records to store in the cache.",
				},
				UseDefaultDatabase: BoolProperty{
					Value:       true,
					Description: "Indicates whether to use the default database for the cache.",
				},
			}, {
				NodeId: IntProperty{
					Value:       2,
					Description: "The ID of the node.",
				},
				Address: StringProperty{
					Description: "Address and port of redis server",
					Value:       "0.0.0.0:6380",
				},
				FailureIntervals: []FailureInterval{
					{
						Start: 0.4,
						End:   0.7,
					},
				},
				MaxSize: IntProperty{
					Value:       1000000,
					Description: "The maximum number of records to store in the cache.",
				},
				UseDefaultDatabase: BoolProperty{
					Value:       true,
					Description: "Indicates whether to use the default database for the cache.",
				},
			},
			{
				NodeId: IntProperty{
					Value:       3,
					Description: "The ID of the node.",
				},
				Address: StringProperty{
					Description: "Address and port of redis server",
					Value:       "0.0.0.0:6381",
				},
				MaxSize: IntProperty{
					Value:       1000000,
					Description: "The maximum number of records to store in the cache.",
				},
				UseDefaultDatabase: BoolProperty{
					Value:       true,
					Description: "Indicates whether to use the default database for the cache.",
				},
			},
			{
				NodeId: IntProperty{
					Value:       4,
					Description: "The ID of the node.",
				},
				Address: StringProperty{
					Description: "Address and port of redis server",
					Value:       "0.0.0.0:6382",
				},
				MaxSize: IntProperty{
					Value:       1000000,
					Description: "The maximum number of records to store in the cache.",
				},
				UseDefaultDatabase: BoolProperty{
					Value:       true,
					Description: "Indicates whether to use the default database for the cache.",
				},
			},
		},
		VirtualNodes: IntProperty{
			Value:       50000,
			Description: "The number of virtual nodes.",
		},
	},
	Performance: PerformanceConfig{
		BatchSize: IntProperty{
			Value:       1,
			Description: "The number of operations to batch together in a single transaction. Batch processing is disabled if set to 1.",
		},
		PerformDataIntegrityChecks: BoolProperty{
			Value:       false,
			Description: "Enables verification of data integrity during database operations. Requires 'FieldSizeDistribution' to be set to 'constant'.",
		},
		EnableDroppingDataOnStart: BoolProperty{
			Value:       false,
			Description: "Enables dropping any pre-existing data in the database upon startup.",
		},
		MaxFields: IntProperty{
			Value:       10,
			Description: "The maximum number of fields (columns) to include in the database table.",
		},
		AvFieldSizeBytes: IntProperty{
			Value:       500,
			Description: "The average size (in bytes) of each field stored in the database.",
		},
		FieldSizeDistribution: StringProperty{
			Value:       "constant",
			Description: "The type of distribution used to vary the length of fields in data records. Options are 'constant', 'unfiorm', 'zipfian', and 'histogram'",
		},
		InsertCount: IntProperty{
			Value:       100000,
			Description: "If `WriteAllFields` is true, this is the total number of records to insert during the workload execution.",
		},
		InsertionRetryInterval: IntProperty{
			Value:       1,
			Description: "The time in seconds to wait before retrying a failed insert operation. This controls the\nback-off strategy for handling write failures.",
		},
		InsertionRetryLimit: IntProperty{
			Value:       3,
			Description: "The maximum number of times to retry a failed insert operation.",
		},
		MaxExecutionTime: IntProperty{
			Value:       30,
			Description: "The maximum time to run the benchmark before it is forcibly stopped.",
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
			Value:       8000,
			Description: "The total number of operations to perform during the workload execution.",
		},
		RecordCount: IntProperty{
			Value:       300000,
			Description: "If `DoTransactions` is false, and `InsertCount` is 0, this is the total number of records to insert during the workload execution. This value must be greater than `KeyRangeLowerBound` +`InsertCount`.",
		},
		TargetOperationsPerSec: IntProperty{
			Value:       1200,
			Description: "The target number of operations per second that the workload should aim to achieve.",
		},
		ThreadCount: IntProperty{
			Value:       500,
			Description: "The number of concurrent threads to use when executing the workload.",
		},
	},
	Workload: WorkloadConfig{
		WorkloadIdentifier: StringProperty{
			Value:       "workload1",
			Description: "The name of the workload to be executed (for logging).",
		},
		Command: StringProperty{
			Value:       "<>",
			Description: "The specific command to run as part of the workload. This is set automatically by the program.",
		},
		DoTransactions: BoolProperty{
			Value:       false,
			Description: "Determines whether to perform a mix of different database operations or limit to insertion\noperations only. Set to true for initial data loading.",
		},
		ExponentialFrac: FloatProperty{
			Value:       0.8571428571,
			Description: "Fraction parameter for generating distributions based on exponential function.",
		},
		ExponentialPercentile: FloatProperty{
			Value:       95.0,
			Description: "The target percentile for the exponential distribution.",
		},
		HotspotDataFraction: FloatProperty{
			Value:       0.2,
			Description: "The fraction of data that will be considered 'hot' for generating hotspots in the workload.",
		},
		HotspotOpnFraction: FloatProperty{
			Value:       0.1,
			Description: "The fraction of operations that will target the 'hot' data.",
		},
		HashInsertOrder: BoolProperty{
			Value:       true,
			Description: "Enables hashing the order in which records are inserted.",
		},
		KeyRangeLowerBound: IntProperty{
			Value:       500,
			Description: "The starting point (lower bound) for key values used in insert operations.",
		},
		KeyPrefix: StringProperty{
			Value:       "key",
			Description: "The prefix to be used for keys in the workload.",
		},
		ReadAllFields: BoolProperty{
			Value:       false,
			Description: "Indicates whether all fields should be read in read operations.",
		},
		WriteAllFields: BoolProperty{
			Value:       false,
			Description: "Indicates whether all fields should be written in write operations.",
		},
		ReadModifyWriteProportion: FloatProperty{
			Value:       0.02,
			Description: "The proportion of read-modify-write operations in the workload.",
		},
		ReadProportion: FloatProperty{
			Value:       0.93,
			Description: "The proportion of read operations in the workload.",
		},
		InsertProportion: FloatProperty{
			Value:       0.01,
			Description: "The proportion of insert operations in the workload.",
		},
		UpdateProportion: FloatProperty{
			Value:       0.03,
			Description: "The proportion (from 0.0 to 1.0) of update operations in the workload. If the value is 0.0, then updating is disabled.",
		},
		ScanProportion: FloatProperty{
			Value:       0.01,
			Description: "The proportion (from 0.0 to 1.0) of scan operations in the workload. If the value is 0.0, then scanning is disabled.",
		},
		RequestDistribution: StringProperty{
			Value:       "uniform",
			Description: "The distribution of request types in the workload (to simulate different access patterns on the dataset).\nOptions are 'uniform', 'sequential', 'zipfian', 'latest', 'hotspot', and 'exponential'.",
		},
		ScanLengthDistribution: StringProperty{
			Value:       "uniform",
			Description: "The distribution for the number of records to scan during scan operations (to simulate\ndifferent data access spreads). Options are 'uniform' and 'zipfian'.",
		},
	},
	Measurements: MeasurementsConfig{
		MetricsOutputDir: StringProperty{
			Value:       "data/",
			Description: "The directory where measurement data files are to be saved.",
		},
		MeasurementType: StringProperty{
			Value:       "histogram",
			Description: "Specifies the type of measurement for performance metrics. Valid values are 'histogram', 'raw', and 'csv'.",
		},
		RawOutputDir: StringProperty{
			Value:       "data/raw.txt",
			Description: "Directory for outputting raw measurement data (if there is any).",
		},
		HistogramPercentilesExport: BoolProperty{
			Value:       false,
			Description: "Enables the export of percentile data from histogram measurements.",
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
			Description: "The duration in seconds between the start of the workload execution and when metrics are collected (allows the system to reach a steady state).",
		},
		ZeroPadding: IntProperty{
			Value:       1,
			Description: "The amount of zero-padding for numeric fields (for a fixed width representation).",
		},
		CassandraCluster: StringProperty{
			Value:       "127.0.0.1:9043",
			Description: "The host and port of the Cassandra cluster.",
		},
		CassandraKeyspace: StringProperty{
			Value:       "measurements",
			Description: "Keyspace to use within the Cassandra database.",
		},
		CassandraTableName: StringProperty{
			Value:       "measurements_table",
			Description: "Name of the table to use within the Cassandra keyspace.",
		},
		ReplicationStrategy: StringProperty{
			Description: "Replication strategy to use for the Cassandra keyspace.",
			Value:       "SimpleStrategy",
		},
		ReplicationFactor: IntProperty{
			Description: "Replication factor to use for the Cassandra keyspace.",
			Value:       1,
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

func (c *Config) ToString() string {
	var sb strings.Builder
	r := reflect.ValueOf(c).Elem() // Dereference the pointer to get the struct

	for i := 0; i < r.NumField(); i++ {
		field := r.Field(i)
		fieldType := r.Type().Field(i)

		sb.WriteString(fmt.Sprintf("%s:\n", fieldType.Name))

		if field.Kind() == reflect.Struct {
			for j := 0; j < field.NumField(); j++ {
				nestedField := field.Field(j)
				nestedFieldType := field.Type().Field(j)

				sb.WriteString(fmt.Sprintf("  %s: ", nestedFieldType.Name))

				if nestedField.Kind() == reflect.Slice {
					sb.WriteString("[\n")
					for k := 0; k < nestedField.Len(); k++ {
						elem := nestedField.Index(k)
						sb.WriteString(fmt.Sprintf("    %v\n", elem.Interface()))
					}
					sb.WriteString("  ]\n")
				} else {
					sb.WriteString(fmt.Sprintf("%v\n", nestedField.Interface()))
				}
			}
		} else {
			sb.WriteString(fmt.Sprintf("  %v\n", field.Interface()))
		}
		sb.WriteString("\n")
	}

	return sb.String()
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
