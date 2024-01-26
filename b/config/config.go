package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
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
	TimeoutMs             IntProperty    `yaml:"TimeoutMs"`
}

type NodeConfig struct {
	NodeId             IntProperty       `yaml:"NodeId"`
	Address            StringProperty    `yaml:"Address"`
	FailureIntervals   []FailureInterval `yaml:"FailureIntervals"`
	MaxMemoryMbs       IntProperty       `yaml:"MaxMemoryMbs"`
	MaxMemoryPolicy    StringProperty    `yaml:"MaxMemoryPolicy"`
	UseDefaultDatabase BoolProperty      `yaml:"UseDefaultDatabase"`
}

type CacheConfig struct {
	VirtualNodes         IntProperty    `yaml:"VirtualNodes"`
	Nodes                []NodeConfig   `yaml:"Nodes"`
	NumHottestKeysBackup IntProperty    `yaml:"NumHottestKeysBackup"`
	BackUpAddress        StringProperty `yaml:"BackUpAddress"`
}

type WorkloadConfig struct {
	PerformDataIntegrityChecks BoolProperty   `yaml:"PerformDataIntegrityChecks"`
	EnableDroppingDataOnStart  BoolProperty   `yaml:"EnableDroppingDataOnStart"`
	MaxFields                  IntProperty    `yaml:"MaxFields"`
	AvFieldSizeBytes           IntProperty    `yaml:"AvFieldSizeBytes"`
	FieldSizeDistribution      StringProperty `yaml:"FieldSizeDistribution"`
	NumUniqueKeys              IntProperty    `yaml:"NumUniqueKeys"`
	DbOperationRetryLimit      IntProperty    `yaml:"DbOperationRetryLimit"`
	TargetExecutionTime        IntProperty    `yaml:"TargetExecutionTime"`
	TargetOperationsPerSec     IntProperty    `yaml:"TargetOperationsPerSec"`
	WorkloadIdentifier         StringProperty `yaml:"WorkloadIdentifier"`
	Command                    StringProperty `yaml:"Command"`
	ExponentialFrac            FloatProperty  `yaml:"ExponentialFrac"`
	ExponentialPercentile      FloatProperty  `yaml:"ExponentialPercentile"`
	HotspotDataFraction        FloatProperty  `yaml:"HotspotDataFraction"`
	HotspotOpnFraction         FloatProperty  `yaml:"HotspotOpnFraction"`
	HashInsertOrder            BoolProperty   `yaml:"HashInsertOrder"`
	InsertProportion           FloatProperty  `yaml:"InsertProportion"`
	KeyPrefix                  StringProperty `yaml:"KeyPrefix"`
	ReadAllFields              BoolProperty   `yaml:"ReadAllFields"`
	RequestDistribution        StringProperty `yaml:"RequestDistribution"`
	ZipfianConstant            FloatProperty  `yaml:"ZipfianConstant"`
}

type MeasurementsConfig struct {
	MetricsOutputDir StringProperty `yaml:"MetricsOutputDir"`
	WarmUpTime       IntProperty    `yaml:"WarmUpTime"`
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
		TimeoutMs: IntProperty{
			Description: "Timeout in milliseconds for Cassandra operations.",
			Value:       3000,
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
						Start: 0.4,
						End:   0.6,
					},
				},
				MaxMemoryMbs: IntProperty{
					Value:       10,
					Description: "The maximum number of megabytes to store in the cache.",
				},
				MaxMemoryPolicy: StringProperty{
					Value:       "allkeys-lfu",
					Description: "The policy to use for evicting records when the cache is full. Options can be found on: https://redis.io/docs/reference/eviction/#eviction-policies",
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
				MaxMemoryMbs: IntProperty{
					Value:       10,
					Description: "The maximum number of records to store in the cache.",
				},
				MaxMemoryPolicy: StringProperty{
					Value:       "allkeys-lfu",
					Description: "The policy to use for evicting records when the cache is full. Options can be found on: https://redis.io/docs/reference/eviction/#eviction-policies",
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
				MaxMemoryMbs: IntProperty{
					Value:       10,
					Description: "The maximum number of megabytes to store in the cache.",
				},
				MaxMemoryPolicy: StringProperty{
					Value:       "allkeys-lfu",
					Description: "The policy to use for evicting records when the cache is full. Options can be found on: https://redis.io/docs/reference/eviction/#eviction-policies",
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
				MaxMemoryMbs: IntProperty{
					Value:       10,
					Description: "The maximum number of megabytes to store in the cache.",
				},
				MaxMemoryPolicy: StringProperty{
					Value:       "allkeys-lfu",
					Description: "The policy to use for evicting records when the cache is full. Options can be found on: https://redis.io/docs/reference/eviction/#eviction-policies",
				},
				UseDefaultDatabase: BoolProperty{
					Value:       true,
					Description: "Indicates whether to use the default database for the cache.",
				},
			},
			//{
			//	NodeId: IntProperty{
			//		Value:       5,
			//		Description: "The ID of the node.",
			//	},
			//	Address: StringProperty{
			//		Description: "Address and port of redis server",
			//		Value:       "0.0.0.0:6383",
			//	},
			//	MaxMemoryMbs: IntProperty{
			//		Value:       100,
			//		Description: "The maximum number of megabytes to store in the cache.",
			//	},
			//	MaxMemoryPolicy: StringProperty{
			//		Value:       "allkeys-lfu",
			//		Description: "The policy to use for evicting records when the cache is full. Options can be found on: https://redis.io/docs/reference/eviction/#eviction-policies",
			//	},
			//	UseDefaultDatabase: BoolProperty{
			//		Value:       true,
			//		Description: "Indicates whether to use the default database for the cache.",
			//	},
			//},
			//{
			//	NodeId: IntProperty{
			//		Value:       6,
			//		Description: "The ID of the node.",
			//	},
			//	Address: StringProperty{
			//		Description: "Address and port of redis server",
			//		Value:       "0.0.0.0:6384",
			//	},
			//	MaxMemoryMbs: IntProperty{
			//		Value:       100,
			//		Description: "The maximum number of megabytes to store in the cache.",
			//	},
			//	MaxMemoryPolicy: StringProperty{
			//		Value:       "allkeys-lfu",
			//		Description: "The policy to use for evicting records when the cache is full. Options can be found on: https://redis.io/docs/reference/eviction/#eviction-policies",
			//	},
			//	UseDefaultDatabase: BoolProperty{
			//		Value:       true,
			//		Description: "Indicates whether to use the default database for the cache.",
			//	},
			//},
			//{
			//	NodeId: IntProperty{
			//		Value:       7,
			//		Description: "The ID of the node.",
			//	},
			//	Address: StringProperty{
			//		Description: "Address and port of redis server",
			//		Value:       "0.0.0.0:6385",
			//	},
			//	MaxMemoryMbs: IntProperty{
			//		Value:       100,
			//		Description: "The maximum number of megabytes to store in the cache.",
			//	},
			//	MaxMemoryPolicy: StringProperty{
			//		Value:       "allkeys-lfu",
			//		Description: "The policy to use for evicting records when the cache is full. Options can be found on: https://redis.io/docs/reference/eviction/#eviction-policies",
			//	},
			//	UseDefaultDatabase: BoolProperty{
			//		Value:       true,
			//		Description: "Indicates whether to use the default database for the cache.",
			//	},
			//},
		},
		VirtualNodes: IntProperty{
			Value:       50000,
			Description: "The number of virtual nodes.",
		},
		NumHottestKeysBackup: IntProperty{
			Description: "The number of hottest keys to backup.",
			Value:       1000,
		},
		BackUpAddress: StringProperty{
			Description: "Address and port of redis server",
			Value:       "0.0.0.0:6383",
		},
	},
	Workload: WorkloadConfig{
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
			Value:       "uniform",
			Description: "The type of distribution used to vary the length of fields in data records. Options are 'constant', 'unfiorm', and 'zipfian'",
		},
		NumUniqueKeys: IntProperty{
			Value:       20000,
			Description: "If `WriteAllFields` is true, this is the total number of records to insert during the workload execution.",
		},
		DbOperationRetryLimit: IntProperty{
			Value:       1,
			Description: "The maximum number of times to retry a failed insert operation.",
		},
		TargetExecutionTime: IntProperty{
			Value:       7,
			Description: "The target duration to run the benchmark for after the warmup time.",
		},
		TargetOperationsPerSec: IntProperty{
			Value:       5000,
			Description: "The target number of operations per second that the workload should aim to achieve.",
		},
		WorkloadIdentifier: StringProperty{
			Value:       "workload1",
			Description: "The name of the workload to be executed (for logging).",
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
		KeyPrefix: StringProperty{
			Value:       "key",
			Description: "The prefix to be used for keys in the workload.",
		},
		ReadAllFields: BoolProperty{
			Value:       false,
			Description: "Indicates whether all fields should be read in read operations.",
		},
		InsertProportion: FloatProperty{
			Value:       0.02,
			Description: "The proportion of insert operations in the workload.",
		},
		RequestDistribution: StringProperty{
			Value:       "zipfian",
			Description: "The distribution of request types in the workload (to simulate different access patterns on the dataset).\nOptions are 'uniform', 'sequential', 'zipfian', 'latest', 'hotspot', and 'exponential'.",
		},
		ZipfianConstant: FloatProperty{
			Value:       0.99,
			Description: "The constant to use for the zipfian distribution.",
		},
	},
	Measurements: MeasurementsConfig{
		MetricsOutputDir: StringProperty{
			Value:       "data/",
			Description: "The directory where measurement data files are to be saved.",
		},
		WarmUpTime: IntProperty{
			Value:       6,
			Description: "The duration in seconds between the start of the workload execution and when metrics are collected (allows the system to reach a steady state).",
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
	return defaultConfig_

	//defaultConfig := defaultConfig_
	//yamlData, err := yaml.Marshal(&defaultConfig)
	//if err != nil {
	//	fmt.Println(err)
	//	os.Exit(1)
	//}
	//yamlFilePath := filepath.Join("config", "default.yaml")
	//err = os.WriteFile(yamlFilePath, yamlData, 0644)
	//if err != nil {
	//	fmt.Println(err)
	//	os.Exit(1)
	//}
	//
	//err = os.WriteFile("/Users/hanma/cloud computing research/research/b/tool/property_files/0.yaml", yamlData, 0644)
	//if err != nil {
	//	fmt.Println(err)
	//	os.Exit(1)
	//}
	//
	//return defaultConfig
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
