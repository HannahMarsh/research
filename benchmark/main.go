package benchmark

import (
	"fmt"
	"log"
	"shared"
)

// CMUTrace /*
type CMUTrace struct {
	Timestamp       uint32
	ObjectID        uint64
	ObjectSize      uint32
	NextAccessVTime int64 // Use int64 to hold -1
}

func readTraceFile(filePath string) ([]CMUTrace, error) {
	// open the .zst file
	// ...

	// create a zstd decompressor
	// ...

	// read and parse the data into CMUTrace structs
	// ...
}

func main() {
	config := shared.LoadConfig()
	// Use the config...
	fmt.Println(config)

	// example
	traces, err := readTraceFile("/app/traces/io_traces.ns0.oracleGeneral.zst")
	if err != nil {
		log.Fatal(err)
	}
}
