package main

import (
	"benchmark/bench"
	_ "benchmark/bench"
	_ "benchmark/db"
	"benchmark/measurements"
	_ "benchmark_config"
	"context"
	"flag"
	"fmt"
	_ "io"
	"log"
	_ "math"
	_ "math/rand"
	_ "net/http"
	"os"
	_ "strconv"
	"sync"
	"time"
)

// getFlags parses command line flags and returns boolean flags
func getFlags() (bool, string, string) {
	var local bool
	var help bool
	var metricsPath string
	flag.BoolVar(&help, "help", false, "Display usage")
	flag.BoolVar(&local, "l", false, "use local ip addresses for cache nodes_config")
	flag.StringVar(&metricsPath, "mp", "metrics", "path to store metrics")

	flag.Parse()

	if help == true {
		fmt.Println("Usage: <program> [-help] [-l] [-mp]")
		flag.PrintDefaults()
		os.Exit(1)
	}
	var cr = "remote"
	if local {
		cr = "local"
	}
	return local, cr, metricsPath
}

// main is the entry point for the program
func main() {

	var wg sync.WaitGroup

	_, cr, metricsPath := getFlags()

	b := bench.NewBenchmark(cr, metricsPath)

	// initialize node ring for consistent hashing
	b.NodeRing = bench.NewNodeRing(len(b.NodeConfigs), b.VirtualNodes)

	// set the start time
	b.Start = time.Now()

	// initialize metrics
	b.M = measurements.NewMetrics(b.Start, b.Start.Add(b.MaxDuration))

	// make new plotter
	p := measurements.NewPlotter(b)

	// context creation for managing the lifecycle of the benchmark
	// ctx, cancel := context.WithCancel(context.Background())

	// context will be cancelled after maxDuration
	ctx, cancel := context.WithTimeout(context.Background(), b.MaxDuration)
	defer cancel()

	// start failure simulation routine
	go b.SimulateNodeFailures(ctx)

	b.StartCacheNodes()

	// start generating requests
	log.Printf("Starting generating requests...")

	// todo prepopulate database
	go b.GenerateRequests(ctx)
	//cancel()

	// wait for the context to be cancelled (i.e., timeout)
	<-ctx.Done()

	wg.Wait() // wait for all goroutines to finish

	// todo remove this
	time.Sleep(2 * time.Second)

	// make the plots
	p.MakePlots()

	fmt.Println("Benchmark program finished.")

}
