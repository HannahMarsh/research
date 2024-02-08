// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"benchmark/client"
	bconfig "benchmark/config"
	"benchmark/db"
	"benchmark/metrics"
	"benchmark/util"
	"benchmark/workload"
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Register workload

	"github.com/spf13/cobra"
)

var (
	propertyFile   string
	workloadId     string
	propertyValues []string

	globalContext context.Context
	globalCancel  context.CancelFunc

	globalDB       db.DB
	globalCache    *client.CacheWrapper
	globalWorkload *workload.Workload
	globalProps    *bconfig.Config
	warmUpTime     time.Time
)

func main() {

	globalContext, globalCancel = context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	closeDone := make(chan struct{}, 1)
	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		globalCancel()

		select {
		case <-sc:
			// send signal again, return directly
			fmt.Printf("\nGot signal [%v] again to exit.\n", sig)
			os.Exit(1)
		case <-time.After(10 * time.Second):
			fmt.Print("\nWait 10s for closed, force exit\n")
			os.Exit(1)
		case <-closeDone:
			return
		}
	}()

	rootCmd := &cobra.Command{
		Use:   "go-benchmark",
		Short: "Go Benchmark",
	}

	rootCmd.AddCommand(
		newRunCommand(),
		newLoadCommand(),
	)

	cobra.EnablePrefixMatching = true

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}

	globalCancel()
	err := globalDB.Close()
	if err != nil {
		panic(err)
	}

	closeDone <- struct{}{}
}

func initialGlobal(onProperties func()) {
	var err error
	globalProps, err = bconfig.NewConfig(propertyFile)

	if onProperties != nil {
		onProperties()
	}

	addr := globalProps.Logging.DebugPprof.Value

	// http.HandleFunc("/debug/pprof/profile", Profile)

	go func() {
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			panic(err)
		}
	}()

	metrics.Init(globalProps)

	workloadName := globalProps.Workload.WorkloadIdentifier

	warmUpTime = time.Now().Add(time.Duration(globalProps.Measurements.WarmUpTime.Value) * time.Second)

	if globalWorkload, err = workload.NewWorkload(globalProps, warmUpTime); err != nil {
		util.Fatalf("create workload %s failed %v", workloadName, err)
	}

	if globalDB, err = db.NewDatabase(globalProps); err != nil {
		util.Fatalf("create db failed: %v", err)
	}
	globalDB = client.NewDbWrapper(globalDB, globalProps)
	globalCache = client.NewCache(globalProps, globalContext)

}

func initialLoadGlobal(onProperties func()) {
	var err error
	globalProps, err = bconfig.NewConfig(propertyFile)

	if onProperties != nil {
		onProperties()
	}

	addr := globalProps.Logging.DebugPprof.Value
	go func() {
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			panic(err)
		}
	}()

	metrics.Init(globalProps)

	workloadName := globalProps.Workload.WorkloadIdentifier

	warmUpTime = time.Now()

	if globalWorkload, err = workload.NewWorkload(globalProps, warmUpTime); err != nil {
		util.Fatalf("create workload %s failed %v", workloadName, err)
	}

	if globalDB, err = db.NewDatabase(globalProps); err != nil {
		util.Fatalf("create db failed: %v", err)
	}
	globalDB = client.NewDbWrapper(globalDB, globalProps)
	globalCache = nil
}

func runClientCommandFunc(cmd *cobra.Command, args []string, command string) {

	// Parse the flags
	if err := cmd.Flags().Parse(args); err != nil {
		fmt.Printf("Error parsing flags: %v\n", err)
		return
	}

	initialGlobal(func() {
		globalProps.Workload.Command.Value = command

		if cmd.Flags().Changed("wid") {
			// We set the threadArg via command line.
			globalProps.Workload.WorkloadIdentifier.Value = workloadId
		}
	})

	fmt.Println("**********************************************")
	fmt.Printf("Generating a %s request distribution with a keyrange of [%d, %d]. Key prefix = `%s`\n", globalProps.Workload.RequestDistribution.Value, 0, globalProps.Workload.NumUniqueKeys.Value-1, globalProps.Workload.KeyPrefix.Value)
	fmt.Printf("Target Run time Duration (after %ds warm-up): %ds\n", globalProps.Measurements.WarmUpTime.Value, globalProps.Workload.TargetExecutionTime.Value)
	fmt.Printf("Target Operations Per Sec: %s\n", humanize.Comma(int64(globalProps.Workload.TargetOperationsPerSec.Value)))
	fmt.Printf("Approx Total Operations: %s\n", humanize.Comma(int64(globalProps.Workload.TargetExecutionTime.Value*globalProps.Workload.TargetOperationsPerSec.Value)))
	fmt.Println("**********************************************")

	c := client.NewClient(globalProps, globalWorkload, globalDB, globalCache)

	ticker := time.NewTicker(1 * time.Second)
	start := time.Now()
	go dispTimer(start, ticker)

	c.Run(globalContext)

	globalContext.Done()

	ticker.Stop()
	fmt.Printf("\r  - Done running benchmark. Took %d seconds.%s\n", int(time.Since(start).Seconds())-globalProps.Measurements.WarmUpTime.Value, "              ")
	fmt.Println("\n**********************************************")

	end := time.Now()
	time.Sleep(4 * time.Second)

	metrics.PlotMetrics(start, end)
}

func loadClientCommandFunc(cmd *cobra.Command, args []string, command string) {

	// Parse the flags
	if err := cmd.Flags().Parse(args); err != nil {
		fmt.Printf("Error parsing flags: %v\n", err)
		return
	}

	initialLoadGlobal(func() {
		globalProps.Workload.WorkloadIdentifier.Value = "load1"
		globalProps.Workload.Command.Value = command
		globalProps.Workload.RequestDistribution.Value = "sequential"
		globalProps.Workload.TargetOperationsPerSec.Value = 500
		globalProps.Database.TimeoutMs.Value = 1000
		globalProps.Workload.TargetExecutionTime.Value = int(1.2 * float64(globalProps.Workload.NumUniqueKeys.Value/globalProps.Workload.TargetOperationsPerSec.Value))
		globalProps.Measurements.WarmUpTime.Value = 0

		if cmd.Flags().Changed("wid") {
			// We set the threadArg via command line.
			globalProps.Workload.WorkloadIdentifier.Value = workloadId
		}
	})

	fmt.Println("**********************************************")
	fmt.Printf("Generating a %s request distribution with a keyrange of [%d, %d]. Key prefix = `%s`\n", globalProps.Workload.RequestDistribution.Value, 0, globalProps.Workload.NumUniqueKeys.Value-1, globalProps.Workload.KeyPrefix.Value)
	fmt.Printf("Target Run time Duration (after %ds warm-up): %ds\n", globalProps.Measurements.WarmUpTime.Value, globalProps.Workload.TargetExecutionTime.Value)
	fmt.Printf("Target Operations Per Sec: %s\n", humanize.Comma(int64(globalProps.Workload.TargetOperationsPerSec.Value)))
	fmt.Printf("Approx Total Operations: %s\n", humanize.Comma(int64(globalProps.Workload.TargetExecutionTime.Value*globalProps.Workload.TargetOperationsPerSec.Value)))
	fmt.Println("**********************************************")

	c := client.NewClient(globalProps, globalWorkload, globalDB, nil)

	start := time.Now()

	c.Load(globalContext)

	globalContext.Done()

	fmt.Printf("\r  - Done loading data to benchmark. Took %d seconds.%s\n", int(time.Since(start).Seconds())-globalProps.Measurements.WarmUpTime.Value, "              ")
	fmt.Println("\n**********************************************")

	time.Sleep(1 * time.Second)

	metrics.PlotMetrics(start, time.Now())
}

func dispTimer(start time.Time, ticker *time.Ticker) {
	defer ticker.Stop()

	spaces := "                                             "

	fmt.Printf("Warming up...\n  - remaining time: %ds%s", globalProps.Measurements.WarmUpTime.Value, spaces)

	endWarmUp := start.Add(time.Duration(globalProps.Measurements.WarmUpTime.Value) * time.Second)

	end := endWarmUp.Add(time.Duration(globalProps.Workload.TargetExecutionTime.Value) * time.Second)

	digits := int(math.Log10(end.Sub(start).Seconds())) + 1
	warmupDigits := int(math.Log10(endWarmUp.Sub(start).Seconds())) + 1
	afterWarmUp := false

	maxExecutionTime := start.Add(time.Duration(globalProps.Workload.TargetExecutionTime.Value+globalProps.Measurements.WarmUpTime.Value+5) * time.Second)

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			if now.After(maxExecutionTime) {
				fmt.Printf("\r  - max execution time reached, forcefully exiting.%s\n", spaces)
				globalCancel()
				return
			}
			if !afterWarmUp {
				if !now.Before(endWarmUp) {
					remainingTime := int(math.Round(end.Sub(time.Now()).Seconds()))
					fmt.Printf("\r  - Done warming up. Took %d seconds.%s\nRunning benchmark...\n  - remaining time: %ss%s", globalProps.Measurements.WarmUpTime.Value, spaces, fmt.Sprintf(fmt.Sprintf("%%0%dd", digits), remainingTime), spaces)
					afterWarmUp = true
				} else {
					remainingTime := int(math.Round(endWarmUp.Sub(time.Now()).Seconds()))
					fmt.Printf("\r  - remaining time: %ss%s", fmt.Sprintf(fmt.Sprintf("%%0%dd", warmupDigits), remainingTime), spaces)
				}
			} else {
				remainingTime := int(math.Round(end.Sub(now).Seconds()))
				if remainingTime >= 0 {
					fmt.Printf("\r  - remaining time: %ss%s", fmt.Sprintf(fmt.Sprintf("%%0%dd", digits), remainingTime), spaces)
				} else {
					// fmt.Printf("\r  - Duration: %ss%s", fmt.Sprintf(fmt.Sprintf("%%0%dd", digits), int(math.Round(time.Now().Sub(start).Seconds()))), spaces)
				}
			}
		case <-globalContext.Done():

			return // Exit the loop when the global context is canceled
		}
	}
}

func runTransCommandFunc(cmd *cobra.Command, args []string) {
	runClientCommandFunc(cmd, args, "run")
}

func loadTransCommandFunc(cmd *cobra.Command, args []string) {
	loadClientCommandFunc(cmd, args, "load")
}

func initClientCommand(m *cobra.Command) {
	m.Flags().StringVar(&propertyFile, "property_file", "P", "Specify a property file")
	m.Flags().StringVar(&workloadId, "wid", "", "Specify the workload id")
	m.Flags().StringArrayVarP(&propertyValues, "prop", "p", nil, "Specify a property value with name=value")
}

func newRunCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "run",
		Short: "run benchmark",
		Run:   runTransCommandFunc,
	}

	initClientCommand(m)
	return m
}

func newLoadCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "load",
		Short: "load benchmark",
		Run:   loadTransCommandFunc,
	}

	initClientCommand(m)
	return m
}
