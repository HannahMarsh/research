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
	"benchmark/metrics"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"math"
	"time"
)

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

	time.Sleep(1 * time.Second)

	metrics.PlotMetrics(start, time.Now())
}

func loadClientCommandFunc(cmd *cobra.Command, args []string, command string) {

	// Parse the flags
	if err := cmd.Flags().Parse(args); err != nil {
		fmt.Printf("Error parsing flags: %v\n", err)
		return
	}

	initialLoadGlobal(func() {
		globalProps.Workload.Command.Value = command
		globalProps.Workload.RequestDistribution.Value = "sequential"
		globalProps.Workload.TargetOperationsPerSec.Value = 2000
		globalProps.Workload.TargetExecutionTime.Value = globalProps.Workload.NumUniqueKeys.Value / globalProps.Workload.TargetOperationsPerSec.Value
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
