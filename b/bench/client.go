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
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"math"
	"time"
)

func runClientCommandFunc(cmd *cobra.Command, args []string, doTransactions bool, command string) {

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
	fmt.Printf("Target Run time Duration: %ds\n", globalProps.Workload.TargetExecutionTime.Value)
	fmt.Printf("Target Operations Per Sec: %s\n", humanize.Comma(int64(globalProps.Workload.TargetOperationsPerSec.Value)))
	fmt.Printf("Approx Total Operations: %s\n", humanize.Comma(int64(globalProps.Workload.TargetExecutionTime.Value*globalProps.Workload.TargetOperationsPerSec.Value)))
	fmt.Println("**********************************************")

	start := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	go dispTimer(start, ticker, time.Duration(globalProps.Workload.TargetExecutionTime.Value)*time.Second)

	c := client.NewClient(globalProps, globalWorkload, globalDB, globalCache)
	c.Run(globalContext)

	ticker.Stop()

	fmt.Println("\n**********************************************")
	fmt.Printf("Run finished, takes %s\n", time.Now().Sub(start))
}

func dispTimer(start time.Time, ticker *time.Ticker, estimatedRunningTime time.Duration) {
	fmt.Println("Running benchmark...")
	timer := time.NewTimer(0)
	defer ticker.Stop()

	end := time.Now().Add(estimatedRunningTime)
	digits := int(math.Log10(estimatedRunningTime.Seconds())) + 1

	for {
		select {
		case <-ticker.C:
			remainingTime := int(math.Round(end.Sub(time.Now()).Seconds()))
			if remainingTime >= 0 {
				fmt.Printf("\rEstimated remaining time: %ss       ", fmt.Sprintf(fmt.Sprintf("%%0%dd", digits), remainingTime))
			} else {
				fmt.Printf("\rDuration: %ss                ", fmt.Sprintf(fmt.Sprintf("%%0%dd", digits), int(math.Round(time.Now().Sub(start).Seconds()))))
			}
		case <-globalContext.Done():
			timer.Stop()
			fmt.Printf("\n")
			return // Exit the loop when the global context is canceled
		}
	}
}

func runTransCommandFunc(cmd *cobra.Command, args []string) {
	runClientCommandFunc(cmd, args, true, "run")
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
