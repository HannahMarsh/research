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

		if cmd.Flags().Changed("threads") {
			// We set the threadArg via command line.
			globalProps.Workload.ThreadCount.Value = threadsArg
		}

		if cmd.Flags().Changed("target") {
			globalProps.Workload.TargetOperationsPerSec.Value = targetArg
		}

		if cmd.Flags().Changed("interval") {
			globalProps.Logging.LogInterval.Value = reportInterval
		}
	})

	//fmt.Println("***************** properties *****************")
	//fmt.Printf("%s", globalProps.ToString())
	////r := reflect.ValueOf(globalProps).Elem() // Dereference the pointer to get the struct
	////
	////for i := 0; i < r.NumField(); i++ {
	////	field := r.Field(i)
	////	fmt.Printf("\t%s = %v\n", r.Type().Field(i).Name, field.Interface())
	////}

	fmt.Println("**********************************************")
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
	//measurement.Output()
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

func runLoadCommandFunc(cmd *cobra.Command, args []string) {
	runClientCommandFunc(cmd, args, false, "load")
}

func runTransCommandFunc(cmd *cobra.Command, args []string) {
	runClientCommandFunc(cmd, args, true, "run")
}

var (
	threadsArg     int
	targetArg      int
	reportInterval int
)

func initClientCommand(m *cobra.Command) {
	m.Flags().StringVar(&propertyFile, "property_file", "P", "Specify a property file")
	m.Flags().StringArrayVarP(&propertyValues, "prop", "p", nil, "Specify a property value with name=value")
	m.Flags().StringVar(&tableName, "table", "", "Use the table name instead of the default \"usertable\"")
	m.Flags().IntVar(&threadsArg, "threads", 1, "Execute using n threads - can also be specified as the \"threadcount\" property")
	m.Flags().IntVar(&targetArg, "target", 0, "Attempt to do n operations per second (default: unlimited) - can also be specified as the \"target\" property")
	m.Flags().IntVar(&reportInterval, "interval", 10, "Interval of outputting measurements in seconds")
}

func newLoadCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "load",
		Short: "YCSB load benchmark",
		Args:  cobra.MinimumNArgs(1),
		Run:   runLoadCommandFunc,
	}

	initClientCommand(m)
	return m
}

func newRunCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "run",
		Short: "run benchmark",
		//Args:  cobra.MinimumNArgs(1),
		Run: runTransCommandFunc,
	}

	initClientCommand(m)
	return m
}
