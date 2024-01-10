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
	"benchmark/measurement"
	"fmt"
	"github.com/spf13/cobra"
	"reflect"
	"time"
)

func runClientCommandFunc(cmd *cobra.Command, args []string, doTransactions bool, command string) {

	// Parse the flags
	if err := cmd.Flags().Parse(args); err != nil {
		fmt.Printf("Error parsing flags: %v\n", err)
		return
	}

	fmt.Printf("Debug: propertyFile = %s\n", propertyFile)

	initialGlobal(func() {
		doTransFlag := true
		if !doTransactions {
			doTransFlag = false
		}
		globalProps.Workload.DoTransactions.Value = doTransFlag
		globalProps.Workload.Command.Value = command

		if cmd.Flags().Changed("threads") {
			// We set the threadArg via command line.
			globalProps.Performance.ThreadCount.Value = threadsArg
		}

		if cmd.Flags().Changed("target") {
			globalProps.Performance.TargetOperationsPerSec.Value = targetArg
		}

		if cmd.Flags().Changed("interval") {
			globalProps.Logging.LogInterval.Value = reportInterval
		}
	})

	fmt.Println("***************** properties *****************")
	r := reflect.ValueOf(globalProps).Elem() // Dereference the pointer to get the struct

	for i := 0; i < r.NumField(); i++ {
		field := r.Field(i)
		fmt.Printf("\t%s = %v\n", r.Type().Field(i).Name, field.Interface())
	}

	fmt.Println("**********************************************")

	c := client.NewClient(globalProps, globalWorkload, globalDB, globalCache)
	start := time.Now()
	c.Run(globalContext)
	fmt.Println("**********************************************")
	fmt.Printf("Run finished, takes %s\n", time.Now().Sub(start))
	measurement.Output()
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
	fmt.Printf("propertyFile: %s\n", propertyFile)
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
