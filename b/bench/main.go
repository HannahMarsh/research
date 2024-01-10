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
	"benchmark/cache"
	"benchmark/client"
	bconfig "benchmark/config"
	"benchmark/db"
	"benchmark/measurement"
	"benchmark/util"
	"benchmark/workload"
	"context"
	"fmt"
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
	propertyValues []string
	tableName      string

	globalContext context.Context
	globalCancel  context.CancelFunc

	globalDB       db.DB
	globalCache    *cache.Cache
	globalWorkload *workload.Workload
	globalProps    *bconfig.Config
)

func initialGlobal(onProperties func()) {
	var err error
	globalProps, err = bconfig.NewConfig(propertyFile)

	if onProperties != nil {
		onProperties()
	}

	addr := globalProps.Logging.DebugPprof
	go func() {
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			panic(err)
		}
	}()

	measurement.InitMeasure(globalProps)

	if len(tableName) == 0 {
		tableName = globalProps.Database.CassandraTableName
	}

	workloadName := globalProps.Workload.Workload

	if globalWorkload, err = workload.NewWorkload(globalProps); err != nil {
		util.Fatalf("create workload %s failed %v", workloadName, err)
	}

	if globalDB, err = db.NewDatabase(globalProps); err != nil {
		util.Fatalf("create db failed: %v", err)
	}
	globalDB = client.DbWrapper{P: globalProps, DB: globalDB}
	globalCache = cache.NewCache(globalProps)

	// todo add each cache node from config
}

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
		newShellCommand(),
		newLoadCommand(),
		newRunCommand(),
	)

	cobra.EnablePrefixMatching = true

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}

	globalCancel()
	if globalDB != nil {
		err := globalDB.Close()
		if err != nil {
			panic(err)
		}
	}

	closeDone <- struct{}{}
}
