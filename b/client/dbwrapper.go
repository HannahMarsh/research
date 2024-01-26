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

package client

import (
	bconfig "benchmark/config"
	"benchmark/db"
	metrics2 "benchmark/metrics"
	"context"
	"fmt"
	"time"
)

type DbWrapper struct {
	DB db.DB
	P  *bconfig.Config
}

func dbMeasure(start time.Time, latency time.Duration, operationType string, err error) {

	if err != nil {
		metrics2.AddMeasurement(metrics2.DATABASE_OPERATION, start,
			map[string]interface{}{
				metrics2.SUCCESSFUL: false,
				metrics2.OPERATION:  operationType,
				metrics2.ERROR:      err.Error(),
				metrics2.LATENCY:    latency.Seconds(),
			})
		return
	} else {
		metrics2.AddMeasurement(metrics2.DATABASE_OPERATION, start,
			map[string]interface{}{
				metrics2.SUCCESSFUL: true,
				metrics2.OPERATION:  operationType,
				metrics2.LATENCY:    latency.Seconds(),
			})
	}
}

func (d DbWrapper) Close() error {
	return d.DB.Close()
}

func (d DbWrapper) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return d.DB.InitThread(ctx, threadID, threadCount)
}

func (d DbWrapper) CleanupThread(ctx context.Context) {
	d.DB.CleanupThread(ctx)
}

var timeout int64 = 100

func (d DbWrapper) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	//start := time.Now()
	//defer func() {
	//	dbMeasure(start, "READ", err)
	//}()
	//
	//return d.CassandraDB.Read(ctx, table, key, fields)

	start := time.Now()
	var operationType = metrics2.READ

	value, err := d.DB.Read(ctx, table, key, fields)
	latency := time.Now().Sub(start)
	if err == nil && latency.Milliseconds() > timeout {
		err = fmt.Errorf("operation %s timeout", operationType)
	}
	go dbMeasure(start, latency, operationType, err)
	return value, err
}

func (d DbWrapper) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	start := time.Now()
	var operationType = metrics2.INSERT

	err := d.DB.Insert(ctx, table, key, values)
	latency := time.Now().Sub(start)
	if err == nil && latency.Milliseconds() > timeout {
		//err = fmt.Errorf("operation %s timeout", operationType)
	}
	go dbMeasure(start, latency, operationType, err)
	return err
}
