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
	"time"
)

type DbWrapper struct {
	DB      db.DB
	P       *bconfig.Config
	timeout time.Duration
}

func NewDbWrapper(db db.DB, p *bconfig.Config) *DbWrapper {
	return &DbWrapper{
		DB:      db,
		P:       p,
		timeout: time.Duration(p.Database.TimeoutMs.Value) * time.Millisecond,
	}
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

func (d DbWrapper) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {

	// Create a context with a timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	type getResult struct {
		result map[string][]byte
		err    error
	}

	// Channel to capture the output from Get
	getChan := make(chan getResult, 1)

	start := time.Now()
	var operationType = metrics2.READ

	go func() {
		result, err := d.DB.Read(ctx, table, key, fields)
		getChan <- getResult{result, err}
	}()

	// handle the result or timeout
	select {
	case getRes := <-getChan:
		go dbMeasure(start, time.Now().Sub(start), operationType, getRes.err)
		return getRes.result, getRes.err
	case <-ctxWithTimeout.Done():
		err := ctxWithTimeout.Err()
		go dbMeasure(start, time.Now().Sub(start), operationType, err)
		return nil, err
	}
}

func (d DbWrapper) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {

	// Create a context with a timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	// Channel to capture the output from Get
	getChan := make(chan error, 1)

	start := time.Now()
	var operationType = metrics2.INSERT

	go func() {
		err := d.DB.Insert(ctx, table, key, values)
		getChan <- err
	}()

	// handle the result or timeout
	select {
	case err := <-getChan:
		go dbMeasure(start, time.Now().Sub(start), operationType, err)
		return err
	case <-ctxWithTimeout.Done():
		err := ctxWithTimeout.Err()
		go dbMeasure(start, time.Now().Sub(start), operationType, err)
		return err
	}
}
