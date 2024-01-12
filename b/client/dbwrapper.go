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
	DB db.DB
	P  *bconfig.Config
}

func dbMeasure(start time.Time, operationType string, err error) {
	latency := time.Now().Sub(start)
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

func (d DbWrapper) Read(ctx context.Context, table string, key string, fields []string) (_ map[string][]byte, err error) {
	start := time.Now()
	defer func() {
		dbMeasure(start, "READ", err)
	}()

	return d.DB.Read(ctx, table, key, fields)
}

func (d DbWrapper) BatchRead(ctx context.Context, table string, keys []string, fields []string) (_ []map[string][]byte, err error) {
	batchDB, ok := d.DB.(db.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			dbMeasure(start, "BATCH_READ", err)
		}()
		return batchDB.BatchRead(ctx, table, keys, fields)
	}
	for _, key := range keys {
		_, err := d.DB.Read(ctx, table, key, fields)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (d DbWrapper) Scan(ctx context.Context, table string, startKey string, count int, fields []string) (_ []map[string][]byte, err error) {
	start := time.Now()
	defer func() {
		dbMeasure(start, "SCAN", err)
	}()

	return d.DB.Scan(ctx, table, startKey, count, fields)
}

func (d DbWrapper) Update(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	start := time.Now()
	defer func() {
		dbMeasure(start, "UPDATE", err)
	}()

	return d.DB.Update(ctx, table, key, values)
}

func (d DbWrapper) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) (err error) {
	batchDB, ok := d.DB.(db.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			dbMeasure(start, "BATCH_UPDATE", err)
		}()
		return batchDB.BatchUpdate(ctx, table, keys, values)
	}
	for i := range keys {
		err := d.DB.Update(ctx, table, keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (d DbWrapper) Insert(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	start := time.Now()
	defer func() {
		dbMeasure(start, "INSERT", err)
	}()

	return d.DB.Insert(ctx, table, key, values)
}

func (d DbWrapper) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) (err error) {
	batchDB, ok := d.DB.(db.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			dbMeasure(start, "BATCH_INSERT", err)
		}()
		return batchDB.BatchInsert(ctx, table, keys, values)
	}
	for i := range keys {
		err := d.DB.Insert(ctx, table, keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (d DbWrapper) Delete(ctx context.Context, table string, key string) (err error) {
	start := time.Now()
	defer func() {
		dbMeasure(start, "DELETE", err)
	}()

	return d.DB.Delete(ctx, table, key)
}

func (d DbWrapper) BatchDelete(ctx context.Context, table string, keys []string) (err error) {
	batchDB, ok := d.DB.(db.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			dbMeasure(start, "BATCH_DELETE", err)
		}()
		return batchDB.BatchDelete(ctx, table, keys)
	}
	for _, key := range keys {
		err := d.DB.Delete(ctx, table, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d DbWrapper) Analyze(ctx context.Context, table string) error {
	if analyzeDB, ok := d.DB.(db.AnalyzeDB); ok {
		return analyzeDB.Analyze(ctx, table)
	}
	return nil
}
