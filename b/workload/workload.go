package workload

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	"benchmark/db"
	"benchmark/generator"
	metrics2 "benchmark/metrics"
	"benchmark/util"
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	_ "os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Workload struct {
	p          *bconfig.Config
	fieldNames []string

	fieldLengthGenerator generator.Generator
	readAllFields        bool
	dataIntegrity        bool

	keySequence                  generator.Generator
	operationChooser             generator.Discrete
	keyChooser                   generator.Generator
	fieldChooser                 generator.Generator
	transactionInsertKeySequence generator.AcknowledgedCounter

	warmUpTime time.Time

	valuePool sync.Pool
}

type State struct {
	randPool sync.Pool
	// fieldNames is a copy of core.fieldNames to be goroutine-local
	fieldNames []string
}

type contextKey string
type operationType int64

const (
	read operationType = iota + 1
	insert
)

const stateKey = contextKey("core")

// NewWorkload implements the WorkloadCreator Create interface.
func NewWorkload(p *bconfig.Config, warmUpTime time.Time) (*Workload, error) {
	c := new(Workload)
	c.p = p
	c.warmUpTime = warmUpTime
	c.fieldNames = make([]string, int64(c.p.Workload.MaxFields.Value))
	for i := int64(0); i < int64(c.p.Workload.MaxFields.Value); i++ {
		c.fieldNames[i] = fmt.Sprintf("field%d", i)
	}
	c.fieldLengthGenerator = getFieldLengthGenerator(p)

	recordCount := (p.Workload.TargetExecutionTime.Value + p.Measurements.WarmUpTime.Value) * p.Workload.TargetOperationsPerSec.Value

	if int64(recordCount) < int64(p.Workload.NumUniqueKeys.Value) {
		util.Fatalf("TargetExecutionTime(%d) * TargetOperationsPerSec(%d) = %d must be bigger than NumUniqueKeys(%d)",
			p.Workload.TargetExecutionTime.Value, p.Workload.TargetOperationsPerSec.Value, recordCount, p.Workload.NumUniqueKeys.Value)
	}
	if c.p.Workload.PerformDataIntegrityChecks.Value && p.Workload.FieldSizeDistribution.Value != "constant" {
		util.Fatal("must have constant field size to check data integrity")
	}

	c.keySequence = generator.NewCounter(int64(0))
	c.operationChooser = createOperationGenerator(p)
	var keyrangeUpperBound = int64(p.Workload.NumUniqueKeys.Value)

	c.transactionInsertKeySequence = generator.NewAcknowledgedCounter(int64(recordCount))
	switch p.Workload.RequestDistribution.Value {
	case "uniform":
		c.keyChooser = generator.NewUniform(int64(1), keyrangeUpperBound)
	case "sequential":
		c.keyChooser = generator.NewSequential(int64(1), keyrangeUpperBound)
	case "zipfian":
		insertProportion := p.Workload.InsertProportion.Value
		opCount := recordCount
		expectedNewKeys := int64(float64(opCount) * insertProportion * 2.0)
		keyrangeUpperBound = int64(1) + int64(p.Workload.NumUniqueKeys.Value) + expectedNewKeys
		c.keyChooser = generator.NewScrambledZipfian(int64(1), keyrangeUpperBound, p.Workload.ZipfianConstant.Value)
	case "latest":
		c.keyChooser = generator.NewSkewedLatest(&c.transactionInsertKeySequence)
	case "hotspot":
		hotsetFraction := p.Workload.HotspotDataFraction.Value
		hotopnFraction := p.Workload.HotspotOpnFraction.Value
		c.keyChooser = generator.NewHotspot(int64(1), keyrangeUpperBound, hotsetFraction, hotopnFraction)
	case "exponential":
		percentile := p.Workload.ExponentialPercentile.Value
		frac := p.Workload.ExponentialFrac.Value
		c.keyChooser = generator.NewExponential(percentile, float64(int64(recordCount))*frac)
	default:
		util.Fatalf("unknown request distribution %s", p.Workload.RequestDistribution.Value)
	}
	c.fieldChooser = generator.NewUniform(0, int64(c.p.Workload.MaxFields.Value)-1)
	c.valuePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, c.p.Workload.AvFieldSizeBytes.Value+1)
		},
	}

	return c, nil
}

// GetRand provides a *rand.Rand from the pool.
func (state *State) GetRand() *rand.Rand {
	return state.randPool.Get().(*rand.Rand)
}

// PutRand puts a *rand.Rand back into the pool.
func (state *State) PutRand(r *rand.Rand) {
	state.randPool.Put(r)
}

func getFieldLengthGenerator(p *bconfig.Config) generator.Generator {
	var fieldLengthGenerator generator.Generator

	switch strings.ToLower(p.Workload.FieldSizeDistribution.Value) {
	case "constant":
		fieldLengthGenerator = generator.NewConstant(int64(p.Workload.AvFieldSizeBytes.Value))
	case "uniform":
		fieldLengthGenerator = generator.NewUniform(1, int64(p.Workload.AvFieldSizeBytes.Value))
	case "zipfian":
		fieldLengthGenerator = generator.NewZipfianWithRange(1, int64(p.Workload.AvFieldSizeBytes.Value), generator.ZipfianConstant)
	default:
		util.Fatalf("unknown field length distribution %s", p.Workload.FieldSizeDistribution.Value)
	}

	return fieldLengthGenerator
}

func createOperationGenerator(p *bconfig.Config) generator.Discrete {
	insertProportion := p.Workload.InsertProportion.Value
	readProportion := 1.0 - insertProportion

	operationChooser := generator.NewDiscrete()

	if readProportion > 0 {
		operationChooser.Add(readProportion, int64(read))
	}

	if insertProportion > 0 {
		operationChooser.Add(insertProportion, int64(insert))
	}

	return operationChooser
}

func workloadMeasure(start time.Time, operationType string, err error, hitDatabase bool) {
	latency := time.Now().Sub(start)
	if err != nil {
		metrics2.AddMeasurement(metrics2.TRANSACTION, start,
			map[string]interface{}{
				metrics2.SUCCESSFUL: false,
				metrics2.OPERATION:  operationType,
				metrics2.ERROR:      err.Error(),
				metrics2.LATENCY:    latency.Seconds(),
				metrics2.DATABASE:   hitDatabase,
			})
		return
	} else {
		metrics2.AddMeasurement(metrics2.TRANSACTION, start,
			map[string]interface{}{
				metrics2.SUCCESSFUL: true,
				metrics2.OPERATION:  operationType,
				metrics2.LATENCY:    latency.Seconds(),
				metrics2.DATABASE:   hitDatabase,
			})
	}
}

func (c *Workload) InitThread(ctx context.Context, _ int, _ int) context.Context {
	// r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fieldNames := make([]string, len(c.fieldNames))
	copy(fieldNames, c.fieldNames)
	state := &State{
		randPool: sync.Pool{
			New: func() interface{} {
				// Each rand.Rand has a unique seed to avoid generating the same sequence.
				return rand.New(rand.NewSource(time.Now().UnixNano()))
			}},
		fieldNames: fieldNames,
	}
	return context.WithValue(ctx, stateKey, state)
}

func (c *Workload) buildKeyName(keyNum int64) string {
	if c.p.Workload.HashInsertOrder.Value {
		keyNum = util.Hash64(keyNum)
	}

	prefix := "key"
	return fmt.Sprintf("%s%0[3]*[2]d", prefix, keyNum, int64(c.p.Measurements.ZeroPadding.Value))
}

func (c *Workload) nextKeyNum(state *State) int64 {
	r := state.GetRand()
	defer state.PutRand(r)
	keyNum := int64(0)
	if _, ok := c.keyChooser.(*generator.Exponential); ok {
		keyNum = -1
		for keyNum < 0 {
			keyNum = c.transactionInsertKeySequence.Last() - c.keyChooser.Next(r)
		}
	} else {
		keyNum = c.keyChooser.Next(r)
	}
	return keyNum
}

func getKeyAndValues(c *Workload, state *State) (string, map[string][]byte) {
	r := state.GetRand()
	defer state.PutRand(r)
	dbKey := c.buildKeyName(c.keySequence.Next(r))
	values := c.buildValues(state, dbKey)
	return dbKey, values
}

func (c *Workload) buildSingleValue(state *State, key string) map[string][]byte {
	values := make(map[string][]byte, 1)

	r := state.GetRand()
	defer state.PutRand(r)
	fieldKey := state.fieldNames[c.fieldChooser.Next(r)]

	var buf []byte
	if c.p.Workload.PerformDataIntegrityChecks.Value {
		buf = c.buildDeterministicValue(state, key, fieldKey)
	} else {
		buf = c.buildRandomValue(state)
	}

	values[fieldKey] = buf

	return values
}

func (c *Workload) buildValues(state *State, key string) map[string][]byte {
	values := make(map[string][]byte, int64(c.p.Workload.MaxFields.Value))

	for _, fieldKey := range state.fieldNames {
		var buf []byte
		if c.p.Workload.PerformDataIntegrityChecks.Value {
			buf = c.buildDeterministicValue(state, key, fieldKey)
		} else {
			buf = c.buildRandomValue(state)
		}

		values[fieldKey] = buf
	}
	return values
}

func (c *Workload) getValueBuffer(size int) []byte {
	buf := c.valuePool.Get().([]byte)
	if cap(buf) >= size {
		return buf[0:size]
	}

	return make([]byte, size)
}

func (c *Workload) putValues(values map[string][]byte) {
	for _, value := range values {
		c.valuePool.Put(value)
	}
}

func (c *Workload) buildRandomValue(state *State) []byte {
	r := state.GetRand()
	defer state.PutRand(r)
	buf := c.getValueBuffer(int(c.fieldLengthGenerator.Next(r)))
	util.RandBytes(r, buf)
	return buf
}

func (c *Workload) buildDeterministicValue(state *State, key string, fieldKey string) []byte {
	// TODO: use pool for the buffer
	r := state.GetRand()
	defer state.PutRand(r)
	size := c.fieldLengthGenerator.Next(r)
	buf := c.getValueBuffer(int(size + 21))
	b := bytes.NewBuffer(buf[0:0])
	b.WriteString(key)
	b.WriteByte(':')
	b.WriteString(strings.ToLower(fieldKey))
	for int64(b.Len()) < size {
		b.WriteByte(':')
		n := util.BytesHash64(b.Bytes())
		b.WriteString(strconv.FormatUint(uint64(n), 10))
	}
	b.Truncate(int(size))
	return b.Bytes()
}

func (c *Workload) verifyRow(state *State, key string, values map[string][]byte) {
	if len(values) == 0 {
		// null data here, need panic?
		return
	}

	for fieldKey, value := range values {
		expected := c.buildDeterministicValue(state, key, fieldKey)
		if !bytes.Equal(expected, value) {
			util.Fatalf("unexpected deterministic value, expect %q, but got %q", expected, value)
		}
	}
}

func (c *Workload) DoTransaction(ctx context.Context, db db.DB, cache_ cache.Cache) {

	if c.p.Workload.InsertProportion.Value < 1.0 && time.Now().Before(c.warmUpTime) {
		state := ctx.Value(stateKey).(*State)
		dbKey, values := getKeyAndValues(c, state)
		defer c.putValues(values)
		_, _ = cache_.Set(ctx, dbKey, values)
		return
	}

	state := ctx.Value(stateKey).(*State)
	r := state.GetRand()
	defer state.PutRand(r)
	operation := operationType(c.operationChooser.Next(r))

	switch operation {
	case read:
		keyName := c.buildKeyName(c.nextKeyNum(state))
		fields := state.fieldNames
		if !c.p.Workload.ReadAllFields.Value {
			fields = append(fields, state.fieldNames[c.fieldChooser.Next(r)])
		}
		if values := c.doTransactionRead(ctx, db, cache_, keyName, fields); values != nil {
			if c.p.Workload.PerformDataIntegrityChecks.Value {
				c.verifyRow(state, keyName, values)
			}
		}
	case insert:
		keyNum := c.transactionInsertKeySequence.Next(r)
		keyName := c.buildKeyName(keyNum)
		values := c.buildValues(state, keyName)
		defer c.transactionInsertKeySequence.Acknowledge(keyNum)
		defer c.putValues(values)
		c.doTransactionInsert(ctx, db, cache_, keyName, values)
	default:
		panic("unhandled default case")
	}
}

func (c *Workload) doTransactionRead(ctx context.Context, db db.DB, cache_ cache.Cache, keyName string, fields []string) map[string][]byte {
	start := time.Now()

	var hitDatabase = false
	var values map[string][]byte = nil
	var err error = nil

	// First, attempt to get the value from the cache
	if cachedValues, cacheErr, _ := cache_.Get(ctx, keyName, fields); cacheErr == nil && cachedValues != nil {
		// Cache hit
		values = cachedValues

	} else { // Cache miss, go to database
		hitDatabase = true
		for retries := 0; retries < c.p.Workload.DbOperationRetryLimit.Value; retries++ {
			dbValues, dbErr := db.Read(ctx, c.p.Database.CassandraTableName.Value, keyName, fields)

			if dbErr != nil && dbErr.Error() == "not found" {
				break
			}
			if dbErr == nil {
				// Successfully got values from database
				values = dbValues
				_, _ = cache_.Set(ctx, keyName, dbValues)
				break
			} else {
				// Database error
				err = dbErr
				go workloadMeasure(start, metrics2.READ, dbErr, true)

				select {
				case <-ctx.Done():
					if errors.Is(ctx.Err(), context.Canceled) {
						return nil
					}
				default:
				}
			}
		}
	}

	if err == nil {
		go workloadMeasure(start, metrics2.READ, nil, hitDatabase)
	}
	return values
}

func (c *Workload) doTransactionInsert(ctx context.Context, db db.DB, cache_ cache.Cache, dbKey string, values map[string][]byte) {
	start := time.Now()

	for retries := 0; retries < c.p.Workload.DbOperationRetryLimit.Value; retries++ {
		err := db.Insert(ctx, c.p.Database.CassandraTableName.Value, dbKey, values)

		if err != nil {
			_, _ = cache_.Set(ctx, dbKey, values)
			go workloadMeasure(start, metrics2.INSERT, err, true)
			return
		}

		go workloadMeasure(start, metrics2.INSERT, nil, true)

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}
		default:
		}
	}
}
