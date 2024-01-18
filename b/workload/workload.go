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
	"fmt"
	"math"
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
	scanLength                   generator.Generator

	warmUpTime time.Time

	valuePool sync.Pool
}

type State struct {
	r *rand.Rand
	// fieldNames is a copy of core.fieldNames to be goroutine-local
	fieldNames []string
}

type contextKey string
type operationType int64

const (
	read operationType = iota + 1
	update
	insert
	scan
	readModifyWrite
)

const stateKey = contextKey("core")

// NewWorkload implements the WorkloadCreator Create interface.
func NewWorkload(p *bconfig.Config, warmUpTime time.Time) (*Workload, error) {
	c := new(Workload)
	c.p = p
	c.warmUpTime = warmUpTime
	c.fieldNames = make([]string, int64(c.p.Performance.MaxFields.Value))
	for i := int64(0); i < int64(c.p.Performance.MaxFields.Value); i++ {
		c.fieldNames[i] = fmt.Sprintf("field%d", i)
	}
	c.fieldLengthGenerator = getFieldLengthGenerator(p)
	if c.p.Performance.RecordCount.Value == 0 {
		c.p.Performance.RecordCount.Value = math.MaxInt32
	}
	if int64(c.p.Performance.RecordCount.Value) < int64(p.Workload.KeyRangeLowerBound.Value)+int64(p.Performance.InsertCount.Value) {
		util.Fatalf("record count %d must be bigger than insert KeyRangeLowerBound %d + InsertCount %d",
			int64(c.p.Performance.RecordCount.Value), int64(p.Workload.KeyRangeLowerBound.Value), int64(p.Performance.InsertCount.Value))
	}
	if c.p.Performance.PerformDataIntegrityChecks.Value && p.Performance.FieldSizeDistribution.Value != "constant" {
		util.Fatal("must have constant field size to check data integrity")
	}

	c.keySequence = generator.NewCounter(int64(p.Workload.KeyRangeLowerBound.Value))
	c.operationChooser = createOperationGenerator(p)
	var keyrangeLowerBound = int64(p.Workload.KeyRangeLowerBound.Value)
	var keyrangeUpperBound = int64(p.Workload.KeyRangeLowerBound.Value) + int64(p.Performance.InsertCount.Value) - 1

	c.transactionInsertKeySequence = generator.NewAcknowledgedCounter(int64(c.p.Performance.RecordCount.Value))
	switch p.Workload.RequestDistribution.Value {
	case "uniform":
		c.keyChooser = generator.NewUniform(keyrangeLowerBound, keyrangeUpperBound)
	case "sequential":
		c.keyChooser = generator.NewSequential(keyrangeLowerBound, keyrangeUpperBound)
	case "zipfian":
		insertProportion := p.Workload.InsertProportion.Value
		opCount := p.Performance.OperationCount.Value
		expectedNewKeys := int64(float64(opCount) * insertProportion * 2.0)
		keyrangeUpperBound = int64(p.Workload.KeyRangeLowerBound.Value) + int64(p.Performance.InsertCount.Value) + expectedNewKeys
		c.keyChooser = generator.NewScrambledZipfian(keyrangeLowerBound, keyrangeUpperBound, generator.ZipfianConstant)
	case "latest":
		c.keyChooser = generator.NewSkewedLatest(&c.transactionInsertKeySequence)
	case "hotspot":
		hotsetFraction := p.Workload.HotspotDataFraction.Value
		hotopnFraction := p.Workload.HotspotOpnFraction.Value
		c.keyChooser = generator.NewHotspot(keyrangeLowerBound, keyrangeUpperBound, hotsetFraction, hotopnFraction)
	case "exponential":
		percentile := p.Workload.ExponentialPercentile.Value
		frac := p.Workload.ExponentialFrac.Value
		c.keyChooser = generator.NewExponential(percentile, float64(int64(c.p.Performance.RecordCount.Value))*frac)
	default:
		util.Fatalf("unknown request distribution %s", p.Workload.RequestDistribution.Value)
	}
	fmt.Println(fmt.Sprintf("Using request distribution '%s' a keyrange of [%d %d]", p.Workload.RequestDistribution.Value, keyrangeLowerBound, keyrangeUpperBound))

	c.fieldChooser = generator.NewUniform(0, int64(c.p.Performance.MaxFields.Value)-1)

	switch p.Workload.ScanLengthDistribution.Value {
	case "uniform":
		c.scanLength = generator.NewUniform(int64(p.Performance.MinScanLength.Value), int64(p.Performance.MaxScanLength.Value))
	case "zipfian":
		c.scanLength = generator.NewZipfianWithRange(int64(p.Performance.MinScanLength.Value), int64(p.Performance.MaxScanLength.Value), generator.ZipfianConstant)
	default:
		util.Fatalf("distribution %s not allowed for scan length", p.Workload.ScanLengthDistribution.Value)
	}
	c.valuePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, c.p.Performance.AvFieldSizeBytes.Value)
		},
	}

	return c, nil
}

func getFieldLengthGenerator(p *bconfig.Config) generator.Generator {
	var fieldLengthGenerator generator.Generator

	switch strings.ToLower(p.Performance.FieldSizeDistribution.Value) {
	case "constant":
		fieldLengthGenerator = generator.NewConstant(int64(p.Performance.AvFieldSizeBytes.Value))
	case "uniform":
		fieldLengthGenerator = generator.NewUniform(1, int64(p.Performance.AvFieldSizeBytes.Value))
	case "zipfian":
		fieldLengthGenerator = generator.NewZipfianWithRange(1, int64(p.Performance.AvFieldSizeBytes.Value), generator.ZipfianConstant)
	case "histogram":
		fieldLengthGenerator = generator.NewHistogramFromFile(p.Measurements.FieldLengthHistogramFile.Value)
	default:
		util.Fatalf("unknown field length distribution %s", p.Performance.FieldSizeDistribution.Value)
	}

	return fieldLengthGenerator
}

func createOperationGenerator(p *bconfig.Config) generator.Discrete {
	readProportion := p.Workload.ReadProportion.Value
	updateProportion := p.Workload.UpdateProportion.Value
	insertProportion := p.Workload.InsertProportion.Value
	scanProportion := p.Workload.ScanProportion.Value
	readModifyWriteProportion := p.Workload.ReadModifyWriteProportion.Value

	operationChooser := generator.NewDiscrete()
	if readProportion > 0 {
		operationChooser.Add(readProportion, int64(read))
	}

	if updateProportion > 0 {
		operationChooser.Add(updateProportion, int64(update))
	}

	if insertProportion > 0 {
		operationChooser.Add(insertProportion, int64(insert))
	}

	if scanProportion > 0 {
		operationChooser.Add(scanProportion, int64(scan))
	}

	if readModifyWriteProportion > 0 {
		operationChooser.Add(readModifyWriteProportion, int64(readModifyWrite))
	}

	return operationChooser
}

func (c *Workload) InitThread(ctx context.Context, _ int, _ int) context.Context {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fieldNames := make([]string, len(c.fieldNames))
	copy(fieldNames, c.fieldNames)
	state := &State{
		r:          r,
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

func (c *Workload) buildSingleValue(state *State, key string) map[string][]byte {
	values := make(map[string][]byte, 1)

	r := state.r
	fieldKey := state.fieldNames[c.fieldChooser.Next(r)]

	var buf []byte
	if c.p.Performance.PerformDataIntegrityChecks.Value {
		buf = c.buildDeterministicValue(state, key, fieldKey)
	} else {
		buf = c.buildRandomValue(state)
	}

	values[fieldKey] = buf

	return values
}

func (c *Workload) buildValues(state *State, key string) map[string][]byte {
	values := make(map[string][]byte, int64(c.p.Performance.MaxFields.Value))

	for _, fieldKey := range state.fieldNames {
		var buf []byte
		if c.p.Performance.PerformDataIntegrityChecks.Value {
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
	// TODO: use pool for the buffer
	r := state.r
	buf := c.getValueBuffer(int(c.fieldLengthGenerator.Next(r)))
	util.RandBytes(r, buf)
	return buf
}

func (c *Workload) buildDeterministicValue(state *State, key string, fieldKey string) []byte {
	// TODO: use pool for the buffer
	r := state.r
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

func (c *Workload) DoInsert(ctx context.Context, db db.DB, cache_ cache.Cache) error {
	state := ctx.Value(stateKey).(*State)
	r := state.r
	keyNum := c.keySequence.Next(r)
	dbKey := c.buildKeyName(keyNum)
	values := c.buildValues(state, dbKey)
	defer c.putValues(values)

	start := time.Now()

	numOfRetries := int64(0)

	var err error
	for {
		err = db.Insert(ctx, c.p.Database.CassandraTableName.Value, dbKey, values)

		_, _ = cache_.Set(ctx, dbKey, values)

		go workloadMeasure(start, metrics2.INSERT, err, true)

		if err == nil {
			break
		}

		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
		default:
		}

		// Retry if configured. Without retrying, the load process will fail
		// even if one single insertion fails. User can optionally configure
		// an insertion retry limit (default is 0) to enable retry.
		numOfRetries++
		if numOfRetries > int64(c.p.Performance.InsertionRetryLimit.Value) {
			break
		}

		// Sleep for a random time betweensz [0.8, 1.2)*insertionRetryInterval
		//sleepTimeMs := float64((int64(c.p.Performance.InsertionRetryInterval.Value) * 1000)) * (0.8 + 0.4*r.Float64())

		//time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
	}

	return err
}

func (c *Workload) DoBatchInsert(ctx context.Context, batchSize int, d db.DB, cache_ cache.Cache) error {
	batchDB, ok := d.(db.BatchDB)
	if !ok {
		return fmt.Errorf("the %T does't implement the batchDB interface", d)
	}
	state := ctx.Value(stateKey).(*State)
	r := state.r
	var keys []string
	var values []map[string][]byte
	for i := 0; i < batchSize; i++ {
		keyNum := c.keySequence.Next(r)
		dbKey := c.buildKeyName(keyNum)
		keys = append(keys, dbKey)
		values = append(values, c.buildValues(state, dbKey))
	}
	defer func() {
		for _, value := range values {
			c.putValues(value)
		}
	}()

	numOfRetries := int64(0)
	var err error
	for {
		err = batchDB.BatchInsert(ctx, c.p.Database.CassandraTableName.Value, keys, values)
		if err != nil {
			break
		}

		// Update the cache with the new values after a successful database insert
		for i, key := range keys {
			err, _ = cache_.Set(ctx, key, values[i])
			if err != nil {
				//break
			}
		}

		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
		default:
		}

		// Retry if configured. Without retrying, the load process will fail
		// even if one single insertion fails. User can optionally configure
		// an insertion retry limit (default is 0) to enable retry.
		numOfRetries++
		if numOfRetries > int64(c.p.Performance.InsertionRetryLimit.Value) {
			break
		}

		// Sleep for a random time betweensz [0.8, 1.2)*insertionRetryInterval
		sleepTimeMs := float64((int64(c.p.Performance.InsertionRetryInterval.Value) * 1000)) * (0.8 + 0.4*r.Float64())

		time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
	}
	return err
}

func (c *Workload) DoBatchTransaction(ctx context.Context, batchSize int, d db.DB, cache_ cache.Cache) (error, bool) {
	batchDB, ok := d.(db.BatchDB)
	if !ok {
		return fmt.Errorf("the %T does't implement the batchDB interface", d), false
	}
	state := ctx.Value(stateKey).(*State)
	r := state.r

	operation := operationType(c.operationChooser.Next(r))
	switch operation {
	case read:
		return c.doBatchTransactionRead(ctx, batchSize, batchDB, cache_, state)
	case insert:
		return c.doBatchTransactionInsert(ctx, batchSize, batchDB, cache_, state)
	case update:
		return c.doBatchTransactionUpdate(ctx, batchSize, batchDB, cache_, state)
	case scan:
		panic("The batch mode doesn't support the scan operation")
	default:
		return nil, false
	}
}

func (c *Workload) nextKeyNum(state *State) int64 {
	r := state.r
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

func (c *Workload) doTransactionRead(ctx context.Context, db db.DB, cache_ cache.Cache, state *State) (errr error, hitDatabase bool) {
	start := time.Now()
	r := state.r
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var fields []string
	if !c.p.Workload.ReadAllFields.Value {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	numOfRetries := int64(0)

	for {
		// First, attempt to get the value from the cache
		cachedValue, err, _ := cache_.Get(ctx, keyName, fields)
		if err == nil && cachedValue != nil {
			// CacheWrapper hit, use the cachedValue
			// todo  handle the cached value
			if c.p.Performance.PerformDataIntegrityChecks.Value {
				c.verifyRow(state, keyName, cachedValue)
			}
			go workloadMeasure(start, metrics2.READ, nil, false)
			return nil, false
		}

		// cache miss
		values, err := db.Read(ctx, c.p.Database.CassandraTableName.Value, keyName, fields)
		if err == nil && values != nil {
			_, _ = cache_.Set(ctx, keyName, values)
			go workloadMeasure(start, metrics2.READ, nil, true)
			return nil, true
		}

		if c.p.Performance.PerformDataIntegrityChecks.Value {
			c.verifyRow(state, keyName, values)
		}

		go workloadMeasure(start, metrics2.READ, err, true)

		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil, true
			}
		default:
		}

		// Retry if configured. Without retrying, the load process will fail
		// even if one single insertion fails. User can optionally configure
		// an insertion retry limit (default is 0) to enable retry.
		numOfRetries++
		if numOfRetries > int64(c.p.Performance.InsertionRetryLimit.Value) {
			break
		}
	}

	return nil, true
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

func (c *Workload) doTransactionReadModifyWrite(ctx context.Context, db db.DB, cache_ cache.Cache, state *State) (errr error, hitDatabase bool) {
	start := time.Now()
	defer func() {
		workloadMeasure(start, metrics2.READ_MODIFY_WRITE, errr, hitDatabase)
	}()

	r := state.r
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var fields []string
	if !c.p.Workload.ReadAllFields.Value {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	var values map[string][]byte
	if c.p.Workload.WriteAllFields.Value {
		values = c.buildValues(state, keyName)
	} else {
		values = c.buildSingleValue(state, keyName)
	}
	defer c.putValues(values)

	readValues, err := db.Read(ctx, c.p.Database.CassandraTableName.Value, keyName, fields)
	if err != nil {
		return err, true
	}

	if err = db.Update(ctx, c.p.Database.CassandraTableName.Value, keyName, values); err != nil {
		return err, true
	}
	if err, _ = cache_.Set(ctx, keyName, values); err != nil {
		//return err
	}

	if c.p.Performance.PerformDataIntegrityChecks.Value {
		c.verifyRow(state, keyName, readValues)
	}

	return nil, true
}

func (c *Workload) doTransactionInsert(ctx context.Context, db db.DB, cache_ cache.Cache, state *State) (errr error, hitDatabase bool) {
	start := time.Now()
	defer func() {
		workloadMeasure(start, metrics2.INSERT, errr, hitDatabase)
	}()
	r := state.r
	keyNum := c.transactionInsertKeySequence.Next(r)
	defer c.transactionInsertKeySequence.Acknowledge(keyNum)
	dbKey := c.buildKeyName(keyNum)
	values := c.buildValues(state, dbKey)
	defer c.putValues(values)

	if err := db.Insert(ctx, c.p.Database.CassandraTableName.Value, dbKey, values); err != nil {
		return err, true
	}
	if err, _ := cache_.Set(ctx, dbKey, values); err != nil {
		//return err
	}
	return nil, true
}

// If all keys are in the cache, it uses those values. However, if any key is missing, it should perform a scan operation on the database for the entire range.
func (c *Workload) doTransactionScan(ctx context.Context, db db.DB, cache_ cache.Cache, state *State) (err error, hitDatabase bool) {
	start := time.Now()
	defer func() {
		workloadMeasure(start, metrics2.SCAN, err, hitDatabase)
	}()
	r := state.r
	keyNum := c.nextKeyNum(state)
	startKeyName := c.buildKeyName(keyNum)

	scanLen := c.scanLength.Next(r)

	var fields []string
	if !c.p.Workload.ReadAllFields.Value {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	// Check if the range is in the cache
	values := make([]map[string][]byte, 0, int(scanLen))
	cacheMiss := false
	for i := 0; i < int(scanLen); i++ {
		keyName := c.buildKeyName(keyNum + int64(i))
		value, err, _ := cache_.Get(ctx, keyName, fields)
		if err != nil || value == nil {
			// CacheWrapper miss detected
			cacheMiss = true
			break
		}
		values = append(values, value)
		if c.p.Performance.PerformDataIntegrityChecks.Value {
			c.verifyRow(state, keyName, value)
		}
	}

	if cacheMiss {
		// Perform the scan in the database if any key is not in cache
		dbValues, err_ := db.Scan(ctx, c.p.Database.CassandraTableName.Value, startKeyName, int(scanLen), fields)
		if err_ != nil {
			return err_, true
		}
		values = dbValues // Use values from the database

		// update the cache with the new values from the database
		for _, value := range dbValues {
			key := value["key"] // todo is "key" the identifier in the returned map?
			cacheErr, _ := cache_.Set(ctx, string(key), value)
			if cacheErr != nil {
				//log.Printf("Failed to update cache for key %s: %v", string(key), cacheErr)
			}
		}
	} else {
		return nil, false
	}

	return nil, true

	// if _, err := db.Scan(ctx, c.p.Database.CassandraTableName.Value, startKeyName, int(scanLen), fields); err != nil {
	//		return err
	//	}
}

func (c *Workload) DoTransaction(ctx context.Context, db db.DB, cache_ cache.Cache) (error, bool) {
	state := ctx.Value(stateKey).(*State)
	r := state.r

	if time.Now().Before(c.warmUpTime) {
		dbKey := c.buildKeyName(c.keySequence.Next(r))
		values := c.buildValues(state, dbKey)
		err, _ := cache_.Set(ctx, dbKey, values)
		if err != nil {
		}
		return nil, false
	}

	operation := operationType(c.operationChooser.Next(r))
	switch operation {
	case read:
		return c.doTransactionRead(ctx, db, cache_, state)
	case update:
		return c.doTransactionUpdate(ctx, db, cache_, state)
	case insert:
		return c.doTransactionInsert(ctx, db, cache_, state)
	case scan:
		return c.doTransactionScan(ctx, db, cache_, state)
	default:
		return c.doTransactionReadModifyWrite(ctx, db, cache_, state)
	}
}

func (c *Workload) doTransactionUpdate(ctx context.Context, db db.DB, cache_ cache.Cache, state *State) (err error, hitDatabase bool) {
	start := time.Now()
	defer func() {
		workloadMeasure(start, metrics2.UPDATE, err, hitDatabase)
	}()
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var values map[string][]byte
	if c.p.Workload.WriteAllFields.Value {
		values = c.buildValues(state, keyName)
	} else {
		values = c.buildSingleValue(state, keyName)
	}

	defer c.putValues(values)

	// Perform the update to the database
	errr := db.Update(ctx, c.p.Database.CassandraTableName.Value, keyName, values)
	if errr != nil {
		return errr, true
	}

	// Update the cache with the new values after a successful database insert
	cacheErr, _ := cache_.Set(ctx, keyName, values)
	if cacheErr != nil {
		//return err
	}

	return nil, true
}

func (c *Workload) doBatchTransactionRead(ctx context.Context, batchSize int, db db.BatchDB, cache_ cache.Cache, state *State) (errr error, hitDatabase bool) {
	start := time.Now()
	defer func() {
		workloadMeasure(start, metrics2.BATCH_READ, errr, hitDatabase)
	}()
	r := state.r
	var fields []string

	if !c.p.Workload.ReadAllFields.Value {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	keys := make([]string, batchSize)
	for i := 0; i < batchSize; i++ {
		keys[i] = c.buildKeyName(c.nextKeyNum(state))
	}

	// Prepare the slice for cache misses
	cacheMissKeys := make([]string, 0, batchSize)
	values := make([]map[string][]byte, 0, batchSize)

	// Attempt to get the value from the cache
	for _, key := range keys {
		cachedValue, err, _ := cache_.Get(ctx, key, fields)
		if err == nil && cachedValue != nil {
			// CacheWrapper hit, use the cachedValue
			values = append(values, cachedValue)
			if c.p.Performance.PerformDataIntegrityChecks.Value {
				// Verify the integrity of the cached value
				c.verifyRow(state, key, cachedValue)
			}
		} else if err != nil {
			// If an error occurred while fetching from cache, handle it
			//return err
		} else {
			// CacheWrapper miss, add this key to the list of misses
			cacheMissKeys = append(cacheMissKeys, key)
		}
	}

	// If there were cache misses, read from the database
	if len(cacheMissKeys) > 0 {
		dbValues, err := db.BatchRead(ctx, c.p.Database.CassandraTableName.Value, cacheMissKeys, fields)
		if err != nil {
			return err, true
		}

		// Add database read values to the total values
		values = append(values, dbValues...)

		if c.p.Performance.PerformDataIntegrityChecks.Value {
			// Verify the integrity of the values read from the database
			for _, dbValue := range dbValues {
				key := dbValue["key"] // Assuming "key" is the identifier in the returned map
				c.verifyRow(state, string(key), dbValue)
			}
		}
	} else {
		return nil, false
	}

	return nil, true
}

func (c *Workload) doBatchTransactionInsert(ctx context.Context, batchSize int, db db.BatchDB, cache_ cache.Cache, state *State) (errr error, hitDatabase bool) {
	start := time.Now()
	defer func() {
		workloadMeasure(start, metrics2.BATCH_INSERT, errr, hitDatabase)
	}()
	r := state.r
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keyNum := c.transactionInsertKeySequence.Next(r)
		keyName := c.buildKeyName(keyNum)
		keys[i] = keyName
		if c.p.Workload.WriteAllFields.Value {
			values[i] = c.buildValues(state, keyName)
		} else {
			values[i] = c.buildSingleValue(state, keyName)
		}
		c.transactionInsertKeySequence.Acknowledge(keyNum)
	}

	defer func() {
		for _, value := range values {
			c.putValues(value)
		}
	}()

	// Perform the batch insert to the database
	err := db.BatchInsert(ctx, c.p.Database.CassandraTableName.Value, keys, values)
	if err != nil {
		return err, true
	}

	// Update the cache with the new values after a successful database insert
	for i, key := range keys {
		cacheErr, _ := cache_.Set(ctx, key, values[i])
		if cacheErr != nil {
			//return err
		}
	}

	return nil, true
}

func (c *Workload) doBatchTransactionUpdate(ctx context.Context, batchSize int, db db.BatchDB, cache_ cache.Cache, state *State) (errr error, hitDatabase bool) {
	start := time.Now()
	defer func() {
		workloadMeasure(start, metrics2.BATCH_UPDATE, errr, hitDatabase)
	}()
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keyNum := c.nextKeyNum(state)
		keyName := c.buildKeyName(keyNum)
		keys[i] = keyName
		if c.p.Workload.WriteAllFields.Value {
			values[i] = c.buildValues(state, keyName)
		} else {
			values[i] = c.buildSingleValue(state, keyName)
		}
	}

	defer func() {
		for _, value := range values {
			c.putValues(value)
		}
	}()

	// Perform the batch update to the database
	err := db.BatchUpdate(ctx, c.p.Database.CassandraTableName.Value, keys, values)
	if err != nil {
		return err, true
	}

	// Update the cache with the new values after a successful database update
	for i, key := range keys {
		cacheErr, _ := cache_.Set(ctx, key, values[i])
		if cacheErr != nil {
			//return err
		}
	}

	return nil, true
}
