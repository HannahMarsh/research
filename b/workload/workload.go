package workload

import (
	bconfig "benchmark/config"
	"benchmark/db"
	"benchmark/generator"
	"benchmark/measurement"
	"benchmark/util"
	"bytes"
	"context"
	"fmt"
	"github.com/magiconair/properties"
	"math"
	"math/rand"
	_ "os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Workload struct {
	p *properties.Properties

	table      string
	fieldCount int64
	fieldNames []string

	fieldLengthGenerator generator.Generator
	readAllFields        bool
	writeAllFields       bool
	dataIntegrity        bool

	keySequence                  generator.Generator
	operationChooser             generator.Discrete
	keyChooser                   generator.Generator
	fieldChooser                 generator.Generator
	transactionInsertKeySequence generator.AcknowledgedCounter
	scanLength                   generator.Generator
	orderedInserts               bool
	recordCount                  int64
	zeroPadding                  int64
	insertionRetryLimit          int64
	insertionRetryInterval       int64

	valuePool sync.Pool
}

type WorkloadState struct {
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

// Create implements the WorkloadCreator Create interface.
func NewWorkload(p *properties.Properties) (*Workload, error) {
	c := new(Workload)
	c.p = p
	c.table = p.GetString(bconfig.TableName, bconfig.TableNameDefault)
	c.fieldCount = p.GetInt64(bconfig.FieldCount, bconfig.FieldCountDefault)
	c.fieldNames = make([]string, c.fieldCount)
	for i := int64(0); i < c.fieldCount; i++ {
		c.fieldNames[i] = fmt.Sprintf("field%d", i)
	}
	c.fieldLengthGenerator = getFieldLengthGenerator(p)
	c.recordCount = p.GetInt64(bconfig.RecordCount, bconfig.RecordCountDefault)
	if c.recordCount == 0 {
		c.recordCount = int64(math.MaxInt32)
	}

	requestDistrib := p.GetString(bconfig.RequestDistribution, bconfig.RequestDistributionDefault)
	minScanLength := p.GetInt64(bconfig.MinScanLength, bconfig.MinScanLengthDefault)
	maxScanLength := p.GetInt64(bconfig.MaxScanLength, bconfig.MaxScanLengthDefault)
	scanLengthDistrib := p.GetString(bconfig.ScanLengthDistribution, bconfig.ScanLengthDistributionDefault)

	insertStart := p.GetInt64(bconfig.InsertStart, bconfig.InsertStartDefault)
	insertCount := p.GetInt64(bconfig.InsertCount, c.recordCount-insertStart)
	if c.recordCount < insertStart+insertCount {
		util.Fatalf("record count %d must be bigger than insert start %d + count %d",
			c.recordCount, insertStart, insertCount)
	}
	c.zeroPadding = p.GetInt64(bconfig.ZeroPadding, bconfig.ZeroPaddingDefault)
	c.readAllFields = p.GetBool(bconfig.ReadAllFields, bconfig.ReadALlFieldsDefault)
	c.writeAllFields = p.GetBool(bconfig.WriteAllFields, bconfig.WriteAllFieldsDefault)
	c.dataIntegrity = p.GetBool(bconfig.DataIntegrity, bconfig.DataIntegrityDefault)
	fieldLengthDistribution := p.GetString(bconfig.FieldLengthDistribution, bconfig.FieldLengthDistributionDefault)
	if c.dataIntegrity && fieldLengthDistribution != "constant" {
		util.Fatal("must have constant field size to check data integrity")
	}

	if p.GetString(bconfig.InsertOrder, bconfig.InsertOrderDefault) == "hashed" {
		c.orderedInserts = false
	} else {
		c.orderedInserts = true
	}

	c.keySequence = generator.NewCounter(insertStart)
	c.operationChooser = createOperationGenerator(p)
	var keyrangeLowerBound int64 = insertStart
	var keyrangeUpperBound int64 = insertStart + insertCount - 1

	c.transactionInsertKeySequence = generator.NewAcknowledgedCounter(c.recordCount)
	switch requestDistrib {
	case "uniform":
		c.keyChooser = generator.NewUniform(keyrangeLowerBound, keyrangeUpperBound)
	case "sequential":
		c.keyChooser = generator.NewSequential(keyrangeLowerBound, keyrangeUpperBound)
	case "zipfian":
		insertProportion := p.GetFloat64(bconfig.InsertProportion, bconfig.InsertProportionDefault)
		opCount := p.GetInt64(bconfig.OperationCount, 0)
		expectedNewKeys := int64(float64(opCount) * insertProportion * 2.0)
		keyrangeUpperBound = insertStart + insertCount + expectedNewKeys
		c.keyChooser = generator.NewScrambledZipfian(keyrangeLowerBound, keyrangeUpperBound, generator.ZipfianConstant)
	case "latest":
		c.keyChooser = generator.NewSkewedLatest(&c.transactionInsertKeySequence)
	case "hotspot":
		hotsetFraction := p.GetFloat64(bconfig.HotspotDataFraction, bconfig.HotspotDataFractionDefault)
		hotopnFraction := p.GetFloat64(bconfig.HotspotOpnFraction, bconfig.HotspotOpnFractionDefault)
		c.keyChooser = generator.NewHotspot(keyrangeLowerBound, keyrangeUpperBound, hotsetFraction, hotopnFraction)
	case "exponential":
		percentile := p.GetFloat64(bconfig.ExponentialPercentile, bconfig.ExponentialPercentileDefault)
		frac := p.GetFloat64(bconfig.ExponentialFrac, bconfig.ExponentialFracDefault)
		c.keyChooser = generator.NewExponential(percentile, float64(c.recordCount)*frac)
	default:
		util.Fatalf("unknown request distribution %s", requestDistrib)
	}
	fmt.Println(fmt.Sprintf("Using request distribution '%s' a keyrange of [%d %d]", requestDistrib, keyrangeLowerBound, keyrangeUpperBound))

	c.fieldChooser = generator.NewUniform(0, c.fieldCount-1)
	switch scanLengthDistrib {
	case "uniform":
		c.scanLength = generator.NewUniform(minScanLength, maxScanLength)
	case "zipfian":
		c.scanLength = generator.NewZipfianWithRange(minScanLength, maxScanLength, generator.ZipfianConstant)
	default:
		util.Fatalf("distribution %s not allowed for scan length", scanLengthDistrib)
	}

	c.insertionRetryLimit = p.GetInt64(bconfig.InsertionRetryLimit, bconfig.InsertionRetryLimitDefault)
	c.insertionRetryInterval = p.GetInt64(bconfig.InsertionRetryInterval, bconfig.InsertionRetryIntervalDefault)

	fieldLength := p.GetInt64(bconfig.FieldLength, bconfig.FieldLengthDefault)
	c.valuePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, fieldLength)
		},
	}

	return c, nil
}

func getFieldLengthGenerator(p *properties.Properties) generator.Generator {
	var fieldLengthGenerator generator.Generator
	fieldLengthDistribution := p.GetString(bconfig.FieldLengthDistribution, bconfig.FieldLengthDistributionDefault)
	fieldLength := p.GetInt64(bconfig.FieldLength, bconfig.FieldLengthDefault)
	fieldLengthHistogram := p.GetString(bconfig.FieldLengthHistogramFile, bconfig.FieldLengthHistogramFileDefault)

	switch strings.ToLower(fieldLengthDistribution) {
	case "constant":
		fieldLengthGenerator = generator.NewConstant(fieldLength)
	case "uniform":
		fieldLengthGenerator = generator.NewUniform(1, fieldLength)
	case "zipfian":
		fieldLengthGenerator = generator.NewZipfianWithRange(1, fieldLength, generator.ZipfianConstant)
	case "histogram":
		fieldLengthGenerator = generator.NewHistogramFromFile(fieldLengthHistogram)
	default:
		util.Fatalf("unknown field length distribution %s", fieldLengthDistribution)
	}

	return fieldLengthGenerator
}

func createOperationGenerator(p *properties.Properties) generator.Discrete {
	readProportion := p.GetFloat64(bconfig.ReadProportion, bconfig.ReadProportionDefault)
	updateProportion := p.GetFloat64(bconfig.UpdateProportion, bconfig.UpdateProportionDefault)
	insertProportion := p.GetFloat64(bconfig.InsertProportion, bconfig.InsertProportionDefault)
	scanProportion := p.GetFloat64(bconfig.ScanProportion, bconfig.ScanProportionDefault)
	readModifyWriteProportion := p.GetFloat64(bconfig.ReadModifyWriteProportion, bconfig.ReadModifyWriteProportionDefault)

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
	state := &WorkloadState{
		r:          r,
		fieldNames: fieldNames,
	}
	return context.WithValue(ctx, stateKey, state)
}

func (c *Workload) buildKeyName(keyNum int64) string {
	if !c.orderedInserts {
		keyNum = util.Hash64(keyNum)
	}

	prefix := "key"
	return fmt.Sprintf("%s%0[3]*[2]d", prefix, keyNum, c.zeroPadding)
}

func (c *Workload) buildSingleValue(state *WorkloadState, key string) map[string][]byte {
	values := make(map[string][]byte, 1)

	r := state.r
	fieldKey := state.fieldNames[c.fieldChooser.Next(r)]

	var buf []byte
	if c.dataIntegrity {
		buf = c.buildDeterministicValue(state, key, fieldKey)
	} else {
		buf = c.buildRandomValue(state)
	}

	values[fieldKey] = buf

	return values
}

func (c *Workload) buildValues(state *WorkloadState, key string) map[string][]byte {
	values := make(map[string][]byte, c.fieldCount)

	for _, fieldKey := range state.fieldNames {
		var buf []byte
		if c.dataIntegrity {
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

func (c *Workload) buildRandomValue(state *WorkloadState) []byte {
	// TODO: use pool for the buffer
	r := state.r
	buf := c.getValueBuffer(int(c.fieldLengthGenerator.Next(r)))
	util.RandBytes(r, buf)
	return buf
}

func (c *Workload) buildDeterministicValue(state *WorkloadState, key string, fieldKey string) []byte {
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

func (c *Workload) verifyRow(state *WorkloadState, key string, values map[string][]byte) {
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

func (c *Workload) DoInsert(ctx context.Context, db db.DB) error {
	state := ctx.Value(stateKey).(*WorkloadState)
	r := state.r
	keyNum := c.keySequence.Next(r)
	dbKey := c.buildKeyName(keyNum)
	values := c.buildValues(state, dbKey)
	defer c.putValues(values)

	numOfRetries := int64(0)

	var err error
	for {
		err = db.Insert(ctx, c.table, dbKey, values)
		if err != nil {
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
		if numOfRetries > c.insertionRetryLimit {
			break
		}

		// Sleep for a random time betweensz [0.8, 1.2)*insertionRetryInterval
		sleepTimeMs := float64((c.insertionRetryInterval * 1000)) * (0.8 + 0.4*r.Float64())

		time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
	}

	return err
}

func (c *Workload) DoBatchInsert(ctx context.Context, batchSize int, d db.DB) error {
	batchDB, ok := d.(db.BatchDB)
	if !ok {
		return fmt.Errorf("the %T does't implement the batchDB interface", d)
	}
	state := ctx.Value(stateKey).(*WorkloadState)
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
		err = batchDB.BatchInsert(ctx, c.table, keys, values)
		if err != nil {
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
		if numOfRetries > c.insertionRetryLimit {
			break
		}

		// Sleep for a random time betweensz [0.8, 1.2)*insertionRetryInterval
		sleepTimeMs := float64((c.insertionRetryInterval * 1000)) * (0.8 + 0.4*r.Float64())

		time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
	}
	return err
}

func (c *Workload) DoBatchTransaction(ctx context.Context, batchSize int, d db.DB) error {
	batchDB, ok := d.(db.BatchDB)
	if !ok {
		return fmt.Errorf("the %T does't implement the batchDB interface", d)
	}
	state := ctx.Value(stateKey).(*WorkloadState)
	r := state.r

	operation := operationType(c.operationChooser.Next(r))
	switch operation {
	case read:
		return c.doBatchTransactionRead(ctx, batchSize, batchDB, state)
	case insert:
		return c.doBatchTransactionInsert(ctx, batchSize, batchDB, state)
	case update:
		return c.doBatchTransactionUpdate(ctx, batchSize, batchDB, state)
	case scan:
		panic("The batch mode don't support the scan operation")
	default:
		return nil
	}
}

func (c *Workload) nextKeyNum(state *WorkloadState) int64 {
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

func (c *Workload) doTransactionRead(ctx context.Context, db db.DB, state *WorkloadState) error {
	r := state.r
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var fields []string
	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	values, err := db.Read(ctx, c.table, keyName, fields)
	if err != nil {
		return err
	}

	if c.dataIntegrity {
		c.verifyRow(state, keyName, values)
	}

	return nil
}

func (c *Workload) doTransactionReadModifyWrite(ctx context.Context, db db.DB, state *WorkloadState) error {
	start := time.Now()
	defer func() {
		measurement.Measure("READ_MODIFY_WRITE", start, time.Now().Sub(start))
	}()

	r := state.r
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var fields []string
	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	var values map[string][]byte
	if c.writeAllFields {
		values = c.buildValues(state, keyName)
	} else {
		values = c.buildSingleValue(state, keyName)
	}
	defer c.putValues(values)

	readValues, err := db.Read(ctx, c.table, keyName, fields)
	if err != nil {
		return err
	}

	if err := db.Update(ctx, c.table, keyName, values); err != nil {
		return err
	}

	if c.dataIntegrity {
		c.verifyRow(state, keyName, readValues)
	}

	return nil
}

func (c *Workload) doTransactionInsert(ctx context.Context, db db.DB, state *WorkloadState) error {
	r := state.r
	keyNum := c.transactionInsertKeySequence.Next(r)
	defer c.transactionInsertKeySequence.Acknowledge(keyNum)
	dbKey := c.buildKeyName(keyNum)
	values := c.buildValues(state, dbKey)
	defer c.putValues(values)

	return db.Insert(ctx, c.table, dbKey, values)
}

func (c *Workload) doTransactionScan(ctx context.Context, db db.DB, state *WorkloadState) error {
	r := state.r
	keyNum := c.nextKeyNum(state)
	startKeyName := c.buildKeyName(keyNum)

	scanLen := c.scanLength.Next(r)

	var fields []string
	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	_, err := db.Scan(ctx, c.table, startKeyName, int(scanLen), fields)

	return err
}

func (c *Workload) DoTransaction(ctx context.Context, db db.DB) error {
	state := ctx.Value(stateKey).(*WorkloadState)
	r := state.r

	operation := operationType(c.operationChooser.Next(r))
	switch operation {
	case read:
		return c.doTransactionRead(ctx, db, state)
	case update:
		return c.doTransactionUpdate(ctx, db, state)
	case insert:
		return c.doTransactionInsert(ctx, db, state)
	case scan:
		return c.doTransactionScan(ctx, db, state)
	default:
		return c.doTransactionReadModifyWrite(ctx, db, state)
	}
}

func (c *Workload) doTransactionUpdate(ctx context.Context, db db.DB, state *WorkloadState) error {
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var values map[string][]byte
	if c.writeAllFields {
		values = c.buildValues(state, keyName)
	} else {
		values = c.buildSingleValue(state, keyName)
	}

	defer c.putValues(values)

	return db.Update(ctx, c.table, keyName, values)
}

func (c *Workload) doBatchTransactionRead(ctx context.Context, batchSize int, db db.BatchDB, state *WorkloadState) error {
	r := state.r
	var fields []string

	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	keys := make([]string, batchSize)
	for i := 0; i < batchSize; i++ {
		keys[i] = c.buildKeyName(c.nextKeyNum(state))
	}

	_, err := db.BatchRead(ctx, c.table, keys, fields)
	if err != nil {
		return err
	}

	// TODO should we verify the result?
	return nil
}

func (c *Workload) doBatchTransactionInsert(ctx context.Context, batchSize int, db db.BatchDB, state *WorkloadState) error {
	r := state.r
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keyNum := c.transactionInsertKeySequence.Next(r)
		keyName := c.buildKeyName(keyNum)
		keys[i] = keyName
		if c.writeAllFields {
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

	return db.BatchInsert(ctx, c.table, keys, values)
}

func (c *Workload) doBatchTransactionUpdate(ctx context.Context, batchSize int, db db.BatchDB, state *WorkloadState) error {
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keyNum := c.nextKeyNum(state)
		keyName := c.buildKeyName(keyNum)
		keys[i] = keyName
		if c.writeAllFields {
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

	return db.BatchUpdate(ctx, c.table, keys, values)
}
