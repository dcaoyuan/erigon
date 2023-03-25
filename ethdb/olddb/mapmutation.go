package olddb

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"sync"
	"time"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/ethdb"

	"github.com/segmentio/kafka-go"
)

type mapmutation struct {
	puts   map[string]map[string][]byte // table -> key -> value ie. blocks -> hash -> blockBod
	db     kv.RwTx
	quit   <-chan struct{}
	clean  func()
	mu     sync.RWMutex
	size   int
	count  uint64
	tmpdir string

	traces []kafka.Message
}

// --- kafka
var once sync.Once
var kafkaWriter *kafka.Writer

var KAFKA_SERVERS = "192.168.1.102:9092"
var KAFKA_TOPIC = "eth-mainnet-incoming"

// --- end of kafka

// NewBatch - starts in-mem batch
//
// Common pattern:
//
// batch := db.NewBatch()
// defer batch.Rollback()
// ... some calculations on `batch`
// batch.Commit()
func NewHashBatch(tx kv.RwTx, quit <-chan struct{}, tmpdir string) *mapmutation {
	once.Do(func() {
		//checkCommittedBlockNumber()

		kafkaWriter = &kafka.Writer{
			Addr:         kafka.TCP(KAFKA_SERVERS),
			Topic:        KAFKA_TOPIC,
			RequiredAcks: kafka.RequireOne,
			BatchSize:    1,           // make sure send each one to kafka at once to complete block execution.
			BatchBytes:   209_715_200, // 200M (pre-compressed). kafka-go checks if each message is less than this.
		}
	})

	clean := func() {}
	if quit == nil {
		ch := make(chan struct{})
		clean = func() { close(ch) }
		quit = ch
	}

	return &mapmutation{
		db:     tx,
		puts:   make(map[string]map[string][]byte),
		quit:   quit,
		clean:  clean,
		tmpdir: tmpdir,

		// --- kafka
		traces: []kafka.Message{},
		// --- end of kafka
	}
}

// --- kafka
func (m *mapmutation) AddTrace(trace kafka.Message) {
	m.traces = append(m.traces, trace)
}

func timestamp(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

func checkCommittedBlockNumber() (*big.Int, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", KAFKA_SERVERS, KAFKA_TOPIC, 0)
	if err != nil {
		log.Error("failed to dial leader:", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	lastOffset, err := conn.ReadLastOffset()
	if err != nil {
		log.Error("failed to read last offset:", err)
	}

	if lastOffset > 0 {
		// seeking to the latest means waiting for new messages; -1 will read the latest produced message
		conn.Seek(1, kafka.SeekEnd)
		msg, err := conn.ReadMessage(1e6) // 1MB max
		if err != nil {
			log.Error("failed to read:", err)
		} else {
			return new(big.Int).SetInt64(timestamp(msg.Time)), nil
		}
	} else {
		return new(big.Int).SetInt64(-1), nil
	}

	//log.Info(fmt.Sprintf("committedBlock is %d", committedBlock))
	return nil, nil
}

// --- end of kafka

func (m *mapmutation) RwKV() kv.RwDB {
	if casted, ok := m.db.(ethdb.HasRwKV); ok {
		return casted.RwKV()
	}
	return nil
}

func (m *mapmutation) getMem(table string, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.puts[table]; !ok {
		return nil, false
	}
	if value, ok := m.puts[table][*(*string)(unsafe.Pointer(&key))]; ok {
		return value, ok
	}

	return nil, false
}

func (m *mapmutation) IncrementSequence(bucket string, amount uint64) (res uint64, err error) {
	v, ok := m.getMem(kv.Sequence, []byte(bucket))
	if !ok && m.db != nil {
		v, err = m.db.GetOne(kv.Sequence, []byte(bucket))
		if err != nil {
			return 0, err
		}
	}

	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, currentV+amount)
	if err = m.Put(kv.Sequence, []byte(bucket), newVBytes); err != nil {
		return 0, err
	}

	return currentV, nil
}
func (m *mapmutation) ReadSequence(bucket string) (res uint64, err error) {
	v, ok := m.getMem(kv.Sequence, []byte(bucket))
	if !ok && m.db != nil {
		v, err = m.db.GetOne(kv.Sequence, []byte(bucket))
		if err != nil {
			return 0, err
		}
	}
	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	return currentV, nil
}

// Can only be called from the worker thread
func (m *mapmutation) GetOne(table string, key []byte) ([]byte, error) {
	if value, ok := m.getMem(table, key); ok {
		return value, nil
	}
	if m.db != nil {
		// TODO: simplify when tx can no longer be parent of mutation
		value, err := m.db.GetOne(table, key)
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, nil
}

// Can only be called from the worker thread
func (m *mapmutation) Get(table string, key []byte) ([]byte, error) {
	value, err := m.GetOne(table, key)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, ethdb.ErrKeyNotFound
	}

	return value, nil
}

func (m *mapmutation) Last(table string) ([]byte, []byte, error) {
	c, err := m.db.Cursor(table)
	if err != nil {
		return nil, nil, err
	}
	defer c.Close()
	return c.Last()
}

func (m *mapmutation) Has(table string, key []byte) (bool, error) {
	if _, ok := m.getMem(table, key); ok {
		return ok, nil
	}
	if m.db != nil {
		return m.db.Has(table, key)
	}
	return false, nil
}

// puts a table key with a value and if the table is not found then it appends a table
func (m *mapmutation) Put(table string, k, v []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.puts[table]; !ok {
		m.puts[table] = make(map[string][]byte)
	}

	stringKey := string(k)

	var ok bool
	if _, ok = m.puts[table][stringKey]; !ok {
		m.size += len(v) - len(m.puts[table][stringKey])
		m.puts[table][stringKey] = v
		return nil
	}
	m.puts[table][stringKey] = v
	m.size += len(k) + len(v)
	m.count++

	return nil
}

func (m *mapmutation) Append(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *mapmutation) AppendDup(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *mapmutation) BatchSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *mapmutation) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForEach(bucket, fromPrefix, walker)
}

func (m *mapmutation) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForPrefix(bucket, prefix, walker)
}

func (m *mapmutation) ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForAmount(bucket, prefix, amount, walker)
}

func (m *mapmutation) Delete(table string, k []byte) error {
	return m.Put(table, k, nil)
}

func (m *mapmutation) doCommit(tx kv.RwTx) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	count := 0
	total := float64(m.count)
	for table, bucket := range m.puts {
		collector := etl.NewCollector("", m.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer collector.Close()
		for key, value := range bucket {
			collector.Collect([]byte(key), value)
			count++
			select {
			default:
			case <-logEvery.C:
				progress := fmt.Sprintf("%.1fM/%.1fM", float64(count)/1_000_000, total/1_000_000)
				log.Info("Write to db", "progress", progress, "current table", table)
				tx.CollectMetrics()
			}
		}
		if err := collector.Load(m.db, table, etl.IdentityLoadFunc, etl.TransformArgs{Quit: m.quit}); err != nil {
			return err
		}
	}

	tx.CollectMetrics()
	return nil
}

func (m *mapmutation) Commit() error {
	if m.db == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.doCommit(m.db); err != nil {
		return err
	}

	// --- kafka
	log.Info("Writing to kafka ...", "blocks", len(m.traces))
	for _, msg := range m.traces {
		err := kafkaWriter.WriteMessages(context.Background(), msg)

		blocknumber := timestamp(msg.Time)
		if err != nil {
			log.Error("Kafka error at block", "block", blocknumber, "err", err)
		} else {
			log.Info(fmt.Sprintf("CommitTrace: block %v, rlp %v", blocknumber, len(msg.Value)))
		}
	}
	// end of kafka

	m.puts = map[string]map[string][]byte{}
	m.size = 0
	m.count = 0
	m.clean()
	return nil
}

func (m *mapmutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = map[string]map[string][]byte{}
	m.size = 0
	m.count = 0
	m.size = 0
	m.clean()

	// --- kafka
	m.traces = []kafka.Message{}
	// --- end of kafka
}

func (m *mapmutation) Close() {
	m.Rollback()
}

func (m *mapmutation) Begin(ctx context.Context, flags ethdb.TxFlags) (ethdb.DbWithPendingMutations, error) {
	panic("mutation can't start transaction, because doesn't own it")
}

func (m *mapmutation) panicOnEmptyDB() {
	if m.db == nil {
		panic("Not implemented")
	}
}

func (m *mapmutation) SetRwKV(kv kv.RwDB) {
	hasRwKV, ok := m.db.(ethdb.HasRwKV)
	if !ok {
		log.Warn("Failed to convert mapmutation type to HasRwKV interface")
	}
	hasRwKV.SetRwKV(kv)
}
