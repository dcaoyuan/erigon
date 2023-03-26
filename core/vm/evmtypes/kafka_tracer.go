// --- added by dcaoyuan

package evmtypes

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"

	"github.com/segmentio/kafka-go"
)

var once sync.Once
var instance *KafkaTraces

func GetKafkaTraces() *KafkaTraces {
	once.Do(func() {
		instance = &KafkaTraces{
			kafkaWriter: &kafka.Writer{
				Addr:         kafka.TCP(KAFKA_SERVERS),
				Topic:        KAFKA_TOPIC,
				RequiredAcks: kafka.RequireOne,
				BatchSize:    1,           // make sure send each one to kafka at once to complete block execution.
				BatchBytes:   209_715_200, // 200M (pre-compressed). kafka-go checks if each message is less than this.
			},

			traces: make(map[int64]kafka.Message),
		}

		//checkCommittedBlockNumber()
	})

	return instance
}

var KAFKA_SERVERS = "192.168.1.102:9092"
var KAFKA_TOPIC = "eth-mainnet-incoming"

type BlockNums []int64

func (a BlockNums) Len() int           { return len(a) }
func (a BlockNums) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BlockNums) Less(i, j int) bool { return a[i] < a[j] }

type KafkaTraces struct {
	kafkaWriter *kafka.Writer
	traces      map[int64]kafka.Message
}

func (m *KafkaTraces) AddTrace(trace kafka.Message) {
	blockNum := timestamp(trace.Time)
	m.traces[blockNum] = trace
}

func (m *KafkaTraces) ResetTraces() {
	m.traces = make(map[int64]kafka.Message)
}

func (m *KafkaTraces) CommitTraces() {
	log.Info("Writing to kafka ...", "blocks", len(m.traces))

	// sort by block number
	blockNums := make(BlockNums, 0, len(m.traces))
	for k := range m.traces {
		blockNums = append(blockNums, k)
	}
	sort.Sort(blockNums)

	for _, k := range blockNums {
		msg := m.traces[k]
		err := m.kafkaWriter.WriteMessages(context.Background(), msg)

		blocknumber := timestamp(msg.Time)
		if err != nil {
			log.Error("Kafka error at block", "block", blocknumber, "err", err)
		} else {
			log.Info(fmt.Sprintf("CommitTrace: block %v, rlp %v", blocknumber, len(msg.Value)))
		}
	}

	m.ResetTraces()
}

func (m *KafkaTraces) RollbackTraces() {
	m.traces = make(map[int64]kafka.Message)
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

type KafkaTracer struct {
	blockTrace BlockTrace
	nextCallId uint
}

func NewKafkaTracer(block *types.Block) *KafkaTracer {
	return &KafkaTracer{
		blockTrace: BlockTrace{
			Block:   block,
			Txs:     []TxTrace{},
			Rewards: []RewardTrace{},
		},
		nextCallId: 0,
	}
}

type OpId uint

const (
	KECCAK256_OP    OpId = 0x20
	SSTORE_OP       OpId = 0x55
	CREATE_OP       OpId = 0xF0
	CALL_OP         OpId = 0xF1
	CALLCODE_OP     OpId = 0xF2
	DELEGATECALL_OP OpId = 0xF4
	CREATE2_OP      OpId = 0xF5
	STATICCALL_OP   OpId = 0xFA
)

type BlockTrace struct {
	Block    *types.Block
	Txs      []TxTrace
	Rewards  []RewardTrace
	Receipts types.Receipts
}

type TxTrace struct {
	Hash       common.Hash
	From       common.Address
	To         *common.Address
	Value      *uint256.Int
	Input      []byte
	GasPrice   *uint256.Int
	Gas        uint64
	IsCreation bool // contract creation

	Ops []OpTrace

	GasFee  *GasFeeTrace
	GasUsed uint64
	Output  []byte
	Err     string
	Status  uint8 // 0 - failed, 1 - success

	// Store pointer to make sure it points to the same one in Ops
	callstack []*CallTrace // not pubic so won't be rlpencoded
}

type GasFeeTrace struct {
	Payer       common.Address
	PayerAmount uint256.Int
	Payee       common.Address
	PayeeAmount uint256.Int
	Burnt       common.Address
	BurntAmount uint256.Int
}

type RewardTrace struct {
	Recipient common.Address
	Amount    uint256.Int
}

type OpTrace interface {
	GetOpId() OpId
	GetCallId() uint
	SetCallId(id uint)
}

type BaseOp struct {
	OpId   OpId
	CallId uint // in which call
}

func (op *BaseOp) GetOpId() OpId     { return op.OpId }
func (op *BaseOp) GetCallId() uint   { return op.CallId }
func (op *BaseOp) SetCallId(id uint) { op.CallId = id }

type CallTrace struct {
	BaseOp
	Id       uint // id of myself
	Caller   common.Address
	Callee   common.Address
	Depth    uint
	Input    []byte
	Output   []byte
	Transfer *TransferTrace
}

func (ct *CallTrace) SetTransfer(sender common.Address, recipient common.Address, value uint256.Int) {
	ct.Transfer.Sender = sender
	ct.Transfer.Recipient = recipient
	ct.Transfer.Value = value
}

func (ct *CallTrace) SetOutput(output []byte) {
	outCp := make([]byte, len(output))
	copy(outCp, output)

	ct.Output = outCp
}

type TransferTrace struct {
	Sender    common.Address
	Recipient common.Address
	Value     uint256.Int
}

type Keccak256Trace struct {
	BaseOp
	In  []byte
	Out []byte
}

func NewKeccak256Trace(in []byte, out []byte) *Keccak256Trace {
	inCp := make([]byte, len(in))
	copy(inCp, in)
	outCp := make([]byte, len(out))
	copy(outCp, out)

	return &Keccak256Trace{
		BaseOp{OpId: KECCAK256_OP, CallId: 0},
		inCp,
		outCp,
	}
}

type SStoreTrace struct {
	BaseOp
	Offset   [32]byte
	OldValue [32]byte
	NewValue [32]byte
}

func NewSStoreTrace(offset uint256.Int, oldValue uint256.Int, newValue uint256.Int) *SStoreTrace {
	return &SStoreTrace{
		BaseOp{OpId: SSTORE_OP, CallId: 0},
		offset.Bytes32(),
		oldValue.Bytes32(),
		newValue.Bytes32(),
	}
}

func (tx *TxTrace) AddOp(op OpTrace) {
	if tx.CurrentCall() != nil {
		op.SetCallId(tx.CurrentCall().Id)
	}
	tx.Ops = append(tx.Ops, op)
}

func (tx *TxTrace) PushCall(id uint, opId OpId, caller common.Address, callee common.Address, depth uint, input []byte) {
	inCp := make([]byte, len(input))
	copy(inCp, input)

	call := CallTrace{
		BaseOp:   BaseOp{OpId: opId, CallId: 0},
		Id:       id,
		Caller:   caller,
		Callee:   callee,
		Depth:    depth,
		Input:    inCp,
		Transfer: &TransferTrace{},
	}

	tx.AddOp(&call)
	tx.callstack = append(tx.callstack, &call)
}

func (tx *TxTrace) PopCall() *CallTrace {
	n := len(tx.callstack) - 1
	top := tx.callstack[n]
	tx.callstack = tx.callstack[:n]

	return top
}

func (tx *TxTrace) CurrentCall() *CallTrace {
	n := len(tx.callstack) - 1
	if n >= 0 {
		return tx.callstack[n]
	} else {
		return nil
	}
}

// --- implementation of KafkaTracer interface

func (kt *KafkaTracer) AddTx(hash common.Hash, from common.Address, to *common.Address, value *uint256.Int, input []byte, gasPrice *uint256.Int, gas uint64) {
	inCp := make([]byte, len(input))
	copy(inCp, input)

	tx := TxTrace{
		Hash:       hash,
		From:       from,
		To:         to,
		Value:      value,
		Input:      inCp,
		GasPrice:   gasPrice,
		Gas:        gas,
		IsCreation: to == nil,
		Ops:        []OpTrace{},
		GasFee:     &GasFeeTrace{},
		callstack:  []*CallTrace{},
	}

	kt.blockTrace.Txs = append(kt.blockTrace.Txs, tx)
}

func (kt *KafkaTracer) CurrentTx() *TxTrace {
	n := len(kt.blockTrace.Txs) - 1

	if n >= 0 {
		return &kt.blockTrace.Txs[n]
	} else {
		return nil
	}
}

func (kt *KafkaTracer) AddReward(receipent common.Address, amount uint256.Int) {
	kt.blockTrace.Rewards = append(kt.blockTrace.Rewards, RewardTrace{Recipient: receipent, Amount: amount})
}

func (kt *KafkaTracer) SetReceipts(receipts types.Receipts) {
	kt.blockTrace.Receipts = receipts
}

func (kt *KafkaTracer) BlockTrace() BlockTrace {
	return kt.blockTrace
}

func (kt *KafkaTracer) NextCallId() uint {
	kt.nextCallId++

	return kt.nextCallId
}

func (kt *KafkaTracer) EncodeTrace() (*kafka.Message, error) {
	blockNumber := kt.blockTrace.Block.Number()

	rlpBlock, err := rlp.EncodeToBytes(kt.blockTrace)
	if err != nil {
		return nil, err
	} else {
		return &kafka.Message{
			Time:  makeTime(blockNumber.Int64()),
			Value: rlpBlock,
		}, nil
	}
}

func (kt *KafkaTracer) CommitTraces_test() {
	detail := false

	log.Info(fmt.Sprintf("CommitTraces: %v, %v", kt.blockTrace.Block.Number(), len(kt.blockTrace.Txs)))
	for _, tx := range kt.blockTrace.Txs {
		log.Info(fmt.Sprintf("tx: %v, gas: %v", tx.Hash, tx.GasFee))
		if detail {
			for _, op := range tx.Ops {
				switch op.GetOpId() {
				case CALL_OP, CALLCODE_OP, DELEGATECALL_OP, STATICCALL_OP, CREATE_OP, CREATE2_OP:
					log.Info(fmt.Sprintf("CALL:  %v, callId: %v, id: %v, depth: %v", op.GetOpId(), op.GetCallId(), op.(*CallTrace).Id, op.(*CallTrace).Depth))
				case KECCAK256_OP:
					log.Info(fmt.Sprintf("KECC: %v, callId: %v, in: %v, out: %v", op.GetOpId(), op.GetCallId(), op.(*Keccak256Trace).In, op.(*Keccak256Trace).Out))
				case SSTORE_OP:
					log.Info(fmt.Sprintf("SSTO: %v, callId: %v, old: %v, new: %v", op.GetOpId(), op.GetCallId(), op.(*SStoreTrace).OldValue, op.(*SStoreTrace).NewValue))
				default:
					log.Info(fmt.Sprintf("op: %v, callId: %v", op.GetOpId(), op.GetCallId()))
				}
			}
		}
	}
}

// see kafka.makeTime
func makeTime(t int64) time.Time {
	if t <= 0 {
		return time.Time{}
	}
	return time.Unix(t/1000, (t%1000)*int64(time.Millisecond)).UTC()
}

func timestamp(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}
