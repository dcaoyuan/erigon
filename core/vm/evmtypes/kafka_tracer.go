// --- added by dcaoyuan

package evmtypes

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"

	kafka "github.com/segmentio/kafka-go"
)

var once sync.Once
var kafkaWriter *kafka.Writer

var KAFKA_SERVERS = "192.168.1.102:9092"
var KAFKA_KEY = []byte("eth")
var KAFKA_TOPIC = "eth-mainnet-incoming"

type KafkaTracer interface {
	AddTx(hash common.Hash, from common.Address, to *common.Address, value *uint256.Int, input []byte, gasPrice *uint256.Int, gas uint64)
	CurrentTx() *TxTrace
	SetReceipts(receipts types.Receipts)
	AddReward(Recipient common.Address, Amount uint256.Int)
	NextCallId() uint
	CommitTraces()
}

type kafkaTracer struct {
	block      BlockTrace
	nextCallId uint
}

func NewKafkaTracer(block *types.Block) *kafkaTracer {
	once.Do(func() {
		kafkaWriter = &kafka.Writer{
			Addr:         kafka.TCP(KAFKA_SERVERS),
			Topic:        KAFKA_TOPIC,
			RequiredAcks: kafka.RequireOne,
			BatchTimeout: 100 * time.Millisecond, // Execute 30 blocks/s, 33ms/block when batch sync. 100ms means ~3 blocks.
			BatchBytes:   209_715_200,            // 200M (pre-compressed). kafka-go checks if each message is less than this.
			Compression:  kafka.Zstd,             // Zstd compressed rlp is 50% in size of gzip.
		}
	})

	return &kafkaTracer{
		block: BlockTrace{
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

func (ct *kafkaTracer) AddTx(hash common.Hash, from common.Address, to *common.Address, value *uint256.Int, input []byte, gasPrice *uint256.Int, gas uint64) {
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

	ct.block.Txs = append(ct.block.Txs, tx)
}

func (ct *kafkaTracer) CurrentTx() *TxTrace {
	n := len(ct.block.Txs) - 1

	if n >= 0 {
		return &ct.block.Txs[n]
	} else {
		return nil
	}
}

func (ct *kafkaTracer) AddReward(receipent common.Address, amount uint256.Int) {
	ct.block.Rewards = append(ct.block.Rewards, RewardTrace{Recipient: receipent, Amount: amount})
}

func (ct *kafkaTracer) SetReceipts(receipts types.Receipts) {
	ct.block.Receipts = receipts
}

func (ct *kafkaTracer) NextCallId() uint {
	ct.nextCallId++

	return ct.nextCallId
}

func (ct *kafkaTracer) CommitTraces() {
	rlpBlock, err := rlp.EncodeToBytes(ct.block)
	if err != nil {
		log.Error("Rlp", "err", err)

	} else {
		log.Info(fmt.Sprintf("CommitTraces: block %v, txs %v, rlp %v", ct.block.Block.Number(), len(ct.block.Txs), len(rlpBlock)))

		msg := kafka.Message{
			Key:   KAFKA_KEY,
			Value: rlpBlock,
		}

		err = kafkaWriter.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Error("Kafka", "err", err)
		}
	}
}

func (ct *kafkaTracer) CommitTraces_test() {
	detail := false

	log.Info(fmt.Sprintf("CommitTraces: %v, %v", ct.block.Block.Number(), len(ct.block.Txs)))
	for _, tx := range ct.block.Txs {
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
