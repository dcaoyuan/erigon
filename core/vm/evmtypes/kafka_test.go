// --- added by dcaoyuan

package evmtypes

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"math/big"

	kafka "github.com/segmentio/kafka-go"
)

func TestKafka(*testing.T) {
	blockNumber, _ := new(big.Int).SetString("16714452", 10)
	blockNumberAsTime := makeTime(blockNumber.Int64())
	blockNumberAsTimestamp := timestamp(blockNumberAsTime)

	blockNumberFromBytes := new(big.Int).SetBytes(blockNumber.Bytes()) // yes, big use unsigned
	fmt.Printf("blockNumber [% x], %d, %d\n", blockNumber.Bytes(), blockNumberFromBytes, blockNumberAsTimestamp)
	// --- for java, use: new java.math.BigInteger(1, bytes)

	conn, err := kafka.DialLeader(context.Background(), "tcp", "192.168.1.102:9092", "eth-main", 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	lastOffset, err := conn.ReadLastOffset()
	if err != nil {
		log.Fatal("failed to read last offset:", err)
	}
	fmt.Printf("lastOffset %d\n", lastOffset)

	if lastOffset > 0 {
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))

		// seeking to the latest means waiting for new messages; -1 will read the latest produced message
		conn.Seek(1, kafka.SeekEnd)

		offset, whence := conn.Offset()
		fmt.Printf("offset %d, %d\n", offset, whence)

		msg, err := conn.ReadMessage(1e6) // 1MB max
		if err != nil {
			log.Fatal("failed to read:", err)
		} else {
			fmt.Printf("msg %d\n", timestamp(msg.Time))
		}
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}
}
