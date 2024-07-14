package wrap

import (
	"github.com/ledgerwatch/erigon-lib/kv"
)

type TxContainer struct {
	Tx  kv.RwTx
	Ttx kv.TemporalTx

	// --- kafak
	IsHandledByForkValidator bool
	// --- end of kafka
}
