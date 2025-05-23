// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package shards

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/types"
)

// Accumulator collects state changes in a form that can then be delivered to the RPC daemon
type Accumulator struct {
	plainStateID       uint64
	changes            []*remote.StateChange
	latestChange       *remote.StateChange
	accountChangeIndex map[common.Address]int // For the latest changes, allows finding account change by account's address
	storageChangeIndex map[common.Address]map[common.Hash]int
}

func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

type StateChangeConsumer interface {
	SendStateChanges(ctx context.Context, sc *remote.StateChangeBatch)
}

func (a *Accumulator) Reset(plainStateID uint64) {
	a.changes = nil
	a.latestChange = nil
	a.accountChangeIndex = nil
	a.storageChangeIndex = nil
	a.plainStateID = plainStateID
}

func (a *Accumulator) SendAndReset(ctx context.Context, c StateChangeConsumer, pendingBaseFee uint64, pendingBlobFee uint64, blockGasLimit uint64, finalizedBlock uint64) {
	if a == nil || c == nil || len(a.changes) == 0 {
		return
	}
	sc := &remote.StateChangeBatch{StateVersionId: a.plainStateID, ChangeBatch: a.changes, PendingBlockBaseFee: pendingBaseFee, BlockGasLimit: blockGasLimit, FinalizedBlock: finalizedBlock, PendingBlobFeePerGas: pendingBlobFee}
	c.SendStateChanges(ctx, sc)
	a.Reset(0) // reset here for GC, but there will be another Reset with correct viewID
}

func (a *Accumulator) SetStateID(stateID uint64) {
	a.plainStateID = stateID
}

// StartChange begins accumulation of changes for a new block
func (a *Accumulator) StartChange(h *types.Header, txs [][]byte, unwind bool) {
	a.changes = append(a.changes, &remote.StateChange{})
	a.latestChange = a.changes[len(a.changes)-1]
	a.latestChange.BlockHeight = h.Number.Uint64()
	a.latestChange.BlockHash = gointerfaces.ConvertHashToH256(h.Hash())
	a.latestChange.BlockTime = h.Time
	if unwind {
		a.latestChange.Direction = remote.Direction_UNWIND
	} else {
		a.latestChange.Direction = remote.Direction_FORWARD
	}
	a.accountChangeIndex = make(map[common.Address]int)
	a.storageChangeIndex = make(map[common.Address]map[common.Hash]int)
	if txs != nil {
		a.latestChange.Txs = make([][]byte, len(txs))
		for i := range txs {
			a.latestChange.Txs[i] = common.Copy(txs[i])
		}
	}
}

// ChangeAccount adds modification of account balance or nonce (or both) to the latest change
func (a *Accumulator) ChangeAccount(address common.Address, incarnation uint64, data []byte) {
	i, ok := a.accountChangeIndex[address]
	if !ok || incarnation > a.latestChange.Changes[i].Incarnation {
		// Account has not been changed in the latest block yet
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, &remote.AccountChange{Address: gointerfaces.ConvertAddressToH160(address)})
		a.accountChangeIndex[address] = i
		delete(a.storageChangeIndex, address)
	}
	accountChange := a.latestChange.Changes[i]
	switch accountChange.Action {
	case remote.Action_STORAGE:
		accountChange.Action = remote.Action_UPSERT
	case remote.Action_CODE:
		accountChange.Action = remote.Action_UPSERT_CODE
	case remote.Action_REMOVE:
		//panic("")
	}
	accountChange.Incarnation = incarnation
	accountChange.Data = data
}

// DeleteAccount marks account as deleted
func (a *Accumulator) DeleteAccount(address common.Address) {
	i, ok := a.accountChangeIndex[address]
	if !ok {
		// Account has not been changed in the latest block yet
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, &remote.AccountChange{Address: gointerfaces.ConvertAddressToH160(address)})
		a.accountChangeIndex[address] = i
	}
	accountChange := a.latestChange.Changes[i]
	if accountChange.Action != remote.Action_STORAGE {
		panic("")
	}
	accountChange.Data = nil
	accountChange.Code = nil
	accountChange.StorageChanges = nil
	accountChange.Action = remote.Action_REMOVE
	delete(a.storageChangeIndex, address)
}

// ChangeCode adds code to the latest change
func (a *Accumulator) ChangeCode(address common.Address, incarnation uint64, code []byte) {
	i, ok := a.accountChangeIndex[address]
	if !ok || incarnation > a.latestChange.Changes[i].Incarnation {
		// Account has not been changed in the latest block yet
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, &remote.AccountChange{Address: gointerfaces.ConvertAddressToH160(address), Action: remote.Action_CODE})
		a.accountChangeIndex[address] = i
		delete(a.storageChangeIndex, address)
	}
	accountChange := a.latestChange.Changes[i]
	switch accountChange.Action {
	case remote.Action_STORAGE:
		accountChange.Action = remote.Action_CODE
	case remote.Action_UPSERT:
		accountChange.Action = remote.Action_UPSERT_CODE
	case remote.Action_REMOVE:
		//panic("")
	}
	accountChange.Incarnation = incarnation
	accountChange.Code = code
}

func (a *Accumulator) ChangeStorage(address common.Address, incarnation uint64, location common.Hash, data []byte) {
	i, ok := a.accountChangeIndex[address]
	if !ok || incarnation > a.latestChange.Changes[i].Incarnation {
		// Account has not been changed in the latest block yet
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, &remote.AccountChange{Address: gointerfaces.ConvertAddressToH160(address), Action: remote.Action_STORAGE})
		a.accountChangeIndex[address] = i
		delete(a.storageChangeIndex, address)
	}
	accountChange := a.latestChange.Changes[i]
	//if accountChange.Action == remote.Action_REMOVE {
	//	panic("")
	//}
	accountChange.Incarnation = incarnation
	si, ok1 := a.storageChangeIndex[address]
	if !ok1 {
		si = make(map[common.Hash]int)
		a.storageChangeIndex[address] = si
	}
	j, ok2 := si[location]
	if !ok2 {
		j = len(accountChange.StorageChanges)
		accountChange.StorageChanges = append(accountChange.StorageChanges, &remote.StorageChange{})
		si[location] = j
	}
	storageChange := accountChange.StorageChanges[j]
	storageChange.Location = gointerfaces.ConvertHashToH256(location)
	storageChange.Data = data
}

func (a *Accumulator) CopyAndReset(target *Accumulator) {
	target.changes = a.changes
	a.changes = nil
	target.latestChange = a.latestChange
	a.latestChange = nil
	target.accountChangeIndex = a.accountChangeIndex
	a.accountChangeIndex = nil
	target.storageChangeIndex = a.storageChangeIndex
	a.storageChangeIndex = nil
}
