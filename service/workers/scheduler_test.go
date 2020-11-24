package workers

import (
	"testing"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	ethTypes "github.com/HPISTechnologies/3rd-party/eth/types"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
)

type schedulerTestCase struct {
	msgBatches       [][]*actor.Message
	execResponses    [][]*types.ExecuteResponse
	arbitrateResults [][]*ethCommon.Hash

	executed []ethCommon.Hash
	spawned  []ethCommon.Hash
	included []ethCommon.Hash
}

func runTestCase(t *testing.T, tc *schedulerTestCase) {
	/*
		//scheduler := NewScheduler(1, "scheduler", NewMockArbitrator(t, tc.arbitrateResults), NewMockExecutor(t, tc.execResponses))
		//streamer := NewMockStreamer(t)
		//scheduler.Init("scheduler", streamer)
		scheduler.OnStart()
		for _, batch := range tc.msgBatches {
			scheduler.OnMessageArrived(batch)
		}

		got := streamer.GetReceivedData()
		if !compareHashList(*(got[actor.MsgFinalTxsListExecuted].(*actor.Message).Data.(*[]*ethCommon.Hash)), tc.executed) {
			t.Error("Compare executed txs failed")
		}
		if !compareHashList(*(got[actor.MsgFinalTxsListSpawned].(*actor.Message).Data.(*[]*ethCommon.Hash)), tc.spawned) {
			t.Error("Compare spawned txs failed")
		}
		applyList := *(got[actor.MsgInclusive].(*actor.Message).Data.(*types.ApplyList).Lists)
		included := make([]*ethCommon.Hash, 0, len(applyList))
		for _, item := range applyList {
			included = append(included, &item.Txhash)
		}
		if !compareHashList(included, tc.included) {
			t.Error("Compare included txs failed")
		}

		scheduler.Stop()
	*/
}

func TestScheduler(t *testing.T) {
	testCases := []*schedulerTestCase{
		&schedulerTestCase{
			msgBatches: [][]*actor.Message{
				[]*actor.Message{
					&actor.Message{
						Name: actor.MsgMessagersReaped,
						Data: []*types.StandardMessage{
							&types.StandardMessage{
								TxHash: ethCommon.BytesToHash([]byte{0xff}),
								Native: &ethTypes.Message{},
								Source: 0xcc,
							},
						},
					},
				},
			},
			execResponses: [][]*types.ExecuteResponse{
				[]*types.ExecuteResponse{
					&types.ExecuteResponse{
						Hash:   ethCommon.BytesToHash([]byte{0xff}),
						Status: 1,
					},
				},
			},
			arbitrateResults: [][]*ethCommon.Hash{
				nil,
			},
			executed: []ethCommon.Hash{
				ethCommon.BytesToHash([]byte{0xff}),
			},
			spawned: nil,
			included: []ethCommon.Hash{
				ethCommon.BytesToHash([]byte{0xff}),
			},
		},
	}

	for _, tc := range testCases {
		runTestCase(t, tc)
	}
}

func compareHashList(got []*ethCommon.Hash, expected []ethCommon.Hash) bool {
	if len(got) != len(expected) {
		return false
	}

	dict := make(map[ethCommon.Hash]struct{})
	for _, hash := range got {
		dict[*hash] = struct{}{}
	}

	for _, hash := range expected {
		if _, ok := dict[hash]; !ok {
			return false
		}
	}
	return true
}
