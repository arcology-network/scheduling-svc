package workers

import (
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"go.uber.org/zap"

	"github.com/HPISTechnologies/component-lib/aggregator/aggregator"

	"github.com/HPISTechnologies/component-lib/log"
	schedulingTypes "github.com/HPISTechnologies/scheduling-svc/service/types"
)

type AggreSelector struct {
	actor.WorkerThread
	checklist  *schedulingTypes.CheckList
	aggregator *aggregator.Aggregator
}

//return a Subscriber struct
func NewAggreSelector(concurrency int, groupid string, saveRange int) *AggreSelector {
	agg := AggreSelector{}
	agg.Set(concurrency, groupid)
	agg.checklist = schedulingTypes.NewCheckList(saveRange)
	agg.aggregator = aggregator.NewAggregator()

	return &agg
}

func (a *AggreSelector) OnStart() {
}

func (a *AggreSelector) OnMessageArrived(msgs []*actor.Message) error {

	switch msgs[0].Name {
	case actor.MsgBlockCompleted:
		result := msgs[0].Data.(string)
		if result == actor.MsgBlockCompleted_Success {
			remainingQuantity := a.aggregator.OnClearInfoReceived()
			a.AddLog(log.LogLevel_Debug, "scheduling AggreSelector clear pool", zap.Int("remainingQuantity", remainingQuantity))
		}
	case actor.MsgReaperCommand:
		reaperCommand := msgs[0].Data.(*schedulingTypes.ReaperCommand)

		if reaperCommand.NodeRole.Role == actor.MsgBlockRole_Validate {
			reapingNums := 0
			if reaperCommand.Reapinglist != nil && reaperCommand.Reapinglist.List != nil {
				reapingNums = len(reaperCommand.Reapinglist.List)
			}
			a.AddLog(log.LogLevel_Debug, "scheduling AggreSelector reapingList ", zap.Int("nums", reapingNums))
			result, _ := a.aggregator.OnListReceived(reaperCommand.Reapinglist)
			a.SendMsg(result)
		} else {
			a.AddLog(log.LogLevel_Debug, "scheduling AggreSelector reap Max ", zap.Int("nums", reaperCommand.MaxReap))
			_, result := a.aggregator.Reap(reaperCommand.MaxReap)
			a.SendMsg(result)

		}

	case actor.MsgInclusive:
		inclusive := msgs[0].Data.(*types.InclusiveList)
		inclusive.Mode = types.InclusiveMode_Message
		a.aggregator.OnClearListReceived(inclusive)

		removelist := make([]string, len(inclusive.HashList))
		for i, hash := range inclusive.HashList {
			removelist[i] = string(hash.Bytes())
		}
		a.checklist.OnReceivedList(removelist)

	case actor.MsgMessager:
		messages := msgs[0].Data.([]*types.StandardMessage)
		for i := range messages {
			if a.checklist.Exist(string(messages[i].TxHash.Bytes())) {
				continue
			}
			result := a.aggregator.OnDataReceived(messages[i].TxHash, messages[i])
			a.SendMsg(result)
		}

	}
	return nil
}
func (a *AggreSelector) SendMsg(selectedData *[]*interface{}) {

	if selectedData != nil {

		messagers := make([]*types.StandardMessage, len(*selectedData))
		for i, msg := range *selectedData {
			messagers[i] = (*msg).(*types.StandardMessage)
		}
		a.MsgBroker.Send(actor.MsgMessagersReaped, messagers)

		a.AddLog(log.LogLevel_Debug, "scheduling reapTxs end*****************", zap.Int("txs", len(messagers)))

	}
}
