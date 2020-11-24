package workers

import (
	"fmt"
	"math/big"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/log"
	schedulingTypes "github.com/HPISTechnologies/scheduling-svc/service/types"
	"go.uber.org/zap"
)

type Reaper struct {
	actor.WorkerThread
	maxReap int
}

//return a Subscriber struct
func NewReaper(concurrency int, groupid string, maxReap int) *Reaper {
	reaper := Reaper{
		maxReap: maxReap,
	}
	reaper.Set(concurrency, groupid)
	return &reaper
}

func (r *Reaper) OnStart() {

}

func (r *Reaper) OnMessageArrived(msgs []*actor.Message) error {
	reaperCommand := schedulingTypes.ReaperCommand{
		MaxReap: r.maxReap,
	}

	for _, v := range msgs {
		switch v.Name {
		case actor.MsgNodeRoleCore:
			reaperCommand.NodeRole = v.Data.(*types.NodeRole)
		case actor.MsgReapinglist:
			reaperCommand.Reapinglist = v.Data.(*types.ReapingList)
			if reaperCommand.Reapinglist.List == nil {
				reaperCommand.Reapinglist.List = []*ethCommon.Hash{}
			}
		}
	}
	timestamp := big.NewInt(0)
	if actor.MsgBlockRole_Validate == reaperCommand.NodeRole.Role {
		timestamp = reaperCommand.Reapinglist.Timestamp
		r.AddLog(log.LogLevel_Info, "received ReaperCommand", zap.String("NodeRole", fmt.Sprintf("%v", reaperCommand.NodeRole)), zap.Int("reapinglist", len(reaperCommand.Reapinglist.List)))
	} else {
		timestamp = reaperCommand.NodeRole.Timestamp
		r.AddLog(log.LogLevel_Info, "received ReaperCommand", zap.String("NodeRole", fmt.Sprintf("%v", reaperCommand.NodeRole)), zap.Int("MaxReap", reaperCommand.MaxReap))
	}
	r.MsgBroker.Send(actor.MsgBlockStamp, timestamp)
	r.MsgBroker.Send(actor.MsgReaperCommand, &reaperCommand)
	return nil
}
