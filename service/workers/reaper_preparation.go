package workers

import (
	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
)

type ReaperPreparation struct {
	actor.WorkerThread
}

//return a Subscriber struct
func NewReaperPreparation(concurrency int, groupid string) *ReaperPreparation {
	reaper := ReaperPreparation{}
	reaper.Set(concurrency, groupid)
	return &reaper
}

func (r *ReaperPreparation) OnStart() {

}

func (r *ReaperPreparation) Stop() {

}

func (r *ReaperPreparation) OnMessageArrived(msgs []*actor.Message) error {
	for _, v := range msgs {
		switch v.Name {
		case actor.MsgNodeRoleCore:
			nodeRole := v.Data.(*types.NodeRole)
			if actor.MsgBlockRole_Propose == nodeRole.Role {
				nilReapinglist := types.ReapingList{
					List: []*ethCommon.Hash{},
				}
				r.MsgBroker.Send(actor.MsgReapinglist, &nilReapinglist)
			}
		}
	}

	return nil
}
