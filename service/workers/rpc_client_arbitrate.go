package workers

import (
	"context"
	"fmt"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/common-lib/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/log"
	"github.com/HPISTechnologies/component-lib/rpc"
	"github.com/smallnest/rpcx/client"
	"go.uber.org/zap"
)

type RpcClientArbitrate struct {
	//rpcClient      *rpc.RpcClientPeer
	//arbitratorAddr string
	xclient client.XClient
}

func NewRpcClientArbitrate(zkServers []string) *RpcClientArbitrate {
	return &RpcClientArbitrate{
		//arbitratorAddr: arbitratorAddr,
		xclient: rpc.InitZookeeperRpcClient("arbitrator", zkServers),
	}
}

func (rca *RpcClientArbitrate) Start() {
	//rca.rpcClient = rpc.NewClientPeer(rca.arbitratorAddr, "arbitrator")
}

func (rca *RpcClientArbitrate) Stop() {
	//rca.rpcClient.Stop()
}

func (rca *RpcClientArbitrate) Do(arbitrateList [][][]*types.TxElement, inlog *actor.WorkerThreadLogger) []*ethCommon.Hash {
	results := make([]*ethCommon.Hash, 0, len(arbitrateList))
	for i, list := range arbitrateList {
		if len(list) == 0 {
			continue
		}
		request := actor.Message{
			Msgid: common.GenerateUUID(),
			Name:  actor.MsgArbitrateList,
			Data: &types.ArbitratorRequest{
				TxsListGroup: list,
			},
			Height: inlog.LatestMessage.Height,
			Round:  inlog.LatestMessage.Round,
		}
		response := types.ArbitratorResponse{}

		inlog.Log(log.LogLevel_Debug, "start arbitrate >>>>>>>>>>>>>>>>>>>", zap.Int("group idx", i), zap.Int("txs", len(list)))

		err := rca.xclient.Call(context.Background(), "Arbitrate", &request, &response)
		if err != nil {
			inlog.Log(log.LogLevel_Error, "arbitrate err", zap.String("err", fmt.Sprintf("%v", err.Error())))
			return nil
		} else {
			// rca.rpcClient.Call("Arbitrate", &request, &response)
			inlog.Log(log.LogLevel_Debug, "return arbitrate <<<<<<<<<<<<<<<<<<<<", zap.Int("group idx", i))
			if response.ConflictedList != nil {
				results = append(results, response.ConflictedList...)
			}
		}
	}
	return results
}
