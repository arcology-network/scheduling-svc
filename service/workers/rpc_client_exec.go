package workers

import (
	"math/big"
	"sync"
	"time"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/common-lib/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/log"
	"github.com/HPISTechnologies/component-lib/rpc"
	schedulingTypes "github.com/HPISTechnologies/scheduling-svc/service/types"
	"go.uber.org/zap"
)

type RpcClientExec struct {
	rpcClients []*rpc.RpcClientPeer
	batchNums  int
	execAddrs  []string
}

//return a Subscriber struct
func NewRpcClientExec(concurrency int, groupid string, execAddrs []string, batchNums int) *RpcClientExec {
	return &RpcClientExec{
		batchNums: batchNums,
		execAddrs: execAddrs,
	}
}

func (rce *RpcClientExec) Start() {
	rce.rpcClients = make([]*rpc.RpcClientPeer, len(rce.execAddrs))
	for i, addr := range rce.execAddrs {
		rce.rpcClients[i] = rpc.NewClientPeer(addr, "executor")
	}
}

func (rce *RpcClientExec) Stop() {
	for _, client := range rce.rpcClients {
		client.Stop()
	}
}

func (rce *RpcClientExec) Run(msgs []*schedulingTypes.Message, timestamp *big.Int, msgTemplate *actor.Message, inlog *actor.WorkerThreadLogger) []*types.ExecuteResponse {
	startTime := time.Now()
	params := map[ethCommon.Hash]*[]*schedulingTypes.Message{}
	for _, msg := range msgs {
		if list, ok := params[msg.PrecedingHash]; ok {
			(*list) = append((*list), msg)
		} else {
			params[msg.PrecedingHash] = &[]*schedulingTypes.Message{msg}
		}
	}
	execParams := []types.ExecutorRequest{}
	for h, list := range params {
		precedings := (*list)[0].Precedings
		msgs := []*types.StandardMessage{}
		for _, msg := range *list {
			if len(msgs) >= rce.batchNums {
				execParam := types.ExecutorRequest{
					Msgs:          msgs,
					Precedings:    *precedings,
					PrecedingHash: h,
				}
				execParams = append(execParams, execParam)
				msgs = []*types.StandardMessage{}
			}

			msgs = append(msgs, msg.Message)
		}
		if len(msgs) > 0 {
			execParam := types.ExecutorRequest{
				Msgs:          msgs,
				Precedings:    *precedings,
				PrecedingHash: h,
				Timestamp:     timestamp,
			}
			execParams = append(execParams, execParam)
		}
	}
	inlog.Log(log.LogLevel_Debug, "exec preparation complete,start exec transactions", zap.Int("nums", len(msgs)), zap.Duration("time", time.Now().Sub(startTime)))
	return rce.execTxs(&execParams, msgTemplate, inlog)
}

func (rce *RpcClientExec) execTxs(params *[]types.ExecutorRequest, msgTemplate *actor.Message, inlog *actor.WorkerThreadLogger) []*types.ExecuteResponse {

	chExes := make(chan int, len(rce.rpcClients))
	wg := sync.WaitGroup{}
	for tokenIdx := range rce.rpcClients {
		chExes <- tokenIdx
	}
	totalLen := 0
	execResults := make([][]*types.ExecuteResponse, len(*params))
	for dataIdx, param := range *params {
		totalLen = totalLen + len(param.Msgs)
		execParam := param
		execIdx := <-chExes
		wg.Add(1)
		go func(execIdx, dataIdx int, request actor.Message) {
			response := types.ExecutorResponses{}
			request.Name = actor.MsgTxsToExecute
			request.Msgid = common.GenerateUUID()
			request.Data = &execParam

			inlog.Log(log.LogLevel_Debug, ">>>>>>>>>>>>>>>>>>>>>", zap.Int("execIdx", execIdx), zap.Int("idx", dataIdx), zap.Int("txs", len(execParam.Msgs)))
			rce.rpcClients[execIdx].Call("ExecTxs", &request, &response)
			if response.DfCalls != nil {
				defs := 0
				for i := range response.DfCalls {
					txResult := &types.ExecuteResponse{
						DfCall:  response.DfCalls[i],
						Hash:    response.HashList[i],
						Status:  response.StatusList[i],
						GasUsed: response.GasUsedList[i],
					}
					if txResult.DfCall != nil {
						defs = defs + 1
					}
					execResults[dataIdx] = append(execResults[dataIdx], txResult)
				}
				inlog.Log(log.LogLevel_Debug, "<<<<<<<<<<<<<<<<<<<<< ", zap.Int("idx", dataIdx), zap.Int("defs", defs))
			}
			chExes <- execIdx
			wg.Done()
		}(execIdx, dataIdx, *msgTemplate)
	}
	wg.Wait()
	inlog.Log(log.LogLevel_Debug, ".......................................................... exec completed")
	txResults := make([]*types.ExecuteResponse, 0, totalLen)
	for _, list := range execResults {
		txResults = append(txResults, list...)
	}
	return txResults
}
