package workers

import (
	"math/big"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	schedulingTypes "github.com/HPISTechnologies/scheduling-svc/service/types"
)

type Arbitrator interface {
	Start()
	Stop()
	Do([][][]*types.TxElement, *actor.WorkerThreadLogger) []*ethCommon.Hash
}

type Executor interface {
	Start()
	Stop()
	Run(msgs []*schedulingTypes.Message, timestamp *big.Int, msgTemplate *actor.Message, inlog *actor.WorkerThreadLogger) []*types.ExecuteResponse
}
