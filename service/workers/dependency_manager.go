package workers

import (
	"math/big"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/3rd-party/eth/crypto"
	ethTypes "github.com/HPISTechnologies/3rd-party/eth/types"
	"github.com/HPISTechnologies/common-lib/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/log"
	"github.com/HPISTechnologies/component-lib/mhasher"
	deepgraph "github.com/HPISTechnologies/scheduling-svc/deepgraph"
	schedulingTypes "github.com/HPISTechnologies/scheduling-svc/service/types"
	"go.uber.org/zap"
)

type DependencyManager struct {
	//deepGraphMain *deepgraph.Deepgraph
	workGraph   *deepgraph.Deepgraph
	batchid     int64
	MainHashs   []*ethCommon.Hash
	removedTxs  *dataPool
	spawnedTxs  *dataPool
	executedTxs *dataPool
	arbitrator  Arbitrator
	innerlog    *actor.WorkerThreadLogger
	gasCache    *schedulingTypes.GasCache
	concurrency int
}

//return a Subscriber struct
func NewDependencyManager(arbitrator Arbitrator, concurrency int) *DependencyManager {
	return &DependencyManager{

		MainHashs:   []*ethCommon.Hash{},
		removedTxs:  newDataPool(),
		spawnedTxs:  newDataPool(),
		executedTxs: newDataPool(),
		arbitrator:  arbitrator,
		gasCache:    schedulingTypes.NewGasCache(),
		concurrency: concurrency,
	}
}

func (dm *DependencyManager) Start() {

	dm.workGraph = deepgraph.NewDeepgraph()
	dm.batchid = -1
	dm.arbitrator.Start()
}

func (dm *DependencyManager) Stop() {
	dm.arbitrator.Stop()
}

func getPrecedingHash(hashes []*ethCommon.Hash) ethCommon.Hash {
	if len(hashes) == 0 {
		return ethCommon.Hash{}
	}

	bys2D := [][]byte{}
	for _, h := range hashes {
		bys2D = append(bys2D, h.Bytes())
	}
	return mhasher.GetTxsHash(bys2D)
}
func convertKeyTypes(hashs []*ethCommon.Hash) []deepgraph.KeyType {
	keyTypes := make([]deepgraph.KeyType, len(hashs))
	for i := range hashs {
		keyTypes[i] = deepgraph.KeyType(*hashs[i])
	}
	return keyTypes
}
func convertHash(two []deepgraph.KeyType) []*ethCommon.Hash {
	twoHashs := make([]*ethCommon.Hash, len(two))
	for i, keytyps := range two {
		hash := ethCommon.Hash(keytyps)
		twoHashs[i] = &hash
	}
	return twoHashs
}

func mergeHashList(one, two []*ethCommon.Hash) []*ethCommon.Hash {

	if len(one) == 0 {
		return two
	}
	if len(two) == 0 {
		return one
	}
	returnHashes := append(one, two...)
	return returnHashes
}

func (dm *DependencyManager) insertIntoGraph(msgs []*schedulingTypes.Message) error {
	//insert new batch into work tree

	hashs := make([]deepgraph.KeyType, len(msgs))
	txTypes := make([]deepgraph.VertexType, len(msgs))
	precedings := make([][]deepgraph.KeyType, len(msgs))
	dm.innerlog.Log(log.LogLevel_Debug, "prepare insertIntoGraph msgs into tree >>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

	for i := range msgs {

		directPrecedings := []deepgraph.KeyType{}
		if msgs[i].DirectPrecedings != nil {
			directPrecedings = make([]deepgraph.KeyType, len(*msgs[i].DirectPrecedings))
			for j, hash := range *msgs[i].DirectPrecedings {
				directPrecedings[j] = deepgraph.KeyType(*hash)
			}
		}
		precedings[i] = directPrecedings

		if msgs[i].IsSpawned {
			txTypes[i] = deepgraph.Spawned
		} else {
			txTypes[i] = deepgraph.Original
		}
		hashs[i] = deepgraph.KeyType(msgs[i].Message.TxHash)
	}

	dm.innerlog.Log(log.LogLevel_Debug, "start insertIntoGraph msgs into tree >>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	dm.workGraph.Insert(hashs, dm.workGraph.Ids(hashs), txTypes, precedings)
	dm.workGraph.IncreaseBatch()
	dm.innerlog.Log(log.LogLevel_Debug, "insertIntoGraph msgs into tree ", zap.Int("node length", len(hashs)))
	return nil
}

func (dm *DependencyManager) OnNewBatch(msgs []*schedulingTypes.Message, broker *actor.MessageWrapper, inlog *actor.WorkerThreadLogger) bool {
	dm.innerlog = inlog
	if len(msgs) == 0 {
		accumulatedSpawnedTxs := dm.spawnedTxs.getData()
		broker.Send(actor.MsgFinalTxsListSpawned, &accumulatedSpawnedTxs)

		accumulatedExecutedTxs := dm.executedTxs.getData()

		accumulatedRemovedTxs := dm.removedTxs.getData()
		inclusiveList := getInclusiveList(accumulatedExecutedTxs, accumulatedRemovedTxs)
		broker.Send(actor.MsgInclusive, inclusiveList)
		return true
	}

	dm.batchid = dm.batchid + 1
	if msgs[0].IsSpawned == false {
		//new generation start

		dm.MainHashs = mergeHashList(dm.MainHashs, convertHash(dm.workGraph.GetAll()))

		dm.workGraph = deepgraph.NewDeepgraph()
		dm.gasCache = schedulingTypes.NewGasCache()
	}

	for i := range msgs {

		//get precedings from tree
		currentPrecedings := []deepgraph.KeyType{}
		if msgs[i].DirectPrecedings != nil && len(*msgs[i].DirectPrecedings) > 0 {
			//spawned txs
			//query all precedings from tree

			keyTypes, _, _ := dm.workGraph.GetAncestors(convertKeyTypes(*msgs[i].DirectPrecedings))
			for j := range keyTypes {
				currentPrecedings = append(currentPrecedings, keyTypes[j]...)
			}

			dm.innerlog.Log(log.LogLevel_Debug, "spawned txs  precedings ------------> ", zap.Int("precedings length", len(currentPrecedings)))
		}

		currentHashs := convertHash(currentPrecedings)

		precedings := mergeHashList(dm.MainHashs, currentHashs)
		msgs[i].Precedings = &precedings
		msgs[i].PrecedingHash = getPrecedingHash(precedings)

	}

	go func(messages []*schedulingTypes.Message) {
		dm.insertIntoGraph(messages)
	}(msgs)

	hashlist := make([]*ethCommon.Hash, len(msgs))
	for i, v := range msgs {
		hashlist[i] = &v.Message.TxHash
	}
	dm.executedTxs.addData(hashlist)
	return false
}

func (dm *DependencyManager) OnBlockCompleted() {
	dm.workGraph = deepgraph.NewDeepgraph()
	dm.MainHashs = []*ethCommon.Hash{}
	dm.batchid = -1
	dm.removedTxs.clear()
	dm.spawnedTxs.clear()
	dm.executedTxs.clear()
	dm.gasCache = schedulingTypes.NewGasCache()
}

func (dm *DependencyManager) OnExecResultReceived(results []*types.ExecuteResponse, broker *actor.MessageWrapper, inlog *actor.WorkerThreadLogger) []*schedulingTypes.Message {
	dm.innerlog = inlog

	dm.innerlog.Log(log.LogLevel_Debug, "received result ", zap.Int("result length", len(results)))

	defs := map[string]*[]*types.ExecuteResponse{}

	deleteTxs := []*ethCommon.Hash{}
	for _, result := range results {
		//if failed,delete DictionaryHash,dependency tree
		if result.Status == ethTypes.ReceiptStatusFailed {
			deleteTxs = append(deleteTxs, &result.Hash)
			continue
		}

		dm.gasCache.Add(result.Hash, result.GasUsed)

		//gather arbitrate list
		if result.DfCall == nil {
			continue
		}
		defid := result.DfCall.DeferID

		resultList := defs[defid]
		if resultList == nil {
			resultList = &[]*types.ExecuteResponse{}
			defs[defid] = resultList
		}
		*resultList = append(*resultList, result)
	}

	//remove failed txs
	dm.workGraph.Remove(convertKeyTypes(deleteTxs))

	dm.innerlog.Log(log.LogLevel_Debug, "analyze result completed", zap.Int("defs length", len(defs)), zap.Int("deleteTxs", len(deleteTxs)))
	arbitrateList := &[][][]*types.TxElement{}
	if len(defs) == 0 {

		arbitrateList = dm.getArbitrateHashLatest()

	} else {
		arbitrateList = dm.getArbitrateHashs(&defs)

	}
	dm.innerlog.Log(log.LogLevel_Debug, "gather arbitrateList  completed", zap.Int("arbitrateList length", len(*arbitrateList)))

	dm.gasCache.CostCalculateSort(arbitrateList)
	dm.innerlog.Log(log.LogLevel_Debug, "CostCalculateSort  completed")
	arbitrateResults := dm.arbitrator.Do(*arbitrateList, inlog)
	dm.innerlog.Log(log.LogLevel_Debug, "arbitrate  completed", zap.Int("arbitrateResults length", len(arbitrateResults)))

	/************************************************************/
	arbitrateResults = []*ethCommon.Hash{}
	dm.innerlog.Log(log.LogLevel_Debug, "arbitrate   restore  results to succesful")
	/************************************************************/
	willDeletes := make([]deepgraph.KeyType, 0, len(arbitrateResults))
	for _, hash := range arbitrateResults {
		willDeletes = append(willDeletes, deepgraph.KeyType(*hash))
	}

	willDeletes = dm.findDeleted(*arbitrateList, willDeletes)

	//delete from tree
	dm.workGraph.Remove(willDeletes)

	dm.removedTxs.addData(arbitrateResults)
	accumulatedRemovedTxs := dm.removedTxs.getData()

	spawnedTxs := getSpawnedTxs(results, accumulatedRemovedTxs)
	dm.innerlog.Log(log.LogLevel_Debug, "gather spawnedTxs  completed", zap.Int("spawnedTxs length", len(arbitrateResults)))

	spawnedTxHashes := make([]*ethCommon.Hash, 0, len(spawnedTxs))
	for _, tx := range spawnedTxs {
		dm.innerlog.Log(log.LogLevel_Debug, "tx.DirectPrecedings", zap.Int("DirectPrecedings length", len(*tx.DirectPrecedings)))
		spawnedTxHashes = append(spawnedTxHashes, &tx.Message.TxHash)
	}
	dm.spawnedTxs.addData(spawnedTxHashes)

	return spawnedTxs
}
func (dm *DependencyManager) findDeleted(arbitrateList [][][]*types.TxElement, willDeletes []deepgraph.KeyType) []deepgraph.KeyType {

	if len(arbitrateList) == 0 || len(willDeletes) == 0 {
		return willDeletes
	}
	retdeletes := make([]deepgraph.KeyType, 0, len(willDeletes))
	for i := range willDeletes {
		for ki := range arbitrateList {
			for kj := range arbitrateList[ki] {
				if isExist(arbitrateList[ki][kj], willDeletes[i]) {
					for j := range arbitrateList[ki][kj] {
						retdeletes = append(retdeletes, deepgraph.KeyType(*arbitrateList[ki][kj][j].TxHash))
					}
				}
			}
		}
	}
	return retdeletes
}
func isExist(lines []*types.TxElement, hash deepgraph.KeyType) bool {
	for i := range lines {
		if *lines[i].TxHash == ethCommon.Hash(hash) {
			return true
		}
	}
	return false
}
func (dm *DependencyManager) getArbitrateHashLatest() *[][][]*types.TxElement {

	keyTypes, batches, _ := dm.workGraph.GetSubgraphs()
	dm.innerlog.Log(log.LogLevel_Debug, "getArbitrateHashLatest", zap.Int("keyTypes length", len(keyTypes)))

	txlists := make([][]*types.TxElement, len(keyTypes))

	common.ParallelWorker(len(keyTypes), dm.concurrency, dm.txElementWorker, keyTypes, txlists, batches)

	if len(txlists) > 1 {
		return &[][][]*types.TxElement{txlists}
	}
	return &[][][]*types.TxElement{}
}

func (dm *DependencyManager) txElementWorker(start, end, idx int, args ...interface{}) {
	keyTypes := args[0].([]interface{})[0].([][]deepgraph.KeyType)
	txlist := args[0].([]interface{})[1].([][]*types.TxElement)
	batche := args[0].([]interface{})[2].([][]int)
	for i, rows := range keyTypes[start:end] {
		list := make([]*types.TxElement, len(rows))
		idx := start + i
		for j, key := range rows {
			hash := ethCommon.Hash(key)
			list[j] = &types.TxElement{
				TxHash:  &hash,
				Batchid: uint64(batche[idx][j]),
			}
		}

		txlist[idx] = list
	}
}

func (dm *DependencyManager) getArbitrateHashs(defs *map[string]*[]*types.ExecuteResponse) *[][][]*types.TxElement {
	if defs == nil {
		return &[][][]*types.TxElement{}
	}

	defTxlists := [][][]*types.TxElement{}
	for _, results := range *defs {
		if results == nil {
			continue
		}
		findList := make([]deepgraph.KeyType, 0, len(*results))
		for _, result := range *results {
			findList = append(findList, deepgraph.KeyType(result.Hash))
		}
		findResults, batches, _ := dm.workGraph.GetAncestorFamilies(findList)
		dm.innerlog.Log(log.LogLevel_Debug, "getArbitrateHashs", zap.Int("keyTypes length", len(findResults)), zap.Int("results", len(*results)))

		txlists := make([][]*types.TxElement, len(findResults))

		common.ParallelWorker(len(findResults), dm.concurrency, dm.txElementWorker, findResults, txlists, batches)
		if len(txlists) > 1 {
			defTxlists = append(defTxlists, txlists)
		}
	}
	return &defTxlists
}

func getSpawnedTxs(results []*types.ExecuteResponse, removedTxs []*ethCommon.Hash) []*schedulingTypes.Message {
	allRemovedList := map[ethCommon.Hash]int{}
	for i, hash := range removedTxs {
		allRemovedList[*hash] = i
	}

	defs := map[string][]*types.ExecuteResponse{}
	for _, result := range results {
		if _, ok := allRemovedList[result.Hash]; ok {
			continue
		}
		if result.DfCall == nil {
			continue
		}
		defid := result.DfCall.DeferID
		defs[defid] = append(defs[defid], result)
	}

	spawnedTxs := []*schedulingTypes.Message{}
	for defid, results := range defs {
		signature := results[0].DfCall.Signature
		contractAddress := results[0].DfCall.ContractAddress
		data := crypto.Keccak256([]byte(signature))[:4]
		data = append(data, ethCommon.AlignToEvmForInt(ethCommon.EvmWordSize)...)
		idLen := ethCommon.AlignToEvmForInt(len(defid))
		id := ethCommon.AlignToEvmForString(defid)
		data = append(data, idLen...)
		data = append(data, id...)
		contractAddr := ethCommon.HexToAddress(string(contractAddress))
		message := ethTypes.NewMessage(contractAddr, &contractAddr, 0, new(big.Int).SetInt64(0), 1e9, new(big.Int).SetInt64(0), data, false)
		standardMessager := types.StandardMessage{
			Native: &message,
			TxHash: ethCommon.RlpHash(message),
		}
		directPrecedings := []*ethCommon.Hash{}
		for _, result := range results {
			directPrecedings = append(directPrecedings, &result.Hash)
		}
		msg := schedulingTypes.Message{
			Message:          &standardMessager,
			IsSpawned:        true,
			DirectPrecedings: &directPrecedings,
		}
		spawnedTxs = append(spawnedTxs, &msg)
	}
	return spawnedTxs
}

func getInclusiveList(executed, removed []*ethCommon.Hash) *types.InclusiveList {
	removedDict := make(map[ethCommon.Hash]struct{})
	for _, v := range removed {
		removedDict[*v] = struct{}{}
	}
	listFlag := make([]bool, len(executed))
	for i, hash := range executed {
		_, ok := removedDict[*hash]
		listFlag[i] = !ok
	}

	return &types.InclusiveList{
		HashList:   executed,
		Successful: listFlag,
	}
}
