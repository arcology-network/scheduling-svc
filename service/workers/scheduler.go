package workers

import (
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/HPISTechnologies/common-lib/signature"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/log"
	schedulingTypes "github.com/HPISTechnologies/scheduling-svc/service/types"
)

type Scheduler struct {
	actor.WorkerThread
	schedule *schedulingTypes.ExecutionSchedule
	dm       *DependencyManager
	exec     Executor
}

//return a Subscriber struct
func NewScheduler(concurrency int, groupid string, arbitrator Arbitrator, exec Executor) *Scheduler {
	scheduler := Scheduler{
		dm:   NewDependencyManager(arbitrator, concurrency),
		exec: exec,
	}
	scheduler.Set(concurrency, groupid)
	return &scheduler
}

func (sched *Scheduler) OnStart() {
	sched.dm.Start()
	sched.exec.Start()
}

func (sched *Scheduler) Stop() {
	sched.dm.Stop()
	sched.exec.Stop()
}

func (sched *Scheduler) GetBatch(spawned []*schedulingTypes.Message) []*schedulingTypes.Message {
	messages := []*schedulingTypes.Message{}
	if len(spawned) == 0 {
		originTxs := sched.schedule.GetNextBatch()

		//----------------------------------------------------------------------------
		threads := sched.Concurrency
		txLen := len(originTxs)
		var step = int(math.Max(float64(txLen/threads), float64(txLen%threads)))
		wg := sync.WaitGroup{}
		messages = make([]*schedulingTypes.Message, txLen)
		for counter := 0; counter <= threads; counter++ {

			begin := counter * step
			end := int(math.Min(float64(begin+step), float64(txLen)))

			wg.Add(1)
			go func(begin int, end int, id int) {
				for i := begin; i < end; i++ {
					messages[i] = &schedulingTypes.Message{
						Message:   originTxs[i],
						IsSpawned: false,
					}
				}
				wg.Done()
			}(begin, end, counter)
			if txLen == end {
				break
			}

		}

		wg.Wait()
		//--------------------------------------------------------------------------------
	} else {
		messages = spawned
	}
	return messages
}

func (sched *Scheduler) OnMessageArrived(msgs []*actor.Message) error {
	var timestamp *big.Int
	var messages []*types.StandardMessage
	for _, v := range msgs {
		switch v.Name {
		case actor.MsgBlockStamp:
			timestamp = v.Data.(*big.Int)
		case actor.MsgMessagersReaped:
			messages = v.Data.([]*types.StandardMessage)
		}
	}
	maps := signature.GetParallelFuncMap()
	logid := sched.AddLog(log.LogLevel_Info, "Before NewExecutionSchedule")
	interLog := sched.GetLogger(logid)
	starttime := time.Now()
	sched.schedule = schedulingTypes.NewExecutionSchedule(messages, &maps, interLog)
	logid = sched.AddLog(log.LogLevel_Info, "sched.GetBatch------->")
	msgsToExecute := sched.GetBatch([]*schedulingTypes.Message{})
	logid = sched.AddLog(log.LogLevel_Info, "Before first batch")
	interLog = sched.GetLogger(logid)
	finished := sched.dm.OnNewBatch(msgsToExecute, sched.MsgBroker, interLog)
	for !finished {
		logid := sched.AddLog(log.LogLevel_Info, "start enter exec")
		interLog = sched.GetLogger(logid)
		results := sched.exec.Run(msgsToExecute, timestamp, msgs[0], interLog)
		logid = sched.AddLog(log.LogLevel_Info, "Before DependencyManager.OnExecResultReceived")
		interLog := sched.GetLogger(logid)
		spawned := sched.dm.OnExecResultReceived(results, sched.MsgBroker, interLog)
		msgsToExecute = sched.GetBatch(spawned)
		logid = sched.AddLog(log.LogLevel_Info, "Before DependencyManager.OnNewBatch")
		interLog = sched.GetLogger(logid)
		finished = sched.dm.OnNewBatch(msgsToExecute, sched.MsgBroker, interLog)
	}
	execTime := time.Now().Sub(starttime)
	statisticInfo := types.StatisticalInformation{
		Key:      actor.MsgExecTime,
		TimeUsed: execTime,
		Value:    fmt.Sprintf("%v", execTime),
	}
	sched.MsgBroker.Send(actor.MsgExecTime, &statisticInfo)
	sched.dm.OnBlockCompleted()
	return nil
}
