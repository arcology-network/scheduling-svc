package service

import (
	"net/http"
	"strings"

	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/kafka"
	"github.com/HPISTechnologies/component-lib/streamer"
	"github.com/HPISTechnologies/scheduling-svc/service/workers"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

type Config struct {
	concurrency int
	groupid     string
}

//return a Subscriber struct
func NewConfig() *Config {
	return &Config{
		concurrency: viper.GetInt("concurrency"),
		groupid:     "scheduing",
	}
}

func (cfg *Config) Start() {
	http.Handle("/streamer", promhttp.Handler())
	go http.ListenAndServe(":19008", nil)

	broker := streamer.NewStatefulStreamer()
	//00 initializer
	initializer := actor.NewActor(
		"initializer",
		broker,
		[]string{actor.MsgStarting},
		[]string{
			actor.MsgStartSub,
		},
		[]int{1},
		workers.NewInitializer(cfg.concurrency, cfg.groupid),
	)
	initializer.Connect(streamer.NewDisjunctions(initializer, 1))

	receiveMsgs := []string{
		actor.MsgMessager,
		actor.MsgReapinglist,
		actor.MsgBlockCompleted,
		actor.MsgNodeRoleCore,
	}

	receiveTopics := []string{
		viper.GetString("chkd-scs"),
		viper.GetString("reaplist-scs"),
		viper.GetString("msgexch"),
	}

	//01 kafkaDownloader
	kafkaDownloader := actor.NewActor(
		"kafkaDownloader",
		broker,
		[]string{actor.MsgStartSub},
		receiveMsgs,
		[]int{10000, 1, 1, 1},
		kafka.NewKafkaDownloader(cfg.concurrency, cfg.groupid, receiveTopics, receiveMsgs),
	)
	kafkaDownloader.Connect(streamer.NewDisjunctions(kafkaDownloader, 10000))

	reaperPreparation := actor.NewActor(
		"reaperPreparation",
		broker,
		[]string{
			actor.MsgNodeRoleCore,
		},
		[]string{
			actor.MsgReapinglist,
		},
		[]int{1},
		workers.NewReaperPreparation(cfg.concurrency, cfg.groupid),
	)
	reaperPreparation.Connect(streamer.NewDisjunctions(reaperPreparation, 1))

	reaper := actor.NewActor(
		"reaper",
		broker,
		[]string{
			actor.MsgNodeRoleCore,
			actor.MsgReapinglist,
		},
		[]string{
			actor.MsgReaperCommand,
			actor.MsgBlockStamp,
		},
		[]int{1, 1},
		workers.NewReaper(cfg.concurrency, cfg.groupid, viper.GetInt("rpm")),
	)
	reaper.Connect(streamer.NewConjunctions(reaper))

	aggreSelector := actor.NewActor(
		"aggreSelector",
		broker,
		[]string{
			actor.MsgMessager,
			actor.MsgBlockCompleted,
			actor.MsgInclusive,
			actor.MsgReaperCommand,
		},
		[]string{
			actor.MsgMessagersReaped,
		},
		[]int{1},
		workers.NewAggreSelector(cfg.concurrency, cfg.groupid, viper.GetInt("ckeckrange")),
	)
	aggreSelector.Connect(streamer.NewDisjunctions(aggreSelector, 1))

	relations := map[string]string{}
	relations[actor.MsgFinalTxsListSpawned] = viper.GetString("txs-list-spawned")
	relations[actor.MsgInclusive] = viper.GetString("inclusive-txs")
	relations[actor.MsgExecTime] = viper.GetString("msgexch")

	kafkaUploader := actor.NewActor(
		"kafkaUploader",
		broker,
		[]string{
			actor.MsgFinalTxsListSpawned,
			actor.MsgInclusive,
			actor.MsgExecTime,
		},
		[]string{},
		[]int{},
		kafka.NewKafkaUploader(cfg.concurrency, cfg.groupid, relations),
	)
	kafkaUploader.Connect(streamer.NewDisjunctions(kafkaUploader, 1))

	scheduler := actor.NewActor(
		"scheduler",
		broker,
		[]string{
			actor.MsgMessagersReaped,
			actor.MsgBlockStamp,
		},
		[]string{
			actor.MsgFinalTxsListSpawned,
			actor.MsgInclusive,
			actor.MsgExecTime,
		},
		[]int{1, 1, 1},
		workers.NewScheduler(
			cfg.concurrency, cfg.groupid,
			workers.NewRpcClientArbitrate([]string{viper.GetString("zkUrl")}),
			workers.NewRpcClientExec(cfg.concurrency, cfg.groupid, strings.Split(viper.GetString("execAddrs"), ","), viper.GetInt("batchs")),
		),
	)
	scheduler.Connect(streamer.NewConjunctions(scheduler))

	selfStarter := streamer.NewDefaultProducer("selfStarter", []string{actor.MsgStarting}, []int{1})
	broker.RegisterProducer(selfStarter)

	broker.Serve()

	//start signal
	streamerStarting := actor.Message{
		Name:   actor.MsgStarting,
		Height: 0,
		Round:  0,
		Data:   "start",
	}
	broker.Send(actor.MsgStarting, &streamerStarting)

}

func (cfg *Config) Stop() {

}
