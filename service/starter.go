package service

import (
	cmn "github.com/HPISTechnologies/3rd-party/tm/common"
	"github.com/HPISTechnologies/component-lib/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var StartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start scheduler service Daemon",
	RunE:  startCmd,
}

func init() {
	flags := StartCmd.Flags()

	flags.String("mqaddr", "localhost:9092", "host:port of kafka")

	flags.String("msgexch", "msgexch", "topic for receive msg exchange")
	flags.Int("concurrency", 4, "num of threads")
	flags.Int("rpm", 0, "max num of reap txs,0-not limit")
	flags.String("logcfg", "./log.toml", "log conf path")

	flags.Int("batchs", 500, "max txs of per batch")
	flags.String("arbitrateAddr", "localhost:8972", "arbitrator server address")
	flags.String("execAddrs", "localhost:8973", "exec server address")
	flags.String("chkd-scs", "chkd-scs", "topic for secedule received tx ")
	flags.String("reaplist-scs", "reaplist-scs", "topic of send receive list for smartcontract")
	flags.String("inclusive-txs", "inclusive-txs", "topic for received txlist")
	flags.String("txs-list-spawned", "txs-list-spawned", "topic for send  spawned txs list")

	flags.Bool("draw", false, "draw flow graph")

	flags.String("scheduler", "./scheduler", "scheduler conf path")
	flags.Int("nidx", 0, "node index in cluster")
	flags.String("nname", "node1", "node name in cluster")

	flags.Int("ckeckrange", 2, "scheduling tx pool checking range ")

	flags.String("zkUrl", "127.0.0.1:2181", "url of zookeeper")
	flags.String("rpcSrv", "127.0.0.1:8976", "local server register url ")
}

func startCmd(cmd *cobra.Command, args []string) error {
	log.InitLog("scheduler.log", viper.GetString("logcfg"), "scheduler", viper.GetString("nname"), viper.GetInt("nidx"))
	en := NewConfig()
	en.Start()

	if viper.GetBool("draw") {
		log.CompleteMetaInfo("scheduling")
		return nil
	}

	// Wait forever
	cmn.TrapSignal(func() {
		// Cleanup
		en.Stop()
	})
	return nil
}
