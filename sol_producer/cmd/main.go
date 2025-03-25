package main

import (
	"flag"
	"log"

	"github.com/thescopedao/solana_dex_subscribe/sol_producer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/alikafka"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/track"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/web"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/web/handler"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/utils/logger"
)

func main() {
	configPath := flag.String("config_path", "./", "config file")
	logicLogFile := flag.String("logic_log_file", "./log/sol_producer.log", "logic log file")
	flag.Parse()

	//init logic logger
	logger.Init(*logicLogFile)

	//set log level
	logger.SetLogLevel("debug")

	err := config.LoadConf(*configPath)
	if err != nil {
		log.Fatal("load config failed:", err)
	}

	alikafka.InitKafka()

	handler.SubAddrHistoryTxs()

	track.AddrTask()

	web.Run()
}
