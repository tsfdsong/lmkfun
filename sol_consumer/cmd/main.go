package main

import (
	"flag"
	"log"

	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/alikafka"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/solalter"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/web"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

func main() {
	configPath := flag.String("config_path", "./", "config file")
	logicLogFile := flag.String("logic_log_file", "./log/sol_consumer.log", "logic log file")
	flag.Parse()

	//init logic logger
	logger.Init(*logicLogFile)

	//set log level
	logger.SetLogLevel("debug")

	err := config.LoadConf(*configPath)
	if err != nil {
		log.Fatal("load config failed:", err)
	}

	err = redis.InitRedis()
	if err != nil {
		log.Fatal("init redis failed:", err)
	}

	alikafka.InitKafka()

	serv := solalter.NewAlterService()
	serv.Start()
	defer serv.Close()

	web.Run()
}
