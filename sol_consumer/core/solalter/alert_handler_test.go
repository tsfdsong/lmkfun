package solalter

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

func TestCurated(t *testing.T) {
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
		return
	}

	res := RawTopicData{
		Type: "publish_call",
		Data: "{\"chain\":\"solana\",\"list_id\":\"1867442580562358272\",\"mcap\":\"127848504128.12038\",\"price\":\"216.55274729727435\",\"contract\":\"So11111111111111111111111111111111111111112\",\"thesis\":\"购买1000sol\",\"token_symbol\":\"测试\",\"call_type\":\"First Buy\",\"suggested_amount\":\"1000\",\"timestamp\":1734500908}",
	}

	val, err := res.UnmarshallCurated()
	if err != nil {
		logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubExchange unmarshal kafka message failed")
		return
	}

	handleCuratedCalls(val)

	select {}
}

func TestBitquery(t *testing.T) {
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
		return
	}

	res := RawTopicData{
		Type: "publish_call",
		Data: "{\"chain\":\"solana\",\"list_id\":\"1867442580562358272\",\"mcap\":\"127848504128.12038\",\"price\":\"216.55274729727435\",\"contract\":\"So11111111111111111111111111111111111111112\",\"thesis\":\"购买1000sol\",\"token_symbol\":\"测试\",\"call_type\":\"First Buy\",\"suggested_amount\":\"1000\",\"timestamp\":1734500908}",
	}

	val, err := res.UnmarshallCurated()
	if err != nil {
		logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubExchange unmarshal kafka message failed")
		return
	}

	handleCuratedCalls(val)

	select {}
}

func TestCAKOL(t *testing.T) {
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
		return
	}

	res := RawTopicData{
		Type: "KOL",
		Data: "{\"author\":\"gem_detecter\",\"chain\":\"solana\",\"contract\":\"2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv\",\"created_at\":\"2024-12-20 10:26:16\",\"full_text\":\"Buy #GRIFFAIN here to make 3X when it back to ATH\\nBuy #CHILLGUY here to make 6X when it back to ATH\\nBuy $PENGU here to make 3X when it back to ATH\\nBuy $SHRUB here to make 10X when it back to ATH\\n\\nComment below \\n\\nBuy $… here to make ...Xs when it back to ATH\",\"is_verified\":false,\"market_cap\":1.531021604E9,\"pk\":\"1870053096757334428\",\"price\":0.02435878,\"sentiment\":\"Neutral\",\"symbol\":\"PENGU\",\"update_time\":\"2024-12-20 10:26:48\"}",
	}

	val, err := res.UnmarshallKOL()
	if err != nil {
		logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubExchange unmarshal kafka message failed")
		return
	}

	handleKOLAlert(val)

	select {}
}

func TestSolTransfer(t *testing.T) {

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

	raws := `[{
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "4uisite4b1bbtLD4wC6rPRzrhWijt61RhPadG8nBhNKd",
            "fromUserAccount": "4uisite4b1bbtLD4wC6rPRzrhWijt61RhPadG8nBhNKd",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "7CLNmEos4vabHQ89k8GSFb58EYUk7ny5RfUCAV944if6",
            "fromUserAccount": "7CLNmEos4vabHQ89k8GSFb58EYUk7ny5RfUCAV944if6",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "5zKESV7NRrWAhMvAtVdiMpWEZD5HZ1A8jtsBEcsTcARS",
            "fromUserAccount": "5zKESV7NRrWAhMvAtVdiMpWEZD5HZ1A8jtsBEcsTcARS",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "DNL8D61CesvtemsG4RTWtcv915zgYAaSmSu1LvBJDfvB",
            "fromUserAccount": "DNL8D61CesvtemsG4RTWtcv915zgYAaSmSu1LvBJDfvB",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "7f8mEeF2hvdsnkgNsQ8UC8yzNpozHGmaudHN7TgLzmQT",
            "fromUserAccount": "7f8mEeF2hvdsnkgNsQ8UC8yzNpozHGmaudHN7TgLzmQT",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "84SHsPJyN4tgZbtZUJZnbH7r8EJNVV8PHhSHMoNn8UJF",
            "fromUserAccount": "84SHsPJyN4tgZbtZUJZnbH7r8EJNVV8PHhSHMoNn8UJF",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "DTQFTARj8QXDoFBA48Dq3uavHWAU5gZkYNfSHUWfLB5T",
            "fromUserAccount": "DTQFTARj8QXDoFBA48Dq3uavHWAU5gZkYNfSHUWfLB5T",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "E5x8tnwPUnDZGRns5zcntXnwuNfdjwQnBYsyEsmEt69Y",
            "fromUserAccount": "E5x8tnwPUnDZGRns5zcntXnwuNfdjwQnBYsyEsmEt69Y",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "Ew8bvGeyAVZ7THz1qG9cja9QD5ZxNdZuqgocjpM1aQSx",
            "fromUserAccount": "Ew8bvGeyAVZ7THz1qG9cja9QD5ZxNdZuqgocjpM1aQSx",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "6yd6ZC3tudQdnNwvJCVTgN3zqhmthGKn6RMabQ9TAn9J",
            "fromUserAccount": "6yd6ZC3tudQdnNwvJCVTgN3zqhmthGKn6RMabQ9TAn9J",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "9e7XpGa1tQPm3eCKcBqsov96arRnBxGy6hEMXMTbFp3r",
            "fromUserAccount": "9e7XpGa1tQPm3eCKcBqsov96arRnBxGy6hEMXMTbFp3r",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "HnCfHBEXRmhrZGatRN8Rc4sE3j3Vvm6hDUohKeJeoMgV",
            "fromUserAccount": "HnCfHBEXRmhrZGatRN8Rc4sE3j3Vvm6hDUohKeJeoMgV",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "2unfokYQKH3P9kUA9XpWw7pmsAr822U7iQoAZjKhEXs4",
            "fromUserAccount": "2unfokYQKH3P9kUA9XpWw7pmsAr822U7iQoAZjKhEXs4",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "2pU3xyquzQ1BFnjhER5E9TQoFyB5wy4CR8aBr1D21HDH",
            "fromUserAccount": "2pU3xyquzQ1BFnjhER5E9TQoFyB5wy4CR8aBr1D21HDH",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "FAs3ZjntbUXvnhhCFByD5EL4xiRPbmswxfLL43UsP35C",
            "fromUserAccount": "FAs3ZjntbUXvnhhCFByD5EL4xiRPbmswxfLL43UsP35C",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "5VdBmDDZH4Uq952uBENobaJTZGFN65Ft34bHompiiWtz",
            "fromUserAccount": "5VdBmDDZH4Uq952uBENobaJTZGFN65Ft34bHompiiWtz",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "5QmPjztX3Pugg3HEU1bcBDJnTniHwzVAN9QEuXTE8xoY",
            "fromUserAccount": "5QmPjztX3Pugg3HEU1bcBDJnTniHwzVAN9QEuXTE8xoY",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "NQmMANx83CxDieQXp11Lp8NyNtN3xJFuWfcfZERw5wb",
            "fromUserAccount": "NQmMANx83CxDieQXp11Lp8NyNtN3xJFuWfcfZERw5wb",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "9zFNNkaxXqd3Q6GR1fWFvtvorKgvrG8ayizNn4GrU7ME",
            "fromUserAccount": "9zFNNkaxXqd3Q6GR1fWFvtvorKgvrG8ayizNn4GrU7ME",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        },
        {
            "tx_hash": "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
            "source": "SYSTEM_PROGRAM",
            "timestamp": 1737091493,
            "type": "TRANSFER",
            "date": "2025-01-17 13: 24: 53",
            "from_token": "So11111111111111111111111111111111111111112",
            "fromTokenAccount": "E4DSEUQ3KS1Lmq3zrpMYAj2r8SPa9ZfzNx4bypd5EqKY",
            "fromUserAccount": "E4DSEUQ3KS1Lmq3zrpMYAj2r8SPa9ZfzNx4bypd5EqKY",
            "from_token_amount": 0.00203928,
            "to_token": "So11111111111111111111111111111111111111112",
            "toTokenAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "toUserAccount": "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
            "to_token_amount": 0.00203928,
            "trade_label": "",
            "is_dca_trade": false,
            "wallet_counts": 1,
            "transfer_details": []
        }
    ]`

	var data []model.SolSwapData
	err = json.Unmarshal([]byte(raws), &data)
	if err != nil {
		return
	}

	in := model.SolSwapData{
		TxHash:           "5Jk3RuiuWgJx1M9Q7XetQVM5Zv7gTaueTSL9tNG9hSrGR2HKobQza63bYDFtLT5L783Ni1ySDeKE5pUtCUdryM3x",
		Source:           "SYSTEM_PROGRAM",
		Timestamp:        1737091493,
		Type:             "TRANSFER",
		Date:             "2025-01-17 13:24:53",
		FromToken:        "So11111111111111111111111111111111111111112",
		FromTokenAccount: "",
		FromUserAccount:  "",
		FromTokenAmount:  0.0407856,
		ToToken:          "So11111111111111111111111111111111111111112",
		ToTokenAccount:   "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
		ToUserAccount:    "2HqPaB7uVdyrRdbrryPQVPrkCDFxFkChUPSD7M1TiYWP",
		ToTokenAmount:    0.0407856,
		TradeLabel:       "none",
		IsDCATrade:       false,
		WalletCounts:     len(data),
		TransferDetails:  data,
	}

	err = handleSolSaveRecord([]model.SolSwapData{in}, make(map[string]bool))
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
}

func TestFomoCall(t *testing.T) {
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
		return
	}

	res := RawTopicData{
		Type: "fomo_call",
		Data: "{\"chain\":\"solana\",\"list_id\":\"1870088362813898752\",\"token_address\":\"74SBV4zDXxTRgv1pEMoECskKBkZHc2yGPnc7GYVepump\",\"buy_amount\":326131.777985000000000,\"bought_wallet_count\":3,\"buy_sol_amount\":4417513.465566000000000,\"multiple_buy\":true,\"sell_amount\":16674.857356000000000,\"average_sell_price\":0.124314988173144,\"token_symbol\":\"swarms\",\"time\":28,\"average_buy_price\":0.134379500565001,\"buy_amount_value\":43825.425444000000000,\"sell_amount_value\":2072.934695000000000}",
	}

	val, err := res.UnmarshallFomo()
	if err != nil {
		logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubExchange unmarshal kafka message failed")
		return
	}

	handleFomoCalls(val)

	select {}
}

func TestHistoryHelius(t *testing.T) {

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

	rule := make(map[string]bool)

	cfg := config.GetSolDataConfig()
	for _, v := range cfg.ThreadData {
		rule[v.ContractAddress] = true
	}

	in := model.SolSwapData{
		TxHash:           "5s4KiKqUGpfhBgYWQn6ZPLFeQK3E8fsnzZifddNXYyoqmwFbmAGq6y7tA5q5NaznjsP8Ns7sqqbiDs9D5tasrTYk",
		Source:           "Jupiter Aggregator v6",
		Timestamp:        1737436964,
		Type:             "SWAP",
		Date:             "2025-01-21 05:22:44",
		FromToken:        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
		FromTokenAccount: "5naTTycsRFAwCX3HDG9pVMLBsSYZQQFwd6d8CCL1rPvT",
		FromUserAccount:  "GV9kvNR1x2KVX3kaacQRrcUjaZTzf67PBZcfVD8JA9Ad",
		FromTokenAmount:  7061954.344095,
		ToToken:          "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN",
		ToTokenAccount:   "3PDmdxMxEZ9To8gkrfgLJHEsLFQySCdRrFdsFuS5qozc",
		ToUserAccount:    "GV9kvNR1x2KVX3kaacQRrcUjaZTzf67PBZcfVD8JA9Ad",
		ToTokenAmount:    207868.46494,
		TradeLabel:       "none",
		IsDCATrade:       false,
	}

	err = HandleHeliusHisData([]model.SolSwapData{in}, rule)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
}

func TestBitqueryEVM(t *testing.T) {
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
		return
	}

	rawData := RawBitqueryAltertData{
		FromAddress:     "0xda8f832f6d6035e17f468c77bd8a341be418f574",
		FromToken:       "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
		FromTokenAmount: "0.119271477000000000",
		FromTokenSymbol: "WBNB",
		ToAddress:       "0xda8f832f6d6035e17f468c77bd8a341be418f574",
		ToToken:         "0x42981d0bfbaf196529376ee702f2a9eb9092fcb5",
		ToTokenAmount:   "2826118.717485517",
		ToTokenSymbol:   "SFM",
		DEX:             "uniswap_v2",
		Chain:           "bsc",
		TxHash:          "0xd4dd4883ddbd6d23a3164ba7587f9b29b700e84cc60951ec7c9cc1a2b31a3c8a",
		Timestamp:       "2025-02-10T06:21:45Z",
		Value:           "196.6326755906002",
		Signer:          "0xda8f832f6d6035e17f468c77bd8a341be418f574",
	}

	cacheData := []TrackedAddrCache{
		{
			ListID:            "1879473606096871424",
			Chain:             "bsc",
			Label:             "simon",
			UserAccount:       rawData.FromAddress,
			TxBuyValue:        0.1,
			TxSellValue:       0.2,
			TxReceivedValue:   0.3,
			TxSendValue:       0.4,
			TokenSecurity:     true,
			TokenMarketCap:    5000000000000,
			TokenMarketCapMin: 10.6,
			AgeTime:           0,
			Volume24HMax:      20000,
			Volume24HMin:      13.8,
			HolderCount:       10000,
			TxBuySell:         true,
			TxMintBurn:        false,
			TxTransfer:        false,

			TgTxBuy:      true,
			TgTxSold:     true,
			TgTxReceived: false,
			TgTxSend:     false,
			TgTxCreate:   false,
			IsAddrPublic: true,

			TgTxFirstBuy: false,
			TgTxFreshBuy: false,
			TgTxSellAll:  false,
		},
	}

	err = batchAddItems(rawData.Chain, rawData.FromAddress, cacheData)
	if err != nil {
		log.Fatal("load config failed:", err)
		return
	}

	err = handleEVMBuy(rawData)
	if err != nil {
		log.Fatal("load config failed:", err)
		return
	}
}

func TestMergeEVMTx(t *testing.T) {
	txlistdata := `[ {
      "dex": "uniswap_v2",
      "chain": "bsc",
      "hash": "0x43f71060791415ebe32167e765bf134c1cad2f860b28de20fd10bab0446b1a2b",
      "timestamp": "2025-02-25T06:12:27Z",
      "from_address": "0x8d3c111020dafc00a27d0e153ce1b1576625374d",
      "from_token_address": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
      "from_token_symbol": "WBNB",
      "from_token_amount": "0.025499190686785780",
      "to_address": "0x8d3c111020dafc00a27d0e153ce1b1576625374d",
      "to_token_address": "0xfb6115445bff7b52feb98650c87f44907e58f802",
      "to_token_symbol": "AAVE",
      "to_token_amount": "0.077336652717599894",
      "value": "15.663156209340627",
      "signer": "0x8d3c111020dafc00a27d0e153ce1b1576625374d"
    },
    {
      "dex": "uniswap_v2",
      "chain": "bsc",
      "hash": "0x43f71060791415ebe32167e765bf134c1cad2f860b28de20fd10bab0446b1a2b",
      "timestamp": "2025-02-25T06:12:27Z",
      "from_address": "0x8d3c111020dafc00a27d0e153ce1b1576625374d",
      "from_token_address": "0xfb6115445bff7b52feb98650c87f44907e58f802",
      "from_token_symbol": "AAVE",
      "from_token_amount": "0.077336652717599894",
      "to_address": "0x8d3c111020dafc00a27d0e153ce1b1576625374d",
      "to_token_address": "0xebd3619642d78f0c98c84f6fa9a678653fb5a99b",
      "to_token_symbol": "ASX",
      "to_token_amount": "85.657637154999636315",
      "value": "0",
      "signer": "0x8d3c111020dafc00a27d0e153ce1b1576625374d"
    },
    {
      "dex": "uniswap_v2",
      "chain": "bsc",
      "hash": "0x43f71060791415ebe32167e765bf134c1cad2f860b28de20fd10bab0446b1a2b",
      "timestamp": "2025-02-25T06:12:27Z",
      "from_address": "0x8d3c111020dafc00a27d0e153ce1b1576625374d",
      "from_token_address": "0xebd3619642d78f0c98c84f6fa9a678653fb5a99b",
      "from_token_symbol": "ASX",
      "from_token_amount": "82.231331668799650863",
      "to_address": "0x8d3c111020dafc00a27d0e153ce1b1576625374d",
      "to_token_address": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
      "to_token_symbol": "WBNB",
      "to_token_amount": "0.025654361195681368",
      "value": "15.734538118584123",
      "signer": "0x8d3c111020dafc00a27d0e153ce1b1576625374d"
    }]`

	var list []RawBitqueryAltertData
	err := json.Unmarshal([]byte(txlistdata), &list)
	if err != nil {
		fmt.Printf("json:%v\n", err)
		return
	}

	handleData := make([]RawBitqueryAltertData, 0)

	txhash := "0x43f71060791415ebe32167e765bf134c1cad2f860b28de20fd10bab0446b1a2b"
	resdata, err := mergeTxList(txhash, list)
	if err != nil {
		handleData = append(handleData, list...)
	} else if resdata != nil {
		handleData = append(handleData, *resdata)
	}

	fmt.Printf("data:%v\n", handleData)
}

func TestAddBlackList(t *testing.T) {
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

	var res []model.BlacklistAddress
	ctx := context.Background()
	err = db.GetDB().NewSelect().Model(&res).Scan(ctx)
	if err != nil {
		return
	}

	for _, v := range res {

		key := fmt.Sprintf("rate_limit:%s:%s", v.Chain, v.Address)
		_, err = redis.GetRedisInst().Set(ctx, key, v.Address, 365*24*time.Hour).Result()
		if err != nil {
			return
		}
	}
}
