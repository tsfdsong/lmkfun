package handler

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/alikafka"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/utils/logger"
)

type SolTxRecord struct {
	// bun.BaseModel `bun:"table:lmk_sol_data,alias:oat"`

	TxHash    string `bun:"tx_hash,pk,notnull"`
	Source    string `bun:"source,pk,notnull"`
	Timestamp int    `bun:"timestamp,pk,notnull"`
	Type      string `bun:"type,pk,notnull"`
	Date      string `bun:"date"`

	FromToken        string  `bun:"from_token,pk,notnull"`
	FromTokenAccount string  `bun:"from_token_account"`
	FromUserAccount  string  `bun:"from_user_account,pk,notnull"`
	FromTokenAmount  float64 `bun:"from_token_amount"`
	FromTokenSymbol  string  `bun:"from_token_symbol"`

	ToToken        string  `bun:"to_token,pk,notnull"`
	ToTokenAccount string  `bun:"to_token_account"`
	ToUserAccount  string  `bun:"to_user_account,pk,notnull"`
	ToTokenAmount  float64 `bun:"to_token_amount"`
	ToTokenSymbol  string  `bun:"to_token_symbol"`

	CreateAt        time.Time     `bun:"create_at,nullzero"`
	Value           string        `bun:"value"`
	MarketCap       string        `bun:"marketcap"`
	Price           string        `bun:"price"`
	Change1HPrice   string        `bun:"price_change_1h"`
	Direction       string        `bun:"direction"`
	Supply          string        `bun:"supply"`
	TradeLabel      string        `bun:"trade_label"`
	IsDCATrade      bool          `bun:"is_dca_trade"`
	WalletCounts    int           `bun:"wallet_counts"`
	TransferDetails []interface{} `bun:"transfer_details"`
	IsSymbolValid   bool          `bun:"is_symbol_valid"`
}

func TestParseTransfer(t *testing.T) {

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

	// 读取 JSON 文件
	file, err := os.Open("output.json")
	if err != nil {
		fmt.Printf("读取文件失败: %v\n", err)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("读取文件内容失败: %v\n", err)
		return
	}

	var jsonData []HeliusData
	if err := json.Unmarshal(data, &jsonData); err != nil {
		fmt.Printf("解析 JSON 数据失败: %v\n", err)
		return
	}

	// 分析交易
	results, err := parseHeliusTransfers(jsonData)
	if err != nil {
		fmt.Printf("分析失败: %v\n", err)
		return
	}

	// 保存结果
	resultData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fmt.Printf("格式化结果失败: %v\n", err)
		return
	}

	if err := ioutil.WriteFile("result.json", resultData, 0644); err != nil {
		fmt.Printf("写入文件失败: %v\n", err)
		return
	}

	fmt.Println("分析结果已保存到 result.json")
}

func TestSendTxHis(t *testing.T) {
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

	var resAddr []SolTxRecord
	query := `select * from scope_lmk.lmk_sol_data where tx_hash = '2pfwhjNi9xjKGHqkGLAsb9UtutNaQzJggmQtcDPZ16bbFMm8GomUHXjCD5Lj578UjtWPr9jBnLXBJUr7n9MjXL5g'`

	err = db.GetDB().NewRaw(query).Scan(context.Background(), &resAddr)
	if err != nil {
		logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubAddrHistoryTxs  failed")
		return
	}

	for _, v := range resAddr {
		item := model.SolSwapData{
			TxHash:           v.TxHash,
			Source:           v.Source,
			Timestamp:        v.Timestamp,
			Type:             v.Type,
			Date:             v.Date,
			FromToken:        v.FromToken,
			FromTokenAccount: v.FromTokenAccount,
			FromUserAccount:  v.FromUserAccount,
			FromTokenAmount:  v.FromTokenAmount,
			ToToken:          v.ToToken,
			ToTokenAccount:   v.ToTokenAccount,
			ToUserAccount:    v.ToUserAccount,
			ToTokenAmount:    v.ToTokenAmount,
			TradeLabel:       model.LabelNone,
		}

		// err = sendKafkaMsg([]model.SolSwapData{item})
		// if err != nil {
		// 	logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubAddrHistoryTxs HandleHistoryData failed")

		// 	return
		// }

		// logger.Logrus.WithFields(logrus.Fields{"Data": item}).Info("handTransferData send transfer kafka data")

		ndata := fillTradeLabel(item)

		logger.Logrus.WithFields(logrus.Fields{"Data": ndata}).Info("FillTradeLabel data")

	}

	select {}
}

func TestSendTg(t *testing.T) {
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

	item := model.SolSwapData{
		TxHash:           "56a6pg5gMrgQCFuv9ZdHNkZXAqw8fwf4v83Nmp3YfxeScEhZUXETcjtL5bRuKU3kQxQvhX6RuTKnAJMrezNrKzKh",
		Source:           "DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M",
		Timestamp:        318796646,
		Type:             "SWAP",
		Date:             "February 06, 2025 06:10:02 +UTC",
		FromToken:        "So11111111111111111111111111111111111111112",
		FromTokenAccount: "F1cKJWLwnNwDbPPNEBUM1DBVnsfi4nsggU525Pvh3WQd",
		FromUserAccount:  "F1cKJWLwnNwDbPPNEBUM1DBVnsfi4nsggU525Pvh3WQd",
		FromTokenAmount:  0.254116964,
		ToToken:          "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij",
		ToTokenAccount:   "Gy4inaDWLKs8LC2h33yLNxouoD2mxwz4CPxzRKxm83ab",
		ToUserAccount:    "F1cKJWLwnNwDbPPNEBUM1DBVnsfi4nsggU525Pvh3WQd",
		ToTokenAmount:    0.00052464,
		TradeLabel:       model.LabelNone,
	}

	ndata := fillTradeLabel(item)

	logger.Logrus.WithFields(logrus.Fields{"Data": ndata}).Info("FillTradeLabel data")
	select {}
}

func TestTxParse(t *testing.T) {
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

	tokenAccount := "4e9cdSK8szH8XTRJmEviD7zSMRtbT7xtBU1iK2wrjwcL"
	beforeTxHash := "4ZyGTB8zTepeqQuiiYHuuxj8dbB4Xa6e6d6bQPv5wZoijCnP5g2dsKxHd9HZnz4zdoVrfPT3bnSktdWpEGfq27zY"
	helist, err := getTokenAccountTxs(tokenAccount, beforeTxHash)
	if err != nil {
		return
	}

	err = HandleData(helist)
	if err != nil {
		return
	}
}

func TestGetTxParse(t *testing.T) {
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

	txhash := "fatFjqVwLQLRkNtL1HCs5pJchCov9ChW9BJmUb1n6jWZaQqmCbjfTSYCcDkoWszMobATRFFW4HaZ5zTR1HcS1JH"
	obj, err := getQuickNodeTransaction(txhash)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	fmt.Printf("%v\n", obj)
}
