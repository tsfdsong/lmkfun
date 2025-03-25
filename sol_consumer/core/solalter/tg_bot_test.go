package solalter

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/alikafka"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type TgTestBody struct {
	ChatID      string   `json:"chat_id"`
	Text        string   `json:"text"`
	ReplyMarkup TgMarkup `json:"reply_markup"`
	ParseMode   string   `json:"parse_mode"`
}

func TestAddrSendBotMsg(t *testing.T) {
	fromLabel := "siyt"
	fromAccount := "9atg38QyMRuFUhbr7SLR8yTvhFUsQsoQonfZpb2MLAwD"
	fromAmount := "0.01983"
	fromSymbol := "SOL"
	fromValue := "4.2459996"
	toAmount := "6.26"
	toTokenSymbol := "Pnut"
	price := "214.12"
	txHash := "4Q6JcsNLomnH6yX2ZrxZ9UejPkLADCM8BKE821kDVdXoaHkGSp8kmvRxaYyVgpnHot1ekTxQXMbhKYyKq9NE4GRQ"
	fromToken := "So11111111111111111111111111111111111111112"
	listid := "1867048652718120960"
	totoken := "2qEHjDLDLbuBgRYvsxhc5D6uDWAivNFZGan56P1tpump"
	tomc := "2532000000000000000"

	msg := ConstructSoldBotMessage("solana", fromLabel, fromAccount, fromAmount, fromSymbol, fromValue, toAmount, toTokenSymbol, price, txHash, fromToken, listid, totoken, true, LabelFirstBuy, tomc)

	fmt.Printf("\nmsg:\n%s\n", msg)

	url := "https://api.telegram.org/bot7727332343333:AAGfl6k1zOS4-huCdqi-4a1DWRczt2JmkMQ/sendMessage"
	method := "POST"

	bd := TgTestBody{
		ChatID:      "-4718631554",
		Text:        msg,
		ReplyMarkup: makeMarkup(listid, "Solana", totoken),
		ParseMode:   "MarkdownV2",
	}

	bdbytes, err := json.Marshal(&bd)
	if err != nil {
		fmt.Println(err)
		return
	}

	payloadstr := string(bdbytes)

	fmt.Printf("\nmsg:\n%s\n", payloadstr)

	// 	payloadstr := fmt.Sprintf(`{
	//     "chat_id": "-4718631554",
	//     "text": "%s",
	//     "parse_mode": "MarkdownV2",
	//     "disable_web_page_preview": true
	// }`, msg)

	payload := strings.NewReader(payloadstr)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))

}

func TestExchangeSendBotMsg(t *testing.T) {
	listid := "1866693246224470016"
	exchangename := "coinbase"
	chain := "Solana"
	tokenAddress := "2qEHjDLDLbuBgRYvsxhc5D6uDWAivNFZGan56P1tpump"
	// chain := ""
	// tokenAddress := ""
	tokensymbol := "PNUT-USDT"
	title := "Assets added to the roadmap today: Peanut the Squirrel (PNUT)"
	annouurl := "https://t.co/rRB9d3hSr2"

	msg := ConstructExchangeMessage(listid, exchangename, chain, tokenAddress, tokensymbol, title, annouurl)

	bd := TgTestBody{
		ChatID:      "-4718631554",
		Text:        msg,
		ReplyMarkup: makeMarkup("", chain, tokenAddress),
		ParseMode:   "MarkdownV2",
	}

	bdbytes, err := json.Marshal(&bd)
	if err != nil {
		fmt.Println(err)
		return
	}

	payloadstr := string(bdbytes)

	fmt.Printf("\nmsg:\n%s\n", payloadstr)

	url := "https://api.telegram.org/bot7727332343333:AAGfl6k1zOS4-huCdqi-4a1DWRczt2JmkMQ/sendMessage"
	method := "POST"

	payload := strings.NewReader(payloadstr)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))

}

func TestKOLSendBotMsg(t *testing.T) {
	listid := "1866401756338679808"
	author := "Bullrun_Gravano"
	sentiment := "Negative"
	// chain := ""
	// contractAddr := ""
	chain := "solana"
	contractAddr := "2qEHjDLDLbuBgRYvsxhc5D6uDWAivNFZGan56P1tpump"
	tokensymbol := "JUP"
	mcap := "1300"
	tweeturl := "https://twitter.com/0xFinish/status/1867341490017775968"
	isverified := false

	msg := ConstructKOLMessage(listid, author, sentiment, chain, contractAddr, tokensymbol, mcap, tweeturl, isverified)

	bd := TgTestBody{
		ChatID:      "-4718631554",
		Text:        msg,
		ReplyMarkup: makeMarkup(listid, chain, contractAddr),
		ParseMode:   "MarkdownV2",
	}

	bdbytes, err := json.Marshal(&bd)
	if err != nil {
		fmt.Println(err)
		return
	}

	payloadstr := string(bdbytes)

	fmt.Printf("\nmsg:\n%s\n", payloadstr)

	url := "https://api.telegram.org/bot7727332343333:AAGfl6k1zOS4-huCdqi-4a1DWRczt2JmkMQ/sendMessage"
	method := "POST"

	payload := strings.NewReader(payloadstr)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))

}

func TestCuratedSendBotMsg(t *testing.T) {
	listid := "1866401756338679808"
	calltype := "DecreasePosition"
	chainstr := "Solana"
	contractAddr := "So11111111111111111111111111111111111111112"
	tokensymbolstr := "SOL"
	thesis := "Sell 30%"
	mcap := "127848504128.12038"
	price := "216.55274729727435"
	amount := "30"

	msg := ConstructCuratedMessage(listid, calltype, chainstr, contractAddr, tokensymbolstr, thesis, mcap, price, amount)

	fmt.Printf("\nmsg:\n%s\n", msg)

	url := "https://api.telegram.org/bot7727332343333:AAGfl6k1zOS4-huCdqi-4a1DWRczt2JmkMQ/sendMessage"
	method := "POST"

	bd := TgTestBody{
		ChatID:      "-4718631554",
		Text:        msg,
		ReplyMarkup: makeMarkup("", chainstr, contractAddr),
		ParseMode:   "MarkdownV2",
	}

	bdbytes, err := json.Marshal(&bd)
	if err != nil {
		fmt.Println(err)
		return
	}

	payloadstr := string(bdbytes)

	fmt.Printf("\nmsg:\n%s\n", payloadstr)

	payload := strings.NewReader(payloadstr)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))

}

func TestAddrCreateBotMsg(t *testing.T) {

	chain := "solana"
	fromLabel := "testfun"
	fromAccount := "CqWxsVx3y9sufAq2tFvxEupbnBggWYdRCqeqhnPThYEw"
	toTokenSymbol := "AAAV"
	listid := "1867048652718120960"
	totoken := "gdH7zquvRMvsFRihyECcHdtKH145CKqCgKYkD8Epump"

	msg := ConstructCreateBotMessage(chain, fromLabel, fromAccount, toTokenSymbol, listid, totoken, false)

	bd := TgTestBody{
		ChatID:      "-4718631554",
		Text:        msg,
		ReplyMarkup: makeMarkup(listid, chain, totoken),
		ParseMode:   "MarkdownV2",
	}

	bdbytes, err := json.Marshal(&bd)
	if err != nil {
		fmt.Println(err)
		return
	}

	payloadstr := string(bdbytes)

	fmt.Printf("\nmsg:\n%s\n", payloadstr)

	url := "https://api.telegram.org/bot7727332343333:AAGfl6k1zOS4-huCdqi-4a1DWRczt2JmkMQ/sendMessage"
	method := "POST"

	payload := strings.NewReader(payloadstr)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))

}

func TestFomoCallMsg(t *testing.T) {

	data := &RawFomoCallsData{
		ListID:          "1866401756338679808",
		TokenAddress:    "So11111111111111111111111111111111111111112",
		TokenSymbol:     "WSOL",
		Chain:           "solana",
		BuyWalletCount:  15,
		SellWalletCount: 2,
		BuyAmount:       15.8,
		SellAmount:      6.9,
		BuySolAmount:    10,
		BuyAmountValue:  200.7,

		SellAmountValue: 100.5,

		MultyBuy:     true,
		SpacingTime:  7,
		AvgSellPrice: 199.8,
		AvgBuyPrice:  205.9,
		Timestamp:    100,
	}

	msg := ConstructFomoMessage(data)

	url := "https://api.telegram.org/bot7727332343333:AAGfl6k1zOS4-huCdqi-4a1DWRczt2JmkMQ/sendMessage"
	method := "POST"

	bd := TgTestBody{
		ChatID:      "-4718631554",
		Text:        msg,
		ReplyMarkup: makeMarkup(data.ListID, "Solana", data.TokenAddress),
		ParseMode:   "MarkdownV2",
	}

	bdbytes, err := json.Marshal(&bd)
	if err != nil {
		fmt.Println(err)
		return
	}

	payloadstr := string(bdbytes)

	fmt.Printf("\nmsg:\n%s\n", payloadstr)

	payload := strings.NewReader(payloadstr)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))

}

func TestHandleFomoCallMsg(t *testing.T) {
	configPath := flag.String("config_path", "./", "config file")
	logicLogFile := flag.String("logic_log_file", "./log/sol_consumer.log", "logic log file")
	flag.Parse()
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

	data := &RawFomoCallsData{
		ListID:          "1875132933630562304",
		TokenAddress:    "So11111111111111111111111111111111111111112",
		TokenSymbol:     "WSOL",
		Chain:           "solana",
		BuyWalletCount:  15,
		SellWalletCount: 2,
		BuyAmount:       15.8,
		SellAmount:      6.9,
		BuySolAmount:    10,
		BuyAmountValue:  200.7,

		SellAmountValue: 100.5,

		MultyBuy:     true,
		SpacingTime:  7,
		AvgSellPrice: 199.8,
		AvgBuyPrice:  205.9,
		Timestamp:    time.Now().Unix(),
	}

	err = handleFomoCalls(data)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func TestGetRPCTokenMetaData(t *testing.T) {
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

	data, err := getSolanaMeta("3aN1pCEKEAuya1cWM7tNF6W6Y1T3HzKStdsFbHRdpFbE")
	if err != nil {
		fmt.Printf("%v", err)
		return
	}

	fmt.Printf("meta: %v\n", data)

	select {}
}

func TestUpdate(t *testing.T) {
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

	var resAddr []model.SolTxRecord
	query := `select * from scope_lmk.lmk_sol_data where from_token_symbol = '' ORDER BY create_at desc limit 5000`

	err = db.GetDB().NewRaw(query).Scan(context.Background(), &resAddr)
	if err != nil {
		fmt.Printf("scan db failed, %v", err)
		return
	}

	for i, v := range resAddr {
		fromMeta, err := getSolanaMeta(v.FromToken)
		if err != nil {
			fmt.Printf("from failed, %v", err)
			continue
		}

		toMeta, err := getSolanaMeta(v.ToToken)
		if err != nil {
			fmt.Printf("to failed, %v", err)
			continue
		}

		resAddr[i].FromTokenSymbol = fromMeta.Symbol
		resAddr[i].ToTokenSymbol = toMeta.Symbol

		if v.Direction == "Bought" || v.Direction == "TRANSFER" || v.Direction == "Create" {
			resAddr[i].Supply = toMeta.TotalSupply
		} else {
			resAddr[i].Supply = fromMeta.TotalSupply
		}

	}

	for _, v := range resAddr {
		if v.FromTokenSymbol == "" {
			continue
		}

		fmt.Printf("txhash: %v\n\n", v.TxHash)

		_, err = db.GetDB().NewUpdate().
			Model(&v).
			Column("from_token_symbol", "to_token_symbol", "supply"). // 指定要更新的字段
			WherePK().
			Exec(context.Background())
		if err != nil {
			fmt.Printf("update failed, %v", err)
			return
		}
	}
}

func TestHandleSolBuy(t *testing.T) {
	configPath := flag.String("config_path", "./", "config file")
	logicLogFile := flag.String("logic_log_file", "./log/sol_consumer.log", "logic log file")
	flag.Parse()
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

	data := model.SolSwapData{
		TxHash:    "22GHSWjrNTKhYbrHHZ8vXrp8193uHFCXBHf9EGKPArPgVNVYX8HcsV5MpYoygs4R7r66fJjBXCwDa8U2vg5cEaG2",
		Source:    "Jupiter Aggregator v6",
		Timestamp: 1736943217,
		Type:      "SWAP",
		Date:      "2025-01-15 12:13:37",

		FromToken:        "So11111111111111111111111111111111111111112",
		FromTokenAccount: "7Ja8MzwtbtC1HsGbUqKFM3XgwZtbQbP6GeFSRKJXY199",
		FromUserAccount:  "F1cKJWLwnNwDbPPNEBUM1DBVnsfi4nsggU525Pvh3WQd",
		FromTokenAmount:  0.01,

		ToToken:        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
		ToTokenAccount: "98fSXwCZFF2dqK9AqUkeEphwBPNYpAHEZz3xyaornp6Q",
		ToUserAccount:  "F1cKJWLwnNwDbPPNEBUM1DBVnsfi4nsggU525Pvh3WQd",
		ToTokenAmount:  1.8484829999999999,
	}

	err = handleSOlBuy(data)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func TestHandleBotTg(t *testing.T) {
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

	err = HandleTgBotMessage("1879473606096871424", "test", "Solana", "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh", int(time.Now().Unix()), true)
	if err != nil {
		log.Fatal("HandleTgBotMessage failed:", err)
	}
}
