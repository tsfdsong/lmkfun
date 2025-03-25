package solalter

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type RawCuratedTokenCallsData struct {
	ListID          string `json:"list_id"`
	CallType        string `json:"call_type"`
	Chain           string `json:"chain"`
	ContractAddress string `json:"contract"`
	TokenSymbol     string `json:"token_symbol"`
	Thesis          string `json:"thesis"`
	MarketCap       string `json:"mcap"`
	Price           string `json:"price"`
	SuggestedAmount string `json:"suggested_amount"`
	Timestamp       int    `json:"timestamp"`
}

func handleCuratedCalls(data *RawCuratedTokenCallsData) error {
	if data.Chain == "" || data.ContractAddress == "" || data.TokenSymbol == "" {
		return fmt.Errorf("CA is empty, %s, %s, %s", data.Chain, data.ContractAddress, data.TokenSymbol)
	}

	botbody := ConstructCuratedMessage(data.ListID, data.CallType, data.Chain, data.ContractAddress, data.TokenSymbol, data.Thesis, data.MarketCap, data.Price, data.SuggestedAmount)

	err := HandleTgBotMessage(data.ListID, botbody, data.Chain, data.ContractAddress, int(data.Timestamp), false)
	if err != nil {
		return fmt.Errorf("handle tg bot failed, %v", err)
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": botbody}).Info("handleCuratedCalls send bot msg info")

	return nil
}
