package solalter

import (
	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type RawFomoCallsData struct {
	ListID          string  `json:"list_id"`
	TokenAddress    string  `json:"token_address"`
	TokenSymbol     string  `json:"token_symbol"`
	Chain           string  `json:"chain"`
	BuyWalletCount  int64   `json:"bought_wallet_count"`
	SellWalletCount int64   `json:"sold_wallet_count"`
	BuyAmount       float64 `json:"buy_amount"`
	SellAmount      float64 `json:"sell_amount"`
	BuySolAmount    float64 `json:"buy_sol_amount"`
	BuyAmountValue  float64 `json:"buy_amount_value"`

	SellAmountValue float64 `json:"sell_amount_value"`
	AvgBuyMc        float64 `json:"average_buy_market_cap"`

	MultyBuy     bool    `json:"multiple_buy"`
	SpacingTime  int64   `json:"time"`
	AvgSellPrice float64 `json:"average_sell_price"`
	AvgBuyPrice  float64 `json:"average_buy_price"`
	Timestamp    int64   `json:"timestamp"`
}

func handleFomoCalls(data *RawFomoCallsData) error {
	logger.Logrus.WithFields(logrus.Fields{"Data": data}).Info("handleFomoCalls info")

	botbody := ConstructFomoMessage(data)

	err := HandleTgBotMessage(data.ListID, botbody, data.Chain, data.TokenAddress, int(data.Timestamp), true)
	if err != nil {
		return err
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": botbody}).Info("handleFomoCalls send bot msg success")

	return nil
}
