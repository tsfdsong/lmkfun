package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
)

type SolStableCoinDataCache struct {
	Address  string  `json:"address"`
	Decimals int     `json:"decimals"`
	Symbol   string  `json:"symbol"`
	Name     string  `json:"name"`
	Price    float64 `json:"price"`
	AgeTime  int64   `json:"age_time"`
}

type SOLLatestPriceData struct {
	TradingPair    string  `bun:"trading_pair"`
	TardingTime    string  `bun:"trading_time"`
	LabelO         float64 `bun:"o"`
	LabelL         float64 `bun:"l"`
	LabelH         float64 `bun:"h"`
	LabelC         float64 `bun:"c"`
	SourceData     string  `bun:"source_data"`
	ScopeTimestamp string  `bun:"scope_timestamp"`
}

func getSolLatestPrice() (float64, error) {
	var resAddr BNBLatestPriceData
	query := `SELECT
    *
FROM
    crawler_ods.ods_crawler_binance_kline
WHERE
    trading_pair = 'SOLUSDT'
ORDER BY
    trading_time DESC
LIMIT 1;`

	err := db.GetDB().NewRaw(query).Scan(context.Background(), &resAddr)
	if err != nil {
		return 0, err
	}

	lprice := resAddr.LabelC
	return lprice, nil
}

func GetSolStableCoinMetaData(tokenAddress string) (*SolStableCoinDataCache, error) {
	key := fmt.Sprintf("meta:stablecoin:solana:%s", tokenAddress)
	bt, err := redis.Get(context.Background(), key)
	if err == nil {
		var data SolStableCoinDataCache
		err = json.Unmarshal([]byte(bt), &data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal evm failed, %v", err)
		}

		return &data, nil
	}

	exptime := 24 * 60 * time.Minute

	resData := &SolStableCoinDataCache{
		Address:  tokenAddress,
		Decimals: 0,
		Symbol:   "",
		Name:     "",
		Price:    0,
		AgeTime:  0,
	}
	if tokenAddress == "So11111111111111111111111111111111111111112" {
		exptime = time.Minute

		resData.Decimals = 9
		resData.Symbol = "Wrapped SOL"
		resData.Name = "SOL"
		resData.AgeTime = 0

		lprice, err := getSolLatestPrice()
		if err != nil {
			return nil, fmt.Errorf("get sol price failed, %v", err)
		}

		resData.Price = lprice
	} else if tokenAddress == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" {
		resData.Decimals = 6
		resData.Symbol = "USDC"
		resData.Name = "USD Coin"
		resData.Price = float64(1.0)
		resData.AgeTime = 1721427641
	} else if tokenAddress == "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" {
		resData.Decimals = 6
		resData.Symbol = "USDT"
		resData.Name = "USDT"
		resData.Price = float64(1.0)
		resData.AgeTime = 1737577814
	} else {
		return nil, fmt.Errorf("%s not stable coin, not supported", tokenAddress)
	}

	bytes, err := json.Marshal(&resData)
	if err != nil {
		return nil, fmt.Errorf("marshal failed, %v", err)
	}

	err = redis.Set(context.Background(), key, string(bytes), exptime)
	if err != nil {
		return nil, fmt.Errorf("redis set failed, %v", err)
	}

	return resData, nil
}
