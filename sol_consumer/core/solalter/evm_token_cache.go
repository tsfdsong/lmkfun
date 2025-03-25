package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type EVMMetaDataCacheData struct {
	Address       string  `json:"address"`
	Decimals      int     `json:"decimals"`
	Symbol        string  `json:"symbol"`
	Name          string  `json:"name"`
	Icon          string  `json:"logoURI"`
	Mc            float64 `json:"mc"`
	Price         float64 `json:"price"`
	Change1hPrice float64 `json:"price_change_1h"`
	TotalSupply   float64 `json:"supply"`

	AgeTime      int64   `json:"age_time"`
	Volume24H    float64 `json:"volume_24h"`
	HoldersCount int64   `json:"holder_count"`
}

type BNBLatestPriceData struct {
	TradingPair    string  `bun:"trading_pair"`
	TardingTime    string  `bun:"trading_time"`
	LabelO         float64 `bun:"o"`
	LabelL         float64 `bun:"l"`
	LabelH         float64 `bun:"h"`
	LabelC         float64 `bun:"c"`
	SourceData     string  `bun:"source_data"`
	ScopeTimestamp string  `bun:"scope_timestamp"`
}

func getBNBLatestPrice() (float64, error) {
	var resAddr BNBLatestPriceData
	query := `SELECT
    *
FROM
    crawler_ods.ods_crawler_binance_kline
WHERE
    trading_pair = 'BNBUSDT'
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

func GetEVMTokenMetaData(chain, tokenAddress string) (*EVMMetaDataCacheData, error) {
	key := fmt.Sprintf("meta:evm:%s:%s", chain, tokenAddress)
	bt, err := redis.Get(context.Background(), key)
	if err == nil {
		var data EVMMetaDataCacheData
		err = json.Unmarshal([]byte(bt), &data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal evm failed, %v", err)
		}

		return &data, nil
	}

	exptime := 24 * 60 * time.Minute

	resData := &EVMMetaDataCacheData{
		Address:       tokenAddress,
		Decimals:      0,
		Symbol:        "",
		Name:          "",
		Icon:          "",
		Mc:            0,
		Price:         0,
		Change1hPrice: 0,
		TotalSupply:   0,
		AgeTime:       0,
		Volume24H:     0,
		HoldersCount:  0,
	}
	if chain == "bsc" && tokenAddress == "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c" {
		exptime = time.Minute

		resData.Decimals = 18
		resData.Symbol = "WBNB"
		resData.Name = "WBNB"
		resData.Icon = "https://assets.coingecko.com/coins/images/12591/small/binance-coin-logo.png?1600947313"
		resData.AgeTime = 0

		lprice, err := getBNBLatestPrice()
		if err != nil {
			return nil, fmt.Errorf("get bnb price failed, %v", err)
		}

		resData.Price = lprice
	} else if chain == "bsc" && tokenAddress == "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d" {
		resData.Decimals = 18
		resData.Symbol = "USDC"
		resData.Name = "USD Coin"
		resData.Icon = "https://assets.coingecko.com/coins/images/6319/small/USD_Coin_icon.png?1547042389"
		resData.Price = float64(1.0)
		resData.AgeTime = 0
	} else if chain == "bsc" && tokenAddress == "0x55d398326f99059ff775485246999027b3197955" {
		resData.Decimals = 18
		resData.Symbol = "USDT"
		resData.Name = "Tether"
		resData.Icon = "https://assets.coingecko.com/coins/images/325/small/Tether.png?1668148663"
		resData.Price = float64(1.0)
		resData.AgeTime = 0
	} else {
		birdeyetokenMeta, err := getBirdeyeToken(chain, tokenAddress)
		if err != nil {
			return nil, fmt.Errorf("birdeye get failed { %v }", err)
		}

		resData.Address = birdeyetokenMeta.Address
		resData.Decimals = birdeyetokenMeta.Decimals
		resData.Symbol = birdeyetokenMeta.Symbol
		resData.Name = birdeyetokenMeta.Name
		resData.Icon = birdeyetokenMeta.Icon
		resData.Mc = birdeyetokenMeta.Mc
		resData.Price = birdeyetokenMeta.Price
		resData.Change1hPrice = birdeyetokenMeta.Change1hPrice
		resData.TotalSupply = birdeyetokenMeta.TotalSupply
		resData.AgeTime = birdeyetokenMeta.AgeTime
		resData.Volume24H = birdeyetokenMeta.Volume24H
		resData.HoldersCount = birdeyetokenMeta.HoldersCount
	}

	bytes, err := json.Marshal(&resData)
	if err != nil {
		return nil, fmt.Errorf("marshal failed, %v", err)
	}

	err = redis.Set(context.Background(), key, string(bytes), exptime)
	if err != nil {
		return nil, fmt.Errorf("redis set failed, %v", err)
	}

	go func(in *EVMMetaDataCacheData) {
		record := model.SolDimTokensRecord{
			TokenAddress:     in.Address,
			Name:             in.Name,
			Symbol:           in.Symbol,
			Decimals:         in.Decimals,
			IsERC20:          true,
			IsERC721:         false,
			IsERC1155:        false,
			FromAddress:      "",
			TransactionHash:  "",
			TransactionIndex: 0,
			BlockNumber:      0,
			BlockTimestamp:   0,
			ExtraInfo:        "",
			IsValid:          true,
			IsSupported:      false,
			IsImageSupported: 0,
			TotalSupply:      strconv.FormatFloat(in.TotalSupply, 'f', -1, 64),
			Image:            in.Icon,
		}

		_, err := db.GetDBDim().NewInsert().Model(&record).On("CONFLICT (token_address) DO UPDATE").Set("total_supply = EXCLUDED.total_supply").Set("icon = EXCLUDED.icon").Exec(context.Background())
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"Data": record, "ErrMsg": err}).Error("birdeye insert solana dim tokens record faild")
			return
		}

		logger.Logrus.WithFields(logrus.Fields{"Data": record}).Info("birdeye insert solana dim tokens record success")
	}(resData)

	return resData, nil
}
