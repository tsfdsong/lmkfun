package solalter

import (
	"fmt"
	"math"
	"strconv"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type SolMetaDataCache struct {
	Address       string  `json:"address"`
	Decimals      int     `json:"decimals"`
	Symbol        string  `json:"symbol"`
	Name          string  `json:"name"`
	Mc            float64 `json:"mc"`
	Price         float64 `json:"price"`
	Change1hPrice float64 `json:"price_change_1h"`
	TotalSupply   string  `json:"total_supply"`

	AgeTime      int64   `json:"age_time"`
	Volume24H    float64 `json:"volume_24h"`
	HoldersCount int64   `json:"holder_count"`
}

func isFloatEqual(a float64) bool {
	return math.Abs(a) > 1e-9
}

func GetSolMetaDataCache(chain, token string) (*SolMetaDataCache, error) {
	scanData, err := GetTokenMetaCache(token)
	if err == nil {
		if isFloatEqual(scanData.MarketCap) {
			res := &SolMetaDataCache{
				Address:       scanData.Address,
				Decimals:      scanData.Decimals,
				Symbol:        scanData.Symbol,
				Name:          scanData.Name, 
				Mc:            scanData.MarketCap,
				Price:         scanData.Price,
				Change1hPrice: scanData.PriceChange24H,
				TotalSupply:   scanData.Supply,

				AgeTime:      int64(scanData.CreatedTime),
				Volume24H:    scanData.Volume24H,
				HoldersCount: int64(scanData.Holder),
			}

			return res, nil
		}

		if scanData.Symbol != "" {
			res := &SolMetaDataCache{
				Address:       scanData.Address,
				Decimals:      scanData.Decimals,
				Symbol:        scanData.Symbol,
				Name:          scanData.Name,
				Mc:            scanData.MarketCap,
				Price:         scanData.Price,
				Change1hPrice: scanData.PriceChange24H,
				TotalSupply:   scanData.Supply,
				AgeTime:       int64(scanData.CreatedTime),
				Volume24H:     scanData.Volume24H,
				HoldersCount:  int64(scanData.Holder),
			}

			if !isFloatEqual(scanData.Price) {
				birdeyeData, _ := GetBrideeyeCache("solana", token)

				if birdeyeData != nil && isFloatEqual(birdeyeData.Price) {
					res.Price = birdeyeData.Price
					supply, _ := strconv.ParseFloat(scanData.Supply, 64)
					res.Mc = birdeyeData.Price * supply
				}
			}

			logger.Logrus.WithFields(logrus.Fields{"Token": res}).Info("GetSolMetaDataCache GetTokenMetaCache success")

			return res, nil
		}
	} else {
		logger.Logrus.WithFields(logrus.Fields{"Chain": chain, "Token": token, "ErrMsg": err}).Error("GetSolMetaDataCache GetTokenMetaCache failed")
	}

	birdeyeData, err := GetBrideeyeCache("solana", token)
	if err != nil {
		return nil, err
	}

	if birdeyeData.Symbol != "" {
		res := &SolMetaDataCache{
			Address:       birdeyeData.Address,
			Decimals:      birdeyeData.Decimals,
			Symbol:        birdeyeData.Symbol,
			Name:          birdeyeData.Name,
			Mc:            birdeyeData.Mc,
			Price:         birdeyeData.Price,
			Change1hPrice: birdeyeData.Change1hPrice,
			TotalSupply:   fmt.Sprintf("%f", birdeyeData.TotalSupply),

			AgeTime:      birdeyeData.AgeTime,
			Volume24H:    birdeyeData.Volume24H,
			HoldersCount: birdeyeData.HoldersCount,
		}

		return res, nil
	}

	return nil, fmt.Errorf("sol metadata get failed for %s", token)
}
