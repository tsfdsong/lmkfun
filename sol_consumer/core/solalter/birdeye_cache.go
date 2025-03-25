package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type BirdDetail struct {
	Address              string  `json:"address"`
	Decimals             int     `json:"decimals"`
	Symbol               string  `json:"symbol"`
	Name                 string  `json:"name"`
	LogoURI              string  `json:"logoURI"`
	Liquidity            float64 `json:"liquidity"`
	Price                float64 `json:"price"`
	Supply               float64 `json:"supply"`
	Mc                   float64 `json:"mc"`
	History1HPrice       float64 `json:"history1hPrice"`
	PriceChange1HPercent float64 `json:"priceChange1hPercent"`
	Volume24H            float64 `json:"v24hUSD"`
	Holders              int     `json:"holder"`
}

type BirdeyeData struct {
	Data    BirdDetail `json:"data"`
	Success bool       `json:"success"`
}

type BirdeyeCacheData struct {
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

func getBirdeyeToken(chain, contractAddr string) (*BirdeyeCacheData, error) {
	nchain := chain
	if strings.ToLower(chain) == "eth" || strings.ToLower(chain) == "ethereum" {
		nchain = "ethereum"
		if contractAddr == "0x" || contractAddr == "0x0000000000000000000000000000000000000000" {
			contractAddr = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
		}

	} else if strings.ToLower(chain) == "base" {
		nchain = "base"

		if contractAddr == "0x" || contractAddr == "0x0000000000000000000000000000000000000000" {
			contractAddr = "0x4200000000000000000000000000000000000006"
		}
	} else if strings.ToLower(chain) == "solana" {
		nchain = "solana"
		if contractAddr == "0x0000000000000000000000000000000000000000" {
			contractAddr = "So11111111111111111111111111111111111111112"
		}
	} else if strings.ToLower(chain) == "arb" {
		nchain = "arbitrum"
		if contractAddr == "0x0000000000000000000000000000000000000000" {
			contractAddr = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
		}
	} else if strings.ToLower(chain) == "avax" {
		nchain = "avalanche"
		if contractAddr == "0x0000000000000000000000000000000000000000" {
			contractAddr = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
		}
	} else if strings.ToLower(chain) == "optimistic-ethereum" {
		nchain = "optimism"
		if contractAddr == "0x0000000000000000000000000000000000000000" {
			contractAddr = "0x4200000000000000000000000000000000000006"
		}
	} else if strings.ToLower(chain) == "polygon-pos" {
		nchain = "polygon"
		if contractAddr == "0x0000000000000000000000000000000000000000" {
			contractAddr = "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270"
		}
	} else if strings.ToLower(chain) == "sui" {
		nchain = "sui"
		if contractAddr == "0x0000000000000000000000000000000000000000" {
			contractAddr = "0x2::sui::SUI"
		}
	} else if strings.ToLower(chain) == "zksync" {
		nchain = "zksync"
		if contractAddr == "0x0000000000000000000000000000000000000000" {
			contractAddr = "0x5A7d6b2F92C77FAD6CCaBd7EE0624E64907Eaf3E"
		}
	} else if strings.ToLower(chain) == "bsc" {
		nchain = "bsc"
		if contractAddr == "0x" || contractAddr == "0x0000000000000000000000000000000000000000" {
			contractAddr = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
		}
	}

	url := fmt.Sprintf("https://public-api.birdeye.so/defi/token_overview?address=%s", contractAddr)
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return nil, err
	}

	apiKey := config.GetSolDataConfig().BirdeyeAPIKey

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-API-KEY", apiKey)
	req.Header.Add("x-chain", nchain)

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("%s,%s, %v", chain, contractAddr, res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var data BirdeyeData
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}

	resData := &BirdeyeCacheData{
		Address:       data.Data.Address,
		Decimals:      data.Data.Decimals,
		Symbol:        data.Data.Symbol,
		Name:          data.Data.Name,
		Icon:          data.Data.LogoURI,
		Mc:            data.Data.Mc,
		Price:         data.Data.Price,
		Change1hPrice: data.Data.History1HPrice,
		TotalSupply:   data.Data.Supply,
		AgeTime:       0,
		Volume24H:     data.Data.Volume24H,
		HoldersCount:  int64(data.Data.Holders),
	}

	return resData, nil
}

func SetBirdeeyeCache(chain, token string) error {
	key := fmt.Sprintf("meta:birdeye:%s:%s", chain, token)
	isexists, err := redis.Exists(context.Background(), key)
	if err != nil {
		return fmt.Errorf("check exists failed, %v", err)
	}

	if isexists {
		return nil
	} else {
		data, err := getBirdeyeToken(chain, token)
		if err != nil {
			return fmt.Errorf("birdeye get failed, { %v }", err)
		}

		bt, err := json.Marshal(&data)
		if err != nil {
			return fmt.Errorf("marshal failed, %v", err)
		}

		err = redis.Set(context.Background(), key, string(bt), time.Minute)
		if err != nil {
			return fmt.Errorf("redis set failed, %v", err)
		}

		go func(in *BirdeyeCacheData) {
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
		}(data)
	}

	return nil
}

func GetBrideeyeCache(chain, token string) (*BirdeyeCacheData, error) {
	key := fmt.Sprintf("meta:birdeye:%s:%s", chain, token)
	bt, err := redis.Get(context.Background(), key)
	if err == redis.Nil {
		err = SetBirdeeyeCache(chain, token)
		if err != nil {
			return nil, fmt.Errorf("set cache, %v", err)
		}

		bt, err = redis.Get(context.Background(), key)
		if err != nil {
			return nil, fmt.Errorf("redis get, %v", err)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("get failed, %v", err)
	}

	var data BirdeyeCacheData

	err = json.Unmarshal([]byte(bt), &data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal failed, %v", err)
	}

	if data.Symbol == "" {
		return nil, fmt.Errorf("%s symbol is empty", token)
	}

	return &data, nil
}
