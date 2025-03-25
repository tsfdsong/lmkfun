package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
)

type AttributeDetail struct {
	Address           string `json:"address"`
	Name              string `json:"name"`
	Symbol            string `json:"symbol"`
	Decimals          int    `json:"decimals"`
	ImageURL          string `json:"image_url"`
	CoingeckoCoinID   string `json:"coingecko_coin_id"`
	TotalSupply       string `json:"total_supply"`
	PriceUsd          string `json:"price_usd"`
	FdvUsd            string `json:"fdv_usd"`
	TotalReserveInUsd string `json:"total_reserve_in_usd"`
	VolumeUsd         struct {
		H24 string `json:"h24"`
	} `json:"volume_usd"`
	MarketCapUsd string `json:"market_cap_usd"`
}

type CoingeckoAttributes struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Attributes    AttributeDetail `json:"attributes"`
	Relationships struct {
		TopPools struct {
			Data []struct {
				ID   string `json:"id"`
				Type string `json:"type"`
			} `json:"data"`
		} `json:"top_pools"`
	} `json:"relationships"`
}

type CoingeckoTokenData struct {
	Data []CoingeckoAttributes `json:"data"`
}

func getCoingeckoToken(chain, contractAddr string) (*AttributeDetail, error) {
	nchain := chain
	if strings.ToLower(chain) == "eth" || strings.ToLower(chain) == "ethereum" {
		nchain = "eth"
	} else if strings.ToLower(chain) == "base" {
		nchain = "base"
	}
	url := fmt.Sprintf("https://pro-api.coingecko.com/api/v3/onchain/networks/%s/tokens/multi/%s", nchain, contractAddr)
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	apyKey := config.GetSolDataConfig().CoingeckoAPIKey
	req.Header.Add("x-cg-pro-api-key", apyKey)

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

	var data CoingeckoTokenData
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}

	if len(data.Data) != 1 {
		return nil, fmt.Errorf("%s,%s, data length invalided", chain, contractAddr)
	}

	attrs := data.Data[1].Attributes

	return &attrs, nil
}

func SetCoingeckoCache(chain, token string) error {
	key := fmt.Sprintf("meta:coingecko:%s:%s", chain, token)
	isexists, err := redis.Exists(context.Background(), key)
	if err != nil {
		return err
	}

	if isexists {
		return nil
	} else {
		data, err := getCoingeckoToken(chain, token)
		if err != nil {
			return err
		}

		if data.Symbol == "" {
			return fmt.Errorf("%s cannot get symbol", token)
		}

		bt, err := json.Marshal(&data)
		if err != nil {
			return err
		}

		err = redis.Set(context.Background(), key, string(bt), 30*time.Minute)
		if err != nil {
			return err
		}
	}

	return nil
}

func GetCoingeckoCache(chain, token string) (*AttributeDetail, error) {
	key := fmt.Sprintf("meta:coingecko:%s:%s", chain, token)
	bt, err := redis.Get(context.Background(), key)
	if err == redis.Nil {
		err = SetCoingeckoCache(chain, token)
		if err != nil {
			return nil, err
		}

		bt, err = redis.Get(context.Background(), key)
	}

	if err != nil {
		return nil, err
	}

	var data AttributeDetail

	err = json.Unmarshal([]byte(bt), &data)
	if err != nil {
		return nil, err
	}

	if data.Symbol == "" {
		return nil, fmt.Errorf("%s symbol is empty", token)
	}

	return &data, nil
}
