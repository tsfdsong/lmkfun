package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type TokenMetaData struct {
	Address         string      `json:"address"`
	Name            string      `json:"name"`
	Symbol          string      `json:"symbol"`
	Icon            string      `json:"icon"`
	Decimals        int         `json:"decimals"`
	Holder          int         `json:"holder"`
	Creator         string      `json:"creator"`
	CreateTx        string      `json:"create_tx"`
	CreatedTime     int         `json:"created_time"`
	FirstMintTx     string      `json:"first_mint_tx"`
	FirstMintTime   int         `json:"first_mint_time"`
	MintAuthority   interface{} `json:"mint_authority"`
	FreezeAuthority interface{} `json:"freeze_authority"`
	Supply          string      `json:"supply"`
	Price           float64     `json:"price"`
	Volume24H       float64     `json:"volume_24h"`
	MarketCap       float64     `json:"market_cap"`
	MarketCapRank   int         `json:"market_cap_rank"`
	PriceChange24H  float64     `json:"price_change_24h"`
}

type MetaResponse struct {
	Success  bool          `json:"success"`
	Data     TokenMetaData `json:"data"`
	Metadata interface{}   `json:"metadata"`
}

type TokenSecurityData struct {
	BalanceMutableAuthority struct {
		Authority []any  `json:"authority"`
		Status    string `json:"status"`
	} `json:"balance_mutable_authority"`
	Closable struct {
		Authority []any  `json:"authority"`
		Status    string `json:"status"`
	} `json:"closable"`
	Creators                      []any  `json:"creators"`
	DefaultAccountState           string `json:"default_account_state"`
	DefaultAccountStateUpgradable struct {
		Authority []any  `json:"authority"`
		Status    string `json:"status"`
	} `json:"default_account_state_upgradable"`
	Dex []struct {
		Day struct {
			PriceMax string `json:"price_max"`
			PriceMin string `json:"price_min"`
			Volume   string `json:"volume"`
		} `json:"day"`
		DexName  string `json:"dex_name"`
		FeeRate  string `json:"fee_rate"`
		ID       string `json:"id"`
		LpAmount any    `json:"lp_amount"`
		Month    struct {
			PriceMax string `json:"price_max"`
			PriceMin string `json:"price_min"`
			Volume   string `json:"volume"`
		} `json:"month"`
		OpenTime string `json:"open_time"`
		Price    string `json:"price"`
		Tvl      string `json:"tvl"`
		Type     string `json:"type"`
		Week     struct {
			PriceMax string `json:"price_max"`
			PriceMin string `json:"price_min"`
			Volume   string `json:"volume"`
		} `json:"week"`
	} `json:"dex"`
	Freezable struct {
		Authority []any  `json:"authority"`
		Status    string `json:"status"`
	} `json:"freezable"`
	Holders []struct {
		Account      string `json:"account"`
		Balance      string `json:"balance"`
		IsLocked     int    `json:"is_locked"`
		LockedDetail []any  `json:"locked_detail"`
		Percent      string `json:"percent"`
		Tag          string `json:"tag"`
		TokenAccount string `json:"token_account"`
	} `json:"holders"`
	LpHolders []any `json:"lp_holders"`
	Metadata  struct {
		Description string `json:"description"`
		Name        string `json:"name"`
		Symbol      string `json:"symbol"`
		URI         string `json:"uri"`
	} `json:"metadata"`
	MetadataMutable struct {
		MetadataUpgradeAuthority []struct {
			Address          string `json:"address"`
			MaliciousAddress int    `json:"malicious_address"`
		} `json:"metadata_upgrade_authority"`
		Status string `json:"status"`
	} `json:"metadata_mutable"`
	Mintable struct {
		Authority []any  `json:"authority"`
		Status    string `json:"status"`
	} `json:"mintable"`
	NonTransferable string `json:"non_transferable"`
	TotalSupply     string `json:"total_supply"`
	TransferFee     struct {
	} `json:"transfer_fee"`
	TransferFeeUpgradable struct {
		Authority []any  `json:"authority"`
		Status    string `json:"status"`
	} `json:"transfer_fee_upgradable"`
	TransferHook           []any `json:"transfer_hook"`
	TransferHookUpgradable struct {
		Authority []any  `json:"authority"`
		Status    string `json:"status"`
	} `json:"transfer_hook_upgradable"`
	TrustedToken int `json:"trusted_token"`
}

type TokenSecResponse struct {
	Code    int                          `json:"code"`
	Message string                       `json:"message"`
	Result  map[string]TokenSecurityData `json:"result"`
}

func GetSolTokenMeta(token, apiKey string) (*TokenMetaData, error) {
	url := "https://pro-api.solscan.io/v2.0/token/meta?address=" + token
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("token", apiKey)

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("request tokemeta, %v", res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var result MetaResponse
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	if !result.Success {
		return nil, fmt.Errorf("response failed, %v", result)
	}

	resData := &result.Data

	resData.Supply = strconv.FormatFloat(toDecimal(resData.Supply, resData.Decimals), 'f', -1, 64)

	return &result.Data, nil
}

func SetTokenMetaCache(token, apiKey string) error {
	data, err := GetSolTokenMeta(token, apiKey)
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

	key := fmt.Sprintf("meta:solscan:%s", token)
	err = redis.Set(context.Background(), key, string(bt), time.Minute)
	if err != nil {
		return err
	}

	go func(in *TokenMetaData) {
		record := model.SolDimTokensRecord{
			TokenAddress:     in.Address,
			Name:             in.Name,
			Symbol:           in.Symbol,
			Decimals:         in.Decimals,
			IsERC20:          true,
			IsERC721:         false,
			IsERC1155:        false,
			FromAddress:      in.Creator,
			TransactionHash:  in.FirstMintTx,
			TransactionIndex: 0,
			BlockNumber:      0,
			BlockTimestamp:   in.FirstMintTime,
			ExtraInfo:        "",
			IsValid:          true,
			IsSupported:      false,
			IsImageSupported: 0,
			TotalSupply:      in.Supply,
			Image:            in.Icon,
		}

		_, err := db.GetDBDim().NewInsert().Model(&record).On("CONFLICT (token_address) DO UPDATE").Set("total_supply = EXCLUDED.total_supply").Set("icon = EXCLUDED.icon").Exec(context.Background())
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"Data": record, "ErrMsg": err}).Error("solscan insert solana dim tokens record faild")
			return
		}

		logger.Logrus.WithFields(logrus.Fields{"Data": record}).Info("solscan insert solana dim tokens record success")
	}(data)

	return nil
}

func GetTokenMetaCache(token string) (*TokenMetaData, error) {
	key := fmt.Sprintf("meta:solscan:%s", token)
	bt, err := redis.Get(context.Background(), key)
	if err == redis.Nil {
		apiKey := config.GetSolDataConfig().SolScanAPIKey
		err = SetTokenMetaCache(token, apiKey)
		if err != nil {
			return nil, err
		}

		bt, err = redis.Get(context.Background(), key)
	}

	if err != nil {
		return nil, err
	}

	var data TokenMetaData

	err = json.Unmarshal([]byte(bt), &data)
	if err != nil {
		return nil, err
	}

	if data.Symbol == "" {
		return nil, fmt.Errorf("%s symbol is empty", token)
	}

	return &data, nil
}

func GetSolTokenSecurity(token string) (*TokenSecurityData, error) {
	url := "https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses=" + token
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("request failed, %v", res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var pres TokenSecResponse
	err = json.Unmarshal(body, &pres)
	if err != nil {
		return nil, err
	}

	if pres.Code != 1 {
		return nil, fmt.Errorf("get token security data return error, %v", pres.Message)
	}

	value, ok := pres.Result[token]
	if !ok {
		return nil, fmt.Errorf("failed get token security data")
	}

	return &value, nil
}

func CheckTokenSecrity(token string) (bool, error) {
	data, err := GetSolTokenSecurity(token)
	if err != nil {
		return false, err
	}

	free := data.Freezable.Status
	mint := data.Mintable.Status

	if free == "0" && mint == "0" {
		return true, nil
	}

	return false, nil
}

func SetTokenSecurityCache(token string) error {
	key := fmt.Sprintf("sec:sol:%s", token)

	isexist, err := redis.Exists(context.Background(), key)
	if err != nil {
		return err
	}

	if isexist {
		return nil
	} else {
		isSercrity, err := CheckTokenSecrity(token)
		if err != nil {
			return fmt.Errorf("set cache token security failed, %v", err)
		}

		ctx := context.Background()

		value := "0"
		if isSercrity {
			value = "1"
		}
		err = redis.Set(ctx, key, value, 90*24*time.Hour)
		if err != nil {
			return err
		}
	}

	return nil
}

func GetTokenSerurityCache(token string) (bool, error) {
	key := fmt.Sprintf("sec:sol:%s", token)

	res, err := redis.Get(context.Background(), key)
	if err == redis.Nil {
		err = SetTokenSecurityCache(token)
		if err != nil {
			return false, fmt.Errorf("get cache token security failed, %v", err)
		}

		res, err = redis.Get(context.Background(), key)
	}

	if err != nil {
		return false, err
	}

	isSec := false
	if res == "1" {
		isSec = true
	}

	return isSec, nil
}
