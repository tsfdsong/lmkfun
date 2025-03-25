package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type TrackedAddrCache struct {
	ListID            string  `json:"list_id"`
	Chain             string  `json:"chain"`
	Label             string  `json:"label"`
	UserAccount       string  `json:"user_account"`
	TxBuyValue        float64 `json:"tx_buy_value"`
	TxSellValue       float64 `json:"tx_sell_value"`
	TxReceivedValue   float64 `json:"tx_received_value"`
	TxSendValue       float64 `json:"tx_send_value"`
	TokenSecurity     bool    `json:"token_security"`
	TokenMarketCap    float64 `json:"token_market_cap"`
	TokenMarketCapMin float64 `json:"token_market_cap_min"`
	AgeTime           int64   `json:"age_time"`
	Volume24HMax      float64 `json:"volume_24h_max"`
	Volume24HMin      float64 `json:"volume_24h_min"`
	HolderCount       int64   `json:"token_holder"`
	TxBuySell         bool    `json:"tx_buy_sell"`
	TxMintBurn        bool    `json:"tx_mint_burn"`
	TxTransfer        bool    `json:"tx_transfer"`

	TgTxBuy      bool `json:"tg_push_buy"`
	TgTxSold     bool `json:"tg_push_sell"`
	TgTxReceived bool `json:"tg_push_receive"`
	TgTxSend     bool `json:"tg_push_send"`
	TgTxCreate   bool `json:"tg_push_create"`
	IsAddrPublic bool `json:"is_address_public"`

	TgTxFirstBuy bool `json:"tg_push_first_buy"`
	TgTxFreshBuy bool `json:"tg_push_fresh_buy"`
	TgTxSellAll  bool `json:"tg_push_sell_all"`
}

func delItem(chain, address string) error {
	ctx := context.Background()
	key := fmt.Sprintf("address:%s:%s", chain, address)

	err := redis.GetRedisInst().Del(ctx, key).Err()

	return err
}

func batchAddItems(chain, user string, in []TrackedAddrCache) error {
	ctx := context.Background()
	key := fmt.Sprintf("address:%s:%s", chain, user)

	resData := removeDup(in)

	vbytes, err := json.Marshal(&resData)
	if err != nil {
		return err
	}

	err = redis.GetRedisInst().Set(ctx, key, string(vbytes), time.Hour).Err()
	if err != nil {
		return err
	}

	return nil
}

func removeDup(in []TrackedAddrCache) []TrackedAddrCache {
	unimap := make(map[string]bool)
	result := make([]TrackedAddrCache, 0)

	for _, v := range in {
		lisid := v.ListID

		_, ok := unimap[lisid]

		if !ok {
			result = append(result, v)

			unimap[lisid] = true
		}
	}

	return result
}

func getAllItems(chain, address string) ([]TrackedAddrCache, error) {
	ctx := context.Background()
	key := fmt.Sprintf("address:%s:%s", chain, address)

	values, err := redis.GetRedisInst().Get(ctx, key).Result()
	if err != nil {
		return nil, redis.Nil
	}

	var res []TrackedAddrCache

	err = json.Unmarshal([]byte(values), &res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func UpdateAllTrackAddrCache() error {
	var resAddr []model.SolTrackedAddress
	query := `SELECT * FROM multichain_view_ads.view_ads_sol_addr_tracked`

	err := db.GetDB().NewRaw(query).Scan(context.Background(), &resAddr)
	if err != nil {
		return err
	}

	//chain -> addr -> data
	mapCache := make(map[string]map[string][]TrackedAddrCache, 0)
	for _, item := range resAddr {
		if item.Chain == "" {
			continue
		}

		chain := strings.ToLower(item.Chain)
		addrMap, ok := mapCache[chain]
		if !ok {
			addrMap = make(map[string][]TrackedAddrCache, 0)
		}

		user := item.UserAccount
		cache, ok := addrMap[user]
		if !ok {
			cache = make([]TrackedAddrCache, 0)
		}

		data := TrackedAddrCache{
			ListID:            item.ListID,
			Chain:             item.Chain,
			Label:             item.UserLabel,
			UserAccount:       user,
			TxBuyValue:        item.TxBuyValue,
			TxSellValue:       item.TxSellValue,
			TokenSecurity:     item.TokenSecurity,
			TxReceivedValue:   item.TxReceivedValue,
			TxSendValue:       item.TxSendValue,
			TokenMarketCap:    item.TokenMarketCap,
			TokenMarketCapMin: item.TokenMarketCapMin,
			AgeTime:           item.AgeTime,
			Volume24HMax:      item.Volume24HMax,
			Volume24HMin:      item.Volume24HMin,
			HolderCount:       item.HolderCount,
			TxBuySell:         item.TxBuySell,
			TxMintBurn:        item.TxMintBurn,
			TxTransfer:        item.TxTransfer,
			TgTxBuy:           item.TgTxBuy,
			TgTxSold:          item.TgTxSold,
			TgTxReceived:      item.TgTxReceived,
			TgTxSend:          item.TgTxSend,
			TgTxCreate:        item.TgTxCreate,
			IsAddrPublic:      item.IsAddrPublic,
			TgTxFirstBuy:      item.TgTxFirstBuy,
			TgTxFreshBuy:      item.TgTxFreshBuy,
			TgTxSellAll:       item.TgTxSellAll,
		}

		cache = append(cache, data)
		addrMap[user] = cache

		mapCache[chain] = addrMap
	}

	for chain, addrMap := range mapCache {
		for addr, values := range addrMap {
			err := batchAddItems(chain, addr, values)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Chain": chain, "Address": addr, "Data": values, "ErrMsg": err}).Error("UpdateAllTrackAddrCache add address cache failed")

				continue
			}
		}
	}

	return nil
}

func SetTrackAddrCache(chain, targetAddr string) ([]TrackedAddrCache, error) {
	var resAddr []model.SolTrackedAddress
	query := fmt.Sprintf(`SELECT * FROM multichain_view_ads.view_ads_sol_addr_tracked where chain = '%s' and user_account = '%s'`, chain, targetAddr)
	err := db.GetDB().NewRaw(query).Scan(context.Background(), &resAddr)
	if err != nil {
		return nil, fmt.Errorf("scan db failed, %v", err)
	}

	chain = strings.ToLower(chain)

	resCache := make([]TrackedAddrCache, 0)
	for _, item := range resAddr {
		data := TrackedAddrCache{
			ListID:            item.ListID,
			Chain:             item.Chain,
			Label:             item.UserLabel,
			UserAccount:       item.UserAccount,
			TxBuyValue:        item.TxBuyValue,
			TxSellValue:       item.TxSellValue,
			TxReceivedValue:   item.TxReceivedValue,
			TxSendValue:       item.TxSendValue,
			TokenSecurity:     item.TokenSecurity,
			TokenMarketCap:    item.TokenMarketCap,
			TokenMarketCapMin: item.TokenMarketCapMin,
			AgeTime:           item.AgeTime,
			Volume24HMax:      item.Volume24HMax,
			Volume24HMin:      item.Volume24HMin,
			HolderCount:       item.HolderCount,
			TxBuySell:         item.TxBuySell,
			TxMintBurn:        item.TxMintBurn,
			TxTransfer:        item.TxTransfer,
			TgTxBuy:           item.TgTxBuy,
			TgTxSold:          item.TgTxSold,
			TgTxReceived:      item.TgTxReceived,
			TgTxSend:          item.TgTxSend,
			TgTxCreate:        item.TgTxCreate,
			IsAddrPublic:      item.IsAddrPublic,
			TgTxFirstBuy:      item.TgTxFirstBuy,
			TgTxFreshBuy:      item.TgTxFreshBuy,
			TgTxSellAll:       item.TgTxSellAll,
		}

		resCache = append(resCache, data)
	}

	if len(resCache) > 0 {
		err := batchAddItems(chain, targetAddr, resCache)
		if err != nil {
			return nil, err
		}

		resData := removeDup(resCache)
		return resData, nil
	}

	return nil, fmt.Errorf("address not found, %v, %v", chain, targetAddr)
}

func GetTrackedAddrFromCache(chain, addr string) ([]TrackedAddrCache, error) {
	data, err := getAllItems(chain, addr)
	if err == redis.Nil {
		list, errs := SetTrackAddrCache(chain, addr)
		if errs != nil {
			return nil, fmt.Errorf("%v", errs)
		}

		return list, nil
	}

	if err != nil {
		return nil, fmt.Errorf("get all cache datas failed, %v", err)
	}

	return data, nil
}
