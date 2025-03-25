package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
)

type RawKolData struct {
	Author          string  `json:"author"`
	PK              string  `json:"pk"`
	Chain           string  `json:"chain"`
	ContractAddress string  `json:"contract"`
	Sentiment       string  `json:"sentiment"`
	FullText        string  `json:"full_text"`
	MarketCap       float64 `json:"market_cap"`
	Price           float64 `json:"price"`
	TokenSymbol     string  `json:"symbol"`

	IsVerified bool   `json:"is_verified"`
	UpdateTime string `json:"update_time"`
	CreateTime string `json:"created_at"`
}

type KOLTrackedCache struct {
	ListID          string `json:"list_id"`
	Author          string `json:"author"`
	TgPushWithoutCA bool   `json:"tg_push_without_ca"`
}

func UpdateKOLTrackedCache() ([]KOLTrackedCache, error) {
	var resExch []model.KOLTrackedInfo
	query := `SELECT * FROM multichain_view_ads.view_ads_kol_tracked WHERE token_amount IS NULL`

	err := db.GetDB().NewRaw(query).Scan(context.Background(), &resExch)
	if err != nil {
		return nil, err
	}

	result := make([]KOLTrackedCache, 0)
	kollist := make(map[string][]KOLTrackedCache, 0)
	for _, item := range resExch {
		if item.TrackedInfo == "" {
			continue
		}

		var res []model.SolTrackedInfo

		err = json.Unmarshal([]byte(item.TrackedInfo), &res)
		if err != nil {
			return nil, err
		}

		for _, v := range res {
			if v.Value != "" {
				data := KOLTrackedCache{
					ListID:          item.ListID,
					Author:          v.Value,
					TgPushWithoutCA: item.TgPushWithoutCA,
				}

				list, ok := kollist[data.Author]
				if !ok {
					list = make([]KOLTrackedCache, 0)
				}

				if data.ListID != "" {
					list = append(list, data)
				}

				kollist[data.Author] = list

				result = append(result, data)
			}
		}
	}

	for k, vals := range kollist {
		key := fmt.Sprintf("KOL:%s", k)
		bytes, err := json.Marshal(&vals)
		if err != nil {
			return nil, err
		}

		err = redis.GetRedisInst().Set(context.Background(), key, string(bytes), 10*time.Minute).Err()
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func SetKOLTrackedCache(author string) ([]KOLTrackedCache, error) {
	var resExch []model.KOLTrackedInfo
	query := `SELECT * FROM multichain_view_ads.view_ads_kol_tracked WHERE token_amount IS NULL`

	err := db.GetDB().NewRaw(query).Scan(context.Background(), &resExch)
	if err != nil {
		return nil, err
	}

	kollist := make([]KOLTrackedCache, 0)
	for _, item := range resExch {
		if item.TrackedInfo == "" {
			continue
		}

		var res []model.SolTrackedInfo

		err = json.Unmarshal([]byte(item.TrackedInfo), &res)
		if err != nil {
			return nil, err
		}

		for _, v := range res {
			if v.Value == author {
				data := KOLTrackedCache{
					ListID:          item.ListID,
					Author:          v.Value,
					TgPushWithoutCA: item.TgPushWithoutCA,
				}

				kollist = append(kollist, data)
			}
		}
	}

	bytes, err := json.Marshal(&kollist)
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("KOL:%s", author)
	err = redis.GetRedisInst().Set(context.Background(), key, string(bytes), 10*time.Minute).Err()
	if err != nil {
		return nil, err
	}

	return kollist, nil
}

func GetKOLTrackedCache(author string) ([]KOLTrackedCache, error) {
	key := fmt.Sprintf("KOL:%s", author)
	values, err := redis.GetRedisInst().Get(context.Background(), key).Result()
	if err != nil {
		resdata, err := SetKOLTrackedCache(author)
		if err != nil {
			return nil, err
		}

		return resdata, nil
	}

	var res []KOLTrackedCache
	err = json.Unmarshal([]byte(values), &res)
	if err != nil {
		return nil, err
	}

	return res, nil
}
