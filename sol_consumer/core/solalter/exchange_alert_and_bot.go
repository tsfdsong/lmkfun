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

type RawTopicData struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

type RawExchangeData struct {
	AnnouncementTime string `json:"announcement_time"`
	AnnouncementType string `json:"announcement_type"`
	Title            string `json:"title"`
	Chain            string `json:"chain"`
	ContractAddress  string `json:"contract_address"`
	ExchangeName     string `json:"exchange_name"`
	ListingTime      string `json:"listing_time"`
	ListingType      string `json:"listing_type"`
	OriginalURL      string `json:"original_url"`
	TokenSymbol      string `json:"token_symbol"`
	UpdateTime       string `json:"update_time"`
}

func (d *RawTopicData) Unmarshall() (*RawExchangeData, error) {
	var res RawExchangeData
	if d.Type == "exchange" {
		data := []byte(d.Data)
		err := json.Unmarshal(data, &res)
		if err != nil {
			return nil, err
		}

		return &res, nil
	}

	return nil, fmt.Errorf("type { %s }  is not match", d.Type)
}

func (d *RawTopicData) UnmarshallKOL() (*RawKolData, error) {
	var res RawKolData
	if d.Type == "KOL" {
		data := []byte(d.Data)
		err := json.Unmarshal(data, &res)
		if err != nil {
			return nil, err
		}

		return &res, nil
	}

	return nil, fmt.Errorf("type { %s }  is not match", d.Type)
}

func (d *RawTopicData) UnmarshallCurated() (*RawCuratedTokenCallsData, error) {
	var res RawCuratedTokenCallsData
	if d.Type == "publish_call" {
		data := []byte(d.Data)
		err := json.Unmarshal(data, &res)
		if err != nil {
			return nil, err
		}

		return &res, nil
	}

	return nil, fmt.Errorf("type { %s }  is not match", d.Type)
}

func (d *RawTopicData) UnmarshallFomo() (*RawFomoCallsData, error) {
	var res RawFomoCallsData
	if d.Type == "fomo_call" {
		data := []byte(d.Data)
		err := json.Unmarshal(data, &res)
		if err != nil {
			return nil, err
		}

		return &res, nil
	}

	return nil, fmt.Errorf("type { %s }  is not match", d.Type)
}

type ExchangeTrackedCache struct {
	ListID       string `json:"list_id"`
	ExchangeName string `json:"exchange_name"`
}

func SetExchangeTrackedCache() ([]ExchangeTrackedCache, error) {
	var resExch []model.ExchangeTrackedInfo
	query := `SELECT * FROM multichain_view_ads.view_ads_exchange_tracked WHERE token_amount IS NULL`

	err := db.GetDB().NewRaw(query).Scan(context.Background(), &resExch)
	if err != nil {
		return nil, err
	}

	result := make([]ExchangeTrackedCache, 0)
	namelist := make(map[string][]string, 0)
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
				data := ExchangeTrackedCache{
					ListID:       item.ListID,
					ExchangeName: v.Value,
				}

				list, ok := namelist[data.ExchangeName]
				if !ok {
					list = make([]string, 0)
				}

				if data.ListID != "" {
					list = append(list, data.ListID)
				}

				namelist[data.ExchangeName] = list

				result = append(result, data)
			}
		}
	}

	for k, vals := range namelist {
		key := fmt.Sprintf("Exchange:%s", k)
		err := redis.AddSet(key, vals, 10*time.Minute)
		if err != nil {
			return nil, fmt.Errorf("update spot, %v", err)
		}
	}

	return result, nil
}

func GetExchangeTrackedCache(name string) ([]string, error) {
	key := fmt.Sprintf("Exchange:%s", name)
	res, err := redis.GetSet(key)
	if err == redis.Nil {
		SetExchangeTrackedCache()

		res, err = redis.GetSet(key)
	}

	if err != nil {
		return nil, err
	}

	return res, nil
}
