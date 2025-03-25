package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strconv"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

func mulValue(vale, amount string) float64 {
	a := new(big.Float)
	b := new(big.Float)

	if _, ok := a.SetString(vale); !ok {
		return 0
	}
	if _, ok := b.SetString(amount); !ok {
		return 0
	}

	result := new(big.Float).Quo(a, b)

	data, _ := result.Float64()
	return data
}

func UpdateRecords() {
	var resData []model.SolAlterRecord

	for i := 0; i < 28; i++ {
		var tmpData []model.SolAlterRecord

		index := 1000 * i
		query := fmt.Sprintf("select * from scope_lmk.lmk_list_alert_record_tmp where type = 'address' order by timestamp asc limit 1000 offset %d;", index)
		err := db.GetDB().NewRaw(query).Scan(context.Background(), &tmpData)
		if err != nil {
			log.Fatal("db scan:", err)
			return
		}

		resData = append(resData, tmpData...)
	}

	updateList := make([]model.SolAlterRecord, 0)
	for _, v := range resData {
		data := v.Data

		var obj SolAltertData
		err := json.Unmarshal([]byte(data), &obj)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"Data": v, "ErrMsg": err}).Error("parser data failed")
			continue
		}

		logger.Logrus.WithFields(logrus.Fields{"Data": v}).Info("handle data info")

		if obj.Direction == "Bought" {
			totokenMeta, err := GetSolMetaDataCache("solana", obj.ToToken)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Data": obj, "ErrMsg": err}).Error("GetSolMetaDataCache failed")
				continue
			}

			if obj.Value == "" || obj.ToTokenAmount == "0" || obj.ToTokenAmount == "" {
				logger.Logrus.WithFields(logrus.Fields{"Data": obj}).Error("GetSolMetaDataCache zero value")
				continue
			}

			toprice := mulValue(obj.Value, obj.ToTokenAmount)
			if !isFloatEqual(toprice) {
				logger.Logrus.WithFields(logrus.Fields{"Data": obj}).Error("GetSolMetaDataCache zero price")
				continue
			}

			if totokenMeta.TotalSupply == "" {
				logger.Logrus.WithFields(logrus.Fields{"Data": obj}).Error("GetSolMetaDataCache zero supply")
				continue
			}

			mc := strconv.FormatFloat(calValue(strconv.FormatFloat(toprice, 'f', -1, 64), totokenMeta.TotalSupply), 'f', -1, 64)

			obj.MarketCap = mc
			v.MarketCap = mc

			updateList = append(updateList, v)
		} else if obj.Direction == "Sold" {
			fromtokenMeta, err := GetSolMetaDataCache("solana", obj.FromToken)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Data": obj, "ErrMsg": err}).Error("GetSolMetaDataCache failed")
				continue
			}

			if obj.Value == "" || obj.FromTokenAmount == "0" || obj.FromTokenAmount == "" {
				logger.Logrus.WithFields(logrus.Fields{"Data": obj}).Error("GetSolMetaDataCache zero value")
				continue
			}

			fromprice := mulValue(obj.Value, obj.FromTokenAmount)

			if !isFloatEqual(fromprice) {
				logger.Logrus.WithFields(logrus.Fields{"Data": obj}).Error("GetSolMetaDataCache zero price")
				continue
			}

			if fromtokenMeta.TotalSupply == "" {
				logger.Logrus.WithFields(logrus.Fields{"Data": obj}).Error("GetSolMetaDataCache zero supply")
				continue
			}

			mc := strconv.FormatFloat(calValue(strconv.FormatFloat(fromprice, 'f', -1, 64), fromtokenMeta.TotalSupply), 'f', -1, 64)

			obj.MarketCap = mc
			v.MarketCap = mc

			updateList = append(updateList, v)
		}
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": updateList}).Info("update info")

	for _, v := range updateList {
		_, err := db.GetDB().NewUpdate().
			Model(&v).
			Column("marketcap", "data").
			WherePK().
			Exec(context.Background())
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"Data": v, "ErrMsg": err}).Error("update failed")
			continue
		}
	}
}
