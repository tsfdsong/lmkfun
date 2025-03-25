package solalter

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

func getSolanaMeta(token string) (*RPCTokenMeta, error) {
	data, err := GetNodeCache("solana", token)
	if err == nil {
		return data, nil
	}

	tokenMeta, err := GetSolMetaDataCache("solana", token)
	if err != nil {
		return nil, fmt.Errorf("get meta failed,%s, %v", token, err)
	}

	res := &RPCTokenMeta{
		Address:     token,
		Symbol:      tokenMeta.Symbol,
		Decimals:    tokenMeta.Decimals,
		Name:        tokenMeta.Name,
		TotalSupply: tokenMeta.TotalSupply,
	}

	return res, nil
}

func HandleHeliusHisData(in []model.SolSwapData, tokenRule map[string]bool) error {
	datas := make([]model.SolTxRecord, 0)
	for _, v := range in {
		isvalided := true
		fromsymbol := "Unknown"
		fromMeta, err := getSolanaMeta(v.FromToken)
		if err == nil && fromMeta != nil {
			fromsymbol = fromMeta.Symbol
		} else {
			logger.Logrus.WithFields(logrus.Fields{"TokenAddress": v.FromToken}).Error("HandleHeliusHisData GetNodeCache from failed")
			fromMeta = new(RPCTokenMeta)
			isvalided = false
		}

		tosymbol := "Unknown"
		toMeta, err := getSolanaMeta(v.ToToken)
		if err == nil && toMeta != nil {
			tosymbol = toMeta.Symbol
		} else {
			logger.Logrus.WithFields(logrus.Fields{"TokenAddress": v.ToToken}).Error("HandleHeliusHisData GetNodeCache to failed")

			toMeta = new(RPCTokenMeta)
			isvalided = false
		}

		item := model.SolTxRecord{
			Chain:     "solana",
			TxHash:    v.TxHash,
			Source:    v.Source,
			Timestamp: v.Timestamp,
			Type:      v.Type,
			Date:      v.Date,

			FromToken:        v.FromToken,
			FromTokenAccount: v.FromTokenAccount,
			FromUserAccount:  v.FromUserAccount,
			FromTokenAmount:  v.FromTokenAmount,
			FromTokenSymbol:  fromsymbol,

			ToToken:        v.ToToken,
			ToTokenAccount: v.ToTokenAccount,
			ToUserAccount:  v.ToUserAccount,
			ToTokenAmount:  v.ToTokenAmount,
			ToTokenSymbol:  tosymbol,

			CreateAt:        time.Now(),
			Value:           "",
			MarketCap:       "",
			Price:           "",
			Change1HPrice:   "",
			Direction:       "",
			Supply:          "",
			TradeLabel:      v.TradeLabel,
			IsDCATrade:      v.IsDCATrade,
			WalletCounts:    v.WalletCounts,
			TransferDetails: v.TransferDetails,
			IsSymbolValid:   isvalided,
		}

		if v.Type == "SWAP" {
			_, fromok := tokenRule[v.FromToken]
			_, took := tokenRule[v.ToToken]

			if took && !fromok {
				item.Direction = "Sold"
				item.Supply = fromMeta.TotalSupply
			} else {
				item.Direction = "Bought"
				item.Supply = toMeta.TotalSupply
			}
		} else if v.Type == "TRANSFER" {
			toDataList, err := GetTrackedAddrFromCache("solana", v.ToUserAccount)
			if err != nil || len(toDataList) == 0 {
				fromDataList, err := GetTrackedAddrFromCache("solana", v.FromUserAccount)
				if err != nil || len(fromDataList) == 0 {
					continue
				}

				item.Direction = "Send"
				item.Supply = toMeta.TotalSupply
			} else {
				item.Direction = "Received"
				item.Supply = toMeta.TotalSupply
			}
		} else if v.Type == "CREATE" {
			item.Direction = "Create"
			item.Supply = toMeta.TotalSupply
		} else {
			continue
		}

		datas = append(datas, item)
	}

	if len(datas) > 0 {
		_, err := db.GetDB().NewInsert().Model(&datas).On("CONFLICT DO NOTHING").Exec(context.Background())
		if err != nil {
			return err
		}
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": datas}).Info("HandleHeliusHisData success")

	return nil
}
