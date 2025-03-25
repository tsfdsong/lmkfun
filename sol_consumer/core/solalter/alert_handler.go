package solalter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

const (
	LabelError    string = "error"
	LabelNone     string = "none"
	LabelFirstBuy string = "first_buy"
	LabelFreshBuy string = "fresh_buy"
	LabelSellAll  string = "sell_all"
)

var ErrRateLimit = errors.New("rate limit")
var ErrNotFound = errors.New("address not found")

func checkTimestamp(t int) error {
	now := time.Now().Unix()
	if t > int(now) {
		return fmt.Errorf("timestamp { %d } is more than now { %d } ", t, now)
	}

	return nil
}

func handleSOlSend(val model.SolSwapData) error {
	if val.FromUserAccount == "" && val.WalletCounts > 1 {
		return fmt.Errorf("%s data parse error", val.TxHash)
	}

	err := checkTimestamp(val.Timestamp)
	if err != nil {
		return err
	}

	fromDataList, err := GetTrackedAddrFromCache("solana", val.FromUserAccount)
	if err != nil {
		return err
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": fromDataList, "TxHash": val.TxHash}).Info("handleSOlSend from user account info")

	if len(fromDataList) > 0 {
		totokenAddress := val.ToToken

		tokenMeta, err := GetSolMetaDataCache("solana", totokenAddress)
		if err != nil {
			return fmt.Errorf("to token,%s, %v", totokenAddress, err)
		}

		fromtokenValue := calswapValue(val.FromTokenAmount, tokenMeta.Price)
		alterData := SolAltertData{
			Source:           val.Source,
			Date:             val.Date,
			Type:             val.Type,
			TxHash:           val.TxHash,
			FromToken:        val.FromToken,
			FromTokenSymbol:  tokenMeta.Symbol,
			FromTokenAmount:  strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64),
			FromTokenDecimal: tokenMeta.Decimals,
			ToToken:          val.ToToken,
			ToTokenSymbol:    tokenMeta.Symbol,
			ToTokenAmount:    strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64),
			ToTokenDecimal:   tokenMeta.Decimals,
			Value:            strconv.FormatFloat(fromtokenValue, 'f', -1, 64),
			Price:            strconv.FormatFloat(tokenMeta.Price, 'f', -1, 64),
			FromAccount:      val.FromUserAccount,
			ToAccount:        val.ToUserAccount,
			Direction:        "Send",
			TotalSupply:      tokenMeta.TotalSupply,
			WalletCounta:     val.WalletCounts,
		}

		alby, err := json.Marshal(&alterData)
		if err != nil {
			return fmt.Errorf("marshal from alert data failed,%v", err)
		}

		tosymbol := tokenMeta.Symbol
		if tosymbol == " " {
			tosymbol = `" "`
		}

		writerecords := make([]model.SolAlterRecord, 0)

		for _, fromData := range fromDataList {
			logger.Logrus.WithFields(logrus.Fields{"Data": fromData, "FromUser": val.FromUserAccount, "TxHash": val.TxHash}).Info("handleSOlSend from list data")

			if !fromData.TxTransfer {
				logger.Logrus.WithFields(logrus.Fields{"Data": fromData, "TxHash": val.TxHash}).Error("handleSOlSend input type and Tx type not match")
				continue
			}

			// if fromData.TokenSecurity {
			// 	isSecurity, err := GetTokenSerurityCache(totokenAddress)
			// 	if err != nil {
			// 		logger.Logrus.WithFields(logrus.Fields{"TokenAddress": totokenAddress, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSOlSend get to token security failed")
			// 		continue
			// 	}

			// 	if !isSecurity {
			// 		logger.Logrus.WithFields(logrus.Fields{"TokenAddress": totokenAddress, "TxHash": val.TxHash}).Error("handleSOlSend to token is not security")
			// 		continue
			// 	}
			// }

			if fromtokenValue < fromData.TxSendValue {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "FromValue": fromtokenValue, "TxHash": val.TxHash}).Error("handleSOlSend from values not match rule")
				continue
			}

			record := model.SolAlterRecord{
				ListID:        fromData.ListID,
				UserAccount:   fromData.UserAccount,
				Type:          "address",
				Chain:         "solana",
				TokenAddress:  val.ToToken,
				TokenSymbol:   tosymbol,
				MarketCap:     strconv.FormatFloat(tokenMeta.Mc, 'f', -1, 64),
				PriceChange1H: strconv.FormatFloat(tokenMeta.Change1hPrice, 'f', -1, 64),
				Security:      "safe",
				Data:          string(alby),
				Timestamp:     fmt.Sprintf("%d", val.Timestamp),
				CreateAt:      time.Now(),
			}

			writerecords = append(writerecords, record)

			if !fromData.TgTxSend {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash}).Info("handleSOlSend no need push tx to tg bot")
				continue
			}

			botbody := ConstructSendBotMessage("solana", fromData.Label, val.FromUserAccount, val.ToUserAccount, val.FromToken, strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64), tokenMeta.Symbol, strconv.FormatFloat(fromtokenValue, 'f', -1, 64), strconv.FormatFloat(tokenMeta.Price, 'f', -1, 64), val.TxHash, fromData.ListID, fromData.IsAddrPublic)
			if val.WalletCounts > 1 {
				botbody = ConstructSendBotMessageNoTo("solana", fromData.Label, val.FromUserAccount, val.ToUserAccount, val.FromToken, strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64), tokenMeta.Symbol, strconv.FormatFloat(fromtokenValue, 'f', -1, 64), strconv.FormatFloat(tokenMeta.Price, 'f', -1, 64), val.TxHash, fromData.ListID, val.WalletCounts, fromData.IsAddrPublic)
			}

			err = HandleTgBotMessage(fromData.ListID, botbody, "Solana", val.FromToken, val.Timestamp, true)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSOlSend handle bot failed")
				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TgMsg": botbody, "TxHash": val.TxHash}).Info("handleSOlSend handle bot success")
		}

		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			err = BatchInsertAlertRecords(writerecords)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "ErrMsg": err, "Records": writerecords}).Error("handleSOlSend batch insert alert record failed")
				return err
			}
		}

		logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Records": writerecords}).Info("handleSOlSend batch insert alert record success")

		return nil
	}

	return fmt.Errorf("from address not register,%s, %s", "solana", val.FromUserAccount)
}

func handleSolReceived(val model.SolSwapData) error {
	if val.ToUserAccount == "" && val.WalletCounts > 1 {
		logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash}).Warn("handleSolReceived is a send transaction")

		return fmt.Errorf("%s is a send", val.TxHash)
	}

	err := checkTimestamp(val.Timestamp)
	if err != nil {
		return err
	}

	toDataList, err := GetTrackedAddrFromCache("solana", val.ToUserAccount)
	if err != nil || len(toDataList) == 0 {
		logger.Logrus.WithFields(logrus.Fields{"Data": toDataList, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSolReceived to user account info")

		return ErrNotFound
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": toDataList, "TxHash": val.TxHash}).Info("handleSolReceived to user account info")

	if len(toDataList) > 0 {

		fromtokenAddress := val.FromToken

		tokenMeta, err := GetSolMetaDataCache("solana", fromtokenAddress)
		if err != nil {
			return fmt.Errorf("from token,%s, %v", fromtokenAddress, err)
		}

		totokenValue := calswapValue(val.ToTokenAmount, tokenMeta.Price)
		alterData := SolAltertData{
			Source:           val.Source,
			Date:             val.Date,
			Type:             val.Type,
			TxHash:           val.TxHash,
			FromToken:        val.FromToken,
			FromTokenSymbol:  tokenMeta.Symbol,
			FromTokenAmount:  strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64),
			FromTokenDecimal: tokenMeta.Decimals,
			ToToken:          val.ToToken,
			ToTokenSymbol:    tokenMeta.Symbol,
			ToTokenAmount:    strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64),
			ToTokenDecimal:   tokenMeta.Decimals,
			Value:            strconv.FormatFloat(totokenValue, 'f', -1, 64),
			Price:            strconv.FormatFloat(tokenMeta.Price, 'f', -1, 64),
			FromAccount:      val.FromUserAccount,
			ToAccount:        val.ToUserAccount,
			Direction:        "Received",
			TotalSupply:      tokenMeta.TotalSupply,
			WalletCounta:     val.WalletCounts,
		}

		alby, err := json.Marshal(&alterData)
		if err != nil {
			return fmt.Errorf("marshal to alert data failed,%v", err)
		}

		fromsymbol := tokenMeta.Symbol
		if fromsymbol == " " {
			fromsymbol = `" "`
		}

		writerecords := make([]model.SolAlterRecord, 0)

		for _, toData := range toDataList {
			logger.Logrus.WithFields(logrus.Fields{"Data": toData, "ToUser": val.ToUserAccount, "TxHash": val.TxHash}).Info("handleSolReceived to list data")

			if !toData.TxTransfer {
				logger.Logrus.WithFields(logrus.Fields{"Data": toData, "TxHash": val.TxHash}).Error("handleSolReceived input type and Tx type not match")
				continue
			}

			// if toData.TokenSecurity {
			// 	isSecurity, err := GetTokenSerurityCache(fromtokenAddress)
			// 	if err != nil {
			// 		logger.Logrus.WithFields(logrus.Fields{"FromTokenAddress": fromtokenAddress, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSolReceived get from token security failed")
			// 		continue
			// 	}

			// 	if !isSecurity {
			// 		logger.Logrus.WithFields(logrus.Fields{"FromTokenAddress": fromtokenAddress, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSolReceived to token is not security")
			// 		continue
			// 	}
			// }

			if totokenValue < toData.TxReceivedValue {
				logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "ToValue": totokenValue, "TxHash": val.TxHash}).Error("handleSolReceived to token value not match rule")
				continue
			}

			record := model.SolAlterRecord{
				ListID:        toData.ListID,
				UserAccount:   toData.UserAccount,
				Type:          "address",
				Chain:         "solana",
				TokenAddress:  val.FromToken,
				TokenSymbol:   tokenMeta.Symbol,
				MarketCap:     strconv.FormatFloat(tokenMeta.Mc, 'f', -1, 64),
				PriceChange1H: strconv.FormatFloat(tokenMeta.Change1hPrice, 'f', -1, 64),
				Security:      "safe",
				Data:          string(alby),
				Timestamp:     fmt.Sprintf("%d", val.Timestamp), //convert to utc timestamp
				CreateAt:      time.Now(),
			}

			writerecords = append(writerecords, record)

			if !toData.TgTxReceived {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash}).Info("handleSolReceived no need push tx to tg bot")
				continue
			}

			botbody := ConstructReceivedBotMessage("solana", val.FromUserAccount, toData.Label, val.ToUserAccount, val.ToToken, strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64), fromsymbol, strconv.FormatFloat(totokenValue, 'f', -1, 64), strconv.FormatFloat(tokenMeta.Price, 'f', -1, 64), val.TxHash, toData.ListID, toData.IsAddrPublic)
			if val.WalletCounts > 1 {
				botbody = ConstructReceivedBotMessageNoFrom("solana", val.FromUserAccount, toData.Label, val.ToUserAccount, val.ToToken, strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64), fromsymbol, strconv.FormatFloat(totokenValue, 'f', -1, 64), strconv.FormatFloat(tokenMeta.Price, 'f', -1, 64), val.TxHash, toData.ListID, val.WalletCounts, toData.IsAddrPublic)
			}

			err = HandleTgBotMessage(toData.ListID, botbody, "Solana", val.ToToken, val.Timestamp, true)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSolReceived handle bot failed")
				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "TgMsg": botbody, "TxHash": val.TxHash}).Info("handleSolReceived handle bot success")

		}

		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			err = BatchInsertAlertRecords(writerecords)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "ErrMsg": err, "Records": writerecords}).Error("handleSolReceived batch insert alert record failed")
				return err
			}
		}

		logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Records": writerecords}).Info("handleSolReceived batch insert alert record success")

		return nil
	}

	return fmt.Errorf("from address not register,%s, %s", "solana", val.ToUserAccount)
}

func handleSOlBuyOptimize(val model.SolSwapData) error {
	err := checkTimestamp(val.Timestamp)
	if err != nil {
		return err
	}

	fromDataList, err := GetTrackedAddrFromCache("solana", val.FromUserAccount)
	if err != nil {
		return err
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": fromDataList, "TxHash": val.TxHash}).Info("handleSOlBuyOptimize from user account info")

	if len(fromDataList) > 0 {
		totokenAddress := val.ToToken

		totokenMeta, err := GetSolMetaDataCache("solana", totokenAddress)
		if err != nil {
			return fmt.Errorf("to token,%s, %v", totokenAddress, err)
		}

		fromtokenMeta, err := GetSolStableCoinMetaData(val.FromToken)
		if err != nil {
			return fmt.Errorf("from coin,%s,%v", val.FromToken, err)
		}

		fromtokenValue := calswapValue(val.FromTokenAmount, fromtokenMeta.Price)

		mc := strconv.FormatFloat(totokenMeta.Mc, 'f', -1, 64)
		toprice := strconv.FormatFloat(totokenMeta.Price, 'f', -1, 64)
		if isFloatEqual(fromtokenValue) && totokenMeta.TotalSupply != "" {
			toprice = strconv.FormatFloat(fromtokenValue/val.ToTokenAmount, 'f', -1, 64)
			mc = strconv.FormatFloat(calValue(toprice, totokenMeta.TotalSupply), 'f', -1, 64)
		}

		alterData := SolAltertData{
			Source:           val.Source,
			Date:             val.Date,
			Type:             val.Type,
			TxHash:           val.TxHash,
			FromToken:        val.FromToken,
			FromTokenSymbol:  fromtokenMeta.Symbol,
			FromTokenAmount:  strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64),
			FromTokenDecimal: fromtokenMeta.Decimals,
			ToToken:          val.ToToken,
			ToTokenSymbol:    totokenMeta.Symbol,
			ToTokenAmount:    strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64),
			ToTokenDecimal:   totokenMeta.Decimals,
			Value:            strconv.FormatFloat(fromtokenValue, 'f', -1, 64),
			Price:            toprice,
			FromAccount:      val.FromUserAccount,
			ToAccount:        val.ToUserAccount,
			MarketCap:        mc,
			AgeTime:          totokenMeta.AgeTime,
			Volume24H:        strconv.FormatFloat(totokenMeta.Volume24H, 'f', -1, 64),
			HoldersCount:     totokenMeta.HoldersCount,
			Direction:        "Bought",
			TradeLabel:       val.TradeLabel,
			IsDCATrade:       val.IsDCATrade,
			TotalSupply:      totokenMeta.TotalSupply,
		}

		alby, err := json.Marshal(&alterData)
		if err != nil {
			return fmt.Errorf("marshal from alert data failed,%v", err)
		}

		tosymbol := totokenMeta.Symbol
		if tosymbol == " " {
			tosymbol = `" "`
		}

		writerecords := make([]model.SolAlterRecord, 0)

		for _, fromData := range fromDataList {
			logger.Logrus.WithFields(logrus.Fields{"Data": fromData, "FromUser": val.FromUserAccount, "TxHash": val.TxHash}).Info("handleSOlBuyOptimize from list data")

			if !fromData.TxBuySell {
				logger.Logrus.WithFields(logrus.Fields{"Type": val.Type, "TxBuySell": fromData.TxBuySell, "TxMintBurn": fromData.TxMintBurn, "TxHash": val.TxHash}).Error("handleSOlBuyOptimize input type and Tx type not match")

				continue
			}

			if fromtokenValue < fromData.TxBuyValue {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "FromValue": fromtokenValue, "TxHash": val.TxHash}).Error("handleSOlBuyOptimize from values not match rule")
				continue
			}

			isvalidmc := true
			if fromData.TokenMarketCap != 0 || fromData.TokenMarketCapMin != 0 {
				if totokenMeta.Mc == 0 {
					isvalidmc = false
				}

				if fromData.TokenMarketCap > 0 && totokenMeta.Mc > fromData.TokenMarketCap {
					isvalidmc = false
				}

				if fromData.TokenMarketCapMin > 0 && totokenMeta.Mc < fromData.TokenMarketCapMin {
					isvalidmc = false
				}
			}

			if !isvalidmc {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "MC": totokenMeta.Mc, "TxHash": val.TxHash}).Error("handleSOlBuyOptimize from market cap not match")
				continue
			}

			if fromData.AgeTime != 0 {
				if totokenMeta.AgeTime == 0 || totokenMeta.AgeTime != 0 && fromData.AgeTime != 0 && (totokenMeta.AgeTime+fromData.AgeTime) < time.Now().Unix() {
					logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "AgeTime": totokenMeta.AgeTime, "TxHash": val.TxHash}).Error("handleSOlBuyOptimize from age time not match")
					continue
				}
			}

			isvalidvolume := true
			if fromData.Volume24HMin != 0 || fromData.Volume24HMax != 0 {
				if totokenMeta.Volume24H == 0 {
					isvalidvolume = false
				}
				if fromData.Volume24HMin > 0 && totokenMeta.Volume24H < fromData.Volume24HMin {
					isvalidvolume = false
				}
				if fromData.Volume24HMax > 0 && totokenMeta.Volume24H > fromData.Volume24HMax {
					isvalidvolume = false
				}
			}

			if !isvalidvolume {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "Volume24H": totokenMeta.Volume24H, "TxHash": val.TxHash}).Error("handleSOlBuyOptimize from volume24h not match")
				continue
			}

			if fromData.HolderCount != 0 {
				if totokenMeta.HoldersCount == 0 || fromData.HolderCount != 0 && totokenMeta.HoldersCount < fromData.HolderCount {
					logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "HoldersCount": totokenMeta.HoldersCount, "TxHash": val.TxHash}).Error("handleSOlBuyOptimize from holders not match")
					continue
				}
			}

			record := model.SolAlterRecord{
				ListID:        fromData.ListID,
				UserAccount:   fromData.UserAccount,
				Type:          "address",
				Chain:         "solana",
				TokenAddress:  val.ToToken,
				TokenSymbol:   tosymbol,
				MarketCap:     mc,
				PriceChange1H: strconv.FormatFloat(totokenMeta.Change1hPrice, 'f', -1, 64),
				Security:      "safe",
				Data:          string(alby),
				Timestamp:     fmt.Sprintf("%d", val.Timestamp),
				CreateAt:      time.Now(),
			}

			writerecords = append(writerecords, record)

			isTgSend := false

			if fromData.TgTxBuy {
				isTgSend = true
			} else if fromData.TgTxFirstBuy && val.TradeLabel == LabelFirstBuy {
				isTgSend = true
			} else if fromData.TgTxFreshBuy && val.TradeLabel == LabelFirstBuy {
				isTgSend = true
			} else {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Data": fromData, "TradeLabel": val.TradeLabel}).Info("handleSOlBuyOptimize no need push tx to tg bot")
				isTgSend = false
			}

			if isTgSend {
				botbody := ConstructBuyBotMessage("solana", fromData.Label, val.FromUserAccount, strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64), fromtokenMeta.Symbol, strconv.FormatFloat(fromtokenValue, 'f', -1, 64), strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64), tosymbol, toprice, val.TxHash, val.FromToken, fromData.ListID, totokenAddress, fromData.IsAddrPublic, val.TradeLabel, mc)

				err = HandleTgBotMessage(fromData.ListID, botbody, "Solana", totokenAddress, val.Timestamp, true)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSOlBuyOptimize handle bot failed")
					continue
				}

				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TgMsg": botbody, "TxHash": val.TxHash}).Info("handleSOlBuyOptimize handle bot success")

			}
		}

		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			err = BatchInsertAlertRecords(writerecords)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Record": writerecords, "ErrMsg": err}).Error("handleSOlBuyOptimize batch insert alert record failed")
				return err
			}
		}

		logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Records": writerecords}).Info("handleSOlBuyOptimize batch insert alert record success")

		return nil
	}

	return fmt.Errorf("from address not register,%s, %s", "solana", val.FromUserAccount)
}

func handleSolSoldOptimize(val model.SolSwapData) error {
	err := checkTimestamp(val.Timestamp)
	if err != nil {
		return err
	}

	toDataList, err := GetTrackedAddrFromCache("solana", val.ToUserAccount)
	if err != nil {
		return err
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": toDataList, "TxHash": val.TxHash}).Info("handleSolSoldOptimize to user account info")

	if len(toDataList) > 0 {
		fromtokenAddress := val.FromToken

		fromtokenMeta, err := GetSolMetaDataCache("solana", fromtokenAddress)
		if err != nil {
			return fmt.Errorf("from token,%s, %v", fromtokenAddress, err)
		}

		totokenMeta, err := GetSolStableCoinMetaData(val.ToToken)
		if err != nil {
			return fmt.Errorf("to coin,%s, %v", val.ToToken, err)
		}

		totokenValue := calswapValue(val.ToTokenAmount, totokenMeta.Price)

		mc := strconv.FormatFloat(fromtokenMeta.Mc, 'f', -1, 64)
		fromprice := strconv.FormatFloat(fromtokenMeta.Price, 'f', -1, 64)
		if isFloatEqual(totokenValue) && fromtokenMeta.TotalSupply != "" {
			fromprice := strconv.FormatFloat(totokenValue/val.FromTokenAmount, 'f', -1, 64)
			mc = strconv.FormatFloat(calValue(fromprice, fromtokenMeta.TotalSupply), 'f', -1, 64)
		}

		alterData := SolAltertData{
			Source:           val.Source,
			Date:             val.Date,
			Type:             val.Type,
			TxHash:           val.TxHash,
			FromToken:        val.FromToken,
			FromTokenSymbol:  fromtokenMeta.Symbol,
			FromTokenAmount:  strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64),
			FromTokenDecimal: fromtokenMeta.Decimals,
			ToToken:          val.ToToken,
			ToTokenSymbol:    totokenMeta.Symbol,
			ToTokenAmount:    strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64),
			ToTokenDecimal:   totokenMeta.Decimals,
			Value:            strconv.FormatFloat(totokenValue, 'f', -1, 64),
			Price:            fromprice,
			FromAccount:      val.FromUserAccount,
			ToAccount:        val.ToUserAccount,
			MarketCap:        mc,
			AgeTime:          fromtokenMeta.AgeTime,
			Volume24H:        strconv.FormatFloat(fromtokenMeta.Volume24H, 'f', -1, 64),
			HoldersCount:     fromtokenMeta.HoldersCount,
			Direction:        "Sold",
			TradeLabel:       val.TradeLabel,
			IsDCATrade:       val.IsDCATrade,
			TotalSupply:      fromtokenMeta.TotalSupply,
		}

		alby, err := json.Marshal(&alterData)
		if err != nil {
			return fmt.Errorf("marshal to alert data failed,%v", err)
		}

		fromsymbol := fromtokenMeta.Symbol
		if fromsymbol == " " {
			fromsymbol = `" "`
		}

		writerecords := make([]model.SolAlterRecord, 0)

		for _, toData := range toDataList {
			logger.Logrus.WithFields(logrus.Fields{"Data": toData, "ToUser": val.ToUserAccount, "TxHash": val.TxHash}).Info("handleSolSoldOptimize to list data")

			if !toData.TxBuySell {
				logger.Logrus.WithFields(logrus.Fields{"Data": toData, "TxHash": val.TxHash}).Error("handleSolSoldOptimize input type and Tx type not match")

				continue
			}

			if totokenValue < toData.TxSellValue {
				logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "ToValue": totokenValue, "TxHash": val.TxHash}).Error("handleSolSoldOptimize to token amount not match rule")
				continue
			}

			isvalidmc := true
			if toData.TokenMarketCap != 0 || toData.TokenMarketCapMin != 0 {
				if fromtokenMeta.Mc == 0 {
					isvalidmc = false
				}
				if toData.TokenMarketCap > 0 && fromtokenMeta.Mc > toData.TokenMarketCap {
					isvalidmc = false
				}
				if toData.TokenMarketCapMin > 0 && fromtokenMeta.Mc < toData.TokenMarketCapMin {
					isvalidmc = false
				}
			}

			if !isvalidmc {
				logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "MC": fromtokenMeta.Mc, "TxHash": val.TxHash}).Error("handleSolSoldOptimize to market cap not match")
				continue
			}

			if toData.AgeTime != 0 {
				if fromtokenMeta.AgeTime == 0 || fromtokenMeta.AgeTime != 0 && toData.AgeTime != 0 && (fromtokenMeta.AgeTime+toData.AgeTime) < time.Now().Unix() {
					logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "AgeTime": fromtokenMeta.AgeTime, "TxHash": val.TxHash}).Error("handleSolSoldOptimize to age time not match")
					continue
				}
			}

			isvalidvolume := true
			if toData.Volume24HMin != 0 || toData.Volume24HMax != 0 {
				if fromtokenMeta.Volume24H == 0 {
					isvalidvolume = false
				}

				if toData.Volume24HMin > 0 && fromtokenMeta.Volume24H < toData.Volume24HMin {
					isvalidvolume = false
				}

				if toData.Volume24HMax > 0 && fromtokenMeta.Volume24H > toData.Volume24HMax {
					isvalidvolume = false
				}
			}

			if !isvalidvolume {
				logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "Volume24H": fromtokenMeta.Volume24H, "TxHash": val.TxHash}).Error("handleSolSoldOptimize to volume24h not match")
				continue
			}

			if toData.HolderCount != 0 {
				if fromtokenMeta.HoldersCount == 0 || toData.HolderCount != 0 && fromtokenMeta.HoldersCount < toData.HolderCount {
					logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "HoldersCount": fromtokenMeta.HoldersCount, "TxHash": val.TxHash}).Error("handleSolSoldOptimize to holders not match")
					continue
				}
			}

			if !isFloatEqual(fromtokenMeta.Price) {
				fromtokenMeta.Price = totokenValue / val.FromTokenAmount
			}

			record := model.SolAlterRecord{
				ListID:        toData.ListID,
				UserAccount:   toData.UserAccount,
				Type:          "address",
				Chain:         "solana",
				TokenAddress:  val.FromToken,
				TokenSymbol:   fromtokenMeta.Symbol,
				MarketCap:     mc,
				PriceChange1H: strconv.FormatFloat(fromtokenMeta.Change1hPrice, 'f', -1, 64),
				Security:      "safe",
				Data:          string(alby),
				Timestamp:     fmt.Sprintf("%d", val.Timestamp), //convert to utc timestamp
				CreateAt:      time.Now(),
			}

			writerecords = append(writerecords, record)

			isTgSend := false

			if toData.TgTxSold {
				isTgSend = true
			} else if toData.TgTxSellAll && val.TradeLabel == LabelSellAll {
				isTgSend = true
			} else {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Data": toData, "TradeLabel": val.TradeLabel}).Info("handleSolSoldOptimize no need push tx to tg bot")
				isTgSend = false
			}

			if isTgSend {
				botbody := ConstructSoldBotMessage("solana", toData.Label, val.FromUserAccount, strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64), fromsymbol, strconv.FormatFloat(totokenValue, 'f', -1, 64), strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64), totokenMeta.Symbol, fromprice, val.TxHash, val.FromToken, toData.ListID, fromtokenAddress, toData.IsAddrPublic, val.TradeLabel, mc)

				err = HandleTgBotMessage(toData.ListID, botbody, "Solana", fromtokenAddress, val.Timestamp, true)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSolSoldOptimize handle bot failed")
					continue
				}

				logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "TgMsg": botbody, "TxHash": val.TxHash}).Info("handleSolSoldOptimize handle bot success")
			}
		}

		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			err = BatchInsertAlertRecords(writerecords)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "ErrMsg": err, "Records": writerecords}).Error("handleSolSoldOptimize batch insert alert record failed")
				return err
			}
		}

		logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Records": writerecords}).Info("handleSolSoldOptimize batch insert alert record success")

		return nil
	}

	return fmt.Errorf("from address not register,%s, %s", "solana", val.ToUserAccount)
}

func handleSOlBuy(val model.SolSwapData) error {
	err := checkTimestamp(val.Timestamp)
	if err != nil {
		return err
	}

	fromDataList, err := GetTrackedAddrFromCache("solana", val.FromUserAccount)
	if err != nil {
		return err
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": fromDataList, "TxHash": val.TxHash}).Info("handleSOlBuy from user account info")

	if len(fromDataList) > 0 {
		totokenAddress := val.ToToken

		totokenMeta, err := GetSolMetaDataCache("solana", totokenAddress)
		if err != nil {
			return fmt.Errorf("to token,%s, %v", totokenAddress, err)
		}

		fromtokenMeta, err := GetSolMetaDataCache("solana", val.FromToken)
		if err != nil {
			return fmt.Errorf("from token,%s,%v", val.FromToken, err)
		}

		fromtokenValue := calswapValue(val.FromTokenAmount, fromtokenMeta.Price)

		mc := strconv.FormatFloat(totokenMeta.Mc, 'f', -1, 64)
		toprice := strconv.FormatFloat(totokenMeta.Price, 'f', -1, 64)
		if isFloatEqual(fromtokenValue) && totokenMeta.TotalSupply != "" {
			toprice = strconv.FormatFloat(fromtokenValue/val.ToTokenAmount, 'f', -1, 64)
			mc = strconv.FormatFloat(calValue(toprice, totokenMeta.TotalSupply), 'f', -1, 64)
		}

		alterData := SolAltertData{
			Source:           val.Source,
			Date:             val.Date,
			Type:             val.Type,
			TxHash:           val.TxHash,
			FromToken:        val.FromToken,
			FromTokenSymbol:  fromtokenMeta.Symbol,
			FromTokenAmount:  strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64),
			FromTokenDecimal: fromtokenMeta.Decimals,
			ToToken:          val.ToToken,
			ToTokenSymbol:    totokenMeta.Symbol,
			ToTokenAmount:    strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64),
			ToTokenDecimal:   totokenMeta.Decimals,
			Value:            strconv.FormatFloat(fromtokenValue, 'f', -1, 64),
			Price:            strconv.FormatFloat(fromtokenMeta.Price, 'f', -1, 64),
			FromAccount:      val.FromUserAccount,
			ToAccount:        val.ToUserAccount,
			MarketCap:        mc,
			AgeTime:          totokenMeta.AgeTime,
			Volume24H:        strconv.FormatFloat(totokenMeta.Volume24H, 'f', -1, 64),
			HoldersCount:     totokenMeta.HoldersCount,
			Direction:        "Bought",
			TradeLabel:       val.TradeLabel,
			IsDCATrade:       val.IsDCATrade,
			TotalSupply:      totokenMeta.TotalSupply,
		}

		alby, err := json.Marshal(&alterData)
		if err != nil {
			return fmt.Errorf("marshal from alert data failed,%v", err)
		}

		tosymbol := totokenMeta.Symbol
		if tosymbol == " " {
			tosymbol = `" "`
		}

		writerecords := make([]model.SolAlterRecord, 0)

		for _, fromData := range fromDataList {
			logger.Logrus.WithFields(logrus.Fields{"Data": fromData, "FromUser": val.FromUserAccount, "TxHash": val.TxHash}).Info("handleSOlBuy from list data")

			if !fromData.TxBuySell {
				logger.Logrus.WithFields(logrus.Fields{"Type": val.Type, "TxBuySell": fromData.TxBuySell, "TxMintBurn": fromData.TxMintBurn, "TxHash": val.TxHash}).Error("handleSOlBuy input type and Tx type not match")

				continue
			}

			if fromData.TokenSecurity {
				isSecurity, err := GetTokenSerurityCache(totokenAddress)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"ToTokenAddress": totokenAddress, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSOlBuy get to token security failed")
					continue
				}

				if !isSecurity {
					logger.Logrus.WithFields(logrus.Fields{"ToTokenAddress": totokenAddress, "TxHash": val.TxHash}).Error("handleSOlBuy to token is not security")
					continue
				}
			}

			if fromtokenValue < fromData.TxBuyValue {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "FromValue": fromtokenValue, "TxHash": val.TxHash}).Error("handleSOlBuy from values not match rule")
				continue
			}

			isvalidmc := true
			if fromData.TokenMarketCap != 0 || fromData.TokenMarketCapMin != 0 {
				if totokenMeta.Mc == 0 {
					isvalidmc = false
				}

				if fromData.TokenMarketCap > 0 && totokenMeta.Mc > fromData.TokenMarketCap {
					isvalidmc = false
				}

				if fromData.TokenMarketCapMin > 0 && totokenMeta.Mc < fromData.TokenMarketCapMin {
					isvalidmc = false
				}
			}

			if !isvalidmc {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "MC": totokenMeta.Mc, "TxHash": val.TxHash}).Error("handleSOlBuy from market cap not match")
				continue
			}

			if fromData.AgeTime != 0 {
				if totokenMeta.AgeTime == 0 || totokenMeta.AgeTime != 0 && fromData.AgeTime != 0 && (totokenMeta.AgeTime+fromData.AgeTime) < time.Now().Unix() {
					logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "AgeTime": totokenMeta.AgeTime, "TxHash": val.TxHash}).Error("handleSOlBuy from age time not match")
					continue
				}
			}

			isvalidvolume := true
			if fromData.Volume24HMin != 0 || fromData.Volume24HMax != 0 {
				if totokenMeta.Volume24H == 0 {
					isvalidvolume = false
				}
				if fromData.Volume24HMin > 0 && totokenMeta.Volume24H < fromData.Volume24HMin {
					isvalidvolume = false
				}
				if fromData.Volume24HMax > 0 && totokenMeta.Volume24H > fromData.Volume24HMax {
					isvalidvolume = false
				}
			}

			if !isvalidvolume {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "Volume24H": totokenMeta.Volume24H, "TxHash": val.TxHash}).Error("handleSOlBuy from volume24h not match")
				continue
			}

			if fromData.HolderCount != 0 {
				if totokenMeta.HoldersCount == 0 || fromData.HolderCount != 0 && totokenMeta.HoldersCount < fromData.HolderCount {
					logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "HoldersCount": totokenMeta.HoldersCount, "TxHash": val.TxHash}).Error("handleSOlBuy from holders not match")
					continue
				}
			}

			record := model.SolAlterRecord{
				ListID:        fromData.ListID,
				UserAccount:   fromData.UserAccount,
				Type:          "address",
				Chain:         "solana",
				TokenAddress:  val.ToToken,
				TokenSymbol:   tosymbol,
				MarketCap:     mc,
				PriceChange1H: strconv.FormatFloat(totokenMeta.Change1hPrice, 'f', -1, 64),
				Security:      "safe",
				Data:          string(alby),
				Timestamp:     fmt.Sprintf("%d", val.Timestamp),
				CreateAt:      time.Now(),
			}

			writerecords = append(writerecords, record)

			isTgSend := false

			if fromData.TgTxBuy {
				isTgSend = true
			} else if fromData.TgTxFirstBuy && val.TradeLabel == LabelFirstBuy {
				isTgSend = true
			} else if fromData.TgTxFreshBuy && val.TradeLabel == LabelFirstBuy {
				isTgSend = true
			} else {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Data": fromData, "TradeLabel": val.TradeLabel}).Info("handleSOlBuy no need push tx to tg bot")
				isTgSend = false
			}

			if isTgSend {
				botbody := ConstructBuyBotMessage("solana", fromData.Label, val.FromUserAccount, strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64), fromtokenMeta.Symbol, strconv.FormatFloat(fromtokenValue, 'f', -1, 64), strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64), tosymbol, toprice, val.TxHash, val.FromToken, fromData.ListID, totokenAddress, fromData.IsAddrPublic, val.TradeLabel, mc)

				err = HandleTgBotMessage(fromData.ListID, botbody, "Solana", totokenAddress, val.Timestamp, true)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSOlBuy handle bot failed")
					continue
				}

				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TgMsg": botbody, "TxHash": val.TxHash}).Info("handleSOlBuy handle bot success")

			}
		}

		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			err = BatchInsertAlertRecords(writerecords)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Record": writerecords, "ErrMsg": err}).Error("handleSOlBuy batch insert alert record failed")
				return err
			}
		}

		logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Records": writerecords}).Info("handleSOlBuy batch insert alert record success")

		return nil
	}

	return fmt.Errorf("from address not register,%s, %s", "solana", val.FromUserAccount)
}

func handleSOlCreate(val model.SolSwapData) error {
	err := checkTimestamp(val.Timestamp)
	if err != nil {
		return err
	}

	fromDataList, err := GetTrackedAddrFromCache("solana", val.FromUserAccount)
	if err != nil {
		return err
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": fromDataList, "TxHash": val.TxHash}).Info("handleSOlCreate from user account info")

	if len(fromDataList) > 0 {
		alterData := SolAltertData{
			Source:           val.Source,
			Date:             val.Date,
			Type:             val.Type,
			TxHash:           val.TxHash,
			FromToken:        val.FromToken,
			FromTokenSymbol:  "",
			FromTokenAmount:  strconv.FormatFloat(val.FromTokenAmount, 'f', -1, 64),
			FromTokenDecimal: 0,
			ToToken:          val.ToToken,
			ToTokenSymbol:    "",
			ToTokenAmount:    strconv.FormatFloat(val.ToTokenAmount, 'f', -1, 64),
			ToTokenDecimal:   0,
			Value:            "",
			Price:            "",
			FromAccount:      val.FromUserAccount,
			ToAccount:        val.ToUserAccount,
			Direction:        "Create",
		}

		alby, err := json.Marshal(&alterData)
		if err != nil {
			return fmt.Errorf("marshal from alert data failed,%v", err)
		}

		writerecords := make([]model.SolAlterRecord, 0)

		for _, fromData := range fromDataList {
			logger.Logrus.WithFields(logrus.Fields{"Data": fromData, "FromUser": val.FromUserAccount, "TxHash": val.TxHash}).Info("handleSOlCreate from list data")

			if !fromData.TxMintBurn {
				logger.Logrus.WithFields(logrus.Fields{"Type": val.Type, "TxMintBurn": fromData.TxMintBurn, "TxHash": val.TxHash}).Error("handleSOlCreate input type and Tx type not match")

				continue
			}

			record := model.SolAlterRecord{
				ListID:        fromData.ListID,
				UserAccount:   fromData.UserAccount,
				Type:          "address",
				Chain:         "solana",
				TokenAddress:  val.ToToken,
				TokenSymbol:   "",
				MarketCap:     "",
				PriceChange1H: "",
				Security:      "safe",
				Data:          string(alby),
				Timestamp:     fmt.Sprintf("%d", val.Timestamp),
				CreateAt:      time.Now(),
			}

			writerecords = append(writerecords, record)

			if !fromData.TgTxCreate {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash}).Info("handleSOlCreate no need push tx to tg bot")
				continue
			}

			botbody := ConstructCreateBotMessage("solana", fromData.Label, val.FromUserAccount, "", fromData.ListID, val.ToToken, fromData.IsAddrPublic)

			err = HandleTgBotMessage(fromData.ListID, botbody, "Solana", val.ToToken, val.Timestamp, true)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleSOlCreate handle bot failed")
				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TgMsg": botbody, "TxHash": val.TxHash}).Info("handleSOlCreate handle bot success")

		}

		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			err = BatchInsertAlertRecords(writerecords)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "ErrMsg": err, "Records": writerecords}).Error("handleSOlCreate batch insert alert record failed")
				return err
			}
		}

		logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "Records": writerecords}).Info("handleSOlCreate batch insert alert record success")

		return nil
	}

	return fmt.Errorf("from address not register,%s, %s", "solana", val.FromUserAccount)
}

func handleAddressSwapRule(val model.SolSwapData, tokenRule map[string]bool) error {
	if val.Type == "SWAP" {
		_, fromok := tokenRule[val.FromToken]
		_, took := tokenRule[val.ToToken]

		if took && !fromok {
			return handleSolSoldOptimize(val)
		} else if fromok && !took {
			return handleSOlBuyOptimize(val)
		} else {
			return handleSOlBuy(val)
		}

	} else if val.Type == "TRANSFER" {
		err := handleSolReceived(val)
		if err != nil && errors.Is(err, ErrNotFound) {
			err = handleSOlSend(val)
			if err != nil {
				return err
			}
		}
	} else if val.Type == "CREATE" {
		return handleSOlCreate(val)
	}

	return nil
}

func calValue(amt, price string) float64 {
	a := new(big.Float)
	b := new(big.Float)

	if _, ok := a.SetString(amt); !ok {
		return 0
	}
	if _, ok := b.SetString(price); !ok {
		return 0
	}

	result := new(big.Float).Mul(a, b)

	data, _ := result.Float64()
	return data
}

func calTokenPrice(val, amt string) float64 {
	a := new(big.Float)
	b := new(big.Float)

	if _, ok := a.SetString(val); !ok {
		return 0
	}
	if _, ok := b.SetString(amt); !ok {
		return 0
	}

	result := new(big.Float).Quo(a, b)

	data, _ := result.Float64()
	return data
}

func calswapValue(amt, price float64) float64 {
	a := big.NewFloat(amt)
	b := big.NewFloat(price)

	result := new(big.Float).Mul(a, b)

	data, _ := result.Float64()
	return data
}

func handleEVMBuyWithBirdeye(val RawBitqueryAltertData) error {
	err := CheckAddressRateLimit(val.Chain, val.FromAddress, val.TxHash)
	if err != nil {
		logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("handleEVMBuy CheckAddressRateLimit failed")
		return ErrRateLimit
	}

	fromDataList, err := GetTrackedAddrFromCache(val.Chain, val.FromAddress)
	if err != nil {
		return err
	}

	if len(fromDataList) > 0 {
		totokenAddress := val.ToToken

		totokenMeta, err := GetBrideeyeCache(val.Chain, totokenAddress)
		if err != nil {
			return fmt.Errorf("get to token birdeye failed,%v", err)
		}

		fromtokenMeta, err := GetBrideeyeCache(val.Chain, val.FromToken)
		if err != nil {
			return fmt.Errorf("get from token birdeye failed,%v", err)
		}

		fromtokenValue := calValue(val.FromTokenAmount, strconv.FormatFloat(fromtokenMeta.Price, 'f', -1, 64))

		toprice := strconv.FormatFloat(totokenMeta.Price, 'f', -1, 64)

		mc := strconv.FormatFloat(totokenMeta.Mc, 'f', -1, 64)

		alterData := SolAltertData{
			Source:           val.DEX,
			Date:             val.Timestamp,
			Type:             "SWAP",
			TxHash:           val.TxHash,
			FromToken:        val.FromToken,
			FromTokenSymbol:  fromtokenMeta.Symbol,
			FromTokenAmount:  val.FromTokenAmount,
			FromTokenDecimal: fromtokenMeta.Decimals,
			ToToken:          val.ToToken,
			ToTokenSymbol:    totokenMeta.Symbol,
			ToTokenAmount:    val.ToTokenAmount,
			ToTokenDecimal:   totokenMeta.Decimals,
			Value:            strconv.FormatFloat(fromtokenValue, 'f', -1, 64),
			Price:            strconv.FormatFloat(fromtokenMeta.Price, 'f', -1, 64),
			FromAccount:      val.FromAddress,
			ToAccount:        val.ToAddress,
			Direction:        "Bought",
			MarketCap:        mc,
			AgeTime:          0,
			Volume24H:        "",
			HoldersCount:     0,
			TotalSupply:      fmt.Sprintf("%f", totokenMeta.TotalSupply),
		}

		alby, err := json.Marshal(&alterData)
		if err != nil {
			return fmt.Errorf("marshal from alert data failed,%v", err)
		}

		tosymbol := totokenMeta.Symbol
		if tosymbol == " " {
			tosymbol = `" "`
		}

		parsedTime, err := time.Parse(time.RFC3339, val.Timestamp)
		if err != nil {
			return fmt.Errorf("parse time failed, %v", err)
		}

		unixTimestamp := parsedTime.Unix()
		err = checkTimestamp(int(unixTimestamp))
		if err != nil {
			return err
		}

		writerecords := make([]model.SolAlterRecord, 0)

		for _, fromData := range fromDataList {
			if fromtokenValue < fromData.TxBuyValue {
				logger.Logrus.WithFields(logrus.Fields{"Data": fromData, "FromValue": fromtokenValue}).Error("handleEVMBuy from values not match rule")
				continue
			}

			if !fromData.TxBuySell {
				logger.Logrus.WithFields(logrus.Fields{"TxBuySell": fromData.TxBuySell, "TxHash": val.TxHash}).Error("handleEVMBuy input type and Tx type not match")

				continue
			}

			isvalidmc := true
			if fromData.TokenMarketCap != 0 || fromData.TokenMarketCapMin != 0 {
				tomc, _ := strconv.ParseFloat(mc, 64)
				if tomc == 0 {
					isvalidmc = false
				}

				if fromData.TokenMarketCap > 0 && tomc > fromData.TokenMarketCap {
					isvalidmc = false
				}

				if fromData.TokenMarketCapMin > 0 && tomc < fromData.TokenMarketCapMin {
					isvalidmc = false
				}
			}

			if !isvalidmc {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "MC": mc, "TxHash": val.TxHash}).Error("handleEVMBuy from market cap not match")
				continue
			}

			record := model.SolAlterRecord{
				ListID:        fromData.ListID,
				UserAccount:   fromData.UserAccount,
				Type:          "address",
				Chain:         val.Chain,
				TokenAddress:  val.ToToken,
				TokenSymbol:   tosymbol,
				MarketCap:     strconv.FormatFloat(totokenMeta.Mc, 'f', -1, 64),
				PriceChange1H: strconv.FormatFloat(totokenMeta.Price, 'f', -1, 64),
				Security:      "safe",
				Data:          string(alby),
				Timestamp:     fmt.Sprintf("%d", unixTimestamp),
				CreateAt:      time.Now(),
			}

			writerecords = append(writerecords, record)

			if !fromData.TgTxBuy {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash}).Info("handleEVMBuy no need push tx to tg bot")
				continue
			}

			botbody := ConstructBuyBotMessage(val.Chain, fromData.Label, val.FromAddress, val.FromTokenAmount, fromtokenMeta.Symbol, strconv.FormatFloat(fromtokenValue, 'f', -1, 64), val.ToTokenAmount, tosymbol, toprice, val.TxHash, val.FromToken, fromData.ListID, totokenAddress, fromData.IsAddrPublic, "", mc)

			err = HandleTgBotMessage(fromData.ListID, botbody, val.Chain, totokenAddress, int(unixTimestamp), true)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleEVMBuy handle bot failed")
				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TgMsg": botbody, "TxHash": val.TxHash}).Info("handleEVMBuy handle bot success")

		}

		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			err = BatchInsertAlertRecords(writerecords)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Records": writerecords, "MC": fromtokenMeta.Mc}).Error("handleEVMBuy batch insert alert record failed")
				return err
			}
		}

		logger.Logrus.WithFields(logrus.Fields{"Records": writerecords}).Info("handleEVMBuy batch insert alert record success")

		return nil
	}

	return fmt.Errorf("from address not register,%s, %s", val.Chain, val.FromAddress)
}

func handleEVMBuy(val RawBitqueryAltertData) error {
	err := CheckAddressRateLimit(val.Chain, val.FromAddress, val.TxHash)
	if err != nil {
		logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("handleEVMBuy CheckAddressRateLimit failed")
		return ErrRateLimit
	}

	fromDataList, err := GetTrackedAddrFromCache(val.Chain, val.FromAddress)
	if err != nil {
		return err
	}

	if len(fromDataList) > 0 {
		totokenAddress := val.ToToken

		totokenMeta, err := GetEVMTokenMetaData(val.Chain, totokenAddress)
		if err != nil {
			return fmt.Errorf("get to token evm failed,%v", err)
		}

		fromtokenMeta, err := GetEVMTokenMetaData(val.Chain, val.FromToken)
		if err != nil {
			return fmt.Errorf("get from token evm failed,%v", err)
		}

		fromtokenValue := calValue(val.FromTokenAmount, strconv.FormatFloat(fromtokenMeta.Price, 'f', -1, 64))

		toprice := strconv.FormatFloat(calTokenPrice(fmt.Sprintf("%f", fromtokenValue), val.ToTokenAmount), 'f', -1, 64)

		mc := strconv.FormatFloat(calValue(toprice, fmt.Sprintf("%f", totokenMeta.TotalSupply)), 'f', -1, 64)

		alterData := SolAltertData{
			Source:           val.DEX,
			Date:             val.Timestamp,
			Type:             "SWAP",
			TxHash:           val.TxHash,
			FromToken:        val.FromToken,
			FromTokenSymbol:  fromtokenMeta.Symbol,
			FromTokenAmount:  val.FromTokenAmount,
			FromTokenDecimal: fromtokenMeta.Decimals,
			ToToken:          val.ToToken,
			ToTokenSymbol:    totokenMeta.Symbol,
			ToTokenAmount:    val.ToTokenAmount,
			ToTokenDecimal:   totokenMeta.Decimals,
			Value:            strconv.FormatFloat(fromtokenValue, 'f', -1, 64),
			Price:            strconv.FormatFloat(fromtokenMeta.Price, 'f', -1, 64),
			FromAccount:      val.FromAddress,
			ToAccount:        val.ToAddress,
			Direction:        "Bought",
			MarketCap:        mc,
			AgeTime:          0,
			Volume24H:        "",
			HoldersCount:     0,
			TotalSupply:      fmt.Sprintf("%f", totokenMeta.TotalSupply),
		}

		alby, err := json.Marshal(&alterData)
		if err != nil {
			return fmt.Errorf("marshal from alert data failed,%v", err)
		}

		tosymbol := totokenMeta.Symbol
		if tosymbol == " " {
			tosymbol = `" "`
		}

		parsedTime, err := time.Parse(time.RFC3339, val.Timestamp)
		if err != nil {
			return fmt.Errorf("parse time failed, %v", err)
		}

		unixTimestamp := parsedTime.Unix()
		err = checkTimestamp(int(unixTimestamp))
		if err != nil {
			return err
		}

		writerecords := make([]model.SolAlterRecord, 0)

		for _, fromData := range fromDataList {
			if fromtokenValue < fromData.TxBuyValue {
				logger.Logrus.WithFields(logrus.Fields{"Data": fromData, "FromValue": fromtokenValue}).Error("handleEVMBuy from values not match rule")
				continue
			}

			if !fromData.TxBuySell {
				logger.Logrus.WithFields(logrus.Fields{"TxBuySell": fromData.TxBuySell, "TxHash": val.TxHash}).Error("handleEVMBuy input type and Tx type not match")

				continue
			}

			isvalidmc := true
			if fromData.TokenMarketCap != 0 || fromData.TokenMarketCapMin != 0 {
				tomc, _ := strconv.ParseFloat(mc, 64)
				if tomc == 0 {
					isvalidmc = false
				}

				if fromData.TokenMarketCap > 0 && tomc > fromData.TokenMarketCap {
					isvalidmc = false
				}

				if fromData.TokenMarketCapMin > 0 && tomc < fromData.TokenMarketCapMin {
					isvalidmc = false
				}
			}

			if !isvalidmc {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "MC": mc, "TxHash": val.TxHash}).Error("handleEVMBuy from market cap not match")
				continue
			}

			// if fromData.AgeTime != 0 {
			// 	if totokenMeta.AgeTime == 0 || totokenMeta.AgeTime != 0 && fromData.AgeTime != 0 && (totokenMeta.AgeTime+fromData.AgeTime) < time.Now().Unix() {
			// 		logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "AgeTime": totokenMeta.AgeTime, "TxHash": val.TxHash}).Error("handleEVMBuy from age time not match")
			// 		continue
			// 	}
			// }

			// isvalidvolume := true
			// if fromData.Volume24HMin != 0 || fromData.Volume24HMax != 0 {
			// 	if totokenMeta.Volume24H == 0 {
			// 		isvalidvolume = false
			// 	}
			// 	if fromData.Volume24HMin > 0 && totokenMeta.Volume24H < fromData.Volume24HMin {
			// 		isvalidvolume = false
			// 	}
			// 	if fromData.Volume24HMax > 0 && totokenMeta.Volume24H > fromData.Volume24HMax {
			// 		isvalidvolume = false
			// 	}
			// }

			// if !isvalidvolume {
			// 	logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "Volume24H": totokenMeta.Volume24H, "TxHash": val.TxHash}).Error("handleEVMBuy from volume24h not match")
			// 	continue
			// }

			// if fromData.HolderCount != 0 {
			// 	if totokenMeta.HoldersCount == 0 || fromData.HolderCount != 0 && totokenMeta.HoldersCount < fromData.HolderCount {
			// 		logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "HoldersCount": totokenMeta.HoldersCount, "TxHash": val.TxHash}).Error("handleEVMBuy from holders not match")
			// 		continue
			// 	}
			// }

			record := model.SolAlterRecord{
				ListID:        fromData.ListID,
				UserAccount:   fromData.UserAccount,
				Type:          "address",
				Chain:         val.Chain,
				TokenAddress:  val.ToToken,
				TokenSymbol:   tosymbol,
				MarketCap:     mc,
				PriceChange1H: "",
				Security:      "safe",
				Data:          string(alby),
				Timestamp:     fmt.Sprintf("%d", unixTimestamp),
				CreateAt:      time.Now(),
			}

			writerecords = append(writerecords, record)

			if !fromData.TgTxBuy {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash}).Info("handleEVMBuy no need push tx to tg bot")
				continue
			}

			botbody := ConstructBuyBotMessage(val.Chain, fromData.Label, val.FromAddress, val.FromTokenAmount, fromtokenMeta.Symbol, strconv.FormatFloat(fromtokenValue, 'f', -1, 64), val.ToTokenAmount, tosymbol, toprice, val.TxHash, val.FromToken, fromData.ListID, totokenAddress, fromData.IsAddrPublic, "", mc)

			err = HandleTgBotMessage(fromData.ListID, botbody, val.Chain, totokenAddress, int(unixTimestamp), true)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleEVMBuy handle bot failed")
				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"ListID": fromData.ListID, "TgMsg": botbody, "TxHash": val.TxHash}).Info("handleEVMBuy handle bot success")

		}

		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			err = BatchInsertAlertRecords(writerecords)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Records": writerecords, "MC": fromtokenMeta.Mc}).Error("handleEVMBuy batch insert alert record failed")
				return err
			}
		}

		logger.Logrus.WithFields(logrus.Fields{"Records": writerecords}).Info("handleEVMBuy batch insert alert record success")

		return nil
	}

	return fmt.Errorf("from address not register,%s, %s", val.Chain, val.FromAddress)
}

func handleEVMSold(val RawBitqueryAltertData) error {
	err := CheckAddressRateLimit(val.Chain, val.ToAddress, val.TxHash)
	if err != nil {
		logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("handleEVMSold CheckAddressRateLimit failed")
		return ErrRateLimit
	}

	toDataList, err := GetTrackedAddrFromCache(val.Chain, val.ToAddress)
	if err != nil {
		return err
	}

	if len(toDataList) > 0 {
		fromtokenAddress := val.FromToken

		fromtokenMeta, err := GetEVMTokenMetaData(val.Chain, fromtokenAddress)
		if err != nil {
			return fmt.Errorf("get from evm token meta failed, %v", err)
		}

		totokenMeta, err := GetEVMTokenMetaData(val.Chain, val.ToToken)
		if err != nil {
			return fmt.Errorf("get to evm token meta failed, %v", err)
		}

		totokenValue := calValue(val.ToTokenAmount, strconv.FormatFloat(totokenMeta.Price, 'f', -1, 64))

		fromprice := strconv.FormatFloat(calTokenPrice(fmt.Sprintf("%f", totokenValue), val.FromTokenAmount), 'f', -1, 64)

		mc := strconv.FormatFloat(calValue(fromprice, fmt.Sprintf("%f", fromtokenMeta.TotalSupply)), 'f', -1, 64)

		alterData := SolAltertData{
			Source:           val.DEX,
			Date:             val.Timestamp,
			Type:             "SWAP",
			TxHash:           val.TxHash,
			FromToken:        val.FromToken,
			FromTokenSymbol:  fromtokenMeta.Symbol,
			FromTokenAmount:  val.FromTokenAmount,
			FromTokenDecimal: fromtokenMeta.Decimals,
			ToToken:          val.ToToken,
			ToTokenSymbol:    totokenMeta.Symbol,
			ToTokenAmount:    val.ToTokenAmount,
			ToTokenDecimal:   totokenMeta.Decimals,
			Value:            strconv.FormatFloat(totokenValue, 'f', -1, 64),
			Price:            strconv.FormatFloat(totokenMeta.Price, 'f', -1, 64),
			FromAccount:      val.FromAddress,
			ToAccount:        val.ToAddress,
			Direction:        "Sold",
			MarketCap:        mc,
			AgeTime:          0,
			Volume24H:        "",
			HoldersCount:     0,
			TotalSupply:      strconv.FormatFloat(fromtokenMeta.TotalSupply, 'f', -1, 64),
		}

		alby, err := json.Marshal(&alterData)
		if err != nil {
			return fmt.Errorf("marshal to alert data failed,%v", err)
		}

		fromsymbol := fromtokenMeta.Symbol
		if fromsymbol == " " {
			fromsymbol = `" "`
		}

		parsedTime, err := time.Parse(time.RFC3339, val.Timestamp)
		if err != nil {
			return fmt.Errorf("parse time failed, %v", err)
		}

		unixTimestamp := parsedTime.Unix()
		err = checkTimestamp(int(unixTimestamp))
		if err != nil {
			return err
		}

		writerecords := make([]model.SolAlterRecord, 0)

		for _, toData := range toDataList {
			if !toData.TxBuySell {
				logger.Logrus.WithFields(logrus.Fields{"Data": toData, "TxHash": val.TxHash}).Error("handleEVMSold input type and Tx type not match")

				continue
			}

			if totokenValue < toData.TxSellValue {
				logger.Logrus.WithFields(logrus.Fields{"Data": toData, "ToTokenAddress": val.ToTokenAmount}).Error("handleEVMSold to token amount not match rule")
				continue
			}

			isvalidmc := true
			if toData.TokenMarketCap != 0 || toData.TokenMarketCapMin != 0 {
				frommc, _ := strconv.ParseFloat(mc, 64)
				if frommc == 0 {
					isvalidmc = false
				}
				if toData.TokenMarketCap > 0 && frommc > toData.TokenMarketCap {
					isvalidmc = false
				}
				if toData.TokenMarketCapMin > 0 && frommc < toData.TokenMarketCapMin {
					isvalidmc = false
				}
			}

			if !isvalidmc {
				logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "MC": mc, "TxHash": val.TxHash}).Error("handleEVMSold to market cap not match")
				continue
			}

			// if toData.AgeTime != 0 {
			// 	if fromtokenMeta.AgeTime == 0 || fromtokenMeta.AgeTime != 0 && toData.AgeTime != 0 && (fromtokenMeta.AgeTime+toData.AgeTime) < time.Now().Unix() {
			// 		logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "AgeTime": fromtokenMeta.AgeTime, "TxHash": val.TxHash}).Error("handleEVMSold to age time not match")
			// 		continue
			// 	}
			// }

			// isvalidvolume := true
			// if toData.Volume24HMin != 0 || toData.Volume24HMax != 0 {
			// 	if fromtokenMeta.Volume24H == 0 {
			// 		isvalidvolume = false
			// 	}

			// 	if toData.Volume24HMin > 0 && fromtokenMeta.Volume24H < toData.Volume24HMin {
			// 		isvalidvolume = false
			// 	}

			// 	if toData.Volume24HMax > 0 && fromtokenMeta.Volume24H > toData.Volume24HMax {
			// 		isvalidvolume = false
			// 	}
			// }

			// if !isvalidvolume {
			// 	logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "Volume24H": fromtokenMeta.Volume24H, "TxHash": val.TxHash}).Error("handleEVMSold to volume24h not match")
			// 	continue
			// }

			// if toData.HolderCount != 0 {
			// 	if fromtokenMeta.HoldersCount == 0 || toData.HolderCount != 0 && fromtokenMeta.HoldersCount < toData.HolderCount {
			// 		logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "HoldersCount": fromtokenMeta.HoldersCount, "TxHash": val.TxHash}).Error("handleEVMSold to holders not match")
			// 		continue
			// 	}
			// }

			record := model.SolAlterRecord{
				ListID:        toData.ListID,
				UserAccount:   toData.UserAccount,
				Type:          "address",
				Chain:         val.Chain,
				TokenAddress:  val.FromToken,
				TokenSymbol:   fromtokenMeta.Symbol,
				MarketCap:     mc,
				PriceChange1H: "",
				Security:      "safe",
				Data:          string(alby),
				Timestamp:     fmt.Sprintf("%d", unixTimestamp),
				CreateAt:      time.Now(),
			}

			writerecords = append(writerecords, record)

			if !toData.TgTxSold {
				logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash}).Info("handleEVMSold no need push tx to tg bot")
				continue
			}

			botbody := ConstructSoldBotMessage(val.Chain, toData.Label, val.FromAddress, val.FromTokenAmount, fromsymbol, strconv.FormatFloat(totokenValue, 'f', -1, 64), val.ToTokenAmount, totokenMeta.Symbol, fromprice, val.TxHash, val.FromToken, toData.ListID, fromtokenAddress, toData.IsAddrPublic, "", strconv.FormatFloat(fromtokenMeta.Mc, 'f', -1, 64))

			err = HandleTgBotMessage(toData.ListID, botbody, val.Chain, fromtokenAddress, int(unixTimestamp), true)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "TxHash": val.TxHash, "ErrMsg": err}).Error("handleEVMSold handle bot failed")
				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"ListID": toData.ListID, "TgMsg": botbody, "TxHash": val.TxHash}).Info("handleEVMSold handle bot success")

		}

		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			err = BatchInsertAlertRecords(writerecords)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Records": writerecords, "ErrMsg": err}).Error("handleEVMSold batch insert alert record failed")
				return err
			}
		}

		logger.Logrus.WithFields(logrus.Fields{"Records": writerecords}).Info("handleEVMSold batch insert alert record success")

		return nil
	}

	return fmt.Errorf("to address not register,%s, %s", val.Chain, val.ToAddress)
}

func handleBitQueryRule(val RawBitqueryAltertData, tokenRule map[string]bool) error {
	_, fromok := tokenRule[val.FromToken]
	_, took := tokenRule[val.ToToken]

	if fromok {
		isFromSol := false
		if val.FromToken == "0x" {
			isFromSol = true
		}

		isToSol := false
		if val.FromToken == "0x" {
			isToSol = true
		}
		if !took || isFromSol || !isToSol {
			return handleEVMBuy(val)
		}
	}

	//sold
	if took && !fromok {
		return handleEVMSold(val)
	}

	if !fromok && !took {
		return handleEVMBuyWithBirdeye(val)
	}

	return nil
}

func handleExchangeAlert(data *RawExchangeData) error {
	exchangename := data.ExchangeName

	listids, err := GetExchangeTrackedCache(exchangename)
	if err != nil {
		return fmt.Errorf("get exch tracked info failed, %v", err)
	}

	marketcap := ""
	pricechange1h := ""
	if data.Chain != "" && data.ContractAddress != "" {
		if data.Chain == "solana" {
			isSecurity, err := GetTokenSerurityCache(data.ContractAddress)
			if err != nil {
				return fmt.Errorf("get token security failed, %v", err)
			}

			if !isSecurity {
				return fmt.Errorf("token is not security, %v", data.ContractAddress)
			}
		}

		tokenMeta, err := getBirdeyeToken(data.Chain, data.ContractAddress)
		if err != nil {
			return fmt.Errorf("%s, %s, %v", data.Chain, data.ContractAddress, err)
		}

		marketcap = strconv.FormatFloat(tokenMeta.Mc, 'f', -1, 64)
		pricechange1h = strconv.FormatFloat(tokenMeta.Change1hPrice, 'f', -1, 64)
	}

	parsedTime, _ := time.Parse("2006-01-02 15:04:05", data.AnnouncementTime)
	unixTimestamp := parsedTime.Unix()
	err = checkTimestamp(int(unixTimestamp))
	if err != nil {
		return err
	}

	writerecords := make([]model.SolAlterRecord, 0)

	for _, val := range listids {
		alterData := ExchangeAltertData{
			NewsType:  data.AnnouncementType,
			Direction: "positive",
			Link:      data.OriginalURL,
			ListType:  data.ListingType,
			Title:     data.Title,
		}

		alby, _ := json.Marshal(&alterData)

		record := model.SolAlterRecord{
			ListID:        val,
			UserAccount:   exchangename,
			Type:          "exchange",
			Chain:         data.Chain,
			TokenAddress:  data.ContractAddress,
			TokenSymbol:   data.TokenSymbol,
			MarketCap:     marketcap,
			PriceChange1H: pricechange1h,
			Security:      "safe",
			Data:          string(alby),
			Timestamp:     fmt.Sprintf("%d", unixTimestamp),
			CreateAt:      time.Now(),
		}

		writerecords = append(writerecords, record)

		botbody := ConstructExchangeMessage(val, exchangename, data.Chain, data.ContractAddress, data.TokenSymbol, data.Title, data.OriginalURL)

		err = HandleTgBotMessage(val, botbody, data.Chain, data.ContractAddress, int(unixTimestamp), false)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"ListID": val, "ErrMsg": err}).Error("handleExchangeAlert handle bot failed")
			continue
		}

		logger.Logrus.WithFields(logrus.Fields{"ListID": val, "TgMsg": botbody}).Info("handleExchangeAlert handle bot success")

	}

	err = BatchInsertAlertRecords(writerecords)
	if err != nil {
		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"Records": writerecords, "ErrMsg": err}).Error("handleExchangeAlert batch insert alert record failed")
			return err
		}
	}

	logger.Logrus.WithFields(logrus.Fields{"Records": writerecords}).Info("handleExchangeAlert batch insert alert record success")

	return nil
}

func isZero(f float64) bool {
	epsilon := 1e-9
	return math.Abs(f) < epsilon
}

func handleKOLAlert(data *RawKolData) error {
	author := data.Author

	listids, err := GetKOLTrackedCache(author)
	if err != nil {
		return fmt.Errorf("get kol tracked cache failed, %v", err)
	}

	if len(listids) == 0 {
		return nil
	}

	tokenSymbol := data.TokenSymbol
	marketcap := strconv.FormatFloat(data.MarketCap, 'f', -1, 64)
	pricechange1h := ""
	price := ""
	decimals := 0

	if isZero(data.MarketCap) {
		if data.Chain != "" && data.ContractAddress != "" {
			//birdeye
			tokenMeta, err := getBirdeyeToken(data.Chain, data.ContractAddress)
			if err == nil {
				tokenSymbol = tokenMeta.Symbol
				marketcap = strconv.FormatFloat(tokenMeta.Mc, 'f', -1, 64)
				pricechange1h = strconv.FormatFloat(tokenMeta.Change1hPrice, 'f', -1, 64)
				price = strconv.FormatFloat(tokenMeta.Price, 'f', -1, 64)
				decimals = tokenMeta.Decimals
			} else {
				logger.Logrus.WithFields(logrus.Fields{"Data": data, "ErrMsg": err}).Error("handleKOLAlert getBirdeyeToken failed")
			}
		}
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": data, "ListIDs": listids}).Info("handleKOLAlert info")

	parsedTime, _ := time.Parse("2006-01-02 15:04:05", data.CreateTime)
	unixTimestamp := parsedTime.Unix()
	err = checkTimestamp(int(unixTimestamp))
	if err != nil {
		return err
	}

	writerecords := make([]model.SolAlterRecord, 0)

	for _, val := range listids {
		logger.Logrus.WithFields(logrus.Fields{"ListID": val.ListID, "Data": data}).Info("handleKOLAlert handle kol info")

		link := fmt.Sprintf("https://twitter.com/%s/status/%s", data.Author, data.PK)
		alterData := KOLAltertData{
			NewsType:   "metions",
			Direction:  data.Sentiment,
			Link:       link,
			IsVerified: data.IsVerified,
			Price:      price,
			Decimals:   decimals,
			TgPushCA:   val.TgPushWithoutCA,
		}

		alby, _ := json.Marshal(&alterData)

		record := model.SolAlterRecord{
			ListID:        val.ListID,
			UserAccount:   data.Author,
			Type:          "KOL",
			Chain:         data.Chain,
			TokenAddress:  data.ContractAddress,
			TokenSymbol:   tokenSymbol,
			MarketCap:     marketcap,
			PriceChange1H: pricechange1h,
			Security:      "safe",
			Data:          string(alby),
			Timestamp:     fmt.Sprintf("%d", unixTimestamp),
			CreateAt:      time.Now(),
		}

		writerecords = append(writerecords, record)

		if !val.TgPushWithoutCA {
			logger.Logrus.WithFields(logrus.Fields{"ListID": val, "Data": data}).Warn("handleKOLAlert no need send tg")

			continue
		}

		botbody := ConstructKOLMessage(val.ListID, data.Author, data.Sentiment, data.Chain, data.ContractAddress, tokenSymbol, marketcap, link, data.IsVerified)

		err = HandleTgBotMessage(val.ListID, botbody, data.Chain, data.ContractAddress, int(unixTimestamp), false)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"ListID": val, "ErrMsg": err}).Error("handleKOLAlert handle bot failed")
			continue
		}

		logger.Logrus.WithFields(logrus.Fields{"ListID": val, "Data": data, "TgMsg": botbody}).Info("handleKOLAlert handle bot success")
	}

	err = BatchInsertAlertRecords(writerecords)
	if err != nil {
		err = BatchInsertAlertRecords(writerecords)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"Records": writerecords, "ErrMsg": err}).Error("handleKOLAlert batch insert alert record failed")
			return err
		}
	}

	logger.Logrus.WithFields(logrus.Fields{"Records": writerecords}).Info("handleKOLAlert batch insert alert record success")

	return nil
}

func handleSolSaveRecord(list []model.SolSwapData, tokenRule map[string]bool) error {
	if len(list) < 1 {
		return nil
	}

	datas := make([]model.SolTxRecord, 0)
	for _, v := range list {
		totokenMeta, err := GetSolMetaDataCache("solana", v.ToToken)
		if err != nil {
			return fmt.Errorf("to token,%s, %v", v.ToToken, err)
		}

		fromtokenMeta, err := GetSolMetaDataCache("solana", v.FromToken)
		if err != nil {
			return fmt.Errorf("from token,%s,%v", v.FromToken, err)
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
			FromTokenSymbol:  fromtokenMeta.Symbol,

			ToToken:        v.ToToken,
			ToTokenAccount: v.ToTokenAccount,
			ToUserAccount:  v.ToUserAccount,
			ToTokenAmount:  v.ToTokenAmount,
			ToTokenSymbol:  totokenMeta.Symbol,

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
			IsSymbolValid:   true,
		}

		if v.Type == "SWAP" {
			_, fromok := tokenRule[v.FromToken]
			_, took := tokenRule[v.ToToken]

			if took && !fromok {
				// handleSolSold(v)
				totokenValue := calswapValue(v.ToTokenAmount, totokenMeta.Price)

				item.Value = strconv.FormatFloat(totokenValue, 'f', -1, 64)
				item.MarketCap = strconv.FormatFloat(fromtokenMeta.Mc, 'f', -1, 64)
				item.Price = strconv.FormatFloat(totokenMeta.Price, 'f', -1, 64)
				item.Change1HPrice = strconv.FormatFloat(fromtokenMeta.Change1hPrice, 'f', -1, 64)
				item.Direction = "Sold"
				item.Supply = fromtokenMeta.TotalSupply
			} else {
				// handleSOlBuy(v)
				fromtokenValue := calswapValue(v.FromTokenAmount, fromtokenMeta.Price)

				item.Value = strconv.FormatFloat(fromtokenValue, 'f', -1, 64)
				item.MarketCap = strconv.FormatFloat(totokenMeta.Mc, 'f', -1, 64)
				item.Price = strconv.FormatFloat(fromtokenMeta.Price, 'f', -1, 64)
				item.Change1HPrice = strconv.FormatFloat(totokenMeta.Change1hPrice, 'f', -1, 64)
				item.Direction = "Bought"
				item.Supply = totokenMeta.TotalSupply
			}
		} else if v.Type == "TRANSFER" {
			toDataList, err := GetTrackedAddrFromCache("solana", v.ToUserAccount)
			if err != nil || len(toDataList) == 0 {
				fromDataList, err := GetTrackedAddrFromCache("solana", v.FromUserAccount)
				if err != nil || len(fromDataList) == 0 {
					continue
				}

				//handleSOlSend
				fromtokenValue := calswapValue(v.FromTokenAmount, totokenMeta.Price)

				item.Value = strconv.FormatFloat(fromtokenValue, 'f', -1, 64)
				item.MarketCap = strconv.FormatFloat(totokenMeta.Mc, 'f', -1, 64)
				item.Price = strconv.FormatFloat(totokenMeta.Price, 'f', -1, 64)
				item.Change1HPrice = strconv.FormatFloat(totokenMeta.Change1hPrice, 'f', -1, 64)
				item.Direction = "Send"
				item.Supply = totokenMeta.TotalSupply
			} else {
				//handleSolReceived
				totokenValue := calswapValue(v.ToTokenAmount, totokenMeta.Price)

				item.Value = strconv.FormatFloat(totokenValue, 'f', -1, 64)
				item.MarketCap = strconv.FormatFloat(totokenMeta.Mc, 'f', -1, 64)
				item.Price = strconv.FormatFloat(totokenMeta.Price, 'f', -1, 64)
				item.Change1HPrice = strconv.FormatFloat(totokenMeta.Change1hPrice, 'f', -1, 64)
				item.Direction = "Received"
				item.Supply = totokenMeta.TotalSupply
			}
		} else if v.Type == "CREATE" {
			// handleSOlCreate(v)
			fromtokenValue := calswapValue(v.FromTokenAmount, fromtokenMeta.Price)

			item.Value = strconv.FormatFloat(fromtokenValue, 'f', -1, 64)
			item.MarketCap = strconv.FormatFloat(totokenMeta.Mc, 'f', -1, 64)
			item.Price = strconv.FormatFloat(fromtokenMeta.Price, 'f', -1, 64)
			item.Change1HPrice = strconv.FormatFloat(totokenMeta.Change1hPrice, 'f', -1, 64)
			item.Direction = "Create"
			item.Supply = totokenMeta.TotalSupply
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

	logger.Logrus.WithFields(logrus.Fields{"Data": datas}).Info("handleSolSaveRecord success")

	return nil
}

func mergeTxList(txhash string, list []RawBitqueryAltertData) (*RawBitqueryAltertData, error) {
	if len(list) == 0 {
		return nil, fmt.Errorf("%s no data", txhash)
	}

	if len(list) == 1 {
		return &list[0], nil
	}

	transferSummary := make(map[string]map[string]float64)
	symbolmap := make(map[string]string, 0)

	res := &RawBitqueryAltertData{
		DEX:             "",
		Chain:           "",
		TxHash:          "",
		Timestamp:       "",
		FromAddress:     "",
		FromToken:       "",
		FromTokenSymbol: "",
		FromTokenAmount: "",
		ToAddress:       "",
		ToToken:         "",
		ToTokenSymbol:   "",
		ToTokenAmount:   "",
		Value:           "",
		Signer:          "",
	}
	for _, v := range list {
		if v.TxHash != txhash {
			return nil, fmt.Errorf("txhash %s not match", txhash)
		}

		if res.TxHash == "" && (v.FromAddress == v.Signer || v.ToAddress == v.Signer) {
			res.DEX = v.DEX
			res.Chain = v.Chain
			res.TxHash = v.TxHash
			res.Timestamp = v.Timestamp
			res.Signer = v.Signer
			res.FromAddress = v.Signer
			res.ToAddress = v.Signer
			res.Value = v.Value
		}

		//
		if _, ok := transferSummary[v.FromToken]; !ok {
			transferSummary[v.FromToken] = make(map[string]float64)
		}

		if _, ok := transferSummary[v.ToToken]; !ok {
			transferSummary[v.ToToken] = make(map[string]float64)
		}

		if v.FromAddress == v.Signer {
			fromamt, err := strconv.ParseFloat(v.FromTokenAmount, 64)
			if err != nil {
				return nil, err
			}
			transferSummary[v.FromToken]["sent"] = transferSummary[v.FromToken]["sent"] + fromamt
			symbolmap[v.FromToken] = v.FromTokenSymbol
		}

		if v.ToAddress == v.Signer {
			toamt, err := strconv.ParseFloat(v.ToTokenAmount, 64)
			if err != nil {
				return nil, err
			}
			transferSummary[v.ToToken]["received"] = transferSummary[v.ToToken]["received"] + toamt
			symbolmap[v.ToToken] = v.ToTokenSymbol
		}

	}

	formattedSummary := make(map[string]map[string]float64)
	for mint, summary := range transferSummary {
		netChange := summary["received"] - summary["sent"]
		if netChange > 0 {
			formattedSummary[mint] = map[string]float64{"received": netChange}
		} else if netChange < 0 {
			formattedSummary[mint] = map[string]float64{"sent": netChange}
		}
	}

	if len(formattedSummary) != 2 {
		return nil, fmt.Errorf("not match, %s, %d", txhash, len(formattedSummary))
	}

	for mint, v := range formattedSummary {
		toamtChange := v["received"]
		if toamtChange > 0 {
			res.ToToken = mint
			res.ToTokenSymbol = symbolmap[mint]
			res.ToTokenAmount = strconv.FormatFloat(toamtChange, 'f', -1, 64)
		}

		fromamtChange := v["sent"]
		if fromamtChange < 0 {
			res.FromToken = mint
			res.FromTokenSymbol = symbolmap[mint]
			res.FromTokenAmount = strconv.FormatFloat(float64(0)-fromamtChange, 'f', -1, 64)
		}
	}

	if res.FromToken == "" || res.ToToken == "" {
		return nil, fmt.Errorf("merge failed, %s", txhash)
	}

	return res, nil
}

func handleEVMSaveRecord(list []RawBitqueryAltertData, tokenRule map[string]bool) error {
	if len(list) < 1 {
		return nil
	}

	datas := make([]model.SolTxRecord, 0)
	for _, v := range list {
		totokenMeta, err := GetEVMTokenMetaData(v.Chain, v.ToToken)
		if err != nil {
			return fmt.Errorf("to token,%s, %v", v.ToToken, err)
		}

		fromtokenMeta, err := GetEVMTokenMetaData(v.Chain, v.FromToken)
		if err != nil {
			return fmt.Errorf("from token,%s,%v", v.FromToken, err)
		}

		parsedTime, err := time.Parse(time.RFC3339, v.Timestamp)
		if err != nil {
			return fmt.Errorf("parse time failed, %v", err)
		}

		unixTimestamp := parsedTime.Unix()

		fromamt, err := strconv.ParseFloat(v.FromTokenAmount, 64)
		if err != nil {
			return fmt.Errorf("parse from amount failed, %v", err)
		}

		toamt, err := strconv.ParseFloat(v.ToTokenAmount, 64)
		if err != nil {
			return fmt.Errorf("parse to amount failed, %v", err)
		}

		item := model.SolTxRecord{
			Chain:     v.Chain,
			TxHash:    v.TxHash,
			Source:    v.DEX,
			Timestamp: int(unixTimestamp),
			Type:      "SWAP",
			Date:      v.Timestamp,

			FromToken:        v.FromToken,
			FromTokenAccount: "",
			FromUserAccount:  v.FromAddress,
			FromTokenAmount:  fromamt,
			FromTokenSymbol:  fromtokenMeta.Symbol,

			ToToken:        v.ToToken,
			ToTokenAccount: "",
			ToUserAccount:  v.ToAddress,
			ToTokenAmount:  toamt,
			ToTokenSymbol:  totokenMeta.Symbol,

			CreateAt:        time.Now(),
			Value:           "",
			MarketCap:       "",
			Price:           "",
			Change1HPrice:   "",
			Direction:       "",
			Supply:          "",
			TradeLabel:      "",
			IsDCATrade:      false,
			WalletCounts:    0,
			TransferDetails: make([]model.SolSwapData, 0),
			IsSymbolValid:   true,
		}

		if item.Type == "SWAP" {
			_, fromok := tokenRule[v.FromToken]
			_, took := tokenRule[v.ToToken]

			if took && !fromok {
				// handleEVMSold(v)

				totokenValue := calswapValue(toamt, totokenMeta.Price)
				fromprice := strconv.FormatFloat(totokenValue/fromamt, 'f', -1, 64)

				frommc := strconv.FormatFloat(calValue(fromprice, fmt.Sprintf("%f", fromtokenMeta.TotalSupply)), 'f', -1, 64)

				item.Value = strconv.FormatFloat(totokenValue, 'f', -1, 64)
				item.MarketCap = frommc
				item.Price = fromprice
				item.Change1HPrice = ""
				item.Direction = "Sold"
				item.Supply = strconv.FormatFloat(fromtokenMeta.TotalSupply, 'f', -1, 64)
			} else {
				// handleEVMBuy(v)

				fromtokenValue := calswapValue(fromamt, fromtokenMeta.Price)
				toprice := strconv.FormatFloat(fromtokenValue/toamt, 'f', -1, 64)
				tomc := strconv.FormatFloat(calValue(toprice, fmt.Sprintf("%f", totokenMeta.TotalSupply)), 'f', -1, 64)

				item.Value = strconv.FormatFloat(fromtokenValue, 'f', -1, 64)
				item.MarketCap = tomc
				item.Price = toprice
				item.Change1HPrice = ""
				item.Direction = "Bought"
				item.Supply = strconv.FormatFloat(totokenMeta.TotalSupply, 'f', -1, 64)
			}
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

	logger.Logrus.WithFields(logrus.Fields{"Data": datas}).Info("handleEVMSaveRecord success")

	return nil
}
