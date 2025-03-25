package handler

import (
	"fmt"
	"math"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/utils/logger"
)

func parsePumpFun(in HeliusData) ([]model.SolSwapData, error) {
	feeplayer := in.FeePayer

	if len(in.TokenTransfers) != 1 {
		return nil, fmt.Errorf("token transfer length not match")
	}

	tokenTransfer := in.TokenTransfers[0]

	result := make([]model.SolSwapData, 0)
	t := time.Unix(int64(in.Timestamp), 0)
	timeString := t.Format("2006-01-02 15:04:05")

	if feeplayer == tokenTransfer.ToUserAccount {
		//buy other coin using sol
		fromAmt := float64(0.0)
		for _, v := range in.NativeTransfers {
			if v.FromUserAccount == tokenTransfer.ToUserAccount && v.ToUserAccount == tokenTransfer.FromUserAccount {
				fromAmt = float64(v.Amount) / math.Pow10(9)
			}
		}

		item := model.SolSwapData{
			TxHash:           in.Signature,
			Source:           in.Source,
			Timestamp:        in.Timestamp,
			Type:             in.Type,
			Date:             timeString,
			FromToken:        "So11111111111111111111111111111111111111112",
			FromTokenAccount: feeplayer,
			FromUserAccount:  feeplayer,
			FromTokenAmount:  fromAmt,
			ToToken:          tokenTransfer.Mint,
			ToTokenAccount:   tokenTransfer.ToTokenAccount,
			ToUserAccount:    feeplayer,
			ToTokenAmount:    tokenTransfer.TokenAmount,
			TradeLabel:       "",
			IsDCATrade:       false,
			WalletCounts:     1,
			TransferDetails:  make([]model.SolSwapData, 0),
		}

		result = append(result, item)

	} else if feeplayer == tokenTransfer.FromUserAccount {
		//sell other coin using sol
		toAmt := float64(0)
		for _, v := range in.AccountData {
			if v.Account == tokenTransfer.ToUserAccount {
				toAmt = float64(math.Abs(float64(v.NativeBalanceChange))) / math.Pow10(9)
			}
		}

		item := model.SolSwapData{
			TxHash:           in.Signature,
			Source:           in.Source,
			Timestamp:        in.Timestamp,
			Type:             in.Type,
			Date:             timeString,
			FromToken:        tokenTransfer.Mint,
			FromTokenAccount: tokenTransfer.FromTokenAccount,
			FromUserAccount:  feeplayer,
			FromTokenAmount:  tokenTransfer.TokenAmount,
			ToToken:          "So11111111111111111111111111111111111111112",
			ToTokenAccount:   feeplayer,
			ToUserAccount:    feeplayer,
			ToTokenAmount:    toAmt,
			TradeLabel:       "",
			IsDCATrade:       false,
			WalletCounts:     1,
			TransferDetails:  make([]model.SolSwapData, 0),
		}

		result = append(result, item)
	}

	if len(result) < 1 {
		return nil, fmt.Errorf("no pump.fun data")
	}

	return result, nil
}

func parsePumpFunCreate(in HeliusData) ([]model.SolSwapData, error) {
	feeplayer := in.FeePayer

	result := make([]model.SolSwapData, 0)
	t := time.Unix(int64(in.Timestamp), 0)
	timeString := t.Format("2006-01-02 15:04:05")

	coinmap := make(map[string]map[string]int, 0)

	for _, v := range in.NativeTransfers {
		if v.FromUserAccount != feeplayer && v.ToUserAccount != feeplayer {
			continue
		}

		list, ok := coinmap[v.FromUserAccount]
		if !ok {
			list = make(map[string]int)
		}

		list[v.ToUserAccount] += v.Amount

		coinmap[v.FromUserAccount] = list
	}

	tokenTrans := in.TokenTransfers

	for _, v := range tokenTrans {
		from := v.FromUserAccount
		to := v.ToUserAccount

		if feeplayer != from && feeplayer != to {
			continue
		}

		amt, ok := coinmap[to][from]
		if !ok {
			continue
		}

		amount := toDecimal(fmt.Sprintf("%d", amt), 9)
		item := model.SolSwapData{
			TxHash:           in.Signature,
			Source:           in.Source,
			Timestamp:        in.Timestamp,
			Type:             "CREATE",
			Date:             timeString,
			FromToken:        "So11111111111111111111111111111111111111112",
			FromTokenAccount: v.FromUserAccount,
			FromUserAccount:  v.FromUserAccount,
			FromTokenAmount:  amount,
			ToToken:          v.Mint,
			ToTokenAccount:   v.ToTokenAccount,
			ToUserAccount:    v.ToUserAccount,
			ToTokenAmount:    v.TokenAmount,
			TradeLabel:       "",
			IsDCATrade:       false,
			WalletCounts:     1,
			TransferDetails:  make([]model.SolSwapData, 0),
		}

		result = append(result, item)
	}

	return result, nil
}

func handSwapData(v HeliusData) error {
	if v.Source == "PUMP_FUN" {
		var datait []model.SolSwapData
		var err error
		if v.Type == "CREATE" {
			datait, err = parsePumpFunCreate(v)
			if err != nil {
				return fmt.Errorf("parse pump.fun create failed, %v", err)
			}
		} else {
			datait, err = parseGobleDEX(v)
			if err != nil {
				datait, err = parsePumpFun(v)
				if err != nil {
					return fmt.Errorf("parse pump.fun failed, %v", err)
				}
			}
		}

		datalist := FillLabel(datait)

		err = sendKafkaMsg(datalist)
		if err != nil {
			return fmt.Errorf("send pump.fun kafka failed, %v", err)
		}

		logger.Logrus.WithFields(logrus.Fields{"Data": datalist}).Info("handSwapData send pump.fun kafka data")
	} else {
		datait, err := generateSwapData(v)
		if err != nil {
			datait, err = parseGobleDEX(v)
			if err != nil {
				return fmt.Errorf("parse swap failed, %v", err)
			}
		}

		datalist := FillLabel(datait)

		err = sendKafkaMsg(datalist)
		if err != nil {
			return fmt.Errorf("send kafka failed, %v", err)
		}

		logger.Logrus.WithFields(logrus.Fields{"Data": datalist}).Info("handSwapData send swap kafka data")
	}

	return nil
}
