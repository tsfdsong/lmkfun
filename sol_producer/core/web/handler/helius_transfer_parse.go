package handler

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/redis"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/utils/logger"
)

func getMonitorAddress(addrlist string) (*model.MonitorAddressRecord, error) {
	var resAddr model.MonitorAddressRecord

	err := db.GetDB().NewSelect().Model(&resAddr).Where("address IN (?)", addrlist).Scan(context.Background())
	if err != nil {
		return nil, err
	}

	return &resAddr, nil
}

func getMonitorAddressCache(address string) error {
	ctx := context.Background()
	key := fmt.Sprintf("monitor_address:solana:%s", address)

	value, err := redis.GetRedisInst().Get(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	if err == redis.Nil {
		data, err := getMonitorAddress(address)
		if err != nil {
			data, err = getMonitorAddress(address)
		}

		if err != nil {
			err = redis.GetRedisInst().Set(ctx, key, "0", 5*time.Minute).Err()
			if err != nil {
				return err
			}

			return err
		}

		if data != nil {
			err = redis.GetRedisInst().Set(ctx, key, "1", 5*time.Minute).Err()
			if err != nil {
				return err
			}
		}

		return nil
	}

	if value == "1" {
		return nil
	}

	return fmt.Errorf("not found, %s", address)
}

func CheckTxValid(from, to string) bool {
	errf := getMonitorAddressCache(from)
	errt := getMonitorAddressCache(to)

	istrue := (errf == nil) || (errt == nil)

	return istrue
}

func parseHeliusTransfers(datas []HeliusData) ([]model.SolSwapData, error) {
	var results []model.SolSwapData
	for _, transactionData := range datas {
		result, err := parseGobleDEX(transactionData)
		if err != nil {
			continue
		}
		results = append(results, result...)
	}
	return results, nil
}

func addBigFloat(a, b float64, precision uint) float64 {
	multiplier := math.Pow(10, float64(precision))
	result := math.Round(a*multiplier) + math.Round(b*multiplier)
	res := result / multiplier
	return res
}

func parseCommonTransfer(data HeliusData) ([]model.SolSwapData, error) {
	if data.Type != "TRANSFER" {
		return nil, fmt.Errorf("%s is not TRANSFER", data.Type)
	}

	resData := make([]model.SolSwapData, 0)

	t := time.Unix(int64(data.Timestamp), 0)
	timeString := t.Format("2006-01-02 15:04:05")

	feeplayer := data.FeePayer

	cfg := config.GetHeliusConfig()
	DCAProgramedID := cfg.DCAProgrameID
	isDCATrade := false

	for _, item := range data.Instructions {
		if item.ProgramID == DCAProgramedID {
			isDCATrade = true

			dcaobj, err := ParseOpenDCA(&item)
			if err != nil {
				continue
			}

			dcadata := dcaobj.String()

			item := model.SolSwapData{
				TxHash:    data.Signature,
				Source:    "DCA Program",
				Timestamp: data.Timestamp,
				Type:      "OPENDCA",
				Date:      timeString,

				FromToken:        "",
				FromTokenAccount: "",
				FromUserAccount:  "",
				FromTokenAmount:  0,

				ToToken:         "",
				ToTokenAccount:  "",
				ToUserAccount:   "",
				ToTokenAmount:   0,
				TradeLabel:      "",
				IsDCATrade:      isDCATrade,
				WalletCounts:    1,
				TransferDetails: make([]model.SolSwapData, 0),
				DCAOpenData:     dcadata,
			}

			return []model.SolSwapData{item}, nil
		}
	}

	if len(data.NativeTransfers) > 0 {
		coinmap := make(map[string]map[string]int, 0)

		for _, v := range data.NativeTransfers {
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

		for from, toitem := range coinmap {
			for to, val := range toitem {
				amount := toDecimal(fmt.Sprintf("%d", val), 9)
				item := model.SolSwapData{
					TxHash:    data.Signature,
					Source:    data.Source,
					Timestamp: data.Timestamp,
					Type:      "TRANSFER",
					Date:      timeString,

					FromToken:        "So11111111111111111111111111111111111111112",
					FromTokenAccount: from,
					FromUserAccount:  from,
					FromTokenAmount:  amount,

					ToToken:         "So11111111111111111111111111111111111111112",
					ToTokenAccount:  to,
					ToUserAccount:   to,
					ToTokenAmount:   amount,
					TradeLabel:      "",
					IsDCATrade:      isDCATrade,
					WalletCounts:    1,
					TransferDetails: make([]model.SolSwapData, 0),
				}

				if CheckTxValid(item.FromUserAccount, item.ToUserAccount) {
					resData = append(resData, item)
				} else {
					logger.Logrus.WithFields(logrus.Fields{"Data": item}).Error("parseCommonTransfer tx invalid")
				}
			}
		}
	}

	if len(data.TokenTransfers) > 0 {
		for _, v := range data.TokenTransfers {
			if v.FromUserAccount != feeplayer && v.ToUserAccount != feeplayer {
				continue
			}

			item := model.SolSwapData{
				TxHash:    data.Signature,
				Source:    data.Source,
				Timestamp: data.Timestamp,
				Type:      "TRANSFER",
				Date:      timeString,

				FromToken:        v.Mint,
				FromTokenAccount: v.FromTokenAccount,
				FromUserAccount:  v.FromUserAccount,
				FromTokenAmount:  v.TokenAmount,

				ToToken:         v.Mint,
				ToTokenAccount:  v.ToTokenAccount,
				ToUserAccount:   v.ToUserAccount,
				ToTokenAmount:   v.TokenAmount,
				TradeLabel:      "",
				IsDCATrade:      isDCATrade,
				WalletCounts:    1,
				TransferDetails: make([]model.SolSwapData, 0),
			}

			if CheckTxValid(item.FromUserAccount, item.ToUserAccount) {
				resData = append(resData, item)
			} else {
				logger.Logrus.WithFields(logrus.Fields{"Data": item}).Error("parseCommonTransfer tx invalid")
			}
		}
	}

	grouprecords := make([][]model.SolSwapData, 0)
	firstdata := make([]model.SolSwapData, 0)
	seconddata := make([]model.SolSwapData, 0)
	for _, v := range resData {
		if feeplayer == v.FromUserAccount {
			firstdata = append(firstdata, v)
		} else if feeplayer == v.ToUserAccount {
			seconddata = append(seconddata, v)
		}
	}

	if len(firstdata) > 0 {
		grouprecords = append(grouprecords, firstdata)
	}

	if len(seconddata) > 0 {
		grouprecords = append(grouprecords, seconddata)
	}

	result := make([]model.SolSwapData, 0)

	for _, datalist := range grouprecords {
		mergelist := make(map[string][]model.SolSwapData)
		for _, val := range datalist {
			fmap, ok := mergelist[val.FromToken]
			if !ok {
				fmap = make([]model.SolSwapData, 0)
			}

			fmap = append(fmap, val)
			mergelist[val.FromToken] = fmap
		}

		for _, list := range mergelist {
			if len(list) < 4 {
				result = append(result, list...)
				continue
			}

			frommap := make(map[string]float64)
			tomap := make(map[string]float64)
			totalamount := float64(0.0)

			for _, v := range list {
				frommap[v.FromUserAccount] += v.FromTokenAmount
				tomap[v.ToUserAccount] += v.ToTokenAmount
				totalamount += v.ToTokenAmount
			}

			fromcount := len(frommap)
			tocount := len(tomap)

			if fromcount == 1 && tocount > 1 {
				for addr, amt := range frommap {
					fromaddr := addr
					if totalamount != amt {
						continue
					}

					if CheckTxValid(fromaddr, fromaddr) {
						item := list[0]
						item.FromTokenAmount = amt
						item.ToTokenAccount = ""
						item.ToUserAccount = ""
						item.ToTokenAmount = amt
						item.WalletCounts = len(list)
						item.TransferDetails = list

						result = append(result, item)
					} else {
						for _, val := range list {
							if CheckTxValid(val.ToUserAccount, val.ToUserAccount) {
								result = append(result, val)
							}
						}
					}
				}

			} else if fromcount > 1 && tocount == 1 {
				for addr, amt := range tomap {
					toaddr := addr
					if totalamount != amt {
						continue
					}

					if CheckTxValid(toaddr, toaddr) {
						item := list[0]
						item.FromTokenAccount = ""
						item.FromUserAccount = ""
						item.FromTokenAmount = amt
						item.ToTokenAmount = amt
						item.WalletCounts = len(list)
						item.TransferDetails = list

						result = append(result, item)
					} else {
						for _, val := range list {
							if CheckTxValid(val.FromUserAccount, val.FromUserAccount) {
								result = append(result, val)
							}
						}
					}
				}
			} else {
				result = append(result, list...)
			}
		}
	}

	return result, nil
}

func parseGobleSwap(in HeliusData) (*model.SolSwapData, error) {
	if len(in.TokenTransfers) == 1 && len(in.NativeTransfers) > 0 {
		feeplayer := in.FeePayer

		tokenTransfer := in.TokenTransfers[0]

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

			item := &model.SolSwapData{
				TxHash:           in.Signature,
				Source:           "",
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

			return item, nil
		} else if feeplayer == tokenTransfer.FromUserAccount {
			//sell other coin using sol
			toAmt := float64(0)
			for _, v := range in.AccountData {
				if v.Account == tokenTransfer.ToUserAccount {
					toAmt = float64(math.Abs(float64(v.NativeBalanceChange))) / math.Pow10(9)
				}
			}

			item := &model.SolSwapData{
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

			return item, nil
		}

		return nil, fmt.Errorf("no swap data")
	}

	feePayer := in.FeePayer
	transferSummary := make(map[string]map[string]float64)

	addrlookup := make(map[string]map[string]string)

	t := time.Unix(int64(in.Timestamp), 0)
	timeString := t.Format("2006-01-02 15:04:05")

	res := &model.SolSwapData{
		TxHash:    in.Signature,
		Source:    "", //program_id
		Timestamp: in.Timestamp,
		Type:      "SWAP",
		Date:      timeString,

		FromToken:        "",
		FromTokenAccount: "",
		FromUserAccount:  "",
		FromTokenAmount:  0,

		ToToken:         "",
		ToTokenAccount:  "",
		ToUserAccount:   "",
		ToTokenAmount:   0,
		TradeLabel:      "",
		IsDCATrade:      false,
		WalletCounts:    1,
		TransferDetails: make([]model.SolSwapData, 0),
	}

	for _, transfer := range in.TokenTransfers {
		if transfer.Mint == "" {
			continue
		}

		amount := transfer.TokenAmount

		//save token account
		if _, ok := addrlookup[transfer.Mint]; !ok {
			addrlookup[transfer.Mint] = make(map[string]string, 0)
		}

		//
		if _, ok := transferSummary[transfer.Mint]; !ok {
			transferSummary[transfer.Mint] = make(map[string]float64)
		}

		if transfer.FromUserAccount == feePayer {
			transferSummary[transfer.Mint]["sent"] = addBigFloat(transferSummary[transfer.Mint]["sent"], amount, 18)

			addrlookup[transfer.Mint][feePayer] = transfer.FromTokenAccount
		} else if transfer.ToUserAccount == feePayer {
			transferSummary[transfer.Mint]["received"] = addBigFloat(transferSummary[transfer.Mint]["received"], amount, 18)

			addrlookup[transfer.Mint][feePayer] = transfer.ToTokenAccount
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
		return nil, fmt.Errorf("transfer not match, %s, %d", in.Signature, len(formattedSummary))
	}

	for mint, v := range formattedSummary {
		toamtChange := v["received"]
		if toamtChange > 0 {
			res.ToToken = mint
			res.ToUserAccount = feePayer
			res.ToTokenAccount = addrlookup[mint][feePayer]
			res.ToTokenAmount = toamtChange
		}

		fromamtChange := v["sent"]
		if fromamtChange < 0 {
			res.FromToken = mint
			res.FromUserAccount = feePayer
			res.FromTokenAccount = addrlookup[mint][feePayer]
			res.FromTokenAmount = float64(0) - fromamtChange
		}
	}

	if res.FromUserAccount == res.ToUserAccount {
		return res, nil
	}

	return nil, fmt.Errorf("no swap data, %s", in.Signature)
}

func parseGobleDEX(data HeliusData) ([]model.SolSwapData, error) {
	dexnameMap := make(map[string]string, 0)
	dexmap := config.GetDexConfig()
	for _, v := range dexmap {
		dexnameMap[v.ContractAddress] = v.DexName
	}

	source := ""
	for _, instruction := range data.Instructions {
		if instruction.ProgramID == "ComputeBudget111111111111111111111111111111" ||
			instruction.ProgramID == "11111111111111111111111111111111" ||
			strings.Contains(instruction.ProgramID, "Token") {
			continue
		}

		dexname, ok := dexnameMap[instruction.ProgramID]
		if ok {
			source = dexname
			break
		}

		for _, item := range instruction.InnerInstructions {
			if item.ProgramID == "ComputeBudget111111111111111111111111111111" ||
				item.ProgramID == "11111111111111111111111111111111" ||
				strings.Contains(item.ProgramID, "Token") {
				continue
			}

			dexname, ok := dexnameMap[item.ProgramID]
			if ok {
				source = dexname
				break
			}
		}

		if source != "" {
			break
		}
	}

	if source == "" {
		return nil, fmt.Errorf("program ids not match,%s", data.Signature)
	}

	resData := make([]model.SolSwapData, 0)
	result, err := parseGobleSwap(data)
	if err != nil {
		return nil, err
	}

	result.Source = source
	result.Type = "SWAP"

	if result.FromUserAccount != "" && result.FromUserAccount == result.ToUserAccount && result.ToTokenAmount != 0 {
		resData = append(resData, *result)
	} else {
		logger.Logrus.WithFields(logrus.Fields{"Data": result}).Error("parseGobleDEX tx invalid")
		return nil, fmt.Errorf("not swap, %s", data.Signature)
	}

	return resData, nil
}

func handTransferData(v HeliusData) error {
	datait, err := parseGobleDEX(v)
	if err != nil {
		datait, err = parseCommonTransfer(v)
		if err != nil {
			return fmt.Errorf("parse transfer data, %v", err)
		}
	}

	datalist := FillLabel(datait)

	err = sendKafkaMsg(datalist)
	if err != nil {
		return fmt.Errorf("send kafka failed, %v", err)
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": datalist}).Info("handTransferData send transfer kafka data")

	return nil
}
