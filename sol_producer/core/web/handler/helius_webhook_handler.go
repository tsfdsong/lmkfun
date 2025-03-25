package handler

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"runtime"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/alikafka"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/utils/logger"
)

type HeliusData struct {
	AccountData      []AccountData           `json:"accountData"`
	Description      string                  `json:"description"`
	Events           EventData               `json:"events"`
	Fee              int                     `json:"fee"`
	FeePayer         string                  `json:"feePayer"`
	Instructions     []InstructionsData      `json:"instructions"`
	NativeTransfers  []NativeTransfersDetail `json:"nativeTransfers"`
	Signature        string                  `json:"signature"`
	Slot             int                     `json:"slot"`
	Source           string                  `json:"source"`
	Timestamp        int                     `json:"timestamp"`
	TokenTransfers   []TokenDetails          `json:"tokenTransfers"`
	TransactionError interface{}             `json:"transactionError"`
	Type             string                  `json:"type"`
}

type NativeTransfersDetail struct {
	FromUserAccount string `json:"fromUserAccount"`
	ToUserAccount   string `json:"toUserAccount"`
	Amount          int    `json:"amount"`
}

type AccountData struct {
	Account             string               `json:"account"`
	NativeBalanceChange int64                `json:"nativeBalanceChange"`
	TokenBalanceChanges []TokenBalanceChange `json:"tokenBalanceChanges"`
}

type TokenBalanceChange struct {
	Mint           string         `json:"mint"`
	RawTokenAmount RawTokenAmount `json:"rawTokenAmount"`
	TokenAccount   string         `json:"tokenAccount"`
	UserAccount    string         `json:"userAccount"`
}

type RawTokenAmount struct {
	Decimals    int    `json:"decimals"`
	TokenAmount string `json:"tokenAmount"`
}

type InnerInstructionsData struct {
	Accounts  []string `json:"accounts"`
	Data      string   `json:"data"`
	ProgramID string   `json:"programId"`
}

type InstructionsData struct {
	Accounts          []string                `json:"accounts"`
	Data              string                  `json:"data"`
	InnerInstructions []InnerInstructionsData `json:"innerInstructions"`
	ProgramID         string                  `json:"programId"`
}

type EventData struct {
	Swap SwapData `json:"swap"`
}

type NativeDetail struct {
	Account string `json:"account"`
	Amount  string `json:"amount"`
}

type SwapData struct {
	InnerSwaps   []InnerSwapData      `json:"innerSwaps"`
	NativeFees   []interface{}        `json:"nativeFees"`
	NativeInput  NativeDetail         `json:"nativeInput"`
	NativeOutput NativeDetail         `json:"nativeOutput"`
	TokenFees    []interface{}        `json:"tokenFees"`
	TokenInputs  []TokenBalanceChange `json:"tokenInputs"`
	TokenOutputs []TokenBalanceChange `json:"tokenOutputs"`
}

type InnerSwapData struct {
	NativeFees   []interface{}  `json:"nativeFees"`
	ProgramInfo  ProgramInfo    `json:"programInfo"`
	TokenFees    []interface{}  `json:"tokenFees"`
	TokenInputs  []TokenDetails `json:"tokenInputs"`
	TokenOutputs []TokenDetails `json:"tokenOutputs"`
}

type ProgramInfo struct {
	Account         string `json:"account"`
	InstructionName string `json:"instructionName"`
	ProgramName     string `json:"programName"`
	Source          string `json:"source"`
}

type TokenDetails struct {
	FromTokenAccount string  `json:"fromTokenAccount"`
	FromUserAccount  string  `json:"fromUserAccount"`
	Mint             string  `json:"mint"`
	ToTokenAccount   string  `json:"toTokenAccount"`
	ToUserAccount    string  `json:"toUserAccount"`
	TokenAmount      float64 `json:"tokenAmount"`
	TokenStandard    string  `json:"tokenStandard"`
}

func PrintStack() string {
	var buf [4096]byte
	n := runtime.Stack(buf[:], false)
	return string(buf[:n])
}

func toDecimal(amount string, decimal int) float64 {
	da, ok := new(big.Float).SetString(amount)
	if !ok {
		return float64(0)
	}

	precision := new(big.Float).SetFloat64(math.Pow10(decimal))

	// 计算不带精度的小数值
	result := new(big.Float).Quo(da, precision)
	res, _ := result.Float64()

	return res
}

func generateSwapData(in HeliusData) ([]model.SolSwapData, error) {
	res := make([]model.SolSwapData, 0)

	t := time.Unix(int64(in.Timestamp), 0)
	timeString := t.Format("2006-01-02 15:04:05")

	cfg := config.GetHeliusConfig()
	DCAProgramedID := cfg.DCAProgrameID
	isDCATrade := false

	DCARecived := ""
	DCAUser := ""
	for _, item := range in.Instructions {
		if item.ProgramID == DCAProgramedID {
			isDCATrade = true

			if len(item.Accounts) == 12 {
				DCAUser = item.Accounts[2]
				DCARecived = item.Accounts[5]
			}
		}
	}

	swapdata := in.Events.Swap
	if len(swapdata.TokenInputs) == 0 && len(swapdata.TokenOutputs) == 0 {
		innerdata := in.Events.Swap.InnerSwaps

		if len(innerdata) == 1 {
			v := innerdata[0]
			if len(v.TokenInputs) == 1 && len(v.TokenOutputs) == 1 {
				item := model.SolSwapData{
					TxHash:          in.Signature,
					Source:          in.Source,
					Timestamp:       in.Timestamp,
					Type:            in.Type,
					Date:            timeString,
					TradeLabel:      "",
					IsDCATrade:      isDCATrade,
					WalletCounts:    1,
					TransferDetails: make([]model.SolSwapData, 0),
				}

				if len(v.TokenInputs) == 1 {
					item.FromToken = v.TokenInputs[0].Mint
					item.FromTokenAccount = v.TokenInputs[0].FromTokenAccount
					item.FromUserAccount = v.TokenInputs[0].FromUserAccount
					item.FromTokenAmount = v.TokenInputs[0].TokenAmount
				}
				if len(v.TokenOutputs) == 1 {
					item.ToToken = v.TokenOutputs[0].Mint
					item.ToTokenAccount = v.TokenOutputs[0].ToTokenAccount
					item.ToUserAccount = v.TokenOutputs[0].ToUserAccount
					item.ToTokenAmount = v.TokenOutputs[0].TokenAmount
				}

				if item.FromUserAccount != "" && item.FromUserAccount == item.ToUserAccount && item.ToTokenAmount != 0 {
					res = append(res, item)
				} else {
					logger.Logrus.WithFields(logrus.Fields{"Data": swapdata}).Error("generateSwapData raw swap data failed")
					return nil, fmt.Errorf("swap failed,%s", in.Signature)
				}
			}
		}
	} else {
		first := model.SolSwapData{
			TxHash:          in.Signature,
			Source:          in.Source,
			Timestamp:       in.Timestamp,
			Type:            in.Type,
			Date:            timeString,
			TradeLabel:      "",
			IsDCATrade:      isDCATrade,
			WalletCounts:    1,
			TransferDetails: make([]model.SolSwapData, 0),
		}
		if len(swapdata.TokenInputs) == 1 {
			first.FromToken = swapdata.TokenInputs[0].Mint
			first.FromTokenAccount = swapdata.TokenInputs[0].TokenAccount
			first.FromUserAccount = swapdata.TokenInputs[0].UserAccount
			first.FromTokenAmount = toDecimal(swapdata.TokenInputs[0].RawTokenAmount.TokenAmount, swapdata.TokenInputs[0].RawTokenAmount.Decimals)
		} else if len(swapdata.TokenInputs) == 0 {
			first.FromToken = "So11111111111111111111111111111111111111112"
			first.FromTokenAccount = swapdata.NativeInput.Account
			first.FromUserAccount = swapdata.NativeInput.Account
			first.FromTokenAmount = toDecimal(swapdata.NativeInput.Amount, 9)
		}

		if len(swapdata.TokenOutputs) == 1 {
			first.ToToken = swapdata.TokenOutputs[0].Mint
			first.ToTokenAccount = swapdata.TokenOutputs[0].TokenAccount
			first.ToUserAccount = swapdata.TokenOutputs[0].UserAccount
			first.ToTokenAmount = toDecimal(swapdata.TokenOutputs[0].RawTokenAmount.TokenAmount, swapdata.TokenOutputs[0].RawTokenAmount.Decimals)
		} else if len(swapdata.TokenOutputs) == 0 {
			first.ToToken = "So11111111111111111111111111111111111111112"
			first.ToTokenAccount = swapdata.NativeOutput.Account
			first.ToUserAccount = swapdata.NativeOutput.Account
			first.ToTokenAmount = toDecimal(swapdata.NativeOutput.Amount, 9)
		}

		if first.FromUserAccount != "" && first.FromUserAccount == first.ToUserAccount {
			if first.IsDCATrade && DCARecived != "" {
				first.FromTokenAccount = ""
				first.FromUserAccount = DCAUser
				first.ToTokenAccount = DCARecived
				first.ToUserAccount = DCAUser
			}
			res = append(res, first)
		} else {
			logger.Logrus.WithFields(logrus.Fields{"Data": in.TokenTransfers}).Info("generateSwapData raw swap data info")

			if len(in.TokenTransfers) == 2 {
				if in.TokenTransfers[0].ToUserAccount == in.TokenTransfers[1].FromUserAccount {
					first.FromToken = in.TokenTransfers[0].Mint
					first.FromTokenAccount = in.TokenTransfers[0].FromTokenAccount
					first.FromUserAccount = in.TokenTransfers[0].FromUserAccount
					first.FromTokenAmount = in.TokenTransfers[0].TokenAmount

					first.ToToken = in.TokenTransfers[1].Mint
					first.ToTokenAccount = in.TokenTransfers[1].ToTokenAccount
					first.ToUserAccount = in.TokenTransfers[1].ToUserAccount
					first.ToTokenAmount = in.TokenTransfers[1].TokenAmount
				} else if in.TokenTransfers[1].ToUserAccount == in.TokenTransfers[0].FromUserAccount {
					first.FromToken = in.TokenTransfers[1].Mint
					first.FromTokenAccount = in.TokenTransfers[1].FromTokenAccount
					first.FromUserAccount = in.TokenTransfers[1].FromUserAccount
					first.FromTokenAmount = in.TokenTransfers[1].TokenAmount

					first.ToToken = in.TokenTransfers[0].Mint
					first.ToTokenAccount = in.TokenTransfers[0].ToTokenAccount
					first.ToUserAccount = in.TokenTransfers[0].ToUserAccount
					first.ToTokenAmount = in.TokenTransfers[0].TokenAmount
				}

				if first.FromUserAccount != "" && first.FromUserAccount == first.ToUserAccount {
					res = append(res, first)
				} else {
					logger.Logrus.WithFields(logrus.Fields{"Data": swapdata}).Error("generateSwapData raw innerswap data failed")

					return nil, fmt.Errorf("innerswap failed,%s", in.Signature)
				}
			} else {
				feePayer := in.FeePayer
				transferSummary := make(map[string]map[string]float64)

				addrlookup := make(map[string]map[string]string)

				t := time.Unix(int64(in.Timestamp), 0)
				timeString := t.Format("2006-01-02 15:04:05")

				resData := model.SolSwapData{
					TxHash:    in.Signature,
					Source:    in.Source,
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

					if transfer.FromUserAccount != feePayer && transfer.ToUserAccount != feePayer {
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

					//no need calucate decimals
					if transfer.FromUserAccount == feePayer {
						transferSummary[transfer.Mint]["sent"] = transferSummary[transfer.Mint]["sent"] + amount

						addrlookup[transfer.Mint][feePayer] = transfer.FromTokenAccount
					} else if transfer.ToUserAccount == feePayer {
						transferSummary[transfer.Mint]["received"] = transferSummary[transfer.Mint]["received"] + amount

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
						resData.ToToken = mint
						resData.ToUserAccount = feePayer
						resData.ToTokenAccount = addrlookup[mint][feePayer]
						resData.ToTokenAmount = toamtChange
					}

					fromamtChange := v["sent"]
					if fromamtChange < 0 {
						resData.FromToken = mint
						resData.FromUserAccount = feePayer
						resData.FromTokenAccount = addrlookup[mint][feePayer]
						resData.FromTokenAmount = float64(0) - fromamtChange
					}
				}

				if resData.FromUserAccount == resData.ToUserAccount {
					res = append(res, resData)
				}
			}
		}
	}

	if len(res) < 1 {
		return nil, fmt.Errorf("parse failed")
	}

	return res, nil
}

func sendKafkaMsg(in []model.SolSwapData) error {
	keyStr := "default"
	if len(in) > 0 {
		keyStr = in[0].TxHash
	} else {
		return nil
	}

	data, err := json.Marshal(&in)
	if err != nil {
		return err
	}

	cfg := config.GetKafkaConfig()
	err = alikafka.GetKafkaInst().Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &cfg.Topic, Partition: kafka.PartitionAny},
		Key:            []byte(keyStr),
		Value:          []byte(data),
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	alikafka.GetKafkaInst().Flush(5000)

	return nil
}

func HandleData(in []HeliusData) error {
	for _, v := range in {
		logger.Logrus.WithFields(logrus.Fields{"Data": v}).Info("HandleData raw data")

		if v.Type == "SWAP" || v.Type == "CREATE" {
			err := handSwapData(v)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Data": v, "ErrMsg": err}).Error("HandleData handle swap data failed")

				return err
			}
		} else {
			err := handTransferData(v)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Data": v, "ErrMsg": err}).Error("HandleData handle transfer data failed")
				continue
			}
		}
	}

	return nil
}

func HeliusWebHookHandler(c *gin.Context) {
	r := &Response{
		Code:    http.StatusOK,
		Message: "success",
	}
	defer func(r *Response) {
		err := recover()
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err, "Stack": PrintStack()}).Error("HeliusWebHookHandler panic")
			c.JSON(http.StatusInternalServerError, r)
		} else {
			c.JSON(http.StatusOK, r)
		}
	}(r)

	var inp []HeliusData
	err := c.ShouldBind(&inp)
	if err != nil {
		logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("HeliusWebHookHandler parse parmeter failed")
		r.Code = http.StatusBadRequest
		r.Message = "invalid input parameters"
		return
	}

	if len(inp) == 0 {
		logger.Logrus.WithFields(logrus.Fields{"ErrMsg": "no helius data"}).Error("HeliusWebHookHandler no data")
		r.Code = http.StatusInternalServerError
		r.Message = "no helius data"
		return
	}

	err = HandleData(inp)
	if err != nil {
		logger.Logrus.WithFields(logrus.Fields{"Data": inp, "ErrMsg": err}).Error("HeliusWebHookHandler handle swap data failed")
		r.Code = http.StatusInternalServerError
		r.Message = "handle data failed"
		return
	}

	r.Message = "success"
	r.Data = ""
}
