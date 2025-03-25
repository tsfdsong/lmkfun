package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/alikafka"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/utils/logger"
)

func getBeforeHistoryTxs(addr, txhash string) ([]HeliusData, string, error) {
	apiKey := config.GetHeliusConfig().APIKey

	url := fmt.Sprintf("https://api.helius.xyz/v0/addresses/%s/transactions?api-key=%s&limit=50", addr, apiKey)

	if txhash != "" {
		url = fmt.Sprintf("https://api.helius.xyz/v0/addresses/%s/transactions?api-key=%s&before=%s&limit=50", addr, apiKey, txhash)
	}
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return nil, "", err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, "", fmt.Errorf("status, %v", res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", err
	}

	var result []HeliusData
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, "", err
	}

	firsttxhash := ""
	if len(result) > 0 {
		sort.Slice(result, func(i, j int) bool {
			return result[i].Timestamp > result[j].Timestamp
		})

		firsttxhash = result[len(result)-1].Signature
	}

	return result, firsttxhash, nil
}

func getHistoryTxs(addr string) ([]HeliusData, error) {
	result := make([]HeliusData, 0)

	firsttxhash := ""
	for i := 0; i < 2; i++ {
		tmpData, tmptxhash, err := getBeforeHistoryTxs(addr, firsttxhash)
		if err != nil {
			time.Sleep(1 * time.Second)
			tmpData, tmptxhash, err = getBeforeHistoryTxs(addr, firsttxhash)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Index": i, "Data": addr, "ErrMsg": err, "FirstHash": firsttxhash}).Error("HandleHeliusData getHistoryTxs failed")
				return result, nil
			}
		}

		if tmptxhash == "" || len(tmpData) == 0 {
			break
		}

		result = append(result, tmpData...)

		firsttxhash = tmptxhash
	}

	return result, nil
}

func sendHistoryKafkaMsg(in []model.SolSwapData) error {
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
	err = alikafka.GetKafkaHistoryInst().Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &cfg.HistoryTopic, Partition: kafka.PartitionAny},
		Key:            []byte(keyStr),
		Value:          []byte(data),
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	alikafka.GetKafkaHistoryInst().Flush(15000)

	return nil
}

func handHisSwapData(v HeliusData) error {
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

		err = sendHistoryKafkaMsg(datalist)
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

		err = sendHistoryKafkaMsg(datalist)
		if err != nil {
			return fmt.Errorf("send kafka failed, %v", err)
		}

		logger.Logrus.WithFields(logrus.Fields{"Data": datalist}).Info("handSwapData send swap kafka data")
	}

	return nil
}

func handHisTransferData(v HeliusData) error {
	datait, err := parseGobleDEX(v)
	if err != nil {
		datait, err = parseCommonTransfer(v)
		if err != nil {
			return fmt.Errorf("parse transfer data, %v", err)
		}
	}

	datalist := FillLabel(datait)

	err = sendHistoryKafkaMsg(datalist)
	if err != nil {
		return fmt.Errorf("send kafka failed, %v", err)
	}

	logger.Logrus.WithFields(logrus.Fields{"Data": datalist}).Info("handTransferData send transfer kafka data")

	return nil
}

func HandleHistoryData(addr string) error {
	in, err := getHistoryTxs(addr)
	if err != nil {
		return err
	}

	if len(in) < 1 {
		return nil
	}

	for _, v := range in {
		logger.Logrus.WithFields(logrus.Fields{"Data": v}).Info("HandleHistoryData raw data")

		if v.Type == "SWAP" || v.Type == "CREATE" {
			err := handHisSwapData(v)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Data": v, "ErrMsg": err}).Error("handHisSwapData handle swap data failed")

				continue
			}
		} else {
			err := handHisTransferData(v)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Data": v, "ErrMsg": err}).Error("handHisTransferData handle transfer data failed")
				continue
			}
		}
	}

	return nil
}

func SubAddrHistoryTxs() {
	go func() {
		cfg := config.GetKafkaConfig()
		consumer := alikafka.GetKafkaAddrInst()

		consumer.SubscribeTopics([]string{cfg.AddrTopic}, nil)

		semaphore := make(chan struct{}, 2000)

		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubAddrHistoryTxs read kafka message failed")
				continue
			}

			rawdata := msg.Value

			type RawAddressData struct {
				Address string `json:"address"`
			}
			var res RawAddressData
			err = json.Unmarshal(rawdata, &res)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubAddrHistoryTxs unmarshal kafka message failed")
				continue
			}

			address := res.Address

			semaphore <- struct{}{}

			go func(addr string) {
				defer func() { <-semaphore }()

				logger.Logrus.WithFields(logrus.Fields{"Data": addr}).Info("SubAddrHistoryTxs receive kafka message success")
				err := HandleHistoryData(addr)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"Address": addr, "ErrMsg": err}).Error("SubAddrHistoryTxs HandleHistoryData failed")
					return
				}

				logger.Logrus.WithFields(logrus.Fields{"Address": addr}).Info("SubAddrHistoryTxs HandleHistoryData success")
			}(address)
		}
	}()
}
