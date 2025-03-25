package solalter

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/alikafka"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type AlterService struct {
	TokenRule    map[string]bool
	MaxNum       int
	MInterval    int
	TxHashMap    sync.Map
	Mutex        sync.Mutex
	ServerConfig string
}

func NewAlterService() *AlterService {
	rule := make(map[string]bool)

	cfg := config.GetSolDataConfig()
	for _, v := range cfg.ThreadData {
		rule[v.ContractAddress] = true
	}

	return &AlterService{
		TokenRule:    rule,
		MaxNum:       cfg.ThreadNum,
		MInterval:    cfg.MergeInterval,
		ServerConfig: cfg.ServerConfigList,
	}
}

func (serv *AlterService) Start() {
	if strings.Contains(serv.ServerConfig, "sol") {
		serv.SubSolSwap()
		serv.SubSolHistoryTxs()

		ticker := time.NewTicker(10 * time.Minute)

		go func() {
			for t := range ticker.C {
				err := UpdateAllTrackAddrCache()
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"Time": t.String(), "ErrMsg": err}).Error("cache tracked address failed")

					continue
				}
			}
		}()
	}

	if strings.Contains(serv.ServerConfig, "evm") {
		serv.SubBitQuery()
		serv.HandleEvmTxMerge()
	}

	if strings.Contains(serv.ServerConfig, "exc") {
		serv.SubExchange()

		exchticker := time.NewTicker(5 * time.Minute)

		go func() {
			for t := range exchticker.C {
				_, err := SetExchangeTrackedCache()
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"Time": t.String(), "ErrMsg": err}).Error("cache exchange tracked failed")

					continue
				}

				// logger.Logrus.WithFields(logrus.Fields{"Data": data}).Info("cache exchange tracked success")
			}
		}()

		kolticker := time.NewTicker(5 * time.Minute)

		go func() {
			for t := range kolticker.C {
				_, err := UpdateKOLTrackedCache()
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"Time": t.String(), "ErrMsg": err}).Error("cache kol tracked failed")

					continue
				}

				// logger.Logrus.WithFields(logrus.Fields{"Time": t.String(), "Data": data}).Info("cache kol tracked success")
			}
		}()
	}
}

func (serv *AlterService) Close() {
	alikafka.GetKafkaInst().Close()
	alikafka.GetKafkaExKOLInst().Close()
	alikafka.GetKafkaHistoryInst().Close()
	alikafka.GetKafkaBitqueryInst().Close()
}

func (serv *AlterService) SubSolSwap() {
	go func() {
		cfg := config.GetKafkaConfig()
		consumer := alikafka.GetKafkaInst()

		consumer.SubscribeTopics([]string{cfg.Topic}, nil)

		semaphore := make(chan struct{}, serv.MaxNum)

		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubSolSwap read kafka message failed")
				continue
			}

			rawdata := msg.Value
			var res []model.SolSwapData
			err = json.Unmarshal(rawdata, &res)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err, "Data": rawdata}).Error("SubSolSwap unmarshal kafka message failed")
				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"Data": res}).Info("SubSolSwap receive kafka message success")

			go func(data []model.SolSwapData) {
				addrMap := make(map[string]string, 0)
				for _, v := range data {
					address := v.FromUserAccount
					txhash := v.TxHash

					if address != "" && txhash != "" {
						addrMap[address] = txhash
					}
				}

				for k, v := range addrMap {
					err := CheckAddressRateLimit("solana", k, v)
					if err != nil {
						logger.Logrus.WithFields(logrus.Fields{"Address": k, "TxHash": v, "ErrMsg": err}).Error("SubSolSwap CheckAddressRateLimit failed")
						continue
					}
				}

				err = handleSolSaveRecord(data, serv.TokenRule)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"Data": data, "ErrMsg": err}).Error("SubSolSwap handleSolSaveRecord failed")
					return
				}

				logger.Logrus.WithFields(logrus.Fields{"Data": data}).Info("SubSolSwap check blacklist ratelimit and insert record success")
			}(res)

			for _, data := range res {
				semaphore <- struct{}{}

				go func(data model.SolSwapData) {
					defer func() { <-semaphore }()

					startTime := time.Now().Unix()
					err := handleAddressSwapRule(data, serv.TokenRule)
					if err != nil && !errors.Is(err, ErrRateLimit) {
						err = handleAddressSwapRule(data, serv.TokenRule)
						if err != nil {
							logger.Logrus.WithFields(logrus.Fields{"Data": data, "ErrMsg": err}).Error("SubSolSwap handle swap rule failed")
							return
						}
					}

					endTime := time.Now().Unix()
					logger.Logrus.WithFields(logrus.Fields{"TimeINterval: s": endTime - startTime, "Data": data}).Info("SubSolSwap handle swap rule success")
				}(data)
			}
		}
	}()
}

func (serv *AlterService) SubExchange() {
	go func() {

		cfg := config.GetKafkaConfig()
		consumer := alikafka.GetKafkaExKOLInst()

		consumer.SubscribeTopics([]string{cfg.ExKOLTopic}, nil)
		semaphore := make(chan struct{}, serv.MaxNum)

		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubExchange read kafka message failed")
				continue
			}

			rawdata := msg.Value
			var res RawTopicData
			err = json.Unmarshal(rawdata, &res)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubExchange unmarshal kafka message failed")
				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"Data": res}).Info("SubExchange receive kafka message success")

			semaphore <- struct{}{}

			go func(data RawTopicData) {
				defer func() { <-semaphore }()

				if res.Type == "exchange" {
					val, err := res.Unmarshall()
					if err != nil {
						logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubExchange unmarshal kafka message failed")
						return
					}

					err = handleExchangeAlert(val)
					if err != nil {
						err = handleExchangeAlert(val)
						if err != nil {
							logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("SubExchange handleExchangeAlert failed")
							return
						}
					}

					logger.Logrus.WithFields(logrus.Fields{"Data": val}).Info("SubExchange handleExchangeAlert success")
				}

				if res.Type == "KOL" {
					val, err := res.UnmarshallKOL()
					if err != nil {
						logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubExchange unmarshal kafka message failed")
						return
					}

					err = handleKOLAlert(val)
					if err != nil {
						err = handleKOLAlert(val)
						if err != nil {
							logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("SubExchange handleKOLAlert failed")
							return
						}
					}

					logger.Logrus.WithFields(logrus.Fields{"Data": val}).Info("SubExchange handleKOLAlert success")
				}

				if res.Type == "publish_call" {
					val, err := res.UnmarshallCurated()
					if err != nil {
						logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubExchange unmarshal kafka message failed")
						return
					}

					err = handleCuratedCalls(val)
					if err != nil {
						err = handleCuratedCalls(val)
						if err != nil {
							logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("SubExchange handleCuratedCalls failed")
							return
						}
					}

					logger.Logrus.WithFields(logrus.Fields{"Data": val}).Info("SubExchange handleCuratedCalls success")

				}

				if res.Type == "fomo_call" {
					val, err := res.UnmarshallFomo()
					if err != nil {
						logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubExchange unmarshal kafka message failed")
						return
					}

					err = handleFomoCalls(val)
					if err != nil {
						err = handleFomoCalls(val)
						if err != nil {
							logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("SubExchange handleFomoCalls failed")
							return
						}
					}

					logger.Logrus.WithFields(logrus.Fields{"Data": val}).Info("SubExchange handleFomoCalls success")

				}
			}(res)
		}
	}()
}

func (serv *AlterService) SubSolHistoryTxs() {
	go func() {

		cfg := config.GetKafkaConfig()
		consumer := alikafka.GetKafkaHistoryInst()

		consumer.SubscribeTopics([]string{cfg.HistoryTopic}, nil)

		semaphore := make(chan struct{}, serv.MaxNum)

		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubSolHistoryTxs read kafka message failed")
				continue
			}

			rawdata := msg.Value

			var res []model.SolSwapData
			err = json.Unmarshal(rawdata, &res)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err, "Data": rawdata}).Error("SubSolHistoryTxs unmarshal kafka message failed")
				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"Data": res}).Info("SubSolHistoryTxs receive kafka message success")

			semaphore <- struct{}{}

			go func(data []model.SolSwapData) {
				defer func() { <-semaphore }()

				err := HandleHeliusHisData(data, serv.TokenRule)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"Data": data, "ErrMsg": err}).Error("SubSolHistoryTxs HandleHeliusHisData failed")
					return
				}

				logger.Logrus.WithFields(logrus.Fields{"Data": data}).Info("SubSolHistoryTxs HandleHeliusHisData success")
			}(res)
		}
	}()
}

func (serv *AlterService) HandleEvmTxMerge() {
	go func() {
		ticker := time.NewTicker(time.Duration(serv.MInterval) * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			serv.Mutex.Lock()

			serv.TxHashMap.Range(func(key any, value any) bool {
				txhash := key.(string)
				txlist := value.([]RawBitqueryAltertData)

				handleData := make([]RawBitqueryAltertData, 0)

				resdata, err := mergeTxList(txhash, txlist)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"TxHash": txhash, "ErrMsg": err}).Error("handleEvmTxMerge failed")

					handleData = append(handleData, txlist...)
				} else if resdata != nil {
					handleData = append(handleData, *resdata)
				}

				logger.Logrus.WithFields(logrus.Fields{"Data": txlist}).Info("handleEvmTxMerge info")

				go func(data []RawBitqueryAltertData) {
					err := handleEVMSaveRecord(data, serv.TokenRule)
					if err != nil {
						logger.Logrus.WithFields(logrus.Fields{"Data": data, "ErrMsg": err}).Error("HandleEvmTxMerge handleEVMSaveRecord failed")
						return
					}

					logger.Logrus.WithFields(logrus.Fields{"Data": data}).Info("HandleEvmTxMerge handleEVMSaveRecord success")
				}(handleData)

				for _, v := range handleData {
					go func(data RawBitqueryAltertData) {
						startTime := time.Now().Unix()
						err := handleBitQueryRule(data, serv.TokenRule)
						if err != nil {
							logger.Logrus.WithFields(logrus.Fields{"Data": data, "ErrMsg": err}).Error("SubBitQuery handle swap rule failed")
							return
						}

						endTime := time.Now().Unix()
						logger.Logrus.WithFields(logrus.Fields{"TimeINterval: s": endTime - startTime, "Data": data}).Info("SubBitQuery handle swap rule success")
					}(v)
				}

				serv.TxHashMap.Delete(txhash)
				return true
			})

			serv.Mutex.Unlock()
		}
	}()
}

func (serv *AlterService) SubBitQuery() {
	go func() {
		cfg := config.GetKafkaConfig()
		consumer := alikafka.GetKafkaBitqueryInst()

		consumer.SubscribeTopics([]string{cfg.BitQueryTopic}, nil)

		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubBitQuery read kafka message failed")
				continue
			}

			rawdata := msg.Value

			var res RawBitqueryAltertData
			err = json.Unmarshal(rawdata, &res)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubBitQuery unmarshal kafka message failed")
				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"Data": res}).Info("SubBitQuery receive kafka message success")

			serv.Mutex.Lock()

			if value, ok := serv.TxHashMap.Load(res.TxHash); ok {
				txlist := value.([]RawBitqueryAltertData)
				txlist = append(txlist, res)
				serv.TxHashMap.Store(res.TxHash, txlist)
			} else {
				serv.TxHashMap.Store(res.TxHash, []RawBitqueryAltertData{res})
			}

			serv.Mutex.Unlock()

			_, err = consumer.CommitMessage(msg)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("SubBitQuery CommitMessage failed")
				continue
			}
		}
	}()
}
