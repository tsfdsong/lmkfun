package alikafka

import (
	"os"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/utils/logger"
)

// one DB one client
var kafkaClient *kafka.Producer
var once sync.Once

var kafkaAddrClient *kafka.Consumer
var onceAddr sync.Once

var kafkaHistoryClient *kafka.Producer
var onceHis sync.Once

func InitKafka() error {
	kafkaClient = GetKafkaInst()
	kafkaAddrClient = GetKafkaAddrInst()
	kafkaHistoryClient = GetKafkaHistoryInst()
	return nil
}

func GetKafkaInst() *kafka.Producer {
	once.Do(func() {
		cfg := config.GetKafkaConfig()

		var kafkaconf = &kafka.ConfigMap{
			"api.version.request": "true",
			"message.max.bytes":   1000000,
			"linger.ms":           10,
			"retries":             30,
			"retry.backoff.ms":    1000,
			"acks":                "1"}
		kafkaconf.SetKey("bootstrap.servers", cfg.Host)

		switch cfg.Protocol {
		case "plaintext":
			kafkaconf.SetKey("security.protocol", "plaintext")
		case "sasl_ssl":
			kafkaconf.SetKey("security.protocol", "sasl_ssl")
			kafkaconf.SetKey("ssl.ca.location", "conf/ca-cert.pem")
			kafkaconf.SetKey("sasl.username", cfg.Username)
			kafkaconf.SetKey("sasl.password", cfg.Password)
			kafkaconf.SetKey("sasl.mechanism", "PLAIN")
			kafkaconf.SetKey("enable.ssl.certificate.verification", "false")
			kafkaconf.SetKey("ssl.endpoint.identification.algorithm", "None")
			kafkaconf.SetKey("ssl.ca.location", cfg.CAPath)
		case "sasl_plaintext":
			kafkaconf.SetKey("sasl.mechanism", "PLAIN")
			kafkaconf.SetKey("security.protocol", "sasl_plaintext")
			kafkaconf.SetKey("sasl.username", cfg.Username)
			kafkaconf.SetKey("sasl.password", cfg.Password)
		default:
			logger.Logrus.WithFields(logrus.Fields{"ErrMsg": "unknown protocol" + cfg.Protocol}).Error("unknown kafka protocol")
			os.Exit(1)
		}

		client, err := kafka.NewProducer(kafkaconf)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("connect kafka failed")
			os.Exit(1)
		}

		go func(p *kafka.Producer) {
			for e := range p.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						logger.Logrus.WithFields(logrus.Fields{"Data": ev.TopicPartition}).Error("Delivery message failed")
					} else {
						// logger.Logrus.WithFields(logrus.Fields{"Data": ev.TopicPartition}).Info("Delivery message success")
					}
				}
			}
		}(client)

		kafkaClient = client
	})
	return kafkaClient
}

func GetKafkaAddrInst() *kafka.Consumer {
	onceAddr.Do(func() {
		cfg := config.GetKafkaConfig()

		var kafkaconf = &kafka.ConfigMap{
			"api.version.request":       "true",
			"auto.offset.reset":         "latest",
			"enable.auto.commit":        true,
			"auto.commit.interval.ms":   2000,
			"heartbeat.interval.ms":     3000,
			"session.timeout.ms":        30000,
			"max.poll.interval.ms":      120000,
			"fetch.max.bytes":           1024000,
			"max.partition.fetch.bytes": 256000}
		kafkaconf.SetKey("bootstrap.servers", cfg.Host)
		kafkaconf.SetKey("group.id", cfg.AddrGroupID)

		switch cfg.Protocol {
		case "plaintext":
			kafkaconf.SetKey("security.protocol", "plaintext")
			kafkaconf.SetKey("sasl.username", cfg.Username)
			kafkaconf.SetKey("sasl.password", cfg.Password)
		case "sasl_ssl":
			kafkaconf.SetKey("security.protocol", "sasl_ssl")
			kafkaconf.SetKey("ssl.ca.location", "conf/ca-cert.pem")
			kafkaconf.SetKey("sasl.username", cfg.Username)
			kafkaconf.SetKey("sasl.password", cfg.Password)
			kafkaconf.SetKey("sasl.mechanism", "PLAIN")
			kafkaconf.SetKey("enable.ssl.certificate.verification", "false")
			kafkaconf.SetKey("ssl.endpoint.identification.algorithm", "None")
			kafkaconf.SetKey("ssl.ca.location", cfg.CAPath)
		case "sasl_plaintext":
			kafkaconf.SetKey("sasl.mechanism", "PLAIN")
			kafkaconf.SetKey("security.protocol", "sasl_plaintext")
			kafkaconf.SetKey("sasl.username", cfg.Username)
			kafkaconf.SetKey("sasl.password", cfg.Password)
		default:
			logger.Logrus.WithFields(logrus.Fields{"ErrMsg": "unknown protocol" + cfg.Protocol}).Error("unknown kafka protocol")
			os.Exit(1)
		}

		client, err := kafka.NewConsumer(kafkaconf)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("connect kafka failed")
			os.Exit(1)
		}

		kafkaAddrClient = client
	})
	return kafkaAddrClient
}

func GetKafkaHistoryInst() *kafka.Producer {
	onceHis.Do(func() {
		cfg := config.GetKafkaConfig()

		var kafkaconf = &kafka.ConfigMap{
			"api.version.request": "true",
			"message.max.bytes":   1000000,
			"linger.ms":           10,
			"retries":             30,
			"retry.backoff.ms":    1000,
			"acks":                "1"}
		kafkaconf.SetKey("bootstrap.servers", cfg.Host)

		switch cfg.Protocol {
		case "plaintext":
			kafkaconf.SetKey("security.protocol", "plaintext")
		case "sasl_ssl":
			kafkaconf.SetKey("security.protocol", "sasl_ssl")
			kafkaconf.SetKey("ssl.ca.location", "conf/ca-cert.pem")
			kafkaconf.SetKey("sasl.username", cfg.Username)
			kafkaconf.SetKey("sasl.password", cfg.Password)
			kafkaconf.SetKey("sasl.mechanism", "PLAIN")
			kafkaconf.SetKey("enable.ssl.certificate.verification", "false")
			kafkaconf.SetKey("ssl.endpoint.identification.algorithm", "None")
			kafkaconf.SetKey("ssl.ca.location", cfg.CAPath)
		case "sasl_plaintext":
			kafkaconf.SetKey("sasl.mechanism", "PLAIN")
			kafkaconf.SetKey("security.protocol", "sasl_plaintext")
			kafkaconf.SetKey("sasl.username", cfg.Username)
			kafkaconf.SetKey("sasl.password", cfg.Password)
		default:
			logger.Logrus.WithFields(logrus.Fields{"ErrMsg": "unknown protocol" + cfg.Protocol}).Error("unknown kafka protocol")
			os.Exit(1)
		}

		client, err := kafka.NewProducer(kafkaconf)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err}).Error("connect kafka failed")
			os.Exit(1)
		}

		go func(p *kafka.Producer) {
			for e := range p.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						logger.Logrus.WithFields(logrus.Fields{"Data": ev.TopicPartition}).Error("Delivery message failed")
					} else {
						// logger.Logrus.WithFields(logrus.Fields{"Data": ev.TopicPartition}).Info("Delivery message success")
					}
				}
			}
		}(client)

		kafkaHistoryClient = client
	})
	return kafkaHistoryClient
}
