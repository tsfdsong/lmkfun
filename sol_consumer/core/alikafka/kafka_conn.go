package alikafka

import (
	"os"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

// one DB one client
var kafkaClient *kafka.Consumer
var once sync.Once

var kafkaKOLClient *kafka.Consumer
var onceKOL sync.Once

var kafkaHisClient *kafka.Consumer
var onceHis sync.Once

var kafkaBitqueryClient *kafka.Consumer
var onceBitquery sync.Once

var kafkaProClient *kafka.Producer
var oncePro sync.Once

func InitKafka() error {
	kafkaClient = GetKafkaInst()
	kafkaKOLClient = GetKafkaExKOLInst()
	kafkaHisClient = GetKafkaHistoryInst()

	kafkaProClient = GetKafkaProInst()
	return nil
}

func GetKafkaInst() *kafka.Consumer {
	once.Do(func() {
		cfg := config.GetKafkaConfig()

		var kafkaconf = &kafka.ConfigMap{
			"api.version.request":       "true",
			"auto.offset.reset":         "latest",
			"enable.auto.commit":        true,
			"auto.commit.interval.ms":   1000,
			"heartbeat.interval.ms":     3000,
			"session.timeout.ms":        30000,
			"max.poll.interval.ms":      120000,
			"fetch.max.bytes":           1024000,
			"max.partition.fetch.bytes": 256000}
		kafkaconf.SetKey("bootstrap.servers", cfg.Host)
		kafkaconf.SetKey("group.id", cfg.GroupID)

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

		kafkaClient = client
	})
	return kafkaClient
}

func GetKafkaExKOLInst() *kafka.Consumer {
	onceKOL.Do(func() {
		cfg := config.GetKafkaConfig()

		var kafkaconf = &kafka.ConfigMap{
			"api.version.request":       "true",
			"auto.offset.reset":         "latest",
			"enable.auto.commit":        true,
			"auto.commit.interval.ms":   1000,
			"heartbeat.interval.ms":     3000,
			"session.timeout.ms":        30000,
			"max.poll.interval.ms":      120000,
			"fetch.max.bytes":           1024000,
			"max.partition.fetch.bytes": 256000}
		kafkaconf.SetKey("bootstrap.servers", cfg.Host)
		kafkaconf.SetKey("group.id", cfg.ExKOLGroupID)

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

		kafkaKOLClient = client
	})
	return kafkaKOLClient
}

func GetKafkaHistoryInst() *kafka.Consumer {
	onceHis.Do(func() {
		cfg := config.GetKafkaConfig()

		var kafkaconf = &kafka.ConfigMap{
			"api.version.request":       "true",
			"auto.offset.reset":         "latest",
			"enable.auto.commit":        true,
			"auto.commit.interval.ms":   1000,
			"heartbeat.interval.ms":     3000,
			"session.timeout.ms":        30000,
			"max.poll.interval.ms":      120000,
			"fetch.max.bytes":           1024000,
			"max.partition.fetch.bytes": 256000}
		kafkaconf.SetKey("bootstrap.servers", cfg.Host)
		kafkaconf.SetKey("group.id", cfg.HistoryGroupID)

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

		kafkaHisClient = client
	})
	return kafkaHisClient
}

func GetKafkaBitqueryInst() *kafka.Consumer {
	onceBitquery.Do(func() {
		cfg := config.GetKafkaConfig()

		var kafkaconf = &kafka.ConfigMap{
			"api.version.request":       "true",
			"auto.offset.reset":         "latest",
			"enable.auto.commit":        false,
			"auto.commit.interval.ms":   1000,
			"heartbeat.interval.ms":     20000,
			"session.timeout.ms":        60000,
			"max.poll.interval.ms":      300000,
			"fetch.max.bytes":           1024000,
			"max.partition.fetch.bytes": 256000}
		kafkaconf.SetKey("bootstrap.servers", cfg.Host)
		kafkaconf.SetKey("group.id", cfg.BitQueryGroupID)

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

		kafkaBitqueryClient = client
	})
	return kafkaBitqueryClient
}

func GetKafkaProInst() *kafka.Producer {
	oncePro.Do(func() {
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

		kafkaProClient = client
	})
	return kafkaProClient
}
