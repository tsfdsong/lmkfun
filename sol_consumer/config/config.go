package config

import (
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

// one database one instance
type PostgresqlConfig struct {
	Host       string
	Port       int64
	Account    string
	Password   string
	DBName     string
	SchemaName string
}

type KafkaConfig struct {
	Host            string
	Topic           string
	ExKOLTopic      string
	ProducerTopic   string
	GroupID         string
	ExKOLGroupID    string
	HistoryTopic    string
	HistoryGroupID  string
	BitQueryTopic   string
	BitQueryGroupID string

	Protocol string
	Username string
	Password string
	CAPath   string
}

type RedisConfig struct {
	Host         string `mapstructure:"Host"`
	DB           int64  `mapstructure:"DB"`
	Password     string `mapstructure:"Password"`
	MinIdleConns int64  `mapstructure:"MinIdleConns"`
}

type AddrThreadData struct {
	ContractAddress string
}

type SolServer struct {
	ThreadData       []AddrThreadData
	SolScanAPIKey    string
	TickInterval     int
	BirdeyeAPIKey    string
	CoingeckoAPIKey  string
	ThreadNum        int
	MergeInterval    int
	QuickNodeURL     string
	ServerConfigList string
}

// struct decode must has tag
type Config struct {
	PostgresqlConfig PostgresqlConfig `mapstructure:"PostgresqlConfig"`
	KafkaConf        KafkaConfig      `mapstructure:"KafkaConfig"`
	SolConf          SolServer        `mapstructure:"SolServer"`
	RedisConf        RedisConfig      `mapstructure:"RedisConfig"`
}

var (
	configMutex = sync.RWMutex{}
	config      Config

	configViper     *viper.Viper
	configFlyChange []chan bool
)

func RegistConfChange(c chan bool) {
	configFlyChange = append(configFlyChange, c)
}

func notifyConfChange() {
	for i := 0; i < len(configFlyChange); i++ {
		configFlyChange[i] <- true
	}
}

func watchConfig(c *viper.Viper) error {
	c.WatchConfig()
	cfn := func(e fsnotify.Event) {
		logger.Logrus.WithFields(logrus.Fields{"change": e.String()}).Info("config change and reload it")
		reloadConfig(c)
		notifyConfChange()
	}

	c.OnConfigChange(cfn)
	return nil
}

func LoadConf(configFilePath string) error {
	config = Config{}
	configMutex.Lock()
	defer configMutex.Unlock()

	configViper = viper.New()
	configViper.SetConfigName("config")
	configViper.AddConfigPath(configFilePath) //endwith "/"
	configViper.SetConfigType("yaml")

	if err := configViper.ReadInConfig(); err != nil {
		return err
	}
	if err := configViper.Unmarshal(&config); err != nil {
		return err
	}

	logger.Logrus.WithFields(logrus.Fields{"Config": config}).Info("Load config success")

	if err := watchConfig(configViper); err != nil {
		return err
	}
	return nil
}

func reloadConfig(c *viper.Viper) {
	configMutex.Lock()
	defer configMutex.Unlock()

	if err := c.ReadInConfig(); err != nil {
		logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err.Error()}).Error("config ReLoad failed")
	}

	if err := configViper.Unmarshal(&config); err != nil {
		logger.Logrus.WithFields(logrus.Fields{"ErrMsg": err.Error()}).Error("unmarshal config failed")
	}

	logger.Logrus.WithFields(logrus.Fields{"config": config}).Info("Config ReLoad Success")
}

func GetPostgresqlConfig() PostgresqlConfig {
	configMutex.RLock()
	defer configMutex.RUnlock()
	return config.PostgresqlConfig
}

func GetKafkaConfig() KafkaConfig {
	configMutex.RLock()
	defer configMutex.RUnlock()
	return config.KafkaConf
}

func GetRedisConfig() RedisConfig {
	configMutex.RLock()
	defer configMutex.RUnlock()
	return config.RedisConf
}

func GetSolDataConfig() SolServer {
	configMutex.RLock()
	defer configMutex.RUnlock()
	return config.SolConf
}
