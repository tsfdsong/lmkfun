package solalter

import (
	"context"
	"errors"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type SolAltertData struct {
	Type   string `json:"type"`
	TxHash string `json:"tx_hash"`
	Source string `json:"source"`
	Date   string `json:"date"`

	FromAccount      string `json:"from_account"`
	FromToken        string `json:"from_token"`
	FromTokenSymbol  string `json:"from_token_symbol"`
	FromTokenAmount  string `json:"from_token_amount"`
	FromTokenDecimal int    `json:"from_token_decimals"`
	ToAccount        string `json:"to_account"`
	ToToken          string `json:"to_token"`
	ToTokenSymbol    string `json:"to_token_symbol"`
	ToTokenAmount    string `json:"to_token_amount"`
	ToTokenDecimal   int    `json:"to_token_decimals"`
	Value            string `json:"value"`
	Price            string `json:"price"`
	MarketCap        string `json:"market_cap"`
	AgeTime          int64  `json:"age_time"`
	Volume24H        string `json:"volume_24h"`
	HoldersCount     int64  `json:"holder_count"`
	Direction        string `json:"direction"`
	TradeLabel       string `json:"trade_label"`
	IsDCATrade       bool   `json:"is_dca_trade"`
	TotalSupply      string `json:"supply"`
	WalletCounta     int    `json:"wallet_counts"`
}

type ExchangeAltertData struct {
	NewsType  string `json:"news_type"`
	Direction string `json:"pos_or_neg"`
	Link      string `json:"link"`
	ListType  string `json:"list_type"`
	Title     string `json:"title"`
}

type KOLAltertData struct {
	NewsType   string `json:"news_type"`
	Direction  string `json:"pos_or_neg"`
	Link       string `json:"link"`
	IsVerified bool   `json:"is_verified"`
	Price      string `json:"price"`
	Decimals   int    `json:"decimals"`
	TgPushCA   bool   `json:"tg_push"`
}

type RawBitqueryAltertData struct {
	DEX string `json:"dex"`

	Chain     string `json:"chain"`
	TxHash    string `json:"hash"`
	Timestamp string `json:"timestamp"`

	FromAddress     string `json:"from_address"`
	FromToken       string `json:"from_token_address"`
	FromTokenSymbol string `json:"from_token_symbol"`
	FromTokenAmount string `json:"from_token_amount"`
	ToAddress       string `json:"to_address"`
	ToToken         string `json:"to_token_address"`
	ToTokenSymbol   string `json:"to_token_symbol"`
	ToTokenAmount   string `json:"to_token_amount"`
	Value           string `json:"value"`
	Signer          string `json:"signer"`
}

func InsertAlertRecord(txs *model.SolAlterRecord) error {

	logger.Logrus.WithFields(logrus.Fields{"Record": txs, "Time": time.Now()}).Info("InsertAlertRecord start")

	ctx := context.Background()
	sqlRes, err := db.GetDB().NewInsert().Model(txs).On("CONFLICT DO NOTHING").Exec(ctx)
	if err != nil {
		return err
	}

	num, err := sqlRes.RowsAffected()
	if err != nil {
		return err
	}

	if num < 0 {
		return errors.New("insert empty item")
	}

	logger.Logrus.WithFields(logrus.Fields{"Record": txs, "Time": time.Now()}).Info("InsertAlertRecord end")

	return nil
}

func BatchInsertAlertRecords(txs []model.SolAlterRecord) error {
	if len(txs) < 1 {
		return nil
	}

	ctx := context.Background()
	sqlRes, err := db.GetDB().NewInsert().Model(&txs).On("CONFLICT DO NOTHING").Exec(ctx)
	if err != nil {
		return err
	}

	num, err := sqlRes.RowsAffected()
	if err != nil {
		return err
	}

	if num < 0 {
		return errors.New("insert empty item")
	}

	return nil
}
