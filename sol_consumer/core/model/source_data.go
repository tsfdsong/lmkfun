package model

import (
	"time"

	"github.com/uptrace/bun"
)

type SolSwapData struct {
	TxHash    string `json:"tx_hash"`
	Source    string `json:"source"`
	Timestamp int    `json:"timestamp"`
	Type      string `json:"type"`
	Date      string `json:"date"`

	FromToken        string  `json:"from_token"`
	FromTokenAccount string  `json:"fromTokenAccount"`
	FromUserAccount  string  `json:"fromUserAccount"`
	FromTokenAmount  float64 `json:"from_token_amount"`

	ToToken        string  `json:"to_token"`
	ToTokenAccount string  `json:"toTokenAccount"`
	ToUserAccount  string  `json:"toUserAccount"`
	ToTokenAmount  float64 `json:"to_token_amount"`

	TradeLabel string `json:"trade_label"`
	IsDCATrade bool   `json:"is_dca_trade"`

	WalletCounts    int           `json:"wallet_counts"`
	TransferDetails []SolSwapData `json:"transfer_details"`
}

type SolTrackedInfo struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type SolTrackedAddress struct {
	ListID          string  `bun:"list_id"`
	OwnerID         string  `bun:"owner_id"`
	Name            string  `bun:"name"`
	Description     string  `bun:"description"`
	CoverImg        string  `bun:"cover_img"`
	Type            string  `bun:"type"`
	UserAccount     string  `bun:"user_account"`
	UserLabel       string  `bun:"label"`
	TxBuyValue      float64 `bun:"tx_buy_value"`
	TxSellValue     float64 `bun:"tx_sell_value"`
	TxReceivedValue float64 `bun:"tx_received_value"`
	TxSendValue     float64 `bun:"tx_send_value"`
	TokenSecurity   bool    `bun:"token_security"`
	TokenMarketCap  float64 `bun:"token_market_cap"`

	TokenMarketCapMin float64 `bun:"token_market_cap_min"`
	AgeTime           int64   `bun:"age_time"`
	Volume24HMax      float64 `bun:"volume_24h_max"`
	Volume24HMin      float64 `bun:"volume_24h_min"`
	HolderCount       int64   `bun:"token_holder"`

	TxBuySell  bool        `bun:"tx_buy_sell"`
	TxMintBurn bool        `bun:"tx_mint_burn"`
	TxTransfer bool        `bun:"tx_transfer"`
	Status     string      `bun:"status"`
	GroupID    string      `bun:"group_id"`
	Chain      string      `bun:"chain"`
	ChanelInfo interface{} `bun:"channel_info"`

	TgTxBuy      bool `bun:"tg_push_buy"`
	TgTxSold     bool `bun:"tg_push_sell"`
	TgTxReceived bool `bun:"tg_push_receive"`
	TgTxSend     bool `bun:"tg_push_send"`
	TgTxCreate   bool `bun:"tg_push_create"`
	IsAddrPublic bool `bun:"is_address_public"`

	TgTxFirstBuy bool `bun:"tg_push_first_buy"`
	TgTxFreshBuy bool `bun:"tg_push_fresh_buy"`
	TgTxSellAll  bool `bun:"tg_push_sell_all"`
}

type TgBotInfo struct {
	ListID   string `bun:"list_id"`
	ChatID   string `bun:"chat_id"`
	BotToken string `bun:"bot_token"`
}

type SolAlterRecord struct {
	bun.BaseModel `bun:"table:lmk_list_alert_record,alias:oat"`

	ListID        string    `bun:"list_id,pk,notnull"`
	UserAccount   string    `bun:"user_account,pk,notnull"`
	Type          string    `bun:"type"`
	Chain         string    `bun:"chain"`
	TokenAddress  string    `bun:"token_address"`
	TokenSymbol   string    `bun:"token_symbol"`
	MarketCap     string    `bun:"marketcap"`
	PriceChange1H string    `bun:"price_change_1h"`
	Security      string    `bun:"security"`
	Data          string    `bun:"data"`
	Timestamp     string    `bun:"timestamp,pk,notnull"`
	CreateAt      time.Time `bun:"create_at,nullzero"`
}

type BlacklistAddress struct {
	bun.BaseModel `bun:"table:lmk_overactive_address,alias:oat"`

	Chain        string `bun:"chain"`
	Address      string `bun:"address"`
	ActiveCounts string `bun:"active_counts"`
	UpdateTime   string `bun:"update_time"`
}

type ExchangeTrackedInfo struct {
	ListID         string  `bun:"list_id"`
	OwnerID        string  `bun:"owner_id"`
	Followers      string  `bun:"followers"`
	Name           string  `bun:"name"`
	Description    string  `bun:"description"`
	CoverImg       string  `bun:"cover_img"`
	Type           string  `bun:"type"`
	TrackedInfo    string  `bun:"tracked_info"`
	TokenAmount    float64 `bun:"token_amount"`
	TokenSecurity  bool    `bun:"token_security"`
	TokenMarketCap float64 `bun:"token_market_cap"`
	ListingSpot    bool    `bun:"listing_spot"`
	ListingFutures bool    `bun:"listing_futures"`
	ListingOptions bool    `bun:"listing_options"`
	Status         string  `bun:"status"`
	GroupID        string  `bun:"group_id"`
}

type KOLTrackedInfo struct {
	ListID          string  `bun:"list_id"`
	OwnerID         string  `bun:"owner_id"`
	Followers       string  `bun:"followers"`
	Name            string  `bun:"name"`
	Description     string  `bun:"description"`
	CoverImg        string  `bun:"cover_img"`
	Type            string  `bun:"type"`
	TrackedInfo     string  `bun:"tracked_info"`
	TokenAmount     float64 `bun:"token_amount"`
	TokenSecurity   bool    `bun:"token_security"`
	TokenMarketCap  float64 `bun:"token_market_cap"`
	Status          string  `bun:"status"`
	GroupID         string  `bun:"group_id"`
	TgPushWithoutCA bool    `bun:"alert_token_without_ca"`
}

type SolTxRecord struct {
	bun.BaseModel `bun:"table:lmk_sol_data,alias:oat"`

	Chain     string `bun:"chain,pk,notnull"`
	TxHash    string `bun:"tx_hash,pk,notnull"`
	Source    string `bun:"source,pk,notnull"`
	Timestamp int    `bun:"timestamp,pk,notnull"`
	Type      string `bun:"type,pk,notnull"`
	Date      string `bun:"date"`

	FromToken        string  `bun:"from_token,pk,notnull"`
	FromTokenAccount string  `bun:"from_token_account"`
	FromUserAccount  string  `bun:"from_user_account,pk,notnull"`
	FromTokenAmount  float64 `bun:"from_token_amount"`
	FromTokenSymbol  string  `bun:"from_token_symbol"`

	ToToken        string  `bun:"to_token,pk,notnull"`
	ToTokenAccount string  `bun:"to_token_account"`
	ToUserAccount  string  `bun:"to_user_account,pk,notnull"`
	ToTokenAmount  float64 `bun:"to_token_amount"`
	ToTokenSymbol  string  `bun:"to_token_symbol"`

	CreateAt        time.Time     `bun:"create_at,nullzero"`
	Value           string        `bun:"value"`
	MarketCap       string        `bun:"marketcap"`
	Price           string        `bun:"price"`
	Change1HPrice   string        `bun:"price_change_1h"`
	Direction       string        `bun:"direction"`
	Supply          string        `bun:"supply"`
	TradeLabel      string        `bun:"trade_label"`
	IsDCATrade      bool          `bun:"is_dca_trade"`
	WalletCounts    int           `bun:"wallet_counts"`
	TransferDetails []SolSwapData `bun:"transfer_details"`
	IsSymbolValid   bool          `bun:"is_symbol_valid"`
}
