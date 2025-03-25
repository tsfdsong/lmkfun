package model

const (
	LabelError    string = "error"
	LabelNone     string = "none"
	LabelFirstBuy string = "first_buy"
	LabelFreshBuy string = "fresh_buy"
	LabelSellAll  string = "sell_all"
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

	TradeLabel      string        `json:"trade_label"`
	IsDCATrade      bool          `json:"is_dca_trade"`
	WalletCounts    int           `json:"wallet_counts"`
	TransferDetails []SolSwapData `json:"transfer_details"`
	DCAOpenData     string        `json:"dca_open_data"`
}
