package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/utils/logger"
)

type TxTokenBalance struct {
	AccountIndex  int    `json:"accountIndex"`
	Mint          string `json:"mint"`
	Owner         string `json:"owner"`
	UITokenAmount struct {
		Amount         string  `json:"amount"`
		Decimals       int     `json:"decimals"`
		UIAmount       float64 `json:"uiAmount"`
		UIAmountString string  `json:"uiAmountString"`
	} `json:"uiTokenAmount"`
}

type TransactionMeta struct {
	Err               any `json:"err"`
	Fee               int `json:"fee"`
	InnerInstructions []struct {
		Index        int `json:"index"`
		Instructions []struct {
			Parsed      any      `json:"parsed"`
			Program     string   `json:"program,omitempty"`
			ProgramID   string   `json:"programId"`
			StackHeight int      `json:"stackHeight"`
			Accounts    []string `json:"accounts,omitempty"`
			Data        string   `json:"data,omitempty"`
		} `json:"instructions"`
	} `json:"innerInstructions"`
	LogMessages       []string         `json:"logMessages"`
	PostBalances      []int            `json:"postBalances"`
	PostTokenBalances []TxTokenBalance `json:"postTokenBalances"`
	PreBalances       []int            `json:"preBalances"`
	PreTokenBalances  []TxTokenBalance `json:"preTokenBalances"`
	Rewards           []interface{}    `json:"rewards"`
	Status            struct {
		Ok any `json:"Ok"`
	} `json:"status"`
}

type TransactionddData struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		BlockTime   int             `json:"blockTime"`
		Meta        TransactionMeta `json:"meta"`
		Slot        int             `json:"slot"`
		Transaction struct {
			Message struct {
				AccountKeys []struct {
					Pubkey   string `json:"pubkey"`
					Signer   bool   `json:"signer"`
					Source   string `json:"source"`
					Writable bool   `json:"writable"`
				} `json:"accountKeys"`
				Instructions    []any  `json:"instructions"`
				RecentBlockhash string `json:"recentBlockhash"`
			} `json:"message"`
			Signatures []string `json:"signatures"`
		} `json:"transaction"`
		Version interface{} `json:"version"`
	} `json:"result"`
	ID int `json:"id"`
}

func getTokenAccountTxs(tokenAccount, txhash string) ([]HeliusData, error) {
	apiKey := config.GetHeliusConfig().APIKey

	url := fmt.Sprintf("https://api.helius.xyz/v0/addresses/%s/transactions?api-key=%s&before=%s&limit=1&commitment=confirmed", tokenAccount, apiKey, txhash)

	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("status, %v", res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var result []HeliusData
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type TxHeliusBody struct {
	Jsonrpc string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type ParamsOptions struct {
	Encoding                       string `json:"encoding"`
	MaxSupportedTransactionVersion int    `json:"maxSupportedTransactionVersion"`
}

func getHeliusTransaction(txhash string) (*TransactionddData, error) {
	apiKey := config.GetHeliusConfig().APIKey
	url := "https://mainnet.helius-rpc.com/?api-key=" + apiKey
	method := "POST"

	inplay := fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"id": 1,
		"method": "getTransaction",
		"params": [
			"%s",
			{
				"encoding": "jsonParsed",
				"commitment": "confirmed",
				"maxSupportedTransactionVersion": 0
			}
		]
	}`, txhash)

	payload := strings.NewReader(inplay)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("%s, %s", txhash, res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	logger.Logrus.WithFields(logrus.Fields{"TxHash": txhash, "URL": url, "Payload": payload, "Body": string(body)}).Info("getHeliusTransaction data")

	var result TransactionddData

	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func getSignatureForAddress(tokenAccount, txhash string, limit int) ([]*rpc.TransactionSignature, error) {
	headers := make(map[string]string)

	headers["Content-Type"] = "application/json"

	url := "https://autumn-wispy-patron.solana-mainnet.quiknode.pro/8fdd796d1b326aebb7481d0731424a3d71c60c82/"
	client := rpc.NewWithHeaders(url, headers)

	pubKey := solana.MustPublicKeyFromBase58(tokenAccount)

	before := solana.MustSignatureFromBase58(txhash)
	opts := rpc.GetSignaturesForAddressOpts{
		Limit:      &limit,
		Before:     before,
		Commitment: rpc.CommitmentConfirmed,
	}
	out, err := client.GetSignaturesForAddressWithOpts(
		context.Background(),
		pubKey,
		&opts,
	)
	if err != nil {
		return nil, err
	}

	res := make([]*rpc.TransactionSignature, 0)
	for _, val := range out {
		if val.Err == nil {
			res = append(res, val)
		}
	}

	return res, nil
}

func getQuickNodeTransaction(txhash string) (*TransactionddData, error) {
	url := "https://autumn-wispy-patron.solana-mainnet.quiknode.pro/8fdd796d1b326aebb7481d0731424a3d71c60c82/"
	method := "POST"

	inplay := fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"id": 1,
		"method": "getTransaction",
		"params": [
			"%s",
			{
				"encoding": "jsonParsed",
				"commitment": "confirmed",
				"maxSupportedTransactionVersion": 0
			}
		]
	}`, txhash)

	payload := strings.NewReader(inplay)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("%s, %s", txhash, res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var result TransactionddData

	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func getParsedTransaction(txhash string) (*rpc.GetParsedTransactionResult, error) {
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"

	url := "https://autumn-wispy-patron.solana-mainnet.quiknode.pro/8fdd796d1b326aebb7481d0731424a3d71c60c82/"
	client := rpc.NewWithHeaders(url, headers)

	version := uint64(0)

	sig, err := solana.SignatureFromBase58(txhash)
	if err != nil {
		return nil, err
	}

	out, err := client.GetParsedTransaction(
		context.Background(),
		sig,
		&rpc.GetParsedTransactionOpts{
			Commitment:                     rpc.CommitmentConfirmed,
			MaxSupportedTransactionVersion: &version,
		},
	)
	if err != nil {
		return nil, err
	}

	return out, nil
}

type SPLCreateAccountInfo struct {
	Info struct {
		Lamports float64 `json:"lamports"`
		Account  string  `json:"newAccount"`
		Owner    string  `json:"owner"`
		Source   string  `json:"source"`
		Space    float64 `json:"space"`
	} `json:"info"`
	Type string `json:"type"`
}

type SPLTransferInfo struct {
	Info struct {
		Source      string      `json:"source"`
		Destination string      `json:"destination"`
		Authority   string      `json:"authority"`
		Amount      interface{} `json:"amount"`
	} `json:"info"`
	Type string `json:"type"`
}

func checkFirstBuy(tokenAccount string, in []*rpc.TransactionSignature) bool {
	if len(in) == 0 {
		return true
	}

	if len(in) > 1 {
		return false
	}

	sig := in[0]

	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"

	url := "https://autumn-wispy-patron.solana-mainnet.quiknode.pro/8fdd796d1b326aebb7481d0731424a3d71c60c82/"
	client := rpc.NewWithHeaders(url, headers)

	version := uint64(0)

	out, err := client.GetParsedTransaction(
		context.Background(),
		sig.Signature,
		&rpc.GetParsedTransactionOpts{
			Commitment:                     rpc.CommitmentConfirmed,
			MaxSupportedTransactionVersion: &version,
		},
	)
	if err != nil {
		return false
	}

	isATAInit := false
	isTokenTransfer := false

	innerinstruct := out.Meta.InnerInstructions
	for _, finst := range innerinstruct {
		innerinst := finst.Instructions
		for _, inst := range innerinst {
			logger.Logrus.WithFields(logrus.Fields{"Data": inst}).Info("check data")

			if inst.ProgramId == solana.SystemProgramID {
				if inst.Parsed != nil {
					insinfo, err := inst.Parsed.MarshalJSON()
					if err != nil {
						continue
					}

					var obj SPLCreateAccountInfo
					err = json.Unmarshal(insinfo, &obj)
					if err != nil {
						continue
					}

					if obj.Type == "createAccount" && obj.Info.Account == tokenAccount {
						isATAInit = true
					}
				}
			}

			if inst.ProgramId == solana.TokenProgramID || inst.ProgramId == solana.Token2022ProgramID {
				if inst.Parsed != nil {
					insinfo, err := inst.Parsed.MarshalJSON()
					if err != nil {
						continue
					}

					var obj SPLTransferInfo
					err = json.Unmarshal(insinfo, &obj)
					if err != nil {
						continue
					}

					if (obj.Type == "transfer" || obj.Type == "transferChecked") && (obj.Info.Source == tokenAccount || obj.Info.Destination == tokenAccount) {
						isTokenTransfer = true
					}
				}
			}
		}
	}

	if isATAInit && !isTokenTransfer {
		return true
	}

	return false
}

func fillTradeLabel(val model.SolSwapData) model.SolSwapData {
	val.TradeLabel = model.LabelNone
	if val.Type == "SWAP" {
		tokenRule := make(map[string]bool)

		cfg := config.GetHeliusConfig()
		for _, v := range cfg.ThreadData {
			tokenRule[v.ContractAddress] = true
		}

		_, fromok := tokenRule[val.FromToken]
		_, took := tokenRule[val.ToToken]

		if took && !fromok {
			//sold

			txdetail, err := getQuickNodeTransaction(val.TxHash)
			if err != nil {
				txdetail, err = getQuickNodeTransaction(val.TxHash)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("FillTradeLabel getQuickNodeTransaction sold failed")
					return val
				}
			}

			logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "TxDetails": txdetail}).Info("FillTradeLabel getQuickNodeTransaction sold data")

			meta := txdetail.Result.Meta
			for _, v := range meta.PreTokenBalances {
				if val.FromToken == v.Mint && val.FromUserAccount == v.Owner && val.FromTokenAmount == v.UITokenAmount.UIAmount {
					val.TradeLabel = model.LabelSellAll
					return val
				}
			}
		} else {
			//bought
			datas, err := getSignatureForAddress(val.ToTokenAccount, val.TxHash, 5)
			if err != nil {
				datas, err = getSignatureForAddress(val.ToTokenAccount, val.TxHash, 5)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("FillTradeLabel getSignatureForAddress buy failed")
					return val
				}
			}

			if len(datas) == 0 {
				datas, err = getSignatureForAddress(val.ToTokenAccount, val.TxHash, 5)
				if err != nil {
					logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("FillTradeLabel getSignatureForAddress buy failed")
					return val
				}
			}

			logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "TxLength": len(datas)}).Info("FillTradeLabel getSignatureForAddress buy data")

			isfirst := checkFirstBuy(val.ToTokenAccount, datas)
			if isfirst {
				val.TradeLabel = model.LabelFirstBuy
				return val
			}

			txdetail, err := getQuickNodeTransaction(val.TxHash)
			if err != nil {
				txdetail, err = getQuickNodeTransaction(val.TxHash)
				if err != err {
					logger.Logrus.WithFields(logrus.Fields{"Data": val, "ErrMsg": err}).Error("FillTradeLabel getQuickNodeTransaction buy failed")
					return val
				}
			}

			logger.Logrus.WithFields(logrus.Fields{"TxHash": val.TxHash, "TxDetail": txdetail}).Info("FillTradeLabel getQuickNodeTransaction buy data")

			meta := txdetail.Result.Meta
			for _, v := range meta.PreTokenBalances {
				if val.ToToken == v.Mint && val.ToUserAccount == v.Owner && v.UITokenAmount.UIAmount == 0 {
					val.TradeLabel = model.LabelFreshBuy
					return val
				}
			}
		}
	}

	return val
}

func FillLabel(ins []model.SolSwapData) []model.SolSwapData {
	result := make([]model.SolSwapData, 0)
	for _, val := range ins {
		nval := fillTradeLabel(val)
		result = append(result, nval)
	}

	return result
}
