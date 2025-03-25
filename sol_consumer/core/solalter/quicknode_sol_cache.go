package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/tokenregistry"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
)

type RPCTokenMeta struct {
	Address     string `json:"address"`
	Symbol      string `json:"symbol"`
	Decimals    int    `json:"decimals"`
	Name        string `json:"name"`
	TotalSupply string `json:"total_supply"`
}

func toDecimal(amount string, decimal int) float64 {
	da, ok := new(big.Float).SetString(amount)
	if !ok {
		return float64(0)
	}

	precision := new(big.Float).SetFloat64(math.Pow10(decimal))

	result := new(big.Float).Quo(da, precision)
	res, _ := result.Float64()

	return res
}

func getSolanaRPCMeta(tokenAddress string) (*RPCTokenMeta, error) {
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"

	url := config.GetSolDataConfig().QuickNodeURL
	client := rpc.NewWithHeaders(url, headers)

	pubKey, err := solana.PublicKeyFromBase58(tokenAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid mint address %q: %w", tokenAddress, err)
	}

	ctx := context.Background()
	t, err := tokenregistry.GetTokenRegistryEntry(ctx, client, pubKey)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve token symbol %q: %w", pubKey.String(), err)
	}

	var supply string
	decimals := int(9)
	if tokenAddress != "So11111111111111111111111111111111111111112" {
		result, err := client.GetTokenSupply(context.Background(), pubKey, rpc.CommitmentConfirmed)
		if err != nil {
			return nil, fmt.Errorf("unable to retrieve token supply %q: %w", pubKey.String(), err)
		}

		supply = result.Value.UiAmountString
		decimals = int(result.Value.Decimals)
	} else {
		result, err := client.GetSupply(context.Background(), rpc.CommitmentConfirmed)
		if err != nil {
			return nil, fmt.Errorf("unable to retrieve coin supply %q: %w", pubKey.String(), err)
		}

		supply = strconv.FormatFloat(toDecimal(fmt.Sprintf("%d", result.Value.Circulating), 9), 'f', -1, 64)
	}

	res := &RPCTokenMeta{
		Address:     tokenAddress,
		Symbol:      t.Symbol.String(),
		Decimals:    decimals,
		Name:        t.Name.String(),
		TotalSupply: supply,
	}

	return res, nil
}

func SetNodeCache(chain, token string) error {
	key := fmt.Sprintf("meta:quicknode:%s:%s", chain, token)
	isexists, err := redis.Exists(context.Background(), key)
	if err != nil {
		return fmt.Errorf("check exists failed, %v", err)
	}

	if isexists {
		return nil
	} else {
		data, err := getSolanaRPCMeta(token)
		if err != nil {
			data, err = getSolanaRPCMeta(token)
			if err != nil {
				return fmt.Errorf("quicknode get failed, %v", err)
			}
		}

		bt, err := json.Marshal(&data)
		if err != nil {
			return fmt.Errorf("marshal failed, %v", err)
		}

		err = redis.Set(context.Background(), key, string(bt), 10*time.Minute)
		if err != nil {
			return fmt.Errorf("redis set failed, %v", err)
		}
	}

	return nil
}

func GetNodeCache(chain, token string) (*RPCTokenMeta, error) {
	if strings.ToLower(chain) != "solana" {
		return nil, fmt.Errorf("chain not match for %s", chain)
	}

	key := fmt.Sprintf("meta:quicknode:%s:%s", chain, token)
	bt, err := redis.Get(context.Background(), key)
	if err == redis.Nil {
		err = SetNodeCache(chain, token)
		if err != nil {
			return nil, fmt.Errorf("set cache, %v", err)
		}

		bt, err = redis.Get(context.Background(), key)
		if err != nil {
			return nil, fmt.Errorf("redis get, %v", err)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("get failed, %v", err)
	}

	var data RPCTokenMeta

	err = json.Unmarshal([]byte(bt), &data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal failed, %v", err)
	}

	if data.Symbol == "" {
		return nil, fmt.Errorf("%s symbol is empty", token)
	}

	return &data, nil
}
