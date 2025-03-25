package model

import "github.com/uptrace/bun"

type SolDimTokensRecord struct {
	bun.BaseModel `bun:"table:dim_addr_tokens,alias:oat"`

	TokenAddress     string `bun:"token_address,pk,nullzero"`
	Name             string `bun:"name"`
	Symbol           string `bun:"symbol"`
	Decimals         int    `bun:"decimals"`
	IsERC20          bool   `bun:"is_erc20"`
	IsERC721         bool   `bun:"is_erc721"`
	IsERC1155        bool   `bun:"is_erc1155"`
	FromAddress      string `bun:"from_address"`
	TransactionHash  string `bun:"transaction_hash"`
	TransactionIndex int    `bun:"transaction_index"`
	BlockNumber      int    `bun:"block_number"`
	BlockTimestamp   int    `bun:"block_timestamp"`
	ExtraInfo        string `bun:"extra_info"`
	IsValid          bool   `bun:"is_valid"`
	IsSupported      bool   `bun:"is_supported"`
	IsImageSupported int    `bun:"is_image_supported"`
	TotalSupply      string `bun:"total_supply"`
	Image            string `bun:"icon"`
}
