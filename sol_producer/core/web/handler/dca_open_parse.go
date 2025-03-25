package handler

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/mr-tron/base58"
)

type InstructData struct {
	Perfix           uint64
	ApplicationIdx   uint64
	InAmount         uint64
	InAmountPerCycle uint64
	CycleFrequency   int64
	MinOutAmount     *uint64
	MaxOutAmount     *uint64
	StartAt          *int64
}

type DCAOpenData struct {
	DCAAddress       string `json:"dca_address"`
	User             string `json:"user"`
	Payer            string `json:"payer"`
	InputMint        string `json:"input_mint"`
	OutputMint       string `json:"output_mint"`
	UserAta          string `json:"user_ata"`
	InAta            string `json:"in_ata"`
	OutAta           string `json:"out_ata"`
	Perfix           uint64 `json:"prefix_code"`
	ApplicationIdx   uint64 `json:"app_idx"`
	InAmount         uint64 `json:"in_amount"`
	InAmountPerCycle uint64 `json:"in_amount_pre_cycle"`
	CycleFrequency   int64  `json:"cycle_frequency"`
	MinOutAmount     uint64 `json:"min_out_amount"`
	MaxOutAmount     uint64 `json:"max_out_amount"`
	StartAt          int64  `json:"start_at"`
}

func (t *DCAOpenData) String() string {
	bytes, _ := json.Marshal(&t)
	return string(bytes)
}

func ParseOpenDCA(in *InstructionsData) (*DCAOpenData, error) {
	if len(in.Accounts) != 13 {
		return nil, fmt.Errorf("account length not match, %d", len(in.Accounts))
	}

	data, err := base58.Decode(in.Data)
	if err != nil {
		return nil, err
	}

	if len(data) < binary.Size(InstructData{}) {
		return nil, fmt.Errorf("data length not match")
	}

	offset := 0
	readUint64 := func() uint64 {
		val := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		return val
	}

	readInt64 := func() int64 {
		val := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
		return val
	}

	readOptionalUint64 := func() *uint64 {
		if data[offset] == 0 {
			offset++
			return nil
		}
		offset++
		val := readUint64()
		return &val
	}

	readOptionalInt64 := func() *int64 {
		if data[offset] == 0 {
			offset++
			return nil
		}
		offset++
		val := readInt64()
		return &val
	}

	instruct := &InstructData{
		Perfix:           readUint64(),
		ApplicationIdx:   readUint64(),
		InAmount:         readUint64(),
		InAmountPerCycle: readUint64(),
		CycleFrequency:   readInt64(),
		MinOutAmount:     readOptionalUint64(),
		MaxOutAmount:     readOptionalUint64(),
		StartAt:          readOptionalInt64(),
	}

	res := &DCAOpenData{
		DCAAddress:       in.Accounts[0],
		User:             in.Accounts[1],
		Payer:            in.Accounts[2],
		InputMint:        in.Accounts[3],
		OutputMint:       in.Accounts[4],
		UserAta:          in.Accounts[5],
		InAta:            in.Accounts[6],
		OutAta:           in.Accounts[7],
		Perfix:           instruct.Perfix,
		ApplicationIdx:   instruct.ApplicationIdx,
		InAmount:         instruct.InAmount,
		InAmountPerCycle: instruct.InAmountPerCycle,
		CycleFrequency:   instruct.CycleFrequency,
		MinOutAmount:     *instruct.MinOutAmount,
		MaxOutAmount:     *instruct.MaxOutAmount,
		StartAt:          *instruct.StartAt,
	}

	return res, nil
}
