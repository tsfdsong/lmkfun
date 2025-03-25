package model

import "github.com/uptrace/bun"

type SolAddressTrack struct {
	Type        string `bun:"type"`
	TrackedInfo string `bun:"tracked_info"`
}

type SolTrackedInfo struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type BlacklistAddress struct {
	Address      string `bun:"address"`
	ActiveCounts string `bun:"active_counts"`
	UpdateTime   string `bun:"update_time"`
}

type MonitorAddress struct {
	Address    string `bun:"address"`
	UpdateTime string `bun:"update_time"`
}

type MonitorAddressRecord struct {
	bun.BaseModel `bun:"table:lmk_address_monitor,alias:oat"`

	Address    string `bun:"address"`
	UpdateTime string `bun:"update_time"`
}
