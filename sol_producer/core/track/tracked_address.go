package track

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_producer/utils/logger"
)

type HeliusBody struct {
	WebhookURL  string   `json:"webhookURL"`
	TxTypes     []string `json:"transactionTypes"`
	AddressList []string `json:"accountAddresses"`
	HookTypes   string   `json:"webhookType"`
}

func getTrackedAddr() ([]string, error) {
	var resAddr []string
	query := `
SELECT DISTINCT
    address
FROM
    scope_lmk.lmk_address_monitor
WHERE
    address NOT LIKE '0x%'
    AND address NOT IN (
        SELECT
            address
        FROM
            scope_lmk.lmk_overactive_address
        WHERE
            chain = 'solana'
            AND address NOT IN ( SELECT DISTINCT
                    c.tracked_item AS address
                FROM
                    scope_lmk.lmk_track_white_user a
                    JOIN scope_lmk.lmk_track_info b ON a.user_id = b.user_id
                        AND b.status IN ('open', 'pro_expire_close')
                        AND b.type = 'address'
                        AND b.chain = 'solana'
                    JOIN scope_lmk.lmk_track_item_info c ON b.id = c.track_id))`

	err := db.GetDB().NewRaw(query).Scan(context.Background(), &resAddr)
	if err != nil {
		return nil, err
	}

	return resAddr, nil
}

func updateHeliusAddressAccount(addrs []string) error {
	cfg := config.GetHeliusConfig()

	url := cfg.Host + "/" + cfg.WebhookID + "?api-key=" + cfg.APIKey

	item := HeliusBody{
		WebhookURL:  cfg.WebhookURL + "/sol/webhook",
		TxTypes:     []string{cfg.TxTypes},
		AddressList: addrs,
		HookTypes:   "enhanced",
	}

	bydata, err := json.Marshal(&item)
	if err != nil {
		return err
	}

	payload := strings.NewReader(string(bydata))

	client := &http.Client{}
	req, err := http.NewRequest("PUT", url, payload)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return fmt.Errorf("response failed, %s", res.Status)
	}

	return nil
}

func AddrTask() {
	ticker := time.NewTicker(5 * time.Minute)

	go func() {
		for t := range ticker.C {
			addrlist, err := getTrackedAddr()
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Time": t.String(), "ErrMsg": err}).Error("AddrTask get tracked address failed")

				continue
			}

			err = updateHeliusAddressAccount(addrlist)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"Time": t.String(), "ErrMsg": err}).Error("AddrTask update helius address account failed")

				continue
			}

			logger.Logrus.WithFields(logrus.Fields{"Time": t.String(), "Data": addrlist}).Info("AddrTask update helius address account success")
		}
	}()
}
