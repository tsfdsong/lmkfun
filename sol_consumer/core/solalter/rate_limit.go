package solalter

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

func insertBlackListAddress(txs *model.BlacklistAddress) error {
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

	return nil
}

func checkWihteList(chain, address string) error {
	execsql := fmt.Sprintf(`SELECT DISTINCT
    c.tracked_item AS address
FROM
    scope_lmk.lmk_track_white_user a
    JOIN scope_lmk.lmk_track_info b ON a.user_id = b.user_id
        AND b.status IN ('open', 'pro_expire_close')
        AND b.type = 'address'
        AND b.chain = '%s'
    JOIN scope_lmk.lmk_track_item_info c ON b.id = c.track_id
WHERE
    tracked_item = '%s';`, chain, address)

	var resAddr string
	err := db.GetDB().NewRaw(execsql).Scan(context.Background(), &resAddr)
	if err == sql.ErrNoRows {
		return err
	}

	return nil
}

func CheckAddressRateLimit(chain, address, txhash string) error {
	err := checkWihteList(chain, address)
	if err == nil {
		return nil
	}

	ctx := context.Background()

	key := fmt.Sprintf("rate_limit:%s:%s", chain, address)
	values, err := redis.GetRedisInst().Get(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	if err == nil && values != "" {
		return fmt.Errorf("%s in blacklist", address)
	}

	now := time.Now().Unix()
	err = redis.ZAdd(address, txhash, now)
	if err != nil {
		return err
	}

	_, err = redis.GetRedisInst().ZRemRangeByScore(ctx, address, "0", fmt.Sprintf("%d", now-3600)).Result()
	if err != nil {
		return err
	}

	count, err := redis.GetRedisInst().ZCount(ctx, address, fmt.Sprintf("%d", now-3600), fmt.Sprintf("%d", now)).Result()
	if err != nil {
		return err
	}

	if count > 200 {
		item := model.BlacklistAddress{
			Chain:        chain,
			Address:      address,
			ActiveCounts: fmt.Sprintf("%d", count),
			UpdateTime:   time.Now().String(),
		}

		err = insertBlackListAddress(&item)
		if err != nil {
			return err
		}

		_, err = redis.GetRedisInst().Set(ctx, key, address, 365*24*time.Hour).Result()
		if err != nil {
			return err
		}

		logger.Logrus.WithFields(logrus.Fields{"Data": item}).Info("CheckAddressRateLimit add address to blacklist success")
	}

	redis.GetRedisInst().Expire(ctx, address, time.Duration(3600)*time.Second)

	return nil
}

func chaeckKeyExp(key string, limit int64, exp time.Duration) error {
	ctx := context.Background()
	// Increment the count
	count, err := redis.GetRedisInst().Incr(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to increment key: %v", err)
	}

	if count == 1 {
		err = redis.GetRedisInst().Expire(ctx, key, exp).Err()
		if err != nil {
			return fmt.Errorf("failed to expire key: %v", err)
		}
	}

	if count > int64(limit) {
		return fmt.Errorf("current:maximum %v : %v", count, limit)
	}
	return nil
}

func CheckRateLimit(chain, addr string) error {
	redisKey := fmt.Sprintf("address_rate_limit:%s:%s", chain, addr)

	err := chaeckKeyExp(redisKey, 100, time.Hour)
	if err != nil {
		return fmt.Errorf("CheckAddressRateLimit,%s,%s, %v", chain, addr, err)
	}

	return nil
}
