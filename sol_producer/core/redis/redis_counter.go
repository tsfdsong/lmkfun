package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

func SetCounter(key string, value int64) error {
	ctx := context.Background()

	luaScript := `
        if redis.call("TTL", KEYS[1]) == -1 or redis.call("TTL", KEYS[1]) == -2 then
            redis.call("SET", KEYS[1], ARGV[1])
            return 1
        else
            return 0
        end
    `
	result, err := GetRedisInst().Eval(ctx, luaScript, []string{key}, value).Result()
	if err != nil {
		return err
	}
	if result == int64(0) {
		return fmt.Errorf("key already has expiration set, cannot modify")
	}
	return nil
}

func GetCounterValue(key string) (int64, error) {
	ctx := context.Background()
	value, err := GetRedisInst().Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	intValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, err
	}

	return intValue, nil
}

func SetCounterExpir(key string, expiration time.Duration) error {
	ctx := context.Background()

	luaScript := `
        if redis.call("TTL", KEYS[1]) == -1 or redis.call("TTL", KEYS[1]) == -2 then
            redis.call("EXPIRE", KEYS[1], ARGV[1])
            return 1
        else
            return 0
        end
    `
	result, err := GetRedisInst().Eval(ctx, luaScript, []string{key}, int(expiration.Seconds())).Result()
	if err != nil {
		return err
	}
	if result == int64(0) {
		return fmt.Errorf("key already has expiration set, cannot modify")
	}
	return nil
}

func DelCounter(key string) error {
	ctx := context.Background()
	err := GetRedisInst().Del(ctx, key).Err()
	return err
}
