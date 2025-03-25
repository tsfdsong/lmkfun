package redis

import (
	"context"
	"fmt"
	"time"

	redis "github.com/go-redis/redis/v8"
)

func Set(ctx context.Context, key, value string, expiration time.Duration) error {
	err := GetRedisInst().SetNX(ctx, key, value, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to set %s: %v", key, err)
	}
	return nil
}

func Get(ctx context.Context, key string) (string, error) {
	val, err := GetRedisInst().Get(ctx, key).Result()
	if err == redis.Nil {
		return "", redis.Nil
	} else if err != nil {
		return "", fmt.Errorf("failed to get %s: %v", key, err)
	}

	return val, nil
}

func Exists(ctx context.Context, key string) (bool, error) {
	val, err := GetRedisInst().Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("%s: %v", key, err)
	}

	if val > 0 {
		return true, nil
	} else {
		return false, nil
	}
}

func PipeSet(keys []string, values []string) error {
	ctx := context.Background()

	pipe := GetRedisInst().Pipeline()

	if len(keys) != len(values) {
		return fmt.Errorf("length not match")
	}

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		value := values[i]
		pipe.Set(ctx, key, value, 0)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pipe set failed,%v", err)
	}

	return nil
}

func AddSet(key string, values []string, expiration time.Duration) error {
	ctx := context.Background()
	GetRedisInst().Del(ctx, key)
	for _, v := range values {
		err := GetRedisInst().SAdd(ctx, key, v).Err()
		if err != nil {
			return fmt.Errorf("failed to sadd %s: %v", key, err)
		}
	}

	err := GetRedisInst().Expire(ctx, key, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to expire %s: %v", key, err)
	}
	return nil
}

func GetSet(key string) ([]string, error) {
	ctx := context.Background()
	values, err := GetRedisInst().SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("smember failed, %v", err)

	}
	return values, nil
}

func ZAdd(key, members string, stamp int64) error {
	ctx := context.Background()
	_, err := GetRedisInst().ZAdd(ctx, key, &redis.Z{
		Score:  float64(stamp),
		Member: members,
	}).Result()

	return err
}
