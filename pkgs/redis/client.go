package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"submission-sequencer-finalizer/config"
	"time"

	"github.com/go-redis/redis/v8"
)

var RedisClient *redis.Client

// TODO: Pool size failures to be checked
func NewRedisClient() *redis.Client {
	db, err := strconv.Atoi(config.SettingsObj.RedisDB)
	if err != nil {
		log.Fatalf("Incorrect redis db: %s", err.Error())
	}
	return redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", config.SettingsObj.RedisHost, config.SettingsObj.RedisPort), // Redis server address
		Password:     "",                                                                               // no password set
		DB:           db,
		PoolSize:     1000,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
		DialTimeout:  5 * time.Second,
		IdleTimeout:  5 * time.Minute,
	})
}

func AddToSet(ctx context.Context, set string, keys ...string) error {
	if err := RedisClient.SAdd(ctx, set, keys).Err(); err != nil {
		return fmt.Errorf("unable to add to set: %s", err.Error())
	}
	return nil
}

func GetSetKeys(ctx context.Context, set string) []string {
	return RedisClient.SMembers(ctx, set).Val()
}

func RemoveFromSet(ctx context.Context, set, key string) error {
	return RedisClient.SRem(context.Background(), set, key).Err()
}

func Delete(ctx context.Context, set string) error {
	return RedisClient.Del(ctx, set).Err()
}

func Expire(ctx context.Context, key string, expiration time.Duration) error {
	return RedisClient.Expire(ctx, key, expiration).Err()
}

func Get(ctx context.Context, key string) (string, error) {
	val, err := RedisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		} else {
			return "", err
		}
	}
	return val, nil
}

func Set(ctx context.Context, key, value string) error {
	return RedisClient.Set(ctx, key, value, 0).Err()
}

// Use this when you want to set an expiration
func SetWithExpiration(ctx context.Context, key, value string, expiration time.Duration) error {
	return RedisClient.Set(ctx, key, value, expiration).Err()
}

func Incr(ctx context.Context, key string) (int64, error) {
	result, err := RedisClient.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return result, nil
}

func IncrBy(ctx context.Context, key string, size int64) (int64, error) {
	result, err := RedisClient.IncrBy(ctx, key, size).Result()
	if err != nil {
		return 0, err
	}

	return result, nil
}

func GetSetCardinality(ctx context.Context, key string) (int, error) {
	count, err := RedisClient.SCard(ctx, key).Result()
	if err != nil {
		return 0, err
	}

	return int(count), nil
}

func SetProcessLog(ctx context.Context, key string, logEntry map[string]interface{}, exp time.Duration) error {
	data, err := json.Marshal(logEntry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}

	err = RedisClient.Set(ctx, key, data, exp).Err()
	if err != nil {
		return fmt.Errorf("failed to set log entry in Redis: %w", err)
	}

	return nil
}

func GetDaySize(ctx context.Context, dataMarketAddress string) (*big.Int, error) {
	// Fetch DAY_SIZE for the given data market address from Redis
	daySizeStr, err := RedisClient.HGet(context.Background(), GetDaySizeTableKey(), dataMarketAddress).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch day size for data market %s: %s", dataMarketAddress, err)
	}

	// Convert the day size from string to *big.Int
	daySize, ok := new(big.Int).SetString(daySizeStr, 10)
	if !ok {
		return nil, fmt.Errorf("invalid day size value for data market %s: %s", dataMarketAddress, daySizeStr)
	}

	return daySize, nil
}

func GetDailySnapshotQuota(ctx context.Context, dataMarketAddress string) (*big.Int, error) {
	// Fetch daily snapshot quota for the given data market address from Redis
	dailySnapshotQuotaStr, err := RedisClient.HGet(context.Background(), GetDailySnapshotQuotaTableKey(), dataMarketAddress).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch daily snapshot quota for data market %s: %s", dataMarketAddress, err)
	}

	// Convert the daily snapshot quota from string to *big.Int
	dailySnapshotQuota, ok := new(big.Int).SetString(dailySnapshotQuotaStr, 10)
	if !ok {
		return nil, fmt.Errorf("invalid daily snapshot quota value for data market %s: %s", dataMarketAddress, dailySnapshotQuotaStr)
	}

	return dailySnapshotQuota, nil
}
