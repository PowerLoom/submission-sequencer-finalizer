package config

import (
	"log"
	"os"
	"strconv"
)

var SettingsObj *Settings

type Settings struct {
	RedisHost           string
	RedisPort           string
	RedisDB             string
	IPFSUrl             string
	SlackReportingUrl   string
	TransactionRelayUrl string
	BatchSize           int
	HttpTimeout         int
}

func LoadConfig() {
	config := Settings{
		RedisHost:           getEnv("REDIS_HOST", ""),
		RedisPort:           getEnv("REDIS_PORT", ""),
		RedisDB:             getEnv("REDIS_DB", ""),
		IPFSUrl:             getEnv("IPFS_URL", ""),
		SlackReportingUrl:   getEnv("SLACK_REPORTING_URL", ""),
		TransactionRelayUrl: getEnv("TRANSACTION_RELAY_URL", ""),
	}

	batchSize, batchSizeParseErr := strconv.Atoi(getEnv("BATCH_SIZE", ""))
	if batchSizeParseErr != nil {
		log.Fatalf("Failed to parse BATCH_SIZE environment variable: %v", batchSizeParseErr)
	}
	config.BatchSize = batchSize

	httpTimeout, timeoutParseErr := strconv.Atoi(getEnv("HTTP_TIMEOUT", ""))
	if timeoutParseErr != nil {
		log.Fatalf("Failed to parse HTTP_TIMEOUT environment variable: %v", timeoutParseErr)
	}
	config.HttpTimeout = httpTimeout

	SettingsObj = &config
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
