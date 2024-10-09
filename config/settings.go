package config

import (
	"log"
	"os"
	"strconv"
)

var SettingsObj *Settings

type Settings struct {
	RedisHost               string
	RedisPort               string
	RedisDB                 string
	IPFSUrl                 string
	TxRelayerUrl            string
	SlackReportingUrl       string
	TxRelayerAuthWriteToken string
	DataMarketAddress       string
	BatchSize               int
	BlockTime               int
	HttpTimeout             int
}

func LoadConfig() {
	config := Settings{
		RedisHost:               getEnv("REDIS_HOST", ""),
		RedisPort:               getEnv("REDIS_PORT", ""),
		RedisDB:                 getEnv("REDIS_DB", ""),
		IPFSUrl:                 getEnv("IPFS_URL", ""),
		TxRelayerUrl:            getEnv("TX_RELAYER_URL", ""),
		SlackReportingUrl:       getEnv("SLACK_REPORTING_URL", ""),
		TxRelayerAuthWriteToken: getEnv("TX_RELAYER_AUTH_WRITE_TOKEN", ""),
		DataMarketAddress:       getEnv("DATA_MARKET_CONTRACT", ""),
	}

	batchSize, batchSizeParseErr := strconv.Atoi(getEnv("BATCH_SIZE", ""))
	if batchSizeParseErr != nil {
		log.Fatalf("Failed to parse BATCH_SIZE environment variable: %v", batchSizeParseErr)
	}
	config.BatchSize = batchSize

	blockTime, blockTimeParseErr := strconv.Atoi(getEnv("BLOCK_TIME", ""))
	if blockTimeParseErr != nil {
		log.Fatalf("Failed to parse BLOCK_TIME environment variable: %v", blockTimeParseErr)
	}
	config.BlockTime = blockTime

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
