package config

import (
	"encoding/json"
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
	DataMarketAddresses     []string
	BatchSize               int
	BlockTime               int
	HttpTimeout             int
}

func LoadConfig() {
	dataMarketAddresses := getEnv("DATA_MARKET_ADDRESSES", "[]")
	dataMarketAddressesList := []string{}

	err := json.Unmarshal([]byte(dataMarketAddresses), &dataMarketAddressesList)
	if err != nil {
		log.Fatalf("Failed to parse DATA_MARKET_ADDRESSES environment variable: %v", err)
	}
	if len(dataMarketAddressesList) == 0 {
		log.Fatalf("DATA_MARKET_ADDRESSES environment variable has an empty array")
	}

	config := Settings{
		RedisHost:               getEnv("REDIS_HOST", ""),
		RedisPort:               getEnv("REDIS_PORT", ""),
		RedisDB:                 getEnv("REDIS_DB", ""),
		IPFSUrl:                 getEnv("IPFS_URL", ""),
		TxRelayerUrl:            getEnv("TX_RELAYER_URL", ""),
		SlackReportingUrl:       getEnv("SLACK_REPORTING_URL", ""),
		TxRelayerAuthWriteToken: getEnv("TX_RELAYER_AUTH_WRITE_TOKEN", ""),
		DataMarketAddresses:     dataMarketAddressesList,
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
