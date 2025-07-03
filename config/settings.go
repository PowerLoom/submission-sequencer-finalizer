package config

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

var SettingsObj *Settings

type Settings struct {
	// RPC Helper Configuration
	RPCNodes            []string
	ArchiveRPCNodes     []string
	RPCMaxRetries       int
	RPCRetryDelayMs     int
	RPCMaxRetryDelayMs  int
	RPCRequestTimeoutMs int

	ClientUrl                   string
	ContractAddress             string
	RedisHost                   string
	RedisPort                   string
	RedisDB                     string
	IPFSUrl                     string
	TxRelayerUrl                string
	SlackReportingUrl           string
	TxRelayerAuthWriteToken     string
	BatchSize                   int
	BlockTime                   int
	HttpTimeout                 int
	DataMarketAddresses         []string
	DataMarketContractAddresses []common.Address
	ProcessSwitch               bool
}

func LoadConfig() {
	// Parse RPC nodes from environment variable
	rpcNodesStr := getEnv("RPC_NODES", "[]")
	var rpcNodes []string
	err := json.Unmarshal([]byte(rpcNodesStr), &rpcNodes)
	if err != nil {
		log.Fatalf("Failed to parse RPC_NODES environment variable: %v", err)
	}
	if len(rpcNodes) == 0 {
		// Fallback to legacy PROST_RPC_URL for backward compatibility
		legacyRPCURL := getEnv("PROST_RPC_URL", "")
		if legacyRPCURL != "" {
			rpcNodes = []string{legacyRPCURL}
		} else {
			log.Fatalf("RPC_NODES environment variable has an empty array and no PROST_RPC_URL fallback")
		}
	}

	// Clean quotes from RPC node URLs
	for i, url := range rpcNodes {
		rpcNodes[i] = strings.Trim(url, "\"")
	}

	// Parse archive RPC nodes from environment variable (optional)
	archiveRPCNodesStr := getEnv("ARCHIVE_RPC_NODES", "[]")
	var archiveRPCNodes []string
	err = json.Unmarshal([]byte(archiveRPCNodesStr), &archiveRPCNodes)
	if err != nil {
		log.Fatalf("Failed to parse ARCHIVE_RPC_NODES environment variable: %v", err)
	}

	// Clean quotes from archive RPC node URLs
	for i, url := range archiveRPCNodes {
		archiveRPCNodes[i] = strings.Trim(url, "\"")
	}

	dataMarketAddresses := getEnv("DATA_MARKET_ADDRESSES", "[]")
	dataMarketAddressesList := []string{}

	err = json.Unmarshal([]byte(dataMarketAddresses), &dataMarketAddressesList)
	if err != nil {
		log.Fatalf("Failed to parse DATA_MARKET_ADDRESSES environment variable: %v", err)
	}
	if len(dataMarketAddressesList) == 0 {
		log.Fatalf("DATA_MARKET_ADDRESSES environment variable has an empty array")
	}

	// Clean quotes from data market addresses
	for i, addr := range dataMarketAddressesList {
		dataMarketAddressesList[i] = strings.Trim(addr, "\"")
	}

	processSwitch, processSwitchParseErr := strconv.ParseBool(getEnv("PROCESS_SWITCH", "true"))
	if processSwitchParseErr != nil {
		log.Fatalf("Failed to parse PROCESS_SWITCH environment variable: %v", processSwitchParseErr)
	}

	// Parse RPC helper configuration
	rpcMaxRetries, rpcMaxRetriesErr := strconv.Atoi(getEnv("RPC_MAX_RETRIES", "3"))
	if rpcMaxRetriesErr != nil {
		log.Printf("Invalid RPC_MAX_RETRIES value, using default of 3: %v", rpcMaxRetriesErr)
		rpcMaxRetries = 3
	}

	rpcRetryDelayMs, rpcRetryDelayMsErr := strconv.Atoi(getEnv("RPC_RETRY_DELAY_MS", "500"))
	if rpcRetryDelayMsErr != nil {
		log.Printf("Invalid RPC_RETRY_DELAY_MS value, using default of 500: %v", rpcRetryDelayMsErr)
		rpcRetryDelayMs = 500
	}

	rpcMaxRetryDelayMs, rpcMaxRetryDelayMsErr := strconv.Atoi(getEnv("RPC_MAX_RETRY_DELAY_MS", "30000"))
	if rpcMaxRetryDelayMsErr != nil {
		log.Printf("Invalid RPC_MAX_RETRY_DELAY_MS value, using default of 30000: %v", rpcMaxRetryDelayMsErr)
		rpcMaxRetryDelayMs = 30000
	}

	rpcRequestTimeoutMs, rpcRequestTimeoutMsErr := strconv.Atoi(getEnv("RPC_REQUEST_TIMEOUT_MS", "30000"))
	if rpcRequestTimeoutMsErr != nil {
		log.Printf("Invalid RPC_REQUEST_TIMEOUT_MS value, using default of 30000: %v", rpcRequestTimeoutMsErr)
		rpcRequestTimeoutMs = 30000
	}

	config := Settings{
		// RPC Helper Configuration
		RPCNodes:            rpcNodes,
		ArchiveRPCNodes:     archiveRPCNodes,
		RPCMaxRetries:       rpcMaxRetries,
		RPCRetryDelayMs:     rpcRetryDelayMs,
		RPCMaxRetryDelayMs:  rpcMaxRetryDelayMs,
		RPCRequestTimeoutMs: rpcRequestTimeoutMs,

		// Legacy configuration (keeping for backward compatibility)
		ClientUrl:               getEnv("PROST_RPC_URL", ""),
		ContractAddress:         strings.Trim(getEnv("PROTOCOL_STATE_CONTRACT", ""), "\""),
		RedisHost:               getEnv("REDIS_HOST", ""),
		RedisPort:               getEnv("REDIS_PORT", ""),
		RedisDB:                 getEnv("REDIS_DB", ""),
		IPFSUrl:                 getEnv("IPFS_URL", ""),
		TxRelayerUrl:            getEnv("TX_RELAYER_URL", ""),
		SlackReportingUrl:       getEnv("SLACK_REPORTING_URL", ""),
		TxRelayerAuthWriteToken: getEnv("TX_RELAYER_AUTH_WRITE_TOKEN", ""),
		DataMarketAddresses:     dataMarketAddressesList,
		ProcessSwitch:           processSwitch,
	}

	for _, addr := range config.DataMarketAddresses {
		config.DataMarketContractAddresses = append(config.DataMarketContractAddresses, common.HexToAddress(addr))
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

	// Log cleaned configuration values for debugging
	log.Printf("✅ Configuration loaded successfully:")
	log.Printf("  Protocol State Contract: %s", config.ContractAddress)
	log.Printf("  RPC Nodes: %v", config.RPCNodes)
	log.Printf("  Archive RPC Nodes: %v", config.ArchiveRPCNodes)
	log.Printf("  Data Market Addresses: %v", config.DataMarketAddresses)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
