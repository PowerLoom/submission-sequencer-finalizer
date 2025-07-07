package config

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	rpchelper "github.com/powerloom/go-rpc-helper"
	"github.com/powerloom/go-rpc-helper/reporting"

	"github.com/ethereum/go-ethereum/common"
)

var SettingsObj *Settings

type Settings struct {
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
	RPCNodes                    []string `json:"rpc_nodes"`
	ArchiveNodes                []string `json:"archive_nodes"`
	MaxRetries                  int      `json:"max_retries"`
	RetryDelayMs                int      `json:"retry_delay_ms"`
	MaxRetryDelayS              int      `json:"max_retry_delay_s"`
	RequestTimeoutS             int      `json:"request_timeout_s"`
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
	archiveNodesStr := getEnv("ARCHIVE_NODES", "[]")
	var archiveNodes []string
	err = json.Unmarshal([]byte(archiveNodesStr), &archiveNodes)
	if err != nil {
		log.Fatalf("Failed to parse ARCHIVE_NODES environment variable: %v", err)
	}

	// Clean quotes from archive RPC node URLs
	for i, url := range archiveNodes {
		archiveNodes[i] = strings.Trim(url, "\"")
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

	config := Settings{
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
		RPCNodes:                rpcNodes,
		ArchiveNodes:            archiveNodes,
		MaxRetries:              getEnvInt("MAX_RETRIES", 3),
		RetryDelayMs:            getEnvInt("RETRY_DELAY_MS", 500),
		MaxRetryDelayS:          getEnvInt("MAX_RETRY_DELAY_S", 30),
		RequestTimeoutS:         getEnvInt("REQUEST_TIMEOUT_S", 30),
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
	log.Printf("  Archive Nodes: %v", config.ArchiveNodes)
	log.Printf("  Data Market Addresses: %v", config.DataMarketAddresses)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvInt(key string, defaultValue int) int {
	value := getEnv(key, "")
	if value == "" {
		return defaultValue
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("Failed to parse %s environment variable: %v", key, err)
	}
	return intValue
}

func (s *Settings) ToRPCConfig() *rpchelper.RPCConfig {
	config := &rpchelper.RPCConfig{
		Nodes: func() []rpchelper.NodeConfig {
			var nodes []rpchelper.NodeConfig
			for _, url := range s.RPCNodes {
				nodes = append(nodes, rpchelper.NodeConfig{URL: url})
			}
			return nodes
		}(),
		ArchiveNodes: func() []rpchelper.NodeConfig {
			var nodes []rpchelper.NodeConfig
			for _, url := range s.ArchiveNodes {
				nodes = append(nodes, rpchelper.NodeConfig{URL: url})
			}
			return nodes
		}(),
		MaxRetries:     s.MaxRetries,
		RetryDelay:     time.Duration(s.RetryDelayMs) * time.Millisecond,
		MaxRetryDelay:  time.Duration(s.MaxRetryDelayS) * time.Second,
		RequestTimeout: time.Duration(s.RequestTimeoutS) * time.Second,
	}

	// Configure webhook if SlackReportingUrl is provided
	if s.SlackReportingUrl != "" {
		log.Printf("Configuring webhook alerts with URL: %s", s.SlackReportingUrl)
		config.WebhookConfig = &reporting.WebhookConfig{
			URL:     s.SlackReportingUrl,
			Timeout: 30 * time.Second,
			Retries: 3,
		}
	} else {
		log.Printf("No webhook URL configured - SLACK_REPORTING_URL environment variable not set")
	}

	return config
}
