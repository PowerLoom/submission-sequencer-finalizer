package merkle

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/eigenda"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/redis"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Helper function to load batch data from JSON file
func loadBatchData(t *testing.T) *struct {
	SubmissionIDs []string `json:"submissionIDs"`
	Submissions   []string `json:"submissions"`
	RootHash      string   `json:"roothash"`
	PIDs          []string `json:"pids"`
	CIDs          []string `json:"cids"`
} {
	jsonFile, err := os.ReadFile("../../batchedSubmissions.json")
	if err != nil {
		t.Fatalf("failed to read test data file: %v", err)
	}

	var batchData struct {
		SubmissionIDs []string `json:"submissionIDs"`
		Submissions   []string `json:"submissions"`
		RootHash      string   `json:"roothash"`
		PIDs          []string `json:"pids"`
		CIDs          []string `json:"cids"`
	}

	err = json.Unmarshal(jsonFile, &batchData)
	if err != nil {
		t.Fatalf("failed to unmarshal test data: %v", err)
	}
	return &batchData
}

func TestBuildMerkleTreeIPFS(t *testing.T) {
	batchData := loadBatchData(t)

	// Temporarily save original config to restore later
	oldSettings := config.SettingsObj
	defer func() { config.SettingsObj = oldSettings }()

	config.SettingsObj = &config.Settings{
		Uploader:    "ipfs",
		IPFSUrl:     os.Getenv("IPFS_URL"),
		HttpTimeout: 10, // Default value for tests
		RedisDB:     "0",
	}

	// Initialize Redis client for the test
	oldRedisClient := redis.RedisClient
	defer func() { redis.RedisClient = oldRedisClient }()
	redis.RedisClient = redis.NewRedisClient()

	// Initialize reporting client for the test
	clients.InitializeReportingClient("http://localhost:8080", 10*time.Second)

	// Connect to IPFS
	ipfs.ConnectIPFSNode()

	batchSubmission, err := BuildMerkleTree(batchData.SubmissionIDs, batchData.Submissions, big.NewInt(1), batchData.PIDs, batchData.CIDs, "0x123", 1)

	assert.NoError(t, err)
	assert.NotNil(t, batchSubmission)
	assert.NotEmpty(t, batchSubmission.CID)
}

func TestBuildMerkleTreeEigenDA(t *testing.T) {
	batchData := loadBatchData(t)

	// Temporarily save original config to restore later
	oldSettings := config.SettingsObj
	defer func() { config.SettingsObj = oldSettings }()

	// Set up mock config for EigenDA
	config.SettingsObj = &config.Settings{
		Uploader:          "eigenda",
		EigenDAHostname:   os.Getenv("EIGENDA_HOSTNAME"),
		EigenDAPort:       os.Getenv("EIGENDA_PORT"),
		EigenDAPrivateKey: os.Getenv("EIGENDA_PRIVATE_KEY"),
		HttpTimeout:       10, // Default value for tests
		RedisDB:           "0",
	}

	// Initialize Redis client for the test
	oldRedisClient := redis.RedisClient
	defer func() { redis.RedisClient = oldRedisClient }()
	redis.RedisClient = redis.NewRedisClient()

	// Initialize reporting client for the test
	clients.InitializeReportingClient("http://localhost:8080", 10*time.Second)

	// Configure logger for this test
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)

	// Directly print the private key value for debugging
	fmt.Printf("DEBUG: EIGENDA_PRIVATE_KEY as seen by test: '%s' (length: %d)\n", config.SettingsObj.EigenDAPrivateKey, len(config.SettingsObj.EigenDAPrivateKey))

	// Connect to EigenDA
	err := eigenda.ConnectEigenDA()
	if err != nil {
		// Log the error and skip the rest of the test if connection fails
		assert.Failf(t, "Failed to connect to EigenDA", "Error: %v", err)
		return
	}

	batchSubmission, err := BuildMerkleTree(batchData.SubmissionIDs, batchData.Submissions, big.NewInt(1), batchData.PIDs, batchData.CIDs, "0x123", 1)

	assert.NoError(t, err)
	assert.NotNil(t, batchSubmission)
	assert.NotEmpty(t, batchSubmission.CID)
}
