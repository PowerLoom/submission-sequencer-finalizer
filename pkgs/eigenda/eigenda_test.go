package eigenda

import (
	"os"
	"submission-sequencer-finalizer/config"
	"testing"

	disperserv2 "github.com/Layr-Labs/eigenda/api/grpc/disperser/v2"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
)

func setupTest(t *testing.T) {
	err := godotenv.Load("../../.env")
	if err != nil {
		t.Logf("Error loading .env file, continuing with environment variables: %v", err)
	}

	config.SettingsObj = &config.Settings{
		EigenDAHostname:   os.Getenv("EIGENDA_HOSTNAME"),
		EigenDAPort:       os.Getenv("EIGENDA_PORT"),
		EigenDAPrivateKey: os.Getenv("EIGENDA_PRIVATE_KEY"),
	}

	err = ConnectEigenDA()
	assert.NoError(t, err)
}

func TestStoreOnEigenDA_Success(t *testing.T) {
	// 1. Setup
	setupTest(t)

	// 2. Test Execution
	testData := []byte("this is some test data for actual client")
	blobKeyHex, err := StoreOnEigenDA(testData)

	// 3. Assertions
	assert.NoError(t, err)
	assert.NotEmpty(t, blobKeyHex)

	// Optional: Check status of the dispersed blob
	statusReply, err := GetBlobStatus(blobKeyHex)
	assert.NoError(t, err)
	assert.NotNil(t, statusReply)
	assert.Equal(t, disperserv2.BlobStatus_QUEUED, statusReply.GetStatus())
}

func TestStoreOnEigenDA_DisperserError(t *testing.T) {
	// 1. Setup
	// Temporarily set an invalid private key to force an error during client connection
	config.SettingsObj = &config.Settings{
		EigenDAHostname:   "disperser-testnet-holesky.eigenda.xyz",
		EigenDAPort:       "443",
		EigenDAPrivateKey: "invalid_private_key",
	}

	// 2. Test Execution
	err := ConnectEigenDA()

	// 3. Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create signer")
}

func TestGetBlobStatus_Success(t *testing.T) {
	// 1. Setup
	setupTest(t)

	// Disperse a blob first to get a valid blobKeyHex
	testData := []byte("data for status check")
	blobKeyHex, err := StoreOnEigenDA(testData)
	assert.NoError(t, err)
	assert.NotEmpty(t, blobKeyHex)

	// 2. Test Execution
	statusReply, err := GetBlobStatus(blobKeyHex)

	// 3. Assertions
	assert.NoError(t, err)
	assert.NotNil(t, statusReply)
	assert.Equal(t, disperserv2.BlobStatus_QUEUED, statusReply.GetStatus())
}

func TestGetBlobStatus_InvalidHex(t *testing.T) {
	// 1. Setup
	setupTest(t)

	// 2. Test Execution
	_, err := GetBlobStatus("invalid-hex")

	// 3. Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode blob key")
}
