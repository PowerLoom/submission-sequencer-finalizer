package clients

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"submission-sequencer-finalizer/config"
	"time"
)

type TxRelayerClient struct {
	url    string
	client *http.Client
}

type SubmissionBatchSizeRequest struct {
	EpochID   *big.Int `json:"epochID"`
	Size      int      `json:"batchSize"`
	AuthToken string   `json:"authToken"`
}

type SubmitSubmissionBatchRequest struct {
	DataMarketAddress     string   `json:"dataMarket"`
	BatchCID              string   `json:"batchCID"`
	EpochID               *big.Int `json:"epochID"`
	ProjectIDs            []string `json:"projectIDs"`
	SnapshotCIDs          []string `json:"snapshotCIDs"`
	FinalizedCIDsRootHash string   `json:"finalizedCIDsRootHash"`
	AuthToken             string   `json:"authToken"`
}

var txRelayerClient *TxRelayerClient

// InitializeTxClient initializes the TxRelayerClient with the provided URL and timeout
func InitializeTxClient(url string, timeout time.Duration) {
	txRelayerClient = &TxRelayerClient{
		url: url,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
	}
}

// SendSubmissionBatchSize sends the size of the submission batch for a given epoch
func SendSubmissionBatchSize(epochID *big.Int, size int) error {
	request := SubmissionBatchSizeRequest{
		EpochID:   epochID,
		Size:      size,
		AuthToken: config.SettingsObj.TxRelayerAuthWriteToken,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("unable to marshal batch size request: %w", err)
	}

	url := fmt.Sprintf("%s/submitBatchSize", txRelayerClient.url)

	resp, err := txRelayerClient.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("unable to send submission batch size request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send submission batch size request, status code: %d", resp.StatusCode)
	}

	return nil
}

// SubmitSubmissionBatch submits a batch of submissions for a given epoch
func SubmitSubmissionBatch(dataMarketAddress, batchCID, batchID string, epochID *big.Int, projectIDs, snapshotCIDs []string, finalizedCIDsRootHash string) error {
	request := SubmitSubmissionBatchRequest{
		DataMarketAddress:     dataMarketAddress,
		BatchCID:              batchCID,
		EpochID:               epochID,
		ProjectIDs:            projectIDs,
		SnapshotCIDs:          snapshotCIDs,
		FinalizedCIDsRootHash: fmt.Sprintf("0x%x", finalizedCIDsRootHash),
		AuthToken:             config.SettingsObj.TxRelayerAuthWriteToken,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("unable to marshal batch submission request: %w", err)
	}

	url := fmt.Sprintf("%s/submitSubmissionBatch", txRelayerClient.url)

	resp, err := txRelayerClient.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("unable to send submission batch request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send submission batch request, status code: %d", resp.StatusCode)
	}

	return nil
}
