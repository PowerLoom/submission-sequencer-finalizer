package ipfs

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"math/big"
	"net/http"
	"submission-sequencer-finalizer/config"
	"time"

	shell "github.com/ipfs/go-ipfs-api"

	"github.com/cenkalti/backoff/v4"
	log "github.com/sirupsen/logrus"
)

var IPFSClient *shell.Shell

// Batch represents your data structure
type Batch struct {
	SubmissionIDs []string `json:"submissionIDs"`
	Submissions   []string `json:"submissions"`
	RootHash      string   `json:"roothash"`
	PIDs          []string `json:"pids"`
	CIDs          []string `json:"cids"`
}

type BatchSubmission struct {
	Batch                 *Batch
	CID                   string
	EpochID               *big.Int
	FinalizedCIDsRootHash []byte
}

// ConnectIPFSNode connects to the IPFS node using the provided configuration
func ConnectIPFSNode() {
	log.Debugf("Connecting to IPFS node at: %s", config.SettingsObj.IPFSUrl)

	IPFSClient = shell.NewShellWithClient(
		config.SettingsObj.IPFSUrl,
		&http.Client{
			Timeout: time.Duration(config.SettingsObj.HttpTimeout) * time.Second,
			Transport: &http.Transport{
				TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
				MaxIdleConns:      10,
				IdleConnTimeout:   5 * time.Second,
				DisableKeepAlives: true,
			},
		},
	)
}

// StoreOnIPFS uploads a batch object to IPFS and returns the corresponding CID
func StoreOnIPFS(ipfsClient *shell.Shell, batchData *Batch) (string, error) {
	// Convert the batch data into JSON format
	batchJSON, err := json.Marshal(batchData)
	if err != nil {
		return "", err
	}

	var batchCID string

	// Retry uploading the batch data to IPFS using exponential backoff in case of failures
	err = backoff.Retry(
		func() error {
			// Add the JSON data to IPFS and store the returned CID
			batchCID, err = ipfsClient.Add(bytes.NewReader(batchJSON))
			return err
		},
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), // Retry up to 3 times with exponential backoff
	)

	if err != nil {
		return "", err
	}

	return batchCID, nil
}
