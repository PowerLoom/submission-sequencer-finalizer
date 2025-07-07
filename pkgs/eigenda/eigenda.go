
package eigenda

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/Layr-Labs/eigenda/api/clients/v2"
	authv2 "github.com/Layr-Labs/eigenda/core/auth/v2"
	"github.com/Layr-Labs/eigenda/core"
	"github.com/Layr-Labs/eigenda/encoding/utils/codec"
	log "github.com/sirupsen/logrus"
	"submission-sequencer-finalizer/config"
)

var EigenDAClient clients.DisperserClient

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
	FinalizedCIDsRootHash string
}

func ConnectEigenDA() error {
	// Create signer
	signer, err := authv2.NewLocalBlobRequestSigner(config.SettingsObj.EigenDAPrivateKey)
	if err != nil {
		return fmt.Errorf("failed to create signer: %v", err)
	}

	// Create client
	EigenDAClient, err = clients.NewDisperserClient(&clients.DisperserClientConfig{
		Hostname:          config.SettingsObj.EigenDAHostname,
		Port:              config.SettingsObj.EigenDAPort,
		UseSecureGrpcFlag: true,
	}, signer, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}

	return nil
}

func StoreOnEigenDA(data []byte) (string, error) {
	// Disperse data
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	paddedData := codec.ConvertByPaddingEmptyByte(data)
	
	log.Infof("Dispersing %d bytes to EigenDA", len(paddedData))

	// Disperse blob with version 0 and to quorums 0 and 1
	status, blobKey, err := EigenDAClient.DisperseBlob(ctx, paddedData, 0, []core.QuorumID{0, 1})
	if err != nil {
		return "", fmt.Errorf("failed to disperse blob: %v", err)
	}

	log.Infof("Dispersal status: %v", status)
	
	return fmt.Sprintf("%x", blobKey), nil
}
