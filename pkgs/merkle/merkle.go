package merkle

import (
	"context"
	"fmt"
	"math/big"
	"submission-sequencer-finalizer/pkgs"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/redis"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sergerad/incremental-merkle-tree/imt"
	log "github.com/sirupsen/logrus"
)

// BuildMerkleTree constructs Merkle trees for both submission IDs and finalized CIDs, stores the batch on IPFS, and logs the process
func BuildMerkleTree(submissionIDs, submissionData []string, epochID *big.Int, projectIDs, CIDs []string, dataMarketAddress string, batchID int) (*ipfs.BatchSubmission, error) {
	// Create a new Merkle tree for submission IDs
	submissionIDMerkleTree, err := imt.New()
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to create submissionIDs merkle tree for batch %d of epoch %s within data market %s: %s", batchID, epochID.String(), dataMarketAddress, err.Error())
		clients.SendFailureNotification(pkgs.BuildMerkleTree, errorMsg, time.Now().String(), "High")
		log.Error(errorMsg)
		return nil, err
	}

	// Efficiently update the Merkle tree with submission IDs
	if _, err = UpdateMerkleTree(submissionIDs, submissionIDMerkleTree); err != nil {
		errorMsg := fmt.Sprintf("Error updating submissionIDs merkle tree for batch %d of epoch %s within data market %s: %v", batchID, epochID.String(), dataMarketAddress, err.Error())
		clients.SendFailureNotification(pkgs.BuildMerkleTree, errorMsg, time.Now().String(), "High")
		log.Error(errorMsg)
		return nil, err
	}

	log.Infof("✅ Successfully updated submissionIDs merkle tree for batch %d in epoch %s within data market %s", batchID, epochID.String(), dataMarketAddress)

	// Get the root hash of the submission ID Merkle tree
	submissionIDRootHash := GetRootHash(submissionIDMerkleTree)
	log.Infof("🔍 SubmissionIDs merkle tree root hash for batch %d in epoch %s within data market %s: %s", batchID, epochID.String(), dataMarketAddress, submissionIDRootHash)

	// Create a new batch and store it in IPFS
	batchData := &ipfs.Batch{
		SubmissionIDs: submissionIDs,
		Submissions:   submissionData,
		RootHash:      submissionIDRootHash,
		PIDs:          projectIDs,
		CIDs:          CIDs,
	}

	// Store the batch in IPFS and get the corresponding CID
	batchCID, err := ipfs.StoreOnIPFS(ipfs.IPFSClient, batchData)
	if err != nil {
		errorMsg := fmt.Sprintf("Error storing batch %d data on IPFS for epoch %s within data market %s: %s", batchID, epochID.String(), dataMarketAddress, err.Error())
		clients.SendFailureNotification(pkgs.BuildMerkleTree, errorMsg, time.Now().String(), "High")
		log.Error(errorMsg)
		return nil, err
	}

	log.Infof("📦 Batch %d data stored on IPFS with CID %s for epoch %s within data market %s", batchID, batchCID, epochID.String(), dataMarketAddress)

	// Log the batch processing success
	processLogEntry := map[string]interface{}{
		"epoch_id":          epochID.String(),
		"batch_cid":         batchCID,
		"submissions_count": len(submissionData),
		"submissions":       submissionData,
		"timestamp":         time.Now().Unix(),
	}

	// Store the process log in Redis
	if err = redis.SetProcessLog(context.Background(), redis.TriggeredProcessLog(pkgs.BuildMerkleTree, submissionIDRootHash), processLogEntry, 4*time.Hour); err != nil {
		errorMsg := fmt.Sprintf("Error storing process log for batch %d of epoch %s within data market %s in Redis: %s", batchID, epochID.String(), dataMarketAddress, err.Error())
		clients.SendFailureNotification(pkgs.BuildMerkleTree, errorMsg, time.Now().String(), "High")
		log.Error(errorMsg)
	}

	// Create a new Merkle tree for finalized CIDs
	finalizedCIDMerkleTree, err := imt.New()
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to create finalized CIDs merkle tree for batch %d of epoch %s within data market %s: %s", batchID, epochID.String(), dataMarketAddress, err.Error())
		clients.SendFailureNotification(pkgs.BuildMerkleTree, errorMsg, time.Now().String(), "High")
		log.Error(errorMsg)
		return nil, err
	}

	// Update the Merkle tree with CIDs
	if _, err = UpdateMerkleTree(batchData.CIDs, finalizedCIDMerkleTree); err != nil {
		errorMsg := fmt.Sprintf("Error updating finalized CIDs merkle tree for batch %d of epoch %s within data market %s: %v", batchID, epochID.String(), dataMarketAddress, err.Error())
		clients.SendFailureNotification(pkgs.BuildMerkleTree, errorMsg, time.Now().String(), "High")
		log.Error(errorMsg)
		return nil, err
	}

	// Return the finalized batch submission with the Merkle tree root hash for CIDs
	return &ipfs.BatchSubmission{
		Batch:                 batchData,
		CID:                   batchCID,
		EpochID:               epochID,
		FinalizedCIDsRootHash: GetRootHash(finalizedCIDMerkleTree),
	}, nil
}

// GetRootHash returns the hexadecimal string representation of the root digest of the Merkle tree (with 0x prefix)
func GetRootHash(tree *imt.IncrementalMerkleTree) string {
	return crypto.Keccak256Hash(tree.RootDigest()).Hex()
}

// UpdateMerkleTree adds all provided IDs to the Merkle tree as leaves and returns the updated tree.
func UpdateMerkleTree(ids []string, tree *imt.IncrementalMerkleTree) (*imt.IncrementalMerkleTree, error) {
	for _, id := range ids {
		err := tree.AddLeaf([]byte(id))
		if err != nil {
			log.Errorf("Error adding leaf to Merkle tree: %s\n", err.Error())
			return nil, err
		}
	}
	return tree, nil
}
