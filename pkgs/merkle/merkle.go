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
func BuildMerkleTree(submissionIDs, submissionData []string, epochID *big.Int, projectIDs, CIDs []string) (*ipfs.BatchSubmission, error) {
	// Create a new Merkle tree for submission IDs
	submissionIDMerkleTree, err := imt.New()
	if err != nil {
		clients.SendFailureNotification(pkgs.BuildMerkleTree, fmt.Sprintf("Error creating Merkle tree for submission IDs: %s\n", err.Error()), time.Now().String(), "High")
		log.Errorf("Error creating Merkle tree for submission IDs: %s", err.Error())
		return nil, err
	}

	log.Debugln("Building Merkle tree for epoch: ", epochID.String())

	// Efficiently update the Merkle tree with submission IDs
	if _, err = UpdateMerkleTree(submissionIDs, submissionIDMerkleTree); err != nil {
		return nil, err
	}

	// Get the root hash of the submission ID Merkle tree
	submissionIDRootHash := GetRootHash(submissionIDMerkleTree)
	log.Debugf("Root hash for batch in epoch %s: %s", epochID.String(), submissionIDRootHash)

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
		clients.SendFailureNotification(pkgs.BuildMerkleTree, fmt.Sprintf("Error storing batch on IPFS: %s", err.Error()), time.Now().String(), "High")
		log.Errorf("Error storing batch on IPFS: %s", err.Error())
		return nil, err
	}

	log.Debugf("Stored CID for batch: %s", batchCID)

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
		clients.SendFailureNotification(pkgs.BuildMerkleTree, err.Error(), time.Now().String(), "High")
		log.Errorln("Error storing BuildMerkleTree process log: ", err.Error())
	}

	// Create a new Merkle tree for finalized CIDs
	finalizedCIDMerkleTree, err := imt.New()
	if err != nil {
		clients.SendFailureNotification(pkgs.BuildMerkleTree, fmt.Sprintf("Error creating Merkle tree for CIDs: %s\n", err.Error()), time.Now().String(), "High")
		log.Errorf("Error creating Merkle tree for CIDs: %s\n", err.Error())
		return nil, err
	}

	// Update the Merkle tree with CIDs
	if _, err = UpdateMerkleTree(batchData.CIDs, finalizedCIDMerkleTree); err != nil {
		clients.SendFailureNotification(pkgs.BuildMerkleTree, fmt.Sprintf("Error updating Merkle tree for batch with roothash %s: %s", submissionIDRootHash, err.Error()), time.Now().String(), "High")
		log.Errorln("Unable to get finalized root hash: ", err.Error())
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

// GetRootHash returns the hexadecimal string representation of the root digest of the Merkle tree (without 0x prefix)
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
