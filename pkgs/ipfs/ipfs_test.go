package ipfs

import (
	"fmt"
	"testing"

	shell "github.com/ipfs/go-ipfs-api"
)

func TestStoreOnIPFS(t *testing.T) {
	// Create sample batch data
	batch := &Batch{
		SubmissionIDs: []string{"submission1", "submission2"},
		Submissions:   []string{"data1", "data2"},
		RootHash:      "dummyroothash",
		PIDs:          []string{"pid1"},
		CIDs:          []string{"cid1"},
	}

	// Set up a mock IPFS shell
	ipfsClient := shell.NewShell("http://127.0.0.1:5001")

	// Call StoreOnIPFS function
	batchCID, err := StoreOnIPFS(ipfsClient, batch)
	if err != nil {
		t.Fatalf("Failed to store on IPFS: %v", err)
	}

	// Check if CID starts with "ba" (CIDv1 format)
	if len(batchCID) < 2 || batchCID[:2] != "ba" {
		t.Errorf("Expected CID to start with 'ba', got %s", batchCID)
	}

	fmt.Println("Batch CID:", batchCID)
}
