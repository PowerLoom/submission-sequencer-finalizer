package pkgs

// Process Name Constants
// process : identifier
const (
	FinalizeBatches         = "FinalizeBatches"
	BuildMerkleTree         = "BuildMerkleTree"
	BuildBatchSubmissions   = "BuildBatchSubmissions"
	ArrangeKeysInBatches    = "ArrangeKeysInBatches"
	SendSubmissionBatchSize = "SendSubmissionBatchSize"
)

// General Key Constants
const (
	CurrentDayKey               = "CurrentDayKey"
	DaySizeTableKey             = "DaySizeTableKey"
	ProcessTriggerKey           = "TriggeredSequencerProcess"
	EligibleSubmissionCountsKey = "EligibleSubmissionCountsKey"
)
