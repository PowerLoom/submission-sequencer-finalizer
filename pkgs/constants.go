package pkgs

// Process Name Constants
// process : identifier
const (
	FinalizeBatches                = "FinalizeBatches"
	BuildMerkleTree                = "BuildMerkleTree"
	BuildBatchSubmissions          = "BuildBatchSubmissions"
	ArrangeKeysInBatches           = "ArrangeKeysInBatches"
	SendSubmissionBatchSize        = "SendSubmissionBatchSize"
	SendUpdateRewardsToRelayer     = "SendUpdateRewardsToRelayer"
	SendSubmissionBatchToRelayer   = "SendSubmissionBatchToRelayer"
	UpdateEligibleSubmissionCounts = "UpdateEligibleSubmissionCounts"
)

// General Key Constants
const (
	CurrentDayKey                  = "CurrentDayKey"
	DaySizeTableKey                = "DaySizeTableKey"
	ProcessTriggerKey              = "TriggeredSequencerProcess"
	EligibleSlotSubmissionsKey     = "EligibleSlotSubmissionsKey"
	DailySnapshotQuotaTableKey     = "DailySnapshotQuotaTableKey"
	EligibleSlotSubmissionByDayKey = "EligibleSlotSubmissionByDayKey"
	EligibleNodesCountKey          = "EligibleNodesCountKey"
)
