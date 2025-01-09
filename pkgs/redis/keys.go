package redis

import (
	"fmt"
	"strings"
	"submission-sequencer-finalizer/pkgs"
)

func GetDaySizeTableKey() string {
	return pkgs.DaySizeTableKey
}

func GetDailySnapshotQuotaTableKey() string {
	return pkgs.DailySnapshotQuotaTableKey
}

func GetCurrentDayKey(dataMarketAddress string) string {
	return fmt.Sprintf("%s.%s", pkgs.CurrentDayKey, strings.ToLower(dataMarketAddress))
}

func ContractStateVariableWithDataMarket(dataMarketAddress string, varName string) string {
	return fmt.Sprintf("ProtocolState.%s.%s", dataMarketAddress, varName)
}

func TriggeredProcessLog(process, identifier string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.ProcessTriggerKey, process, identifier)
}

func EligibleSlotSubmissionKey(dataMarketAddress string, slotID, currentDay string) string {
	return fmt.Sprintf("%s.%s.%s.%s", pkgs.EligibleSlotSubmissionsKey, strings.ToLower(dataMarketAddress), currentDay, slotID)
}

func EligibleNodesByDayKey(dataMarketAddress, currentDay string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.EligibleNodeByDayKey, strings.ToLower(dataMarketAddress), currentDay)
}

func EligibleSlotSubmissionsByEpochKey(dataMarketAddress, currentDay, epochID string) string {
	return fmt.Sprintf("%s.%s.%s.%s", pkgs.EligibleSlotSubmissionByEpochKey, strings.ToLower(dataMarketAddress), currentDay, epochID)
}

func DiscardedSubmissionsKey(dataMarketAddress, currentDay, epochID string) string {
	return fmt.Sprintf("%s.%s.%s.%s", pkgs.DiscardedSubmissionKey, strings.ToLower(dataMarketAddress), currentDay, epochID)
}
