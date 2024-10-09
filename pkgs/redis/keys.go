package redis

import (
	"fmt"
	"submission-sequencer-finalizer/pkgs"
)

func TriggeredProcessLog(process, identifier string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.ProcessTriggerKey, process, identifier)
}
