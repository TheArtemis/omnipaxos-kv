package verifier

import "github.com/anishathalye/porcupine"

// Operation represents a single operation from the history JSON file
type Operation struct {
	ClientID   int64                  `json:"client_id"`
	Input      map[string]interface{} `json:"input"`
	Call       int64                  `json:"call"`
	Output     map[string]interface{} `json:"output"`
	ReturnTime int64                  `json:"return_time"`
}

// HistoryResult contains the results of checking a history file
type HistoryResult struct {
	Path           string
	HTMLPath       string
	IsLinearizable bool
	TotalOps       int
	MaxPartialLen  int
	Result         porcupine.CheckResult
}
