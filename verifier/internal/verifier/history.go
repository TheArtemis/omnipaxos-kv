package verifier

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/anishathalye/porcupine"
)

// MergeHistories combines multiple history files into a single merged history
func MergeHistories(historyPaths []string) (string, error) {
	var allOps []Operation

	for _, path := range historyPaths {
		ops, err := loadHistory(path)
		if err != nil {
			return "", fmt.Errorf("error loading %s: %v", path, err)
		}
		allOps = append(allOps, ops...)
	}

	// Sort operations by call time to maintain chronological order
	sort.Slice(allOps, func(i, j int) bool {
		return allOps[i].Call < allOps[j].Call
	})

	// Write merged history to a new file
	mergedPath := filepath.Join(filepath.Dir(historyPaths[0]), "merged-history.json")
	mergedData, err := json.MarshalIndent(allOps, "", "  ")
	if err != nil {
		return "", fmt.Errorf("error marshaling merged history: %v", err)
	}

	if err := os.WriteFile(mergedPath, mergedData, 0644); err != nil {
		return "", fmt.Errorf("error writing merged history: %v", err)
	}

	return mergedPath, nil
}

// loadHistory loads and parses a history JSON file
func loadHistory(historyPath string) ([]Operation, error) {
	data, err := os.ReadFile(historyPath)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	var ops []Operation
	if err := json.Unmarshal(data, &ops); err != nil {
		return nil, fmt.Errorf("error parsing JSON: %v", err)
	}

	return ops, nil
}

// convertToPorcupineOperations converts our Operation format to Porcupine's format
func convertToPorcupineOperations(ops []Operation) []porcupine.Operation {
	history := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		history = append(history, porcupine.Operation{
			ClientId: int(op.ClientID),
			Input:    op.Input,
			Call:     op.Call,
			Output:   op.Output,
			Return:   op.ReturnTime,
		})
	}
	return history
}
