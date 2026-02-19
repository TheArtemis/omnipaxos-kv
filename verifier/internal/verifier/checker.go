package verifier

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

// ProcessHistory processes a single history file and returns the result
func ProcessHistory(historyPath string) HistoryResult {
	fmt.Printf("\n%s\n", Colorize("🧪 Processing "+filepath.Base(historyPath), ColorCyan))

	ops, err := loadHistory(historyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return HistoryResult{Path: historyPath, IsLinearizable: false}
	}

	if len(ops) == 0 {
		fmt.Println(Colorize("  ⚪️ No operations found in history", ColorYellow))
		return HistoryResult{Path: historyPath, IsLinearizable: true, TotalOps: 0}
	}

	// Convert to Porcupine format
	history := convertToPorcupineOperations(ops)

	// Create model and check linearizability
	model := createKVModel()
	result, info := porcupine.CheckOperationsVerbose(model, history, 30*time.Second)

	// Calculate max partial linearization length
	maxPartialLength := calculateMaxPartialLength(info)

	// Generate visualization
	htmlPath := generateVisualization(historyPath, model, info)

	// Print results
	printResults(result, len(history), maxPartialLength)

	return HistoryResult{
		Path:           historyPath,
		HTMLPath:       htmlPath,
		IsLinearizable: result == porcupine.Ok,
		TotalOps:       len(history),
		MaxPartialLen:  maxPartialLength,
		Result:         result,
	}
}

// calculateMaxPartialLength finds the maximum partial linearization length
func calculateMaxPartialLength(info porcupine.LinearizationInfo) int {
	partialLinearizations := info.PartialLinearizations()
	maxPartialLength := 0
	for _, partition := range partialLinearizations {
		for _, linearization := range partition {
			if len(linearization) > maxPartialLength {
				maxPartialLength = len(linearization)
			}
		}
	}
	return maxPartialLength
}

// generateVisualization creates an HTML visualization file
func generateVisualization(historyPath string, model porcupine.Model, info porcupine.LinearizationInfo) string {
	baseName := strings.TrimSuffix(filepath.Base(historyPath), filepath.Ext(historyPath))
	htmlPath := filepath.Join(filepath.Dir(historyPath), baseName+".html")

	htmlFile, err := os.Create(htmlPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", Colorize(fmt.Sprintf("  ⚠️ Warning: Failed to create visualization file: %v", err), ColorYellow))
		return htmlPath
	}
	defer htmlFile.Close()

	if err := porcupine.Visualize(model, info, htmlFile); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", Colorize(fmt.Sprintf("  ⚠️ Warning: Failed to generate visualization: %v", err), ColorYellow))
	} else {
		fmt.Printf("%s\n", Colorize("  🖼️  Generated visualization: "+htmlPath, ColorGreen))
	}

	return htmlPath
}

// printResults prints the linearizability check results
func printResults(result porcupine.CheckResult, totalOps int, maxPartialLength int) {
	isLinearizable := result == porcupine.Ok
	if isLinearizable {
		fmt.Println(Colorize("  ✅ History is linearizable", ColorGreen))
		fmt.Printf("%s\n", Colorize(fmt.Sprintf("  🧮 Total operations: %d", totalOps), ColorGreen))
		if maxPartialLength > 0 {
			fmt.Printf("%s\n", Colorize(fmt.Sprintf("  📌 Max partial linearization length: %d", maxPartialLength), ColorGreen))
		}
	} else {
		fmt.Println(Colorize("  🚫 History is NOT linearizable", ColorRed))
		fmt.Printf("%s\n", Colorize(fmt.Sprintf("  🧮 Total operations: %d", totalOps), ColorRed))
		if maxPartialLength > 0 {
			fmt.Printf("%s\n", Colorize(fmt.Sprintf("  📌 Max partial linearization length: %d (out of %d)",
				maxPartialLength, totalOps), ColorRed))
		}
		fmt.Printf("%s\n", Colorize(fmt.Sprintf("  ⚠️ Check result: %v", result), ColorYellow))
	}
}
