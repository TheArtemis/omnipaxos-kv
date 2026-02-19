package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"verifier/internal/verifier"
)

func main() {
	var serve = flag.Bool("serve", false, "Start a local web server to view the visualization")
	var port = flag.Int("port", 8080, "Port for the web server (default: 8080)")
	flag.Parse()

	if len(flag.Args()) < 1 {
		printUsage()
		os.Exit(1)
	}

	historyPaths := flag.Args()

	// If multiple files, merge them automatically
	var historyPath string
	if len(historyPaths) > 1 {
		mergedPath, err := verifier.MergeHistories(historyPaths)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error merging histories: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s\n\n", verifier.Colorize(fmt.Sprintf("🧩 Merged %d histories into: %s", len(historyPaths), mergedPath), verifier.ColorGreen))
		historyPath = mergedPath
	} else {
		historyPath = historyPaths[0]
	}

	// Process the (merged) history
	result := verifier.ProcessHistory(historyPath)

	// Print summary
	fmt.Println("\n" + verifier.Colorize("📊 Summary", verifier.ColorBold+verifier.ColorBlue))
	status := "✗"
	if result.IsLinearizable {
		status = "✅"
	} else {
		status = "🚫"
	}
	summaryLine := fmt.Sprintf("%s %s: %d ops", status, filepath.Base(result.Path), result.TotalOps)
	if !result.IsLinearizable && result.MaxPartialLen > 0 {
		summaryLine += fmt.Sprintf(" (max partial: %d)", result.MaxPartialLen)
	}
	summaryColor := verifier.ColorGreen
	if !result.IsLinearizable {
		summaryColor = verifier.ColorRed
	}
	fmt.Println(verifier.Colorize(summaryLine, summaryColor))

	// Start server if requested
	if *serve {
		verifier.StartSimpleServer(*port, result.HTMLPath)
	} else {
		fmt.Printf("\n%s\n", verifier.Colorize("🧭 Open "+result.HTMLPath+" in your browser to view the visualization", verifier.ColorBlue))
	}

	// Exit with error if history is not linearizable
	if !result.IsLinearizable {
		os.Exit(1)
	}
	os.Exit(0)
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [flags] <history.json> [history2.json ...]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Multiple history files will be automatically merged into one visualization.\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
}
