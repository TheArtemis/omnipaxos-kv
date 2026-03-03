package verifier

import (
	"fmt"
	"net/http"
	"os"
)

// StartSimpleServer starts a simple HTTP server to serve a single HTML file
func StartSimpleServer(port int, htmlPath string) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, htmlPath)
	})

	url := fmt.Sprintf("http://localhost:%d", port)
	fmt.Printf("\n%s\n", Colorize("🌐 Starting web server on "+url, ColorBlue))
	fmt.Printf("%s\n", Colorize("⏹️  Press Ctrl+C to stop the server", ColorYellow))
	fmt.Printf("%s\n\n", Colorize("🧭 Open "+url+" in your browser to view the visualization", ColorBlue))

	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", Colorize(fmt.Sprintf("  ❌ Error starting server: %v", err), ColorRed))
	}
}
