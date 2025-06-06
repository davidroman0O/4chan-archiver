package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/davidroman0O/4chan-archiver/internal/metadata"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// statusCmd represents the status command
var statusCmd = &cobra.Command{
	Use:   "status [thread-id1] [thread-id2] ...",
	Short: "Check archiving status of threads",
	Long: `Check the archiving status and metadata of one or more threads.

Examples:
  # Check status of a specific thread
  4chan-archiver status --board pol 123456789

  # Check status of multiple threads
  4chan-archiver status --board b 123456789 987654321

  # List all archived threads in a board
  4chan-archiver status --board pol --list`,
	RunE: runStatus,
}

var listAll bool

func init() {
	rootCmd.AddCommand(statusCmd)

	statusCmd.Flags().StringVarP(&board, "board", "b", "pol", "board name (pol, b, etc.)")
	statusCmd.Flags().BoolVar(&listAll, "list", false, "list all archived threads")

	viper.BindPFlag("board", statusCmd.Flags().Lookup("board"))
}

func runStatus(cmd *cobra.Command, args []string) error {
	outputPath := viper.GetString("output")
	if outputPath == "" {
		home, _ := os.UserHomeDir()
		outputPath = filepath.Join(home, "Documents", "4chan-archive")
	}

	boardName := viper.GetString("board")
	metadataManager := metadata.NewManager(outputPath)

	if listAll {
		return listArchivedThreads(metadataManager, boardName, outputPath)
	}

	if len(args) == 0 {
		return fmt.Errorf("provide thread IDs to check status, or use --list to see all archived threads")
	}

	for _, threadID := range args {
		if err := showThreadStatus(metadataManager, boardName, threadID); err != nil {
			fmt.Printf("Error checking thread %s: %v\n", threadID, err)
		}
	}

	return nil
}

func showThreadStatus(metadataManager *metadata.Manager, board, threadID string) error {
	meta, err := metadataManager.LoadMetadata(board, threadID)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	fmt.Printf("\n=== Thread %s (/%s/) ===\n", threadID, board)
	fmt.Printf("Status: %s\n", meta.Status)
	fmt.Printf("Created: %s\n", meta.CreatedAt.Format(time.RFC3339))
	fmt.Printf("Last Updated: %s\n", meta.LastUpdated.Format(time.RFC3339))
	fmt.Printf("Posts Saved: %d\n", meta.PostsCount)
	fmt.Printf("Media Downloaded: %d\n", meta.MediaCount)

	if meta.LastError != "" {
		fmt.Printf("Last Error: %s\n", meta.LastError)
	}

	// Show some recent downloads
	if len(meta.DownloadedMedia) > 0 {
		fmt.Printf("\nRecent Media Downloads:\n")
		count := 0
		for _, mediaInfo := range meta.DownloadedMedia {
			if count >= 5 {
				fmt.Printf("  ... and %d more\n", len(meta.DownloadedMedia)-5)
				break
			}
			fmt.Printf("  %s (%d bytes) - %s\n",
				mediaInfo.Filename,
				mediaInfo.Size,
				mediaInfo.DownloadedAt.Format("2006-01-02 15:04:05"))
			count++
		}
	}

	return nil
}

func listArchivedThreads(metadataManager *metadata.Manager, board, outputPath string) error {
	boardDir := filepath.Join(outputPath, board)

	if _, err := os.Stat(boardDir); os.IsNotExist(err) {
		fmt.Printf("No archived threads found for board /%s/\n", board)
		return nil
	}

	entries, err := os.ReadDir(boardDir)
	if err != nil {
		return fmt.Errorf("failed to read board directory: %w", err)
	}

	fmt.Printf("Archived threads for /%s/:\n\n", board)

	threadCount := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		threadID := entry.Name()

		// Check if it looks like a thread ID (numeric)
		if !isNumeric(threadID) {
			continue
		}

		meta, err := metadataManager.LoadMetadata(board, threadID)
		if err != nil {
			fmt.Printf("%-12s - ERROR: %v\n", threadID, err)
			continue
		}

		status := meta.Status
		if status == "completed" {
			status = "✓ " + status
		} else if status == "failed" {
			status = "✗ " + status
		} else {
			status = "⧗ " + status
		}

		fmt.Printf("%-12s - %s (Posts: %d, Media: %d, Updated: %s)\n",
			threadID,
			status,
			meta.PostsCount,
			meta.MediaCount,
			meta.LastUpdated.Format("2006-01-02 15:04"))

		threadCount++
	}

	if threadCount == 0 {
		fmt.Printf("No archived threads found for board /%s/\n", board)
	} else {
		fmt.Printf("\nTotal: %d archived threads\n", threadCount)
	}

	return nil
}

func isNumeric(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
