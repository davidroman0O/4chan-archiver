package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/davidroman0O/4chan-archiver/internal/archiver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	board          string
	includeContent bool
	includeMedia   bool
	includePosts   bool
	maxConcurrency int
	skipExisting   bool
	databaseMode   string
	source         string
)

// archiveCmd represents the archive command
var archiveCmd = &cobra.Command{
	Use:   "archive [thread-id1] [thread-id2] ...",
	Short: "Archive one or multiple 4chan threads",
	Long: `Archive 4chan threads with controlled downloading and metadata tracking.

Examples:
  # Archive a single thread from /pol/
  4archive archive --board pol 123456789

  # Archive multiple threads
  4archive archive --board b 123456789 987654321 555666777

  # Archive with specific content types
  4archive archive --board pol --media --posts 123456789

  # Archive to specific directory
  4archive archive --board pol --output /path/to/archive 123456789`,
	Args: cobra.MinimumNArgs(1),
	RunE: runArchive,
}

func init() {
	rootCmd.AddCommand(archiveCmd)

	// Archive-specific flags
	archiveCmd.Flags().StringVarP(&board, "board", "b", archiver.DefaultBoard, "board name (pol, b, etc.)")
	archiveCmd.Flags().BoolVar(&includeContent, "content", true, "include thread content/posts")
	archiveCmd.Flags().BoolVar(&includeMedia, "media", true, "include media files (images, videos)")
	archiveCmd.Flags().BoolVar(&includePosts, "posts", true, "include post text and metadata")
	archiveCmd.Flags().IntVarP(&maxConcurrency, "concurrency", "c", 3, "maximum concurrent downloads")
	archiveCmd.Flags().BoolVar(&skipExisting, "skip-existing", true, "skip files that already exist")
	archiveCmd.Flags().StringVar(&databaseMode, "database-mode", archiver.DefaultDatabaseMode,
		"database mode: '"+archiver.DatabaseModeMemory+"', '"+archiver.DatabaseModeFile+"', or '"+archiver.DatabaseModeAuto+"'")
	archiveCmd.Flags().StringVar(&source, "source", archiver.DefaultSource,
		"source: '"+archiver.SourceFourChan+"', '"+archiver.SourceArchivedMoe+"', or '"+archiver.SourceAuto+"'")

	// Bind flags to viper
	viper.BindPFlag("board", archiveCmd.Flags().Lookup("board"))
	viper.BindPFlag("content", archiveCmd.Flags().Lookup("content"))
	viper.BindPFlag("media", archiveCmd.Flags().Lookup("media"))
	viper.BindPFlag("posts", archiveCmd.Flags().Lookup("posts"))
	viper.BindPFlag("concurrency", archiveCmd.Flags().Lookup("concurrency"))
	viper.BindPFlag("skip-existing", archiveCmd.Flags().Lookup("skip-existing"))
	viper.BindPFlag("database-mode", archiveCmd.Flags().Lookup("database-mode"))
	viper.BindPFlag("source", archiveCmd.Flags().Lookup("source"))
}

func runArchive(cmd *cobra.Command, args []string) error {
	threadIDs := args

	// Validate thread IDs
	for _, threadID := range threadIDs {
		if strings.TrimSpace(threadID) == "" {
			return fmt.Errorf("thread ID cannot be empty")
		}
	}

	// Get configuration values
	outputPath := viper.GetString("output")
	if outputPath == "" {
		home, _ := cmd.Flags().GetString("output")
		if home == "" {
			home, _ = cmd.Root().PersistentFlags().GetString("output")
		}
		outputPath = home
	}

	// Create archiver configuration
	config := &archiver.Config{
		Board:          viper.GetString("board"),
		OutputDir:      outputPath,
		RateLimitMs:    viper.GetInt("rate-limit"),
		MaxRetries:     viper.GetInt("max-retries"),
		UserAgent:      viper.GetString("user-agent"),
		Verbose:        viper.GetBool("verbose"),
		IncludeContent: viper.GetBool("content"),
		IncludeMedia:   viper.GetBool("media"),
		IncludePosts:   viper.GetBool("posts"),
		MaxConcurrency: viper.GetInt("concurrency"),
		SkipExisting:   viper.GetBool("skip-existing"),
		DatabaseMode:   viper.GetString("database-mode"),
		Source:         viper.GetString("source"),
	}

	// Create archiver instance
	arch, err := archiver.New(config)
	if err != nil {
		return fmt.Errorf("failed to create archiver: %w", err)
	}

	// Archive threads
	if config.Verbose {
		fmt.Printf("Archiving %d threads from /%s/ to %s\n",
			len(threadIDs), config.Board, config.OutputDir)
	}

	results, err := arch.ArchiveThreads(threadIDs)
	if err != nil {
		return fmt.Errorf("archiving failed: %w", err)
	}

	// Report results
	fmt.Printf("\nArchiving complete!\n")
	for threadID, result := range results {
		fmt.Printf("Thread %s: ", threadID)
		if result.Error != nil {
			fmt.Printf("FAILED - %v\n", result.Error)
			continue
		}

		fmt.Printf("SUCCESS - ")
		fmt.Printf("Downloaded %d media files, ", result.MediaDownloaded)
		fmt.Printf("Saved %d posts, ", result.PostsSaved)
		fmt.Printf("Output: %s\n", filepath.Join(config.OutputDir, config.Board, threadID))
	}

	return nil
}
