package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/davidroman0O/4chan-archiver/internal/archiver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	monitorInterval  time.Duration
	stopOnArchive    bool
	stopOnInactivity time.Duration
	maxDuration      time.Duration

	// Board and content flags to match archive command
	monitorBoard          string
	monitorIncludeContent bool
	monitorIncludeMedia   bool
	monitorIncludePosts   bool
	monitorMaxConcurrency int
	monitorSkipExisting   bool
	monitorDatabaseMode   string
	monitorSource         string
)

var monitorCmd = &cobra.Command{
	Use:   "monitor <thread-id> [thread-id...]",
	Short: "Monitor and continuously archive one or more active 4chan threads",
	Long: `Monitor one or more 4chan threads and continuously archive new posts and media as they are added.
This is useful for active threads where you want to capture all content in real-time.

The monitor will:
- Periodically check the threads for new posts/media
- Download only new content since the last check
- Stop when threads get archived, 404'd, or reach inactivity timeout
- Handle rate limiting and interruptions gracefully

Examples:
  4archive monitor 123456789                                          # Monitor one thread
  4archive monitor 123456789 987654321                              # Monitor multiple threads
  4archive monitor --board b 123456789 --interval 2m                # Custom board and interval
  4archive monitor 123456789 --stop-on-inactivity 30m --output ~/Pictures/.chans
  4archive monitor --board pol 123456789 987654321 --max-duration 6h --verbose`,
	Args: cobra.MinimumNArgs(1),
	RunE: runMonitor,
}

func init() {
	rootCmd.AddCommand(monitorCmd)

	// Board and content flags (same as archive command for consistency)
	monitorCmd.Flags().StringVarP(&monitorBoard, "board", "b", archiver.DefaultBoard, "board name (pol, b, etc.)")
	monitorCmd.Flags().BoolVar(&monitorIncludeContent, "content", true, "include thread content/posts")
	monitorCmd.Flags().BoolVar(&monitorIncludeMedia, "media", true, "include media files (images, videos)")
	monitorCmd.Flags().BoolVar(&monitorIncludePosts, "posts", true, "include post text and metadata")
	monitorCmd.Flags().IntVarP(&monitorMaxConcurrency, "concurrency", "c", 3, "maximum concurrent downloads")
	monitorCmd.Flags().BoolVar(&monitorSkipExisting, "skip-existing", true, "skip files that already exist")
	monitorCmd.Flags().StringVar(&monitorDatabaseMode, "database-mode", archiver.DefaultDatabaseMode,
		"database mode: '"+archiver.DatabaseModeMemory+"', '"+archiver.DatabaseModeFile+"', or '"+archiver.DatabaseModeAuto+"'")
	monitorCmd.Flags().StringVar(&monitorSource, "source", archiver.DefaultSource,
		"source: '"+archiver.SourceFourChan+"', '"+archiver.SourceArchivedMoe+"', or '"+archiver.SourceAuto+"'")

	// Monitoring-specific flags (using same pattern as archive command)
	monitorCmd.Flags().DurationVar(&monitorInterval, "interval", 5*time.Minute,
		"How often to check for updates (e.g., 30s, 2m, 5m)")
	monitorCmd.Flags().DurationVar(&maxDuration, "max-duration", 0,
		"Maximum time to monitor (0 = unlimited, e.g., 1h, 6h, 24h)")
	monitorCmd.Flags().BoolVar(&stopOnArchive, "stop-on-archive", true,
		"Stop monitoring when thread gets archived")
	monitorCmd.Flags().DurationVar(&stopOnInactivity, "stop-on-inactivity", 1*time.Hour,
		"Stop monitoring after no new posts for this duration (0 = never)")

	// Bind flags to viper (same structure as archive command)
	viper.BindPFlag("monitor.board", monitorCmd.Flags().Lookup("board"))
	viper.BindPFlag("monitor.content", monitorCmd.Flags().Lookup("content"))
	viper.BindPFlag("monitor.media", monitorCmd.Flags().Lookup("media"))
	viper.BindPFlag("monitor.posts", monitorCmd.Flags().Lookup("posts"))
	viper.BindPFlag("monitor.concurrency", monitorCmd.Flags().Lookup("concurrency"))
	viper.BindPFlag("monitor.skip_existing", monitorCmd.Flags().Lookup("skip-existing"))
	viper.BindPFlag("monitor.database_mode", monitorCmd.Flags().Lookup("database-mode"))
	viper.BindPFlag("monitor.source", monitorCmd.Flags().Lookup("source"))
	viper.BindPFlag("monitor.interval", monitorCmd.Flags().Lookup("interval"))
	viper.BindPFlag("monitor.max_duration", monitorCmd.Flags().Lookup("max-duration"))
	viper.BindPFlag("monitor.stop_on_archive", monitorCmd.Flags().Lookup("stop-on-archive"))
	viper.BindPFlag("monitor.stop_on_inactivity", monitorCmd.Flags().Lookup("stop-on-inactivity"))
}

func runMonitor(cmd *cobra.Command, args []string) error {
	threadIDs := args

	// Validate monitoring interval
	if monitorInterval < 30*time.Second {
		return fmt.Errorf("monitoring interval must be at least 30 seconds to avoid rate limiting")
	}

	// Create archiver config (same as archive command)
	config := &archiver.Config{
		Board:          viper.GetString("monitor.board"),
		OutputDir:      viper.GetString("output"),
		RateLimitMs:    viper.GetInt("rate-limit"),
		MaxRetries:     viper.GetInt("max-retries"),
		UserAgent:      viper.GetString("user-agent"),
		Verbose:        viper.GetBool("verbose"),
		IncludeContent: viper.GetBool("monitor.content"),
		IncludeMedia:   viper.GetBool("monitor.media"),
		IncludePosts:   viper.GetBool("monitor.posts"),
		MaxConcurrency: viper.GetInt("monitor.concurrency"),
		SkipExisting:   viper.GetBool("monitor.skip_existing"),
		DatabaseMode:   viper.GetString("monitor.database_mode"),
		Source:         viper.GetString("monitor.source"),
	}

	// Force source to 4chan for monitoring (can't monitor archived threads)
	if config.Source != archiver.SourceFourChan {
		fmt.Printf("Warning: Forcing source to 4chan for monitoring (was: %s)\n", config.Source)
		config.Source = archiver.SourceFourChan
	}

	// Create archiver
	arc, err := archiver.New(config)
	if err != nil {
		return fmt.Errorf("failed to create archiver: %w", err)
	}

	// Create monitoring configuration
	monitorConfig := &archiver.MonitorConfig{
		ThreadIDs:        threadIDs,
		Interval:         monitorInterval,
		MaxDuration:      maxDuration,
		StopOnArchive:    stopOnArchive,
		StopOnInactivity: stopOnInactivity,
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Display monitoring info
	if len(threadIDs) == 1 {
		fmt.Printf("🔍 Starting to monitor thread %s on /%s/\n", threadIDs[0], config.Board)
	} else {
		fmt.Printf("🔍 Starting to monitor %d threads on /%s/\n", len(threadIDs), config.Board)
		fmt.Printf("📋 Thread IDs: %v\n", threadIDs)
	}

	fmt.Printf("📋 Check interval: %v\n", monitorInterval)
	if maxDuration > 0 {
		fmt.Printf("⏰ Max duration: %v\n", maxDuration)
	}
	if stopOnInactivity > 0 {
		fmt.Printf("💤 Stop on inactivity: %v\n", stopOnInactivity)
	}
	fmt.Printf("🛑 Stop on archive: %v\n", stopOnArchive)
	fmt.Printf("📁 Output directory: %s\n", config.OutputDir)
	fmt.Printf("\nPress Ctrl+C to stop monitoring gracefully...\n\n")

	// Start monitoring in a goroutine
	resultChan := make(chan error, 1)
	go func() {
		err := arc.MonitorThreads(monitorConfig)
		resultChan <- err
	}()

	// Wait for either completion or signal
	select {
	case err := <-resultChan:
		if err != nil {
			return fmt.Errorf("monitoring failed: %w", err)
		}
		fmt.Printf("\n✅ Monitoring completed successfully\n")
		return nil
	case sig := <-sigChan:
		fmt.Printf("\n⚠️  Received signal %v, stopping monitor gracefully...\n", sig)
		arc.StopMonitoring()

		// Wait for graceful shutdown with timeout
		select {
		case err := <-resultChan:
			if err != nil {
				fmt.Printf("Monitoring stopped with error: %v\n", err)
			} else {
				fmt.Printf("✅ Monitoring stopped gracefully\n")
			}
		case <-time.After(30 * time.Second):
			fmt.Printf("⚠️  Graceful shutdown timed out, forcing exit\n")
		}
		return nil
	}
}
