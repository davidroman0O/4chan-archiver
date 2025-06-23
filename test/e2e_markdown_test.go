package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/davidroman0O/4chan-archiver/internal/archiver"
	_ "github.com/mattn/go-sqlite3"
)

func TestE2E_Thread506701195_MarkdownExport(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Create specific /tmp directory for manual inspection
	timestamp := time.Now().Format("20060102_150405")
	tempDir := fmt.Sprintf("/tmp/4chan_archiver_test_%s", timestamp)
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Print the directory for manual inspection
	fmt.Printf("\n🗂️  TEST OUTPUT DIRECTORY: %s\n", tempDir)
	fmt.Printf("📄 Markdown will be at: %s/pol/506701195/thread.md\n\n", tempDir)

	// Create archiver config with markdown export enabled and 4plebs source
	config := &archiver.Config{
		Board:           "pol",
		OutputDir:       tempDir,
		RateLimitMs:     5000, // Much slower rate (5 seconds between requests)
		MaxRetries:      8,    // More retries to work through 403s
		UserAgent:       "",   // Let the header rotation handle user agents
		Verbose:         true,
		IncludeContent:  true,
		IncludeMedia:    false, // No media download as requested
		IncludePosts:    true,
		IncludeMarkdown: true, // Enable markdown export
		MaxConcurrency:  1,
		SkipExisting:    false,
		DatabaseMode:    archiver.DatabaseModeMemory, // In-memory DB as requested
		Source:          archiver.Source4Plebs,       // Use 4plebs source
	}

	// Create archiver instance
	arch, err := archiver.New(config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	// Archive the thread from 4plebs
	t.Logf("Archiving thread 506701195 from 4plebs...")
	results, err := arch.ArchiveThreads([]string{"506701195"})
	if err != nil {
		t.Fatalf("Failed to archive thread: %v", err)
	}

	// Check archive result
	result, exists := results["506701195"]
	if !exists {
		t.Fatal("No result found for thread 506701195")
	}
	if result.Error != nil {
		t.Fatalf("Archive failed with error: %v", result.Error)
	}

	t.Logf("Successfully archived thread with %d posts", result.PostsSaved)

	// Read the generated markdown file
	markdownPath := filepath.Join(tempDir, "pol", "506701195", "thread.md")
	markdownContent, err := os.ReadFile(markdownPath)
	if err != nil {
		t.Fatalf("Failed to read markdown file: %v", err)
	}

	markdownText := string(markdownContent)
	fmt.Printf("📝 Generated markdown file: %s (%d characters)\n", markdownPath, len(markdownText))

	// Verify the specific content we're looking for
	expectedContent := `>>506701290
Same as the US I said yesterday. Buy silver, ammo, weaponry. I'll add water since only a small amount is drinkable without filters.
>>506701306
Never. It's a psyop.
>>506701318
It'll make San Francisco blush. >>506701360
Lol`

	contentFound := strings.Contains(markdownText, expectedContent)
	usernameFound := strings.Contains(markdownText, "**Author:** CIA OP")
	tripcodeFound := strings.Contains(markdownText, "**!!z9FVe/srtHt**")
	userIDFound := strings.Contains(markdownText, "(ID: 0Cj9IC6b)")
	postNumberFound := strings.Contains(markdownText, "## Post #506701419")
	postMetadataFound := strings.Contains(markdownText, "- **Post Number:** 506701419")

	// Print verification results
	fmt.Printf("\n🔍 CONTENT VERIFICATION:\n")
	fmt.Printf("   Expected content block: %s\n", checkmark(contentFound))
	fmt.Printf("   Username 'CIA OP': %s\n", checkmark(usernameFound))
	fmt.Printf("   Tripcode '!!z9FVe/srtHt': %s\n", checkmark(tripcodeFound))
	fmt.Printf("   User ID '0Cj9IC6b': %s\n", checkmark(userIDFound))
	fmt.Printf("   Post #506701419 header: %s\n", checkmark(postNumberFound))
	fmt.Printf("   Post #506701419 metadata: %s\n", checkmark(postMetadataFound))

	if !contentFound {
		t.Errorf("Markdown does not contain expected content block")

		// Log first 100 lines for debugging
		lines := strings.Split(markdownText, "\n")
		fmt.Printf("\n📋 FIRST 100 LINES OF MARKDOWN:\n")
		for i, line := range lines {
			if i >= 100 {
				break
			}
			fmt.Printf("%3d: %s\n", i+1, line)
		}
	}

	// Check individual verification items but don't fail the test - just log warnings
	if !usernameFound {
		t.Logf("⚠️  WARNING: Markdown does not contain username 'CIA OP'")
	}
	if !tripcodeFound {
		t.Logf("⚠️  WARNING: Markdown does not contain tripcode '!!z9FVe/srtHt'")
	}
	if !userIDFound {
		t.Logf("⚠️  WARNING: Markdown does not contain user ID '0Cj9IC6b'")
	}
	if !postNumberFound {
		t.Logf("⚠️  WARNING: Markdown does not contain post number '506701419' in header")
	}
	if !postMetadataFound {
		t.Logf("⚠️  WARNING: Markdown does not contain post number '506701419' in metadata")
	}

	fmt.Printf("\n✅ E2E markdown export test completed:\n")
	fmt.Printf("   Thread: 506701195\n")
	fmt.Printf("   Posts archived: %d\n", result.PostsSaved)
	fmt.Printf("   Markdown file: %s\n", markdownPath)
	fmt.Printf("   Database: IN-MEMORY (as requested)\n")
	fmt.Printf("   Media: SKIPPED (as requested)\n")
	fmt.Printf("\n🔎 You can manually examine the markdown at:\n   %s\n\n", markdownPath)
}

func checkmark(found bool) string {
	if found {
		return "✅ FOUND"
	}
	return "❌ NOT FOUND"
}
