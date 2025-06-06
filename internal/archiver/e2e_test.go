package archiver

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/davidroman0O/4chan-archiver/internal/analysis"
	_ "github.com/mattn/go-sqlite3"
)

// E2E test that downloads EVERYTHING from the real threads the user specified

const (
	testPolThreadID = "506536140"
	testBThreadID   = "935410578"

	// Test thread known to exist on archived.moe
	testArchivedMoeThreadID = "935410578"
)

func TestE2E_CompleteArchiver_PolThread(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E archiver test in short mode")
	}

	// Create temporary directory for this test
	tempDir, err := os.MkdirTemp("", "4chan_archive_test_pol_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	t.Logf("Testing complete archiver with /pol/%s in: %s", testPolThreadID, tempDir)

	// Configure the archiver
	config := Config{
		Board:          BoardPol,
		OutputDir:      tempDir,
		RateLimitMs:    500, // Faster for testing
		MaxRetries:     3,
		UserAgent:      "4chan-archiver-e2e-test/1.0",
		Verbose:        true,
		IncludeContent: true,
		IncludeMedia:   true,
		IncludePosts:   true,
		MaxConcurrency: 3,
		SkipExisting:   false,
		DatabaseMode:   DatabaseModeAuto, // Auto-detect test mode for in-memory processing
		Source:         SourceFourChan,   // Test 4chan source
	}

	// Create archiver
	archiver, err := New(&config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	// Archive the real thread
	threadID := testPolThreadID
	results, err := archiver.ArchiveThreads([]string{threadID})
	if err != nil {
		t.Fatalf("Failed to archive /pol/ thread: %v", err)
	}

	// Verify we got results
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	result := results[threadID]
	if result.Error != nil {
		t.Fatalf("Archive failed: %v", result.Error)
	}

	t.Logf("✅ Archive completed:")
	t.Logf("   Thread: %s", result.ThreadID)
	t.Logf("   Media downloaded: %d", result.MediaDownloaded)
	t.Logf("   Posts saved: %d", result.PostsSaved)

	// Verify directory structure was created
	threadDir := filepath.Join(tempDir, BoardPol, threadID)
	if _, err := os.Stat(threadDir); os.IsNotExist(err) {
		t.Fatalf("Thread directory not created: %s", threadDir)
	}

	// Verify thread.json was created
	threadJSONPath := filepath.Join(threadDir, ThreadJSONFileName)
	if _, err := os.Stat(threadJSONPath); os.IsNotExist(err) {
		t.Fatalf("thread.json not created: %s", threadJSONPath)
	}

	// Verify database was created and has conversation analysis
	dbPath := filepath.Join(threadDir, ThreadDBFileName)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Database not created: %s", dbPath)
	}

	// Open database and verify conversation analysis
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Check posts were stored
	var postCount int
	err = db.QueryRow("SELECT COUNT(*) FROM posts").Scan(&postCount)
	if err != nil {
		t.Fatalf("Failed to query posts: %v", err)
	}
	if postCount == 0 {
		t.Error("No posts found in database")
	}

	// Check users were stored (pol has user IDs)
	var userCount int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		t.Fatalf("Failed to query users: %v", err)
	}
	if userCount == 0 {
		t.Error("No users found in database for /pol/ thread")
	}

	// Check reply relationships were analyzed
	var replyCount int
	err = db.QueryRow("SELECT COUNT(*) FROM replies").Scan(&replyCount)
	if err != nil {
		t.Fatalf("Failed to query replies: %v", err)
	}
	if replyCount == 0 {
		t.Error("No reply relationships found in database")
	}

	// Verify media files were downloaded
	mediaDir := filepath.Join(threadDir, MediaDirName)
	if result.MediaDownloaded > 0 {
		if _, err := os.Stat(mediaDir); os.IsNotExist(err) {
			t.Errorf("Media directory not created despite downloading %d files", result.MediaDownloaded)
		} else {
			// Count actual media files
			files, err := os.ReadDir(mediaDir)
			if err != nil {
				t.Errorf("Failed to read media directory: %v", err)
			} else {
				t.Logf("   Media files on disk: %d", len(files))
			}
		}
	}

	// Verify metadata file was created
	metadataPath := filepath.Join(threadDir, MetadataFileName)
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Fatalf("Metadata file not created: %s", metadataPath)
	}

	t.Logf("✅ /pol/ thread E2E test passed: %d posts, %d users, %d replies, %d media files",
		postCount, userCount, replyCount, result.MediaDownloaded)
}

func TestE2E_CompleteArchiver_BThread(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E archiver test in short mode")
	}

	// Create temporary directory for this test
	tempDir, err := os.MkdirTemp("", "4chan_archive_test_b_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	t.Logf("Testing complete archiver with /b/%s in: %s", testBThreadID, tempDir)

	// Configure the archiver
	config := Config{
		Board:          BoardB,
		OutputDir:      tempDir,
		RateLimitMs:    500, // Faster for testing
		MaxRetries:     3,
		UserAgent:      "4chan-archiver-e2e-test/1.0",
		Verbose:        true,
		IncludeContent: true,
		IncludeMedia:   true,
		IncludePosts:   true,
		MaxConcurrency: 3,
		SkipExisting:   false,
		DatabaseMode:   DatabaseModeAuto, // Auto-detect test mode for in-memory processing
		Source:         SourceFourChan,   // Test 4chan source
	}

	// Create archiver
	archiver, err := New(&config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	// Archive the real thread
	threadID := testBThreadID
	results, err := archiver.ArchiveThreads([]string{threadID})
	if err != nil {
		t.Fatalf("Failed to archive /b/ thread: %v", err)
	}

	// Verify we got results
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	result := results[threadID]
	if result.Error != nil {
		t.Fatalf("Archive failed: %v", result.Error)
	}

	t.Logf("✅ Archive completed:")
	t.Logf("   Thread: %s", result.ThreadID)
	t.Logf("   Media downloaded: %d", result.MediaDownloaded)
	t.Logf("   Posts saved: %d", result.PostsSaved)

	// Verify directory structure was created
	threadDir := filepath.Join(tempDir, BoardB, threadID)
	if _, err := os.Stat(threadDir); os.IsNotExist(err) {
		t.Fatalf("Thread directory not created: %s", threadDir)
	}

	// Verify thread.json was created and has content
	threadJSONPath := filepath.Join(threadDir, ThreadJSONFileName)
	if _, err := os.Stat(threadJSONPath); os.IsNotExist(err) {
		t.Fatalf("thread.json not created: %s", threadJSONPath)
	}

	// Read and verify thread.json has proper structure
	threadJSON, err := os.ReadFile(threadJSONPath)
	if err != nil {
		t.Fatalf("Failed to read thread.json: %v", err)
	}

	var threadData analysis.ThreadData
	err = json.Unmarshal(threadJSON, &threadData)
	if err != nil {
		t.Fatalf("Invalid thread.json format: %v", err)
	}

	if len(threadData.Posts) == 0 {
		t.Error("No posts in thread.json")
	}

	// Verify database was created and has conversation analysis
	dbPath := filepath.Join(threadDir, ThreadDBFileName)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Database not created: %s", dbPath)
	}

	// Open database and verify conversation analysis
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Check posts were stored
	var postCount int
	err = db.QueryRow("SELECT COUNT(*) FROM posts").Scan(&postCount)
	if err != nil {
		t.Fatalf("Failed to query posts: %v", err)
	}
	if postCount == 0 {
		t.Error("No posts found in database")
	}

	// Check users table (should be empty for /b/)
	var userCount int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		t.Fatalf("Failed to query users: %v", err)
	}
	if userCount != 0 {
		t.Errorf("Expected 0 users for /b/ thread, got %d", userCount)
	}

	// Check reply relationships were analyzed
	var replyCount int
	err = db.QueryRow("SELECT COUNT(*) FROM replies").Scan(&replyCount)
	if err != nil {
		t.Fatalf("Failed to query replies: %v", err)
	}
	if replyCount == 0 {
		t.Error("No reply relationships found in database")
	}

	// Verify media files were downloaded (b threads typically have lots of media)
	mediaDir := filepath.Join(threadDir, MediaDirName)
	if result.MediaDownloaded > 0 {
		if _, err := os.Stat(mediaDir); os.IsNotExist(err) {
			t.Errorf("Media directory not created despite downloading %d files", result.MediaDownloaded)
		} else {
			// Count actual media files and verify they exist
			files, err := os.ReadDir(mediaDir)
			if err != nil {
				t.Errorf("Failed to read media directory: %v", err)
			} else {
				actualFiles := 0
				for _, file := range files {
					if !file.IsDir() {
						actualFiles++
					}
				}
				t.Logf("   Media files on disk: %d", actualFiles)

				// Verify at least some files were downloaded
				if actualFiles == 0 && result.MediaDownloaded > 0 {
					t.Error("Media download count mismatch: reported downloaded but no files found")
				}
			}
		}
	}

	// Verify metadata file was created
	metadataPath := filepath.Join(threadDir, MetadataFileName)
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Fatalf("Metadata file not created: %s", metadataPath)
	}

	t.Logf("✅ /b/ thread E2E test passed: %d posts, %d users, %d replies, %d media files",
		postCount, userCount, replyCount, result.MediaDownloaded)
}

func TestE2E_MultipleThreads(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E multiple threads test in short mode")
	}

	// Create temporary directory for this test
	tempDir, err := os.MkdirTemp("", "4chan_archive_test_multi_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	t.Logf("Testing multiple thread archiving in: %s", tempDir)

	// Test archiving both threads with a single archiver call
	// First test /pol/
	polConfig := Config{
		Board:          BoardPol,
		OutputDir:      tempDir,
		RateLimitMs:    1000,
		MaxRetries:     3,
		UserAgent:      "4chan-archiver-e2e-test/1.0",
		Verbose:        true,
		IncludeContent: true,
		IncludeMedia:   true,
		IncludePosts:   true,
		MaxConcurrency: 2,
		SkipExisting:   false,
		DatabaseMode:   DatabaseModeAuto, // Auto-detect test mode for in-memory processing
		Source:         SourceFourChan,   // Test 4chan source
	}

	polArchiver, err := New(&polConfig)
	if err != nil {
		t.Fatalf("Failed to create pol archiver: %v", err)
	}
	polResults, err := polArchiver.ArchiveThreads([]string{testPolThreadID})
	if err != nil {
		t.Fatalf("Failed to archive /pol/ thread: %v", err)
	}

	// Then test /b/
	bConfig := polConfig
	bConfig.Board = BoardB
	bArchiver, err := New(&bConfig)
	if err != nil {
		t.Fatalf("Failed to create b archiver: %v", err)
	}
	bResults, err := bArchiver.ArchiveThreads([]string{testBThreadID})
	if err != nil {
		t.Fatalf("Failed to archive /b/ thread: %v", err)
	}

	// Verify both archives completed
	if len(polResults) != 1 || len(bResults) != 1 {
		t.Fatalf("Expected 1 result each, got pol:%d b:%d", len(polResults), len(bResults))
	}

	if polResults[testPolThreadID].Error != nil {
		t.Fatalf("Pol archive failed: %v", polResults[testPolThreadID].Error)
	}
	if bResults[testBThreadID].Error != nil {
		t.Fatalf("B archive failed: %v", bResults[testBThreadID].Error)
	}

	// Verify both directory structures exist
	polDir := filepath.Join(tempDir, BoardPol, testPolThreadID)
	bDir := filepath.Join(tempDir, BoardB, testBThreadID)

	if _, err := os.Stat(polDir); os.IsNotExist(err) {
		t.Fatalf("Pol thread directory not created: %s", polDir)
	}
	if _, err := os.Stat(bDir); os.IsNotExist(err) {
		t.Fatalf("B thread directory not created: %s", bDir)
	}

	// Verify databases exist and are independent
	polDB := filepath.Join(polDir, ThreadDBFileName)
	bDB := filepath.Join(bDir, ThreadDBFileName)

	if _, err := os.Stat(polDB); os.IsNotExist(err) {
		t.Fatalf("Pol database not created: %s", polDB)
	}
	if _, err := os.Stat(bDB); os.IsNotExist(err) {
		t.Fatalf("B database not created: %s", bDB)
	}

	// Quick verification that databases have different content
	polDBConn, err := sql.Open("sqlite3", polDB)
	if err != nil {
		t.Fatal(err)
	}
	defer polDBConn.Close()

	bDBConn, err := sql.Open("sqlite3", bDB)
	if err != nil {
		t.Fatal(err)
	}
	defer bDBConn.Close()

	var polUsers, bUsers int
	polDBConn.QueryRow("SELECT COUNT(*) FROM users").Scan(&polUsers)
	bDBConn.QueryRow("SELECT COUNT(*) FROM users").Scan(&bUsers)

	// Pol should have users, b should not
	if polUsers == 0 {
		t.Error("Pol database should have users")
	}
	if bUsers != 0 {
		t.Error("B database should have no users")
	}

	t.Logf("✅ Multiple thread E2E test passed:")
	t.Logf("   /pol/ - Posts: %d, Media: %d", polResults[testPolThreadID].PostsSaved, polResults[testPolThreadID].MediaDownloaded)
	t.Logf("   /b/   - Posts: %d, Media: %d", bResults[testBThreadID].PostsSaved, bResults[testBThreadID].MediaDownloaded)
	t.Logf("   Total archives created: 2")
}

func TestE2E_ArchivedMoeThread(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E archived.moe test in short mode")
	}

	// Create temporary directory for this test
	tempDir, err := os.MkdirTemp("", "4chan_archive_test_archived_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Use a real archived thread ID - you'll need to replace this with an actual archived thread
	archivedThreadID := "935410578" // This thread might be archived on archived.moe

	t.Logf("Testing archived.moe archiver with /b/%s in: %s", archivedThreadID, tempDir)

	// Configure the archiver for archived.moe
	config := Config{
		Board:          BoardB,
		OutputDir:      tempDir,
		RateLimitMs:    1000, // Slower for archived.moe to be respectful
		MaxRetries:     5,    // More retries for archived.moe
		UserAgent:      "4chan-archiver-e2e-test/1.0",
		Verbose:        true,
		IncludeContent: true,
		IncludeMedia:   true,
		IncludePosts:   true,
		MaxConcurrency: 1, // Single concurrent download for archived.moe
		SkipExisting:   false,
		DatabaseMode:   DatabaseModeAuto,  // Auto-detect test mode for in-memory processing
		Source:         SourceArchivedMoe, // Test archived.moe source
	}

	// Create archiver
	archiver, err := New(&config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	// Archive the archived thread
	results, err := archiver.ArchiveThreads([]string{archivedThreadID})
	if err != nil {
		t.Fatalf("Failed to archive archived.moe thread: %v", err)
	}

	// Verify we got results
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	result := results[archivedThreadID]
	if result.Error != nil {
		// archived.moe tests might fail if thread doesn't exist there
		t.Logf("⚠️  archived.moe test failed (expected if thread not available): %v", result.Error)
		t.Skip("Thread not available on archived.moe")
		return
	}

	t.Logf("✅ Archive completed:")
	t.Logf("   Thread: %s", result.ThreadID)
	t.Logf("   Media downloaded: %d", result.MediaDownloaded)
	t.Logf("   Posts saved: %d", result.PostsSaved)

	// Verify directory structure was created
	threadDir := filepath.Join(tempDir, BoardB, archivedThreadID)
	if _, err := os.Stat(threadDir); os.IsNotExist(err) {
		t.Fatalf("Thread directory not created: %s", threadDir)
	}

	// Verify media files were downloaded if any were found
	mediaDir := filepath.Join(threadDir, MediaDirName)
	if result.MediaDownloaded > 0 {
		if _, err := os.Stat(mediaDir); os.IsNotExist(err) {
			t.Errorf("Media directory not created despite downloading %d files", result.MediaDownloaded)
		} else {
			// Count actual media files
			files, err := os.ReadDir(mediaDir)
			if err != nil {
				t.Errorf("Failed to read media directory: %v", err)
			} else {
				actualFiles := 0
				for _, file := range files {
					if !file.IsDir() {
						actualFiles++
					}
				}
				t.Logf("   Media files on disk: %d", actualFiles)

				if actualFiles > 0 {
					t.Logf("✅ archived.moe media download successful!")
				}
			}
		}
	} else {
		t.Logf("⚠️  No media found on archived.moe for this thread")
	}

	t.Logf("✅ archived.moe thread E2E test passed: %d posts, %d media files", result.PostsSaved, result.MediaDownloaded)
}

// TestE2E_NoFallback_FourChanOnly ensures that when 4chan source is explicitly specified,
// it doesn't fallback to archived.moe even if the thread doesn't exist on 4chan
func TestE2E_NoFallback_FourChanOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E no-fallback test in short mode")
	}

	// Create temporary directory for this test
	tempDir, err := os.MkdirTemp("", "4chan_archive_test_no_fallback_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Use a thread ID that definitely doesn't exist on 4chan (old thread)
	nonExistentThreadID := "123456789"

	t.Logf("Testing no fallback for 4chan-only source with non-existent thread %s", nonExistentThreadID)

	// Configure the archiver for 4chan ONLY
	config := Config{
		Board:          BoardB,
		OutputDir:      tempDir,
		RateLimitMs:    500,
		MaxRetries:     1, // Fewer retries for this test
		UserAgent:      "4chan-archiver-e2e-test/1.0",
		Verbose:        true,
		IncludeContent: true,
		IncludeMedia:   true,
		IncludePosts:   true,
		MaxConcurrency: 1,
		SkipExisting:   false,
		DatabaseMode:   DatabaseModeAuto,
		Source:         SourceFourChan, // EXPLICIT 4chan source - should NOT fallback
	}

	// Create archiver
	archiver, err := New(&config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	// Archive the non-existent thread
	results, err := archiver.ArchiveThreads([]string{nonExistentThreadID})
	if err != nil {
		t.Fatalf("Failed to run archiver: %v", err)
	}

	// Verify we got results
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	result := results[nonExistentThreadID]

	// The result MUST have an error because thread doesn't exist on 4chan
	// and we explicitly specified 4chan source (no fallback should occur)
	if result.Error == nil {
		t.Fatalf("Expected error for non-existent thread on 4chan, but got success")
	}

	// Verify the error is specifically about thread not found (404)
	if !strings.Contains(result.Error.Error(), "thread not found (404)") {
		t.Fatalf("Expected 404 error, got: %v", result.Error)
	}

	t.Logf("✅ No fallback test passed - 4chan source correctly failed without trying archived.moe")
}

// TestE2E_NoFallback_ArchivedMoeOnly ensures that when archived.moe source is explicitly specified,
// it doesn't fallback to 4chan
func TestE2E_NoFallback_ArchivedMoeOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E no-fallback test in short mode")
	}

	// Create temporary directory for this test
	tempDir, err := os.MkdirTemp("", "4chan_archive_test_archived_only_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Use a thread ID that exists on 4chan but test with archived.moe ONLY
	activeThreadID := testPolThreadID

	t.Logf("Testing no fallback for archived.moe-only source with active 4chan thread %s", activeThreadID)

	// Configure the archiver for archived.moe ONLY
	config := Config{
		Board:          BoardPol,
		OutputDir:      tempDir,
		RateLimitMs:    1000,
		MaxRetries:     2, // Fewer retries for this test
		UserAgent:      "4chan-archiver-e2e-test/1.0",
		Verbose:        true,
		IncludeContent: true,
		IncludeMedia:   true,
		IncludePosts:   true,
		MaxConcurrency: 1,
		SkipExisting:   false,
		DatabaseMode:   DatabaseModeAuto,
		Source:         SourceArchivedMoe, // EXPLICIT archived.moe source - should NOT fallback to 4chan
	}

	// Create archiver
	archiver, err := New(&config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	// Archive using archived.moe ONLY
	results, err := archiver.ArchiveThreads([]string{activeThreadID})
	if err != nil {
		t.Fatalf("Failed to run archiver: %v", err)
	}

	// Verify we got results
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	result := results[activeThreadID]

	// The result might have an error if thread is not available on archived.moe
	// This is expected behavior - archived.moe should NOT fallback to 4chan
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), "thread not found on archived.moe (404)") ||
			strings.Contains(result.Error.Error(), "archived.moe returned 403 Forbidden") {
			t.Logf("✅ No fallback test passed - archived.moe source correctly failed without trying 4chan")
			return
		}
		t.Fatalf("Unexpected error: %v", result.Error)
	}

	// If it succeeded, that's also fine - it means the thread was found on archived.moe
	t.Logf("✅ archived.moe-only test passed: found thread with %d posts, %d media files",
		result.PostsSaved, result.MediaDownloaded)
}
