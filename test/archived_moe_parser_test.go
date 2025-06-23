package test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/davidroman0O/4chan-archiver/internal/archiver"
	"github.com/davidroman0O/4chan-archiver/internal/database"
	_ "github.com/mattn/go-sqlite3"
)

// TestArchivedMoeParser tests the archived.moe parser against local HTML data
func TestArchivedMoeParser(t *testing.T) {
	testCases := []struct {
		name           string
		htmlFile       string
		threadID       string
		expectedPosts  int
		expectedOP     string
		expectedAuthor string
		expectedID     string
		expectedFlag   string
		checkContent   string
		checkPostNo    int64
	}{
		{
			name:           "archived.moe thread 713439368",
			htmlFile:       "713439368.html",
			threadID:       "713439368",
			expectedPosts:  20, // Adjust based on actual content
			expectedOP:     "Sephiroth did kill Aerith",
			expectedAuthor: "Anonymous",
			expectedID:     "", // Check what user IDs archived.moe has
			expectedFlag:   "", // Check if flags are extracted
			checkContent:   "Sephiroth did kill Aerith",
			checkPostNo:    713439368, // OP post
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. LOAD LOCAL HTML FILE
			htmlPath := filepath.Join("data", tc.htmlFile)
			htmlContent, err := os.ReadFile(htmlPath)
			if err != nil {
				t.Fatalf("Failed to read HTML file %s: %v", htmlPath, err)
			}

			// 2. PARSE HTML WITH ARCHIVED.MOE PARSER
			doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(htmlContent)))
			if err != nil {
				t.Fatalf("Failed to create goquery document: %v", err)
			}

			// Create archiver with test config
			config := &archiver.Config{
				Board:        "v",
				OutputDir:    "/tmp/test_archived_moe",
				Source:       archiver.SourceArchivedMoe,
				DatabaseMode: archiver.DatabaseModeMemory,
				Verbose:      true,
			}

			arch, err := archiver.New(config)
			if err != nil {
				t.Fatalf("Failed to create archiver: %v", err)
			}

			// Parse with archived.moe parser using the unified ParseHTML method
			thread, err := arch.ParseHTML(doc, "https://archived.moe/v/thread/"+tc.threadID)
			if err != nil {
				t.Fatalf("Failed to parse archived.moe HTML: %v", err)
			}

			// 3. SYSTEMATIC VERIFICATION OF PARSED DATA
			t.Logf("Parsed %d posts from archived.moe HTML", len(thread.Posts))

			// Check post count
			if len(thread.Posts) == 0 {
				t.Fatal("No posts were parsed")
			}

			// Check OP exists and has expected content
			op := thread.Posts[0]
			if op.No == 0 {
				t.Error("OP post number is 0")
			}

			// CRITICAL TEST: Verify OP has real post number, not sequential
			if op.No == tc.checkPostNo {
				t.Logf("✅ OP has correct real post number: %d", op.No)
			} else {
				t.Errorf("❌ OP post number mismatch: expected %d, got %d (FAKE SEQUENTIAL NUMBERS DETECTED!)", tc.checkPostNo, op.No)
			}

			// CRITICAL TEST: Verify no posts have sequential numbers (1, 2, 3, 4, 5)
			sequentialCount := 0
			fakeNumbers := []int64{}
			for i, post := range thread.Posts {
				if int64(i+1) == post.No {
					sequentialCount++
					fakeNumbers = append(fakeNumbers, post.No)
				}
			}

			if sequentialCount > 3 { // Some coincidences are OK
				t.Errorf("❌ CRITICAL: Found %d posts with sequential numbering (fake numbers: %v) - archived.moe parser is BROKEN!", sequentialCount, fakeNumbers)
			} else {
				t.Logf("✅ Posts have real 4chan numbers, not sequential")
			}

			// Verify post numbers are realistic (should be > 700M for /v/)
			for _, post := range thread.Posts {
				if post.No < 700000000 && post.No > 100 { // Skip obviously fake small numbers but allow real older posts
					t.Errorf("❌ Post #%d has unrealistic post number (too low for /v/)", post.No)
				}
			}

			// Check for specific content we know exists
			if !strings.Contains(op.Comment, tc.checkContent) {
				t.Errorf("OP content does not contain expected text '%s', got: %s", tc.checkContent, op.Comment)
			}

			// Look for another expected post content
			foundSephiroth := false
			foundFinalFantasy := false
			for _, post := range thread.Posts {
				if strings.Contains(post.Comment, "Sephiroth did kill Aerith") {
					foundSephiroth = true
					t.Logf("✅ Found 'Sephiroth did kill Aerith' post #%d", post.No)
				}
				if strings.Contains(post.Comment, "Final Fantasy set is 2 weeks old") {
					foundFinalFantasy = true
					t.Logf("✅ Found 'Final Fantasy set is 2 weeks old' post #%d", post.No)
				}
			}

			if !foundSephiroth {
				t.Error("❌ Missing expected 'Sephiroth did kill Aerith' content")
			}
			if !foundFinalFantasy {
				t.Error("❌ Missing expected 'Final Fantasy set is 2 weeks old' content")
			}

			// Find specific post with known content
			var targetPost *archiver.Post
			for _, post := range thread.Posts {
				if post.No == tc.checkPostNo {
					targetPost = &post
					break
				}
			}

			if targetPost == nil {
				t.Errorf("Could not find expected post #%d", tc.checkPostNo)
			} else {
				// Verify post data
				if targetPost.Name != tc.expectedAuthor && tc.expectedAuthor != "" {
					t.Errorf("Expected author '%s', got '%s'", tc.expectedAuthor, targetPost.Name)
				}

				if !strings.Contains(targetPost.Comment, tc.checkContent) {
					t.Errorf("Post #%d content does not contain '%s', got: %s", tc.checkPostNo, tc.checkContent, targetPost.Comment)
				}
			}

			// 4. TEST DATABASE EXTRACTION
			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("Failed to open memory database: %v", err)
			}
			defer db.Close()

			// Execute schema
			schemaPath := filepath.Join("..", "internal", "database", "schema", "001_init.sql")
			schemaBytes, err := os.ReadFile(schemaPath)
			if err != nil {
				t.Fatalf("Failed to read schema file: %v", err)
			}

			_, err = db.Exec(string(schemaBytes))
			if err != nil {
				t.Fatalf("Failed to create schema: %v", err)
			}

			// Extract to database
			queries := database.New(db)
			ctx := context.Background()

			err = arch.ExtractToDatabase(ctx, queries, thread, tc.threadID, "v")
			if err != nil {
				t.Fatalf("Failed to extract to database: %v", err)
			}

			// 5. SYSTEMATIC DATABASE VERIFICATION
			t.Log("Verifying database contents...")

			// Check posts in database
			posts, err := queries.GetPostsByThread(ctx, database.GetPostsByThreadParams{
				ThreadID: tc.threadID,
				Board:    "v",
			})
			if err != nil {
				t.Fatalf("Failed to get posts from database: %v", err)
			}

			if len(posts) == 0 {
				t.Fatal("No posts found in database")
			}

			t.Logf("Found %d posts in database", len(posts))

			// Verify OP in database
			var dbOP *database.Post
			for _, post := range posts {
				if post.IsOp.Bool {
					dbOP = &post
					break
				}
			}

			if dbOP == nil {
				t.Fatal("No OP found in database")
			}

			if dbOP.PostNo != op.No {
				t.Errorf("OP post number mismatch: expected %d, got %d", op.No, dbOP.PostNo)
			}

			// CRITICAL: Verify OP has correct data in database
			if dbOP.PostNo != tc.checkPostNo {
				t.Errorf("❌ CRITICAL: Database OP post number wrong: expected %d, got %d (FAKE NUMBERS IN DATABASE!)", tc.checkPostNo, dbOP.PostNo)
			}

			// Verify content exists in database
			if !strings.Contains(dbOP.CleanText.String, tc.checkContent) {
				t.Errorf("Database OP missing expected content '%s', got: %s", tc.checkContent, dbOP.CleanText.String)
			}

			// Check conversation tree (replies)
			conversations, err := queries.GetConversationTree(ctx, database.GetConversationTreeParams{
				ThreadID: tc.threadID,
				Board:    "v",
			})
			if err != nil {
				t.Fatalf("Failed to get conversation tree: %v", err)
			}

			t.Logf("Found %d reply relationships", len(conversations))

			// 6. TEST MARKDOWN GENERATION
			tempDir := t.TempDir()
			markdownPath := filepath.Join(tempDir, "thread.md")

			err = arch.CreateMarkdownFromDatabase(ctx, queries, tc.threadID, "v", markdownPath)
			if err != nil {
				t.Fatalf("Failed to create markdown: %v", err)
			}

			// 7. SYSTEMATIC MARKDOWN VERIFICATION
			markdownContent, err := os.ReadFile(markdownPath)
			if err != nil {
				t.Fatalf("Failed to read generated markdown: %v", err)
			}

			markdownStr := string(markdownContent)

			// Check markdown header
			expectedHeader := "# Thread " + tc.threadID
			if !strings.Contains(markdownStr, expectedHeader) {
				t.Errorf("Markdown missing expected header '%s'", expectedHeader)
			}

			// Check board info
			if !strings.Contains(markdownStr, "**Board:** /v/") {
				t.Error("Markdown missing board information")
			}

			// Check post count
			if !strings.Contains(markdownStr, "**Posts:**") {
				t.Error("Markdown missing post count")
			}

			// CRITICAL: Check OP content with real post number
			expectedOPPostNumber := "Post #713439368"
			if !strings.Contains(markdownStr, expectedOPPostNumber) {
				t.Errorf("❌ CRITICAL: Markdown missing real OP post number '%s'", expectedOPPostNumber)
			} else {
				t.Logf("✅ Markdown contains real OP post number")
			}

			// CRITICAL: Verify markdown doesn't have sequential post numbers
			sequentialInMarkdown := false
			if strings.Contains(markdownStr, "Post #1") ||
				strings.Contains(markdownStr, "Post #2") ||
				strings.Contains(markdownStr, "Post #3") ||
				strings.Contains(markdownStr, "Post #4") ||
				strings.Contains(markdownStr, "Post #5") {
				sequentialInMarkdown = true
				t.Error("❌ CRITICAL: Markdown contains sequential post numbers (1,2,3,4,5) instead of real ones - archived.moe parser is BROKEN!")
			} else {
				t.Log("✅ Markdown uses real post numbers, not sequential")
			}

			// Check specific post content
			if !strings.Contains(markdownStr, tc.checkContent) {
				t.Errorf("Markdown missing specific post content '%s'", tc.checkContent)
			}

			// Check for expected /v/ content
			if !strings.Contains(markdownStr, "Sephiroth") {
				t.Error("Markdown missing expected 'Sephiroth' content")
			}

			if !strings.Contains(markdownStr, "Final Fantasy") {
				t.Error("Markdown missing expected 'Final Fantasy' content")
			}

			// Check clickable links (anchors)
			if !strings.Contains(markdownStr, "<a name=") {
				t.Error("Markdown missing anchor links")
			}

			// 8. PERFORMANCE AND CONSISTENCY CHECKS
			t.Log("Running consistency checks...")

			// Verify all parsed posts are in database
			for _, originalPost := range thread.Posts {
				found := false
				for _, dbPost := range posts {
					if dbPost.PostNo == originalPost.No {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Parsed post #%d not found in database", originalPost.No)
				}
			}

			// Verify timestamps are reasonable
			now := time.Now().Unix()
			for _, post := range posts {
				if post.Timestamp < 1000000000 || post.Timestamp > now {
					t.Errorf("Invalid timestamp for post #%d: %d", post.PostNo, post.Timestamp)
				}
			}

			// Verify content extraction quality
			totalContent := 0
			for _, post := range posts {
				if len(post.CleanText.String) > 0 {
					totalContent++
				}
			}

			contentRatio := float64(totalContent) / float64(len(posts))
			if contentRatio < 0.5 {
				t.Errorf("Poor content extraction: only %.1f%% of posts have content", contentRatio*100)
			} else {
				t.Logf("✅ Good content extraction: %.1f%% of posts have content", contentRatio*100)
			}

			// Check for media extraction
			postsWithMedia := 0
			for _, post := range posts {
				if post.Filename.Valid && post.Filename.String != "" {
					postsWithMedia++
				}
			}
			t.Logf("Found %d posts with media", postsWithMedia)

			// Final summary
			if sequentialInMarkdown {
				t.Error("❌ FINAL VERDICT: archived.moe parser is BROKEN - generates fake sequential post numbers")
			} else {
				t.Logf("✅ archived.moe parser test completed successfully for %s", tc.name)
			}
		})
	}
}

// TestArchivedMoeParserEdgeCases tests edge cases and error conditions
func TestArchivedMoeParserEdgeCases(t *testing.T) {
	config := &archiver.Config{
		Board:        "v",
		OutputDir:    "/tmp/test_archived_moe_edge",
		Source:       archiver.SourceArchivedMoe,
		DatabaseMode: archiver.DatabaseModeMemory,
		Verbose:      true,
	}

	arch, err := archiver.New(config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	// Test empty document
	t.Run("empty_document", func(t *testing.T) {
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader("<html></html>"))
		_, err := arch.ParseHTML(doc, "https://archived.moe/v/thread/test")
		if err == nil {
			t.Error("Expected error for empty document, got nil")
		}
	})

	// Test document with no posts
	t.Run("no_posts", func(t *testing.T) {
		html := `<html><body><div class="not-a-post">Not a post</div></body></html>`
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader(html))
		_, err := arch.ParseHTML(doc, "https://archived.moe/v/thread/test")
		if err == nil {
			t.Error("Expected error for document with no posts, got nil")
		}
	})

	// Test malformed post structure
	t.Run("malformed_posts", func(t *testing.T) {
		html := `<html><body><article class="post"><div>incomplete post</div></article></body></html>`
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader(html))
		thread, err := arch.ParseHTML(doc, "https://archived.moe/v/thread/test")
		// Should not error but might return empty or incomplete posts
		if err != nil {
			t.Logf("Parsing malformed posts returned error (expected): %v", err)
		} else if thread != nil {
			t.Logf("Parsing malformed posts returned %d posts", len(thread.Posts))
		}
	})
}

// TestArchivedMoeVs4PlebsConsistency tests that archived.moe parser works like 4plebs
func TestArchivedMoeVs4PlebsConsistency(t *testing.T) {
	// This test verifies that archived.moe uses the same format as 4plebs
	// and should produce similar results

	config := &archiver.Config{
		Board:        "v",
		OutputDir:    "/tmp/test_consistency",
		Source:       archiver.SourceArchivedMoe,
		DatabaseMode: archiver.DatabaseModeMemory,
		Verbose:      true,
	}

	arch, err := archiver.New(config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	// Load archived.moe HTML
	htmlPath := filepath.Join("data", "713439368.html")
	htmlContent, err := os.ReadFile(htmlPath)
	if err != nil {
		t.Fatalf("Failed to read HTML file: %v", err)
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(htmlContent)))
	if err != nil {
		t.Fatalf("Failed to create goquery document: %v", err)
	}

	// Parse with archived.moe parser
	archivedMoeThread, err := arch.ParseHTML(doc, "https://archived.moe/v/thread/713439368")
	if err != nil {
		t.Fatalf("Failed to parse with archived.moe parser: %v", err)
	}

	// Also try with 4plebs parser to see if format is similar
	fourPlebsThread, err := arch.Parse4PlebsHTML(doc, "test")
	if err != nil {
		t.Logf("4plebs parser failed on archived.moe HTML (expected if different format): %v", err)
	} else {
		t.Logf("4plebs parser worked on archived.moe HTML - formats are similar")

		// Compare results
		if len(archivedMoeThread.Posts) != len(fourPlebsThread.Posts) {
			t.Logf("Different post counts: archived.moe=%d, 4plebs=%d",
				len(archivedMoeThread.Posts), len(fourPlebsThread.Posts))
		}
	}

	// The key test: ensure we're getting real post numbers
	if len(archivedMoeThread.Posts) > 0 {
		op := archivedMoeThread.Posts[0]
		if op.No == 713439368 {
			t.Log("✅ archived.moe parser extracted correct OP post number")
		} else {
			t.Errorf("❌ archived.moe parser extracted wrong OP post number: got %d, expected 713439368", op.No)
		}
	}
}
