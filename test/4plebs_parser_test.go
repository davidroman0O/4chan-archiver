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

// Test4PlebsParser tests the 4plebs parser against local HTML data
func Test4PlebsParser(t *testing.T) {
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
			name:           "4plebs thread 506701195",
			htmlFile:       "506701195.html",
			threadID:       "506701195",
			expectedPosts:  50, // Adjust based on actual content
			expectedOP:     "CIA OP",
			expectedAuthor: "CIA OP",
			expectedID:     "ID:0Cj9IC6b",
			expectedFlag:   "", // Check if flags are extracted
			checkContent:   "CIA OP",
			checkPostNo:    506701195, // OP post
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

			// 2. PARSE HTML WITH 4PLEBS PARSER
			doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(htmlContent)))
			if err != nil {
				t.Fatalf("Failed to create goquery document: %v", err)
			}

			// Create archiver with test config
			config := &archiver.Config{
				Board:        "pol",
				OutputDir:    "/tmp/test_4plebs",
				Source:       archiver.Source4Plebs,
				DatabaseMode: archiver.DatabaseModeMemory,
				Verbose:      true,
			}

			arch, err := archiver.New(config)
			if err != nil {
				t.Fatalf("Failed to create archiver: %v", err)
			}

			// Parse with 4plebs parser
			url := "https://archive.4plebs.org/pol/thread/" + tc.threadID
			thread, err := arch.Parse4PlebsHTML(doc, url)
			if err != nil {
				t.Fatalf("Failed to parse 4plebs HTML: %v", err)
			}

			// 3. SYSTEMATIC VERIFICATION OF PARSED DATA
			t.Logf("Parsed %d posts from 4plebs HTML", len(thread.Posts))

			// Check post count
			if len(thread.Posts) == 0 {
				t.Fatal("No posts were parsed")
			}

			// Check OP exists and has expected content
			op := thread.Posts[0]
			if op.No == 0 {
				t.Error("OP post number is 0")
			}

			// Verify OP has real post number, not sequential
			if op.No == tc.checkPostNo {
				t.Logf("✅ OP has correct real post number: %d", op.No)
			} else {
				t.Errorf("❌ OP post number mismatch: expected %d, got %d", tc.checkPostNo, op.No)
			}

			// Check for CIA OP tripcode
			if strings.Contains(op.Trip, "!!z9FVe/srtHt") {
				t.Logf("✅ Found expected tripcode: %s", op.Trip)
			} else {
				t.Errorf("❌ Missing expected tripcode, got: %s", op.Trip)
			}

			// Check for user ID
			if strings.Contains(op.ID, "0Cj9IC6b") {
				t.Logf("✅ Found expected user ID: %s", op.ID)
			} else {
				t.Errorf("❌ Missing expected user ID, got: %s", op.ID)
			}

			// Verify no posts have sequential numbers (1, 2, 3, 4, 5)
			sequentialCount := 0
			for i, post := range thread.Posts {
				if int64(i+1) == post.No {
					sequentialCount++
				}
			}

			if sequentialCount > 3 { // Some coincidences are OK
				t.Errorf("❌ Found %d posts with sequential numbering - parser may be broken", sequentialCount)
			} else {
				t.Logf("✅ Posts have real 4chan numbers, not sequential")
			}

			// Verify post numbers are realistic (should be > 500M for /pol/)
			for _, post := range thread.Posts {
				if post.No < 500000000 {
					t.Errorf("❌ Post #%d has unrealistic post number (too low)", post.No)
				}
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
				if !strings.Contains(targetPost.Name, tc.expectedAuthor) {
					t.Errorf("Expected author containing '%s', got '%s'", tc.expectedAuthor, targetPost.Name)
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

			err = arch.ExtractToDatabase(ctx, queries, thread, tc.threadID, "pol")
			if err != nil {
				t.Fatalf("Failed to extract to database: %v", err)
			}

			// 5. SYSTEMATIC DATABASE VERIFICATION
			t.Log("Verifying database contents...")

			// Check posts in database
			posts, err := queries.GetPostsByThread(ctx, database.GetPostsByThreadParams{
				ThreadID: tc.threadID,
				Board:    "pol",
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

			// Verify OP has correct data in database
			if dbOP.PostNo != tc.checkPostNo {
				t.Errorf("Database OP post number wrong: expected %d, got %d", tc.checkPostNo, dbOP.PostNo)
			}

			if !strings.Contains(dbOP.Name, tc.expectedAuthor) {
				t.Errorf("Database OP author mismatch: expected %s, got %s", tc.expectedAuthor, dbOP.Name)
			}

			if !strings.Contains(dbOP.Tripcode.String, "!!z9FVe/srtHt") {
				t.Errorf("Database OP missing expected tripcode, got: %s", dbOP.Tripcode.String)
			}

			if !strings.Contains(dbOP.UserID.String, "0Cj9IC6b") {
				t.Errorf("Database OP missing expected user ID, got: %s", dbOP.UserID.String)
			}

			// Check conversation tree (replies)
			conversations, err := queries.GetConversationTree(ctx, database.GetConversationTreeParams{
				ThreadID: tc.threadID,
				Board:    "pol",
			})
			if err != nil {
				t.Fatalf("Failed to get conversation tree: %v", err)
			}

			t.Logf("Found %d reply relationships", len(conversations))

			// 6. TEST MARKDOWN GENERATION
			tempDir := t.TempDir()
			markdownPath := filepath.Join(tempDir, "thread.md")

			err = arch.CreateMarkdownFromDatabase(ctx, queries, tc.threadID, "pol", markdownPath)
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
			if !strings.Contains(markdownStr, "**Board:** /pol/") {
				t.Error("Markdown missing board information")
			}

			// Check post count
			if !strings.Contains(markdownStr, "**Posts:**") {
				t.Error("Markdown missing post count")
			}

			// Check OP content with real post number
			if !strings.Contains(markdownStr, "Post #506701195") {
				t.Error("Markdown missing real OP post number")
			}

			// Verify markdown doesn't have sequential post numbers
			if strings.Contains(markdownStr, "Post #1") ||
				strings.Contains(markdownStr, "Post #2") ||
				strings.Contains(markdownStr, "Post #3") {
				t.Error("❌ Markdown contains sequential post numbers instead of real ones")
			} else {
				t.Log("✅ Markdown uses real post numbers")
			}

			// Check specific post content
			if !strings.Contains(markdownStr, tc.checkContent) {
				t.Errorf("Markdown missing specific post content '%s'", tc.checkContent)
			}

			// Check author information
			if !strings.Contains(markdownStr, tc.expectedAuthor) {
				t.Errorf("Markdown missing author '%s'", tc.expectedAuthor)
			}

			// Check tripcode formatting
			if !strings.Contains(markdownStr, "!!z9FVe/srtHt") {
				t.Error("Markdown missing expected tripcode")
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

			t.Logf("✅ 4plebs parser test completed successfully for %s", tc.name)
		})
	}
}

// Test4PlebsParserEdgeCases tests edge cases and error conditions
func Test4PlebsParserEdgeCases(t *testing.T) {
	config := &archiver.Config{
		Board:        "pol",
		OutputDir:    "/tmp/test_4plebs_edge",
		Source:       archiver.Source4Plebs,
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
		_, err := arch.Parse4PlebsHTML(doc, "test")
		if err == nil {
			t.Error("Expected error for empty document, got nil")
		}
	})

	// Test document with no posts
	t.Run("no_posts", func(t *testing.T) {
		html := `<html><body><div class="not-a-post">Not a post</div></body></html>`
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader(html))
		_, err := arch.Parse4PlebsHTML(doc, "test")
		if err == nil {
			t.Error("Expected error for document with no posts, got nil")
		}
	})

	// Test malformed post structure
	t.Run("malformed_posts", func(t *testing.T) {
		html := `<html><body><article class="post"><div>incomplete post</div></article></body></html>`
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader(html))
		thread, err := arch.Parse4PlebsHTML(doc, "test")
		// Should not error but might return empty or incomplete posts
		if err != nil {
			t.Logf("Parsing malformed posts returned error (expected): %v", err)
		} else if thread != nil {
			t.Logf("Parsing malformed posts returned %d posts", len(thread.Posts))
		}
	})
}
