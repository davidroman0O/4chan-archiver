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

// Test4ChanParser tests the 4chan parser against local HTML data
func Test4ChanParser(t *testing.T) {
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
			name:           "4chan thread 508472630",
			htmlFile:       "508472630.html",
			threadID:       "508472630",
			expectedPosts:  10, // Adjust based on actual content
			expectedOP:     "mcdonalds badge",
			expectedAuthor: "Anonymous",
			expectedID:     "508472998", // Specific post we want to verify
			expectedFlag:   "EG",        // Egypt flag
			checkContent:   "mcdonalds badge",
			checkPostNo:    508472998,
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

			// 2. PARSE HTML WITH 4CHAN PARSER
			doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(htmlContent)))
			if err != nil {
				t.Fatalf("Failed to create goquery document: %v", err)
			}

			// Create archiver with test config
			config := &archiver.Config{
				Board:        "pol",
				OutputDir:    "/tmp/test_4chan",
				Source:       archiver.SourceFourChan,
				DatabaseMode: archiver.DatabaseModeMemory,
				Verbose:      true,
			}

			arch, err := archiver.New(config)
			if err != nil {
				t.Fatalf("Failed to create archiver: %v", err)
			}

			// Parse with 4chan parser
			url := "https://boards.4chan.org/pol/thread/" + tc.threadID
			thread, err := arch.Parse4ChanHTML(doc, url)
			if err != nil {
				t.Fatalf("Failed to parse 4chan HTML: %v", err)
			}

			// 3. SYSTEMATIC VERIFICATION OF PARSED DATA
			t.Logf("Parsed %d posts from 4chan HTML", len(thread.Posts))

			// Check post count
			if len(thread.Posts) == 0 {
				t.Fatal("No posts were parsed")
			}

			// Check OP exists and has expected content
			op := thread.Posts[0]
			if op.No == 0 {
				t.Error("OP post number is 0")
			}

			if !strings.Contains(op.Comment, tc.expectedOP) {
				t.Errorf("OP content does not contain expected text '%s', got: %s", tc.expectedOP, op.Comment)
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
				if targetPost.Name != tc.expectedAuthor {
					t.Errorf("Expected author '%s', got '%s'", tc.expectedAuthor, targetPost.Name)
				}

				if !strings.Contains(targetPost.Comment, tc.checkContent) {
					t.Errorf("Post #%d content does not contain '%s', got: %s", tc.checkPostNo, tc.checkContent, targetPost.Comment)
				}

				if targetPost.Country != tc.expectedFlag {
					t.Errorf("Expected country flag '%s', got '%s'", tc.expectedFlag, targetPost.Country)
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

			// Verify specific post in database
			var dbTargetPost *database.Post
			for _, post := range posts {
				if post.PostNo == tc.checkPostNo {
					dbTargetPost = &post
					break
				}
			}

			if dbTargetPost == nil {
				t.Errorf("Target post #%d not found in database", tc.checkPostNo)
			} else {
				if dbTargetPost.Name != tc.expectedAuthor {
					t.Errorf("Database post author mismatch: expected %s, got %s", tc.expectedAuthor, dbTargetPost.Name)
				}

				if dbTargetPost.Country.String != tc.expectedFlag {
					t.Errorf("Database post country mismatch: expected %s, got %s", tc.expectedFlag, dbTargetPost.Country.String)
				}

				if !strings.Contains(dbTargetPost.CleanText.String, tc.checkContent) {
					t.Errorf("Database post content missing '%s', got: %s", tc.checkContent, dbTargetPost.CleanText.String)
				}
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

			// Check OP content
			if !strings.Contains(markdownStr, tc.expectedOP) {
				t.Errorf("Markdown missing OP content '%s'", tc.expectedOP)
			}

			// Check specific post content
			if !strings.Contains(markdownStr, tc.checkContent) {
				t.Errorf("Markdown missing specific post content '%s'", tc.checkContent)
			}

			// Check post number formatting
			if !strings.Contains(markdownStr, "Post #") {
				t.Error("Markdown missing post number formatting")
			}

			// Check clickable links (anchors)
			if !strings.Contains(markdownStr, "<a name=") {
				t.Error("Markdown missing anchor links")
			}

			// Check author information
			if !strings.Contains(markdownStr, tc.expectedAuthor) {
				t.Errorf("Markdown missing author '%s'", tc.expectedAuthor)
			}

			// Check country flag emoji (if present)
			if tc.expectedFlag == "EG" {
				if !strings.Contains(markdownStr, "🇪🇬") {
					t.Error("Markdown missing Egypt flag emoji")
				}
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

			t.Logf("✅ 4chan parser test completed successfully for %s", tc.name)
		})
	}
}

// Test4ChanParserEdgeCases tests edge cases and error conditions
func Test4ChanParserEdgeCases(t *testing.T) {
	config := &archiver.Config{
		Board:        "pol",
		OutputDir:    "/tmp/test_4chan_edge",
		Source:       archiver.SourceFourChan,
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
		_, err := arch.Parse4ChanHTML(doc, "test")
		if err == nil {
			t.Error("Expected error for empty document, got nil")
		}
	})

	// Test malformed HTML
	t.Run("malformed_html", func(t *testing.T) {
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader("<div><p>incomplete"))
		_, err := arch.Parse4ChanHTML(doc, "test")
		if err == nil {
			t.Error("Expected error for malformed HTML, got nil")
		}
	})
}
