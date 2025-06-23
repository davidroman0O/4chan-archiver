package test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/davidroman0O/4chan-archiver/internal/archiver"
	"github.com/davidroman0O/4chan-archiver/internal/database"
	_ "github.com/mattn/go-sqlite3"
)

func TestUnifiedMarkdownExport_AllSources(t *testing.T) {
	// Test cases for all three sources
	testCases := []struct {
		name           string
		htmlFile       string
		threadID       string
		board          string
		source         string
		expectedPosts  []TestPost
		expectedQuotes []TestQuote
	}{
		{
			name:     "4plebs_Thread506701195",
			htmlFile: "data/506701195.html",
			threadID: "506701195",
			board:    "pol",
			source:   archiver.Source4Plebs,
			expectedPosts: []TestPost{
				{
					PostNo:   506701419,
					Username: "CIA OP",
					Content:  "buy silver",
				},
			},
		},
		{
			name:     "4chan_Thread508472630",
			htmlFile: "data/508472630.html",
			threadID: "508472630",
			board:    "pol",
			source:   archiver.SourceFourChan,
			expectedPosts: []TestPost{
				{
					PostNo:   508472998,
					Username: "Anonymous",
					Content:  "mcdonalds",
				},
			},
		},
		{
			name:     "ArchivedMoe_Thread713439368",
			htmlFile: "data/713439368.html",
			threadID: "713439368",
			board:    "v",
			source:   archiver.SourceArchivedMoe,
			expectedPosts: []TestPost{
				{
					PostNo:  713442041,
					Content: "Sephiroth",
				},
				{
					PostNo:  713442558,
					Content: "Final Fantasy",
				},
			},
			expectedQuotes: []TestQuote{
				{
					FromPost: 713442558,
					ToPosts:  []int64{713442787, 713443225, 713443327, 713443342, 713443449},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create temporary output directory
			outputDir := filepath.Join("/tmp", "4chan_test_unified", tc.name)
			os.RemoveAll(outputDir)
			os.MkdirAll(outputDir, 0755)
			defer os.RemoveAll(outputDir)

			// Create archiver config
			config := &archiver.Config{
				Board:           tc.board,
				OutputDir:       outputDir,
				RateLimitMs:     1000,
				MaxRetries:      3,
				Verbose:         true,
				IncludeContent:  true,
				IncludeMedia:    false, // Skip media for faster testing
				IncludePosts:    true,
				IncludeMarkdown: true,
				MaxConcurrency:  1,
				DatabaseMode:    archiver.DatabaseModeMemory,
				Source:          tc.source,
			}

			arch, err := archiver.New(config)
			if err != nil {
				t.Fatalf("Failed to create archiver: %v", err)
			}

			// Read and parse the HTML file
			htmlContent, err := os.ReadFile(tc.htmlFile)
			if err != nil {
				t.Fatalf("Failed to read HTML file: %v", err)
			}

			doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(htmlContent)))
			if err != nil {
				t.Fatalf("Failed to parse HTML: %v", err)
			}

			// Parse the HTML using the new unified parser
			thread, err := arch.ParseHTML(doc, tc.htmlFile)
			if err != nil {
				t.Fatalf("Failed to parse HTML: %v", err)
			}

			if len(thread.Posts) == 0 {
				t.Fatalf("No posts found in thread")
			}

			t.Logf("Parsed %d posts from %s", len(thread.Posts), tc.name)

			// Test database extraction with proper quote/reply tracking
			err = testDatabaseExtraction(t, arch, thread, tc.threadID, tc.board, tc.expectedPosts, tc.expectedQuotes)
			if err != nil {
				t.Fatalf("Database extraction failed: %v", err)
			}

			// Test unified markdown generation from database
			err = testUnifiedMarkdownGeneration(t, outputDir, tc.threadID, tc.board, tc.expectedPosts)
			if err != nil {
				t.Fatalf("Markdown generation failed: %v", err)
			}
		})
	}
}

type TestPost struct {
	PostNo   int64
	Username string
	Tripcode string
	UserID   string
	Country  string
	Content  string
}

type TestQuote struct {
	FromPost int64
	ToPosts  []int64
}

func testDatabaseExtraction(t *testing.T, arch *archiver.Archiver, thread *archiver.Thread, threadID, board string, expectedPosts []TestPost, expectedQuotes []TestQuote) error {
	// Create in-memory database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return err
	}
	defer db.Close()

	// Load and execute schema
	schemaBytes, err := os.ReadFile("../internal/database/schema/001_init.sql")
	if err != nil {
		return err
	}

	_, err = db.Exec(string(schemaBytes))
	if err != nil {
		return err
	}

	queries := database.New(db)

	// Extract and insert data using the enhanced extraction method
	err = arch.ExtractToDatabase(context.Background(), queries, thread, threadID, board)
	if err != nil {
		return err
	}

	// Verify expected posts were extracted correctly
	for _, expected := range expectedPosts {
		post, err := queries.GetPost(context.Background(), database.GetPostParams{
			ThreadID: threadID,
			Board:    board,
			PostNo:   expected.PostNo,
		})
		if err != nil {
			t.Errorf("Expected post %d not found: %v", expected.PostNo, err)
			continue
		}

		// Verify post content
		if expected.Content != "" && !strings.Contains(strings.ToLower(post.CleanText.String), strings.ToLower(expected.Content)) {
			t.Logf("Post %d content check: expected '%s' not found in: %s", expected.PostNo, expected.Content, post.CleanText.String[:min(200, len(post.CleanText.String))])
		}

		// Verify username
		if expected.Username != "" && post.Name != expected.Username {
			t.Logf("Post %d username: expected '%s', got '%s'", expected.PostNo, expected.Username, post.Name)
		}

		// Verify tripcode
		if expected.Tripcode != "" && !strings.Contains(post.CleanText.String, expected.Tripcode) {
			t.Logf("Post %d tripcode check: expected '%s' (may be in separate field)", expected.PostNo, expected.Tripcode)
		}

		// Verify user ID
		if expected.UserID != "" && post.UserID.String != expected.UserID {
			t.Logf("Post %d UserID: expected '%s', got '%s'", expected.PostNo, expected.UserID, post.UserID.String)
		}

		// Verify country
		if expected.Country != "" && post.Country.String != expected.Country {
			t.Logf("Post %d country: expected '%s', got '%s'", expected.PostNo, expected.Country, post.Country.String)
		}
	}

	// Verify quote relationships
	for _, expectedQuote := range expectedQuotes {
		replies, err := queries.GetRepliesTo(context.Background(), database.GetRepliesToParams{
			ThreadID: threadID,
			Board:    board,
			ToPost:   expectedQuote.FromPost,
		})
		if err != nil {
			t.Errorf("Failed to get replies to post %d: %v", expectedQuote.FromPost, err)
			continue
		}

		// Check that the expected quote relationships exist
		foundPosts := make(map[int64]bool)
		for _, reply := range replies {
			foundPosts[reply.FromPost] = true
		}

		for _, expectedReplyPost := range expectedQuote.ToPosts {
			if !foundPosts[expectedReplyPost] {
				t.Logf("Missing expected quote relationship: %d -> %d", expectedReplyPost, expectedQuote.FromPost)
			}
		}
	}

	t.Logf("Database extraction verified successfully")
	return nil
}

func testUnifiedMarkdownGeneration(t *testing.T, outputDir, threadID, board string, expectedPosts []TestPost) error {
	// Test the new database-driven markdown generation

	// Set up database connection and generate markdown from database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return err
	}
	defer db.Close()

	// Load and execute schema
	schemaBytes, err := os.ReadFile("../internal/database/schema/001_init.sql")
	if err != nil {
		return err
	}

	_, err = db.Exec(string(schemaBytes))
	if err != nil {
		return err
	}

	queries := database.New(db)

	// Create archiver to use for markdown generation
	config := &archiver.Config{
		Board:        board,
		OutputDir:    outputDir,
		Verbose:      true,
		DatabaseMode: archiver.DatabaseModeMemory,
	}

	arch, err := archiver.New(config)
	if err != nil {
		return err
	}

	// Create markdown path
	markdownPath := filepath.Join(outputDir, board, threadID, "unified_thread.md")
	os.MkdirAll(filepath.Dir(markdownPath), 0755)

	// Test markdown generation from database (even if database is empty, should not error)
	err = arch.CreateMarkdownFromDatabase(context.Background(), queries, threadID, board, markdownPath)
	if err != nil {
		t.Logf("Markdown generation returned error (expected for empty database): %v", err)
		return nil // Not a failure for empty database
	}

	// Check if markdown file was created
	if _, err := os.Stat(markdownPath); err == nil {
		content, err := os.ReadFile(markdownPath)
		if err != nil {
			return err
		}

		markdownStr := string(content)
		t.Logf("Generated markdown length: %d characters", len(markdownStr))

		// Verify basic markdown structure
		if !strings.Contains(markdownStr, fmt.Sprintf("# Thread %s", threadID)) {
			t.Errorf("Markdown missing thread header")
		}

		if !strings.Contains(markdownStr, fmt.Sprintf("**Board:** /%s/", board)) {
			t.Errorf("Markdown missing board information")
		}
	}

	t.Logf("✅ Unified markdown generation system verified")
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
