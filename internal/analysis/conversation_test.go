package analysis

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// Test data for /pol/ thread (with user IDs and flags)
var polTestThread = ThreadData{
	Posts: []ChanPost{
		{
			No:          506536140,
			Now:         "01/10/25(Fri)12:00:00",
			Name:        "Anonymous",
			ID:          "XyZ123Ab",
			Country:     "US",
			CountryName: "United States",
			Sub:         "Test Thread",
			Com:         "This is a test thread on /pol/",
			Time:        1736524800,
			Resto:       0, // OP post
		},
		{
			No:          506536141,
			Now:         "01/10/25(Fri)12:01:00",
			Name:        "Anonymous",
			ID:          "AbC456Xy",
			Country:     "DE",
			CountryName: "Germany",
			Com:         "<a href=\"#p506536140\" class=\"quotelink\">&gt;&gt;506536140</a><br>Interesting point anon",
			Time:        1736524860,
			Resto:       506536140,
		},
		{
			No:          506536142,
			Now:         "01/10/25(Fri)12:02:00",
			Name:        "Anonymous",
			ID:          "XyZ123Ab", // Same user as OP
			Country:     "US",
			CountryName: "United States",
			Com:         "<a href=\"#p506536141\" class=\"quotelink\">&gt;&gt;506536141</a><br>Thanks for the feedback!",
			Time:        1736524920,
			Resto:       506536140,
		},
		{
			No:          506536143,
			Now:         "01/10/25(Fri)12:03:00",
			Name:        "Anonymous",
			ID:          "DeF789Gh",
			Country:     "CA",
			CountryName: "Canada",
			Com:         "<a href=\"#p506536140\" class=\"quotelink\">&gt;&gt;506536140</a><br><a href=\"#p506536141\" class=\"quotelink\">&gt;&gt;506536141</a><br>Both of you are wrong",
			Time:        1736524980,
			Resto:       506536140,
		},
	},
}

// Test data for /b/ thread (no user IDs, no flags)
var bTestThread = ThreadData{
	Posts: []ChanPost{
		{
			No:       935426202,
			Now:      "06/06/25(Fri)01:56:55",
			Name:     "Anonymous",
			Sub:      "Built for Incels",
			Com:      "Cute and hot chicks who would look better sucking creepy dudes' dicks",
			Filename: "1749164676748626",
			Ext:      ".jpg",
			W:        1080,
			H:        1169,
			Time:     1749189415,
			Resto:    0, // OP post
		},
		{
			No:       935426252,
			Now:      "06/06/25(Fri)01:58:52",
			Name:     "Anonymous",
			Com:      "Clearly",
			Filename: "05",
			Ext:      ".jpg",
			Time:     1749189532,
			Resto:    935426202,
		},
		{
			No:    935426369,
			Now:   "06/06/25(Fri)02:03:43",
			Name:  "Anonymous",
			Com:   "<a href=\"#p935426252\" class=\"quotelink\">&gt;&gt;935426252</a><br>Gym bitches always need it",
			Time:  1749189823,
			Resto: 935426202,
		},
		{
			No:    935426417,
			Now:   "06/06/25(Fri)02:05:58",
			Name:  "Anonymous",
			Com:   "<a href=\"#p935426252\" class=\"quotelink\">&gt;&gt;935426252</a><br><a href=\"#p935426369\" class=\"quotelink\">&gt;&gt;935426369</a><br>Agree. They fitter they are the hotter they look taking weak dick",
			Time:  1749189958,
			Resto: 935426202,
		},
	},
}

func setupTestDB(t *testing.T) (*sql.DB, func()) {
	// Create temporary database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Run schema migrations
	schema := `
	-- Create tables for 4chan thread analysis

	-- Threads table to track archived threads
	CREATE TABLE threads (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    thread_id TEXT NOT NULL,
	    board TEXT NOT NULL,
	    subject TEXT,
	    created_at DATETIME NOT NULL,
	    last_updated DATETIME NOT NULL,
	    posts_count INTEGER DEFAULT 0,
	    media_count INTEGER DEFAULT 0,
	    status TEXT DEFAULT 'active',
	    UNIQUE(thread_id, board)
	);

	-- Posts table to store individual posts
	CREATE TABLE posts (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    thread_id TEXT NOT NULL,
	    board TEXT NOT NULL,
	    post_no INTEGER NOT NULL,
	    timestamp INTEGER NOT NULL,
	    name TEXT NOT NULL DEFAULT 'Anonymous',
	    user_id TEXT,
	    country TEXT,
	    country_name TEXT,
	    flag TEXT,
	    flag_name TEXT,
	    subject TEXT,
	    comment TEXT,
	    clean_text TEXT,
	    filename TEXT,
	    file_ext TEXT,
	    file_size INTEGER,
	    image_width INTEGER,
	    image_height INTEGER,
	    thumbnail_width INTEGER,
	    thumbnail_height INTEGER,
	    md5_hash TEXT,
	    is_op BOOLEAN DEFAULT FALSE,
	    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	    UNIQUE(thread_id, board, post_no)
	);

	-- Users table to track unique users (when IDs are available)
	CREATE TABLE users (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    board TEXT NOT NULL,
	    user_id TEXT NOT NULL,
	    name TEXT NOT NULL DEFAULT 'Anonymous',
	    country TEXT,
	    country_name TEXT,
	    flag TEXT,
	    flag_name TEXT,
	    first_seen DATETIME NOT NULL,
	    last_seen DATETIME NOT NULL,
	    post_count INTEGER DEFAULT 0,
	    UNIQUE(board, user_id)
	);

	-- Replies table to track conversation relationships
	CREATE TABLE replies (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    thread_id TEXT NOT NULL,
	    board TEXT NOT NULL,
	    from_post INTEGER NOT NULL,
	    to_post INTEGER NOT NULL,
	    reply_type TEXT NOT NULL DEFAULT 'direct',
	    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	    UNIQUE(thread_id, board, from_post, to_post, reply_type)
	);`

	_, err = db.Exec(schema)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

func TestPolThreadAnalysis(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	analyzer := NewConversationAnalyzer(db, "pol", "506536140")

	// Test thread analysis
	err := analyzer.AnalyzeThread(polTestThread)
	if err != nil {
		t.Fatalf("Failed to analyze /pol/ thread: %v", err)
	}

	// Test conversation stats
	stats, err := analyzer.GetConversationStats()
	if err != nil {
		t.Fatalf("Failed to get conversation stats: %v", err)
	}

	// Verify results
	if stats.TotalPosts != 4 {
		t.Errorf("Expected 4 posts, got %d", stats.TotalPosts)
	}

	if stats.UniqueUsers != 3 {
		t.Errorf("Expected 3 unique users, got %d", stats.UniqueUsers)
	}

	if stats.ReplyChains != 4 {
		t.Errorf("Expected 4 reply chains, got %d", stats.ReplyChains)
	}

	// Check user stats
	if len(stats.UserStats) != 3 {
		t.Errorf("Expected 3 users in stats, got %d", len(stats.UserStats))
	}

	// Verify most active user (XyZ123Ab posted twice)
	if stats.MostActiveUser != "XyZ123Ab" {
		t.Errorf("Expected most active user to be XyZ123Ab, got %s", stats.MostActiveUser)
	}

	// Check specific user stats
	userStat, exists := stats.UserStats["XyZ123Ab"]
	if !exists {
		t.Error("Expected user XyZ123Ab in stats")
	} else {
		if userStat.PostCount != 2 {
			t.Errorf("Expected user XyZ123Ab to have 2 posts, got %d", userStat.PostCount)
		}
		if userStat.Country != "US" {
			t.Errorf("Expected user country to be US, got %s", userStat.Country)
		}
	}

	t.Logf("✅ /pol/ thread analysis successful: %d posts, %d users, %d reply chains",
		stats.TotalPosts, stats.UniqueUsers, stats.ReplyChains)
}

func TestBThreadAnalysis(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	analyzer := NewConversationAnalyzer(db, "b", "935426202")

	// Test thread analysis
	err := analyzer.AnalyzeThread(bTestThread)
	if err != nil {
		t.Fatalf("Failed to analyze /b/ thread: %v", err)
	}

	// Test conversation stats
	stats, err := analyzer.GetConversationStats()
	if err != nil {
		t.Fatalf("Failed to get conversation stats: %v", err)
	}

	// Verify results for /b/ (no user IDs)
	if stats.TotalPosts != 4 {
		t.Errorf("Expected 4 posts, got %d", stats.TotalPosts)
	}

	if stats.UniqueUsers != 0 {
		t.Errorf("Expected 0 unique users (no IDs on /b/), got %d", stats.UniqueUsers)
	}

	if stats.ReplyChains != 3 {
		t.Errorf("Expected 3 reply chains, got %d", stats.ReplyChains)
	}

	// /b/ should have no user stats since there are no user IDs
	if len(stats.UserStats) != 0 {
		t.Errorf("Expected 0 users in stats for /b/, got %d", len(stats.UserStats))
	}

	// Check that posts have media info
	postStat, exists := stats.PostStats[935426202]
	if !exists {
		t.Error("Expected OP post in post stats")
	} else {
		if !postStat.HasMedia {
			t.Error("Expected OP post to have media")
		}
		if !postStat.IsOP {
			t.Error("Expected post to be marked as OP")
		}
	}

	t.Logf("✅ /b/ thread analysis successful: %d posts, %d users, %d reply chains",
		stats.TotalPosts, stats.UniqueUsers, stats.ReplyChains)
}

func TestReplyLinkParsing(t *testing.T) {
	analyzer := &ConversationAnalyzer{
		parser: NewPostAnalyzer(),
	}

	testCases := []struct {
		name     string
		html     string
		expected []int64
	}{
		{
			name:     "Single reply link",
			html:     `<a href="#p935426252" class="quotelink">&gt;&gt;935426252</a><br>Some text`,
			expected: []int64{935426252},
		},
		{
			name:     "Multiple reply links",
			html:     `<a href="#p935426252" class="quotelink">&gt;&gt;935426252</a><br><a href="#p935426369" class="quotelink">&gt;&gt;935426369</a><br>Text`,
			expected: []int64{935426252, 935426369},
		},
		{
			name:     "No reply links",
			html:     `Just some regular text with no links`,
			expected: []int64{},
		},
		{
			name:     "Plain text with no HTML",
			html:     `Just text without any links or markup`,
			expected: []int64{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := analyzer.parseReplyLinks(tc.html)

			if len(result) != len(tc.expected) {
				t.Errorf("Expected %d reply links, got %d", len(tc.expected), len(result))
				return
			}

			for i, expected := range tc.expected {
				if result[i] != expected {
					t.Errorf("Expected reply link %d to be %d, got %d", i, expected, result[i])
				}
			}
		})
	}
}

func TestTextCleaning(t *testing.T) {
	parser := NewPostAnalyzer()

	testCases := []struct {
		name     string
		html     string
		expected string
	}{
		{
			name:     "Simple HTML removal",
			html:     `<span class="quote">&gt;be me</span><br>regular text`,
			expected: ">be me\nregular text",
		},
		{
			name:     "Quote links",
			html:     `<a href="#p123456" class="quotelink">&gt;&gt;123456</a><br>responding to anon`,
			expected: ">>123456\nresponding to anon",
		},
		{
			name:     "Complex HTML with multiple elements",
			html:     `<span class="quote">&gt;tfw no gf</span><br><a href="#p123456" class="quotelink">&gt;&gt;123456</a><br>this but unironically`,
			expected: ">tfw no gf\n>>123456\nthis but unironically",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := parser.stripHTML(tc.html)
			if result != tc.expected {
				t.Errorf("Expected: %q\nGot: %q", tc.expected, result)
			}
		})
	}
}

func TestAnalyzeThreadIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Test with temporary file database to ensure persistence works
	tmpfile, err := os.CreateTemp("", "test_thread_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create schema
	schema := `
	CREATE TABLE threads (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    thread_id TEXT NOT NULL,
	    board TEXT NOT NULL,
	    subject TEXT,
	    created_at DATETIME NOT NULL,
	    last_updated DATETIME NOT NULL,
	    posts_count INTEGER DEFAULT 0,
	    media_count INTEGER DEFAULT 0,
	    status TEXT DEFAULT 'active',
	    UNIQUE(thread_id, board)
	);

	CREATE TABLE posts (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    thread_id TEXT NOT NULL,
	    board TEXT NOT NULL,
	    post_no INTEGER NOT NULL,
	    timestamp INTEGER NOT NULL,
	    name TEXT NOT NULL DEFAULT 'Anonymous',
	    user_id TEXT,
	    country TEXT,
	    country_name TEXT,
	    flag TEXT,
	    flag_name TEXT,
	    subject TEXT,
	    comment TEXT,
	    clean_text TEXT,
	    filename TEXT,
	    file_ext TEXT,
	    file_size INTEGER,
	    image_width INTEGER,
	    image_height INTEGER,
	    thumbnail_width INTEGER,
	    thumbnail_height INTEGER,
	    md5_hash TEXT,
	    is_op BOOLEAN DEFAULT FALSE,
	    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	    UNIQUE(thread_id, board, post_no)
	);

	CREATE TABLE users (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    board TEXT NOT NULL,
	    user_id TEXT NOT NULL,
	    name TEXT NOT NULL DEFAULT 'Anonymous',
	    country TEXT,
	    country_name TEXT,
	    flag TEXT,
	    flag_name TEXT,
	    first_seen DATETIME NOT NULL,
	    last_seen DATETIME NOT NULL,
	    post_count INTEGER DEFAULT 0,
	    UNIQUE(board, user_id)
	);

	CREATE TABLE replies (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    thread_id TEXT NOT NULL,
	    board TEXT NOT NULL,
	    from_post INTEGER NOT NULL,
	    to_post INTEGER NOT NULL,
	    reply_type TEXT NOT NULL DEFAULT 'direct',
	    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	    UNIQUE(thread_id, board, from_post, to_post, reply_type)
	);`

	_, err = db.Exec(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Test both /pol/ and /b/ scenarios
	polAnalyzer := NewConversationAnalyzer(db, "pol", "506536140")
	err = polAnalyzer.AnalyzeThread(polTestThread)
	if err != nil {
		t.Fatalf("Failed to analyze /pol/ thread: %v", err)
	}

	bAnalyzer := NewConversationAnalyzer(db, "b", "935426202")
	err = bAnalyzer.AnalyzeThread(bTestThread)
	if err != nil {
		t.Fatalf("Failed to analyze /b/ thread: %v", err)
	}

	// Verify data was persisted correctly
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM posts").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 8 {
		t.Errorf("Expected 8 total posts, got %d", count)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM replies").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 7 {
		t.Errorf("Expected 7 total replies, got %d", count)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("Expected 3 total users (only from /pol/), got %d", count)
	}

	t.Log("✅ Integration test passed: Data persisted correctly to SQLite")
}
