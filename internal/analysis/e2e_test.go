package analysis

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// E2E tests using REAL thread data from the threads the user specified

func fetchRealThreadData(board, threadID string) (*ThreadData, error) {
	url := fmt.Sprintf("https://a.4cdn.org/%s/thread/%s.json", board, threadID)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch thread: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("thread not found or expired: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var threadData ThreadData
	err = json.Unmarshal(body, &threadData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return &threadData, nil
}

func TestE2E_PolThread506536140(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Fetch REAL thread data from /pol/506536140
	threadData, err := fetchRealThreadData("pol", "506536140")
	if err != nil {
		t.Skipf("Could not fetch real thread data (thread may be archived/deleted): %v", err)
	}

	t.Logf("Fetched real /pol/ thread 506536140 with %d posts", len(threadData.Posts))

	// Setup test database
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create analyzer and run E2E test
	analyzer := NewConversationAnalyzer(db, "pol", "506536140")

	// Test complete thread analysis with real data
	err = analyzer.AnalyzeThread(*threadData)
	if err != nil {
		t.Fatalf("Failed to analyze real /pol/ thread: %v", err)
	}

	// Get conversation stats from real data
	stats, err := analyzer.GetConversationStats()
	if err != nil {
		t.Fatalf("Failed to get conversation stats: %v", err)
	}

	// Verify the analysis worked on real data
	if stats.TotalPosts != len(threadData.Posts) {
		t.Errorf("Post count mismatch: expected %d, got %d", len(threadData.Posts), stats.TotalPosts)
	}

	// /pol/ should have user IDs, so we should have users
	if stats.UniqueUsers == 0 {
		t.Error("Expected unique users in /pol/ thread (has user IDs)")
	}

	// Should have conversation relationships
	if stats.ReplyChains == 0 {
		t.Error("Expected reply chains in real thread")
	}

	// Check that we parsed flags/countries correctly for /pol/
	hasCountryData := false
	for _, userStat := range stats.UserStats {
		if userStat.Country != "" {
			hasCountryData = true
			break
		}
	}
	if !hasCountryData && len(stats.UserStats) > 0 {
		t.Error("Expected country data in /pol/ thread")
	}

	// Verify post stats
	if len(stats.PostStats) != len(threadData.Posts) {
		t.Errorf("Post stats count mismatch: expected %d, got %d", len(threadData.Posts), len(stats.PostStats))
	}

	// Check OP post is marked correctly
	opPost := threadData.Posts[0]
	postStat, exists := stats.PostStats[opPost.No]
	if !exists {
		t.Error("OP post not found in post stats")
	} else if !postStat.IsOP {
		t.Error("OP post not marked as OP")
	}

	t.Logf("✅ E2E /pol/ thread analysis successful:")
	t.Logf("   Posts: %d", stats.TotalPosts)
	t.Logf("   Users: %d", stats.UniqueUsers)
	t.Logf("   Reply chains: %d", stats.ReplyChains)
	t.Logf("   Most active user: %s", stats.MostActiveUser)
	t.Logf("   Most replied post: %d", stats.MostRepliedPost)
}

func TestE2E_BThread935426202(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Fetch REAL thread data from /b/935426202
	threadData, err := fetchRealThreadData("b", "935426202")
	if err != nil {
		t.Skipf("Could not fetch real thread data (thread may be archived/deleted): %v", err)
	}

	t.Logf("Fetched real /b/ thread 935426202 with %d posts", len(threadData.Posts))

	// Setup test database
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create analyzer and run E2E test
	analyzer := NewConversationAnalyzer(db, "b", "935426202")

	// Test complete thread analysis with real data
	err = analyzer.AnalyzeThread(*threadData)
	if err != nil {
		t.Fatalf("Failed to analyze real /b/ thread: %v", err)
	}

	// Get conversation stats from real data
	stats, err := analyzer.GetConversationStats()
	if err != nil {
		t.Fatalf("Failed to get conversation stats: %v", err)
	}

	// Verify the analysis worked on real data
	if stats.TotalPosts != len(threadData.Posts) {
		t.Errorf("Post count mismatch: expected %d, got %d", len(threadData.Posts), stats.TotalPosts)
	}

	// /b/ should have NO user IDs, so no unique users
	if stats.UniqueUsers != 0 {
		t.Errorf("Expected 0 unique users in /b/ thread (no user IDs), got %d", stats.UniqueUsers)
	}

	// Should still have conversation relationships even without user IDs
	if len(threadData.Posts) > 1 && stats.ReplyChains == 0 {
		t.Error("Expected reply chains in real thread with multiple posts")
	}

	// /b/ should have NO user stats (no IDs)
	if len(stats.UserStats) != 0 {
		t.Errorf("Expected no user stats for /b/ thread, got %d", len(stats.UserStats))
	}

	// Verify post stats
	if len(stats.PostStats) != len(threadData.Posts) {
		t.Errorf("Post stats count mismatch: expected %d, got %d", len(threadData.Posts), len(stats.PostStats))
	}

	// Check OP post is marked correctly
	opPost := threadData.Posts[0]
	postStat, exists := stats.PostStats[opPost.No]
	if !exists {
		t.Error("OP post not found in post stats")
	} else if !postStat.IsOP {
		t.Error("OP post not marked as OP")
	}

	// Check media detection (thread title suggests images)
	hasMediaPosts := false
	for _, postStat := range stats.PostStats {
		if postStat.HasMedia {
			hasMediaPosts = true
			break
		}
	}
	if !hasMediaPosts {
		t.Log("Warning: No media detected in /b/ thread (may be expected)")
	}

	t.Logf("✅ E2E /b/ thread analysis successful:")
	t.Logf("   Posts: %d", stats.TotalPosts)
	t.Logf("   Users: %d (expected 0 for /b/)", stats.UniqueUsers)
	t.Logf("   Reply chains: %d", stats.ReplyChains)
	t.Logf("   Posts with media: %d", countMediaPosts(stats.PostStats))
}

func TestE2E_ConversationAnalysisComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Fetch both real threads
	polThread, err := fetchRealThreadData("pol", "506536140")
	if err != nil {
		t.Skipf("Could not fetch /pol/ thread: %v", err)
	}

	bThread, err := fetchRealThreadData("b", "935426202")
	if err != nil {
		t.Skipf("Could not fetch /b/ thread: %v", err)
	}

	// Setup database
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Analyze both threads
	polAnalyzer := NewConversationAnalyzer(db, "pol", "506536140")
	err = polAnalyzer.AnalyzeThread(*polThread)
	if err != nil {
		t.Fatalf("Failed to analyze /pol/ thread: %v", err)
	}

	bAnalyzer := NewConversationAnalyzer(db, "b", "935426202")
	err = bAnalyzer.AnalyzeThread(*bThread)
	if err != nil {
		t.Fatalf("Failed to analyze /b/ thread: %v", err)
	}

	// Get stats for comparison
	polStats, err := polAnalyzer.GetConversationStats()
	if err != nil {
		t.Fatalf("Failed to get /pol/ stats: %v", err)
	}

	bStats, err := bAnalyzer.GetConversationStats()
	if err != nil {
		t.Fatalf("Failed to get /b/ stats: %v", err)
	}

	// Compare the two thread types
	t.Logf("📊 Thread Comparison:")
	t.Logf("   /pol/ - Posts: %d, Users: %d, Replies: %d", polStats.TotalPosts, polStats.UniqueUsers, polStats.ReplyChains)
	t.Logf("   /b/   - Posts: %d, Users: %d, Replies: %d", bStats.TotalPosts, bStats.UniqueUsers, bStats.ReplyChains)

	// Verify the key difference: /pol/ has users, /b/ doesn't
	if polStats.UniqueUsers == 0 {
		t.Error("/pol/ thread should have unique users (has IDs)")
	}
	if bStats.UniqueUsers != 0 {
		t.Error("/b/ thread should have no unique users (no IDs)")
	}

	// Both should have posts and conversations
	if polStats.TotalPosts == 0 {
		t.Error("/pol/ thread should have posts")
	}
	if bStats.TotalPosts == 0 {
		t.Error("/b/ thread should have posts")
	}

	// Verify data persistence - check total counts in database
	var totalPosts, totalReplies, totalUsers int

	err = db.QueryRow("SELECT COUNT(*) FROM posts").Scan(&totalPosts)
	if err != nil {
		t.Fatal(err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM replies").Scan(&totalReplies)
	if err != nil {
		t.Fatal(err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&totalUsers)
	if err != nil {
		t.Fatal(err)
	}

	expectedPosts := polStats.TotalPosts + bStats.TotalPosts
	if totalPosts != expectedPosts {
		t.Errorf("Database post count mismatch: expected %d, got %d", expectedPosts, totalPosts)
	}

	// Users should only come from /pol/ (has IDs)
	if totalUsers != polStats.UniqueUsers {
		t.Errorf("Database user count mismatch: expected %d (from /pol/), got %d", polStats.UniqueUsers, totalUsers)
	}

	t.Logf("✅ E2E comparison test passed - both thread types analyzed correctly")
	t.Logf("   Database contains: %d posts, %d replies, %d users", totalPosts, totalReplies, totalUsers)
}

func TestE2E_ReplyChainExtraction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Test reply chain extraction on real /b/ thread (typically more conversational)
	threadData, err := fetchRealThreadData("b", "935426202")
	if err != nil {
		t.Skipf("Could not fetch real thread data: %v", err)
	}

	// Setup database
	db, cleanup := setupTestDB(t)
	defer cleanup()

	analyzer := NewConversationAnalyzer(db, "b", "935426202")
	err = analyzer.AnalyzeThread(*threadData)
	if err != nil {
		t.Fatalf("Failed to analyze thread: %v", err)
	}

	// Test specific reply parsing on real posts
	for i, post := range threadData.Posts {
		if post.Com == "" {
			continue
		}

		// Parse reply links from real HTML
		replyLinks := analyzer.parseReplyLinks(post.Com)

		// Verify parsed links point to real posts in the thread
		for _, replyTo := range replyLinks {
			found := false
			for _, p := range threadData.Posts {
				if p.No == replyTo {
					found = true
					break
				}
			}
			if !found {
				t.Logf("Post %d replies to %d which is not in this thread (cross-thread reply)", post.No, replyTo)
			}
		}

		// Test text cleaning on real HTML
		cleanText := analyzer.parser.stripHTML(post.Com)
		if cleanText == "" && post.Com != "" {
			t.Errorf("Post %d: HTML cleaning failed - input had content but output is empty", post.No)
		}

		if i < 5 { // Log first few for verification
			t.Logf("Post %d: Found %d reply links, cleaned text length: %d", post.No, len(replyLinks), len(cleanText))
		}
	}

	t.Logf("✅ Reply chain extraction test passed on real thread data")
}

// Helper function to count posts with media
func countMediaPosts(postStats map[int64]PostStats) int {
	count := 0
	for _, stat := range postStats {
		if stat.HasMedia {
			count++
		}
	}
	return count
}
