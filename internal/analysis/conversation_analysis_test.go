package analysis

import (
	"fmt"
	"testing"
)

// Sample thread data for testing conversation analysis (similar to conversation_test.go)
var testConversationThread = ThreadData{
	Posts: []ChanPost{
		{
			No:       935410578,
			Now:      "06/06/25(Fri)01:30:15",
			Name:     "Anonymous",
			Sub:      "Test Conversation Thread",
			Com:      "Starting a conversation thread for testing purposes",
			Time:     1749188215,
			Resto:    0, // OP post
			Filename: "test_image",
			Ext:      ".jpg",
		},
		{
			No:    935410601,
			Now:   "06/06/25(Fri)01:31:22",
			Name:  "Anonymous",
			Com:   "<a href=\"#p935410578\" class=\"quotelink\">&gt;&gt;935410578</a><br>Interesting thread OP",
			Time:  1749188282,
			Resto: 935410578,
		},
		{
			No:    935410655,
			Now:   "06/06/25(Fri)01:33:45",
			Name:  "Anonymous",
			Com:   "<a href=\"#p935410578\" class=\"quotelink\">&gt;&gt;935410578</a><br><span class=\"quote\">&gt;test purposes</span><br>What exactly are we testing here?",
			Time:  1749188425,
			Resto: 935410578,
		},
		{
			No:    935410708,
			Now:   "06/06/25(Fri)01:35:12",
			Name:  "Anonymous",
			Com:   "<a href=\"#p935410601\" class=\"quotelink\">&gt;&gt;935410601</a><br><a href=\"#p935410655\" class=\"quotelink\">&gt;&gt;935410655</a><br>Both good questions anons",
			Time:  1749188512,
			Resto: 935410578,
		},
		{
			No:    935410750,
			Now:   "06/06/25(Fri)01:36:33",
			Name:  "Anonymous",
			Com:   "<a href=\"#p935410708\" class=\"quotelink\">&gt;&gt;935410708</a><br><a href=\"#p935410708\" class=\"quotelink\">&gt;&gt;935410708</a><br><a href=\"#p935410708\" class=\"quotelink\">&gt;&gt;935410708</a><br><a href=\"#p935410708\" class=\"quotelink\">&gt;&gt;935410708</a><br>STOP POSTING ANON",
			Time:  1749188593,
			Resto: 935410578,
		},
		{
			No:    935410802,
			Now:   "06/06/25(Fri)01:38:15",
			Name:  "Anonymous",
			Com:   "<span class=\"quote\">&gt;tfw testing conversation analysis</span><br><span class=\"quote\">&gt;it actually works</span><br>Based",
			Time:  1749188695,
			Resto: 935410578,
		},
		{
			No:    935410845,
			Now:   "06/06/25(Fri)01:39:41",
			Name:  "Anonymous",
			Com:   "<a href=\"#p935410802\" class=\"quotelink\">&gt;&gt;935410802</a><br>Agreed, this is pretty cool",
			Time:  1749188781,
			Resto: 935410578,
		},
		{
			No:    935410888,
			Now:   "06/06/25(Fri)01:41:03",
			Name:  "Anonymous",
			Com:   ">>935410845<br>>>935410802<br>Raw text replies without HTML links",
			Time:  1749188863,
			Resto: 935410578,
		},
	},
}

// TestConversationAnalysis_DetailedParsing tests the conversation parsing with detailed output
func TestConversationAnalysis_DetailedParsing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping conversation analysis test in short mode")
	}

	fmt.Printf("\n=== CONVERSATION ANALYSIS TEST ===\n")
	fmt.Printf("Testing thread with sample conversation data\n\n")

	thread := testConversationThread
	fmt.Printf("✓ Using thread with %d posts\n\n", len(thread.Posts))

	// Create post analyzer
	analyzer := NewPostAnalyzer()

	// Parse all posts and extract relationships
	var parsedPosts []ParsedPost
	var allRelations []ReplyRelation

	fmt.Printf("=== PARSING POSTS ===\n")
	for i, post := range thread.Posts {
		if post.Com == "" {
			continue // Skip posts without content
		}

		fmt.Printf("\n--- Post #%d (ID: %d) ---\n", i+1, post.No)
		fmt.Printf("Original HTML: %s\n", truncate(post.Com, 200))

		parsed, err := analyzer.ParsePost(post.No, post.Com)
		if err != nil {
			t.Errorf("Failed to parse post %d: %v", post.No, err)
			continue
		}

		fmt.Printf("Clean Text: %s\n", truncate(parsed.CleanText, 200))

		if len(parsed.ReplyLinks) > 0 {
			fmt.Printf("Reply Links: %v\n", parsed.ReplyLinks)
		}

		if len(parsed.Mentions) > 0 {
			fmt.Printf("Mentions: %v\n", parsed.Mentions)
		}

		if len(parsed.CrossLinks) > 0 {
			fmt.Printf("Cross Links: %v\n", parsed.CrossLinks)
		}

		if len(parsed.ReplyCount) > 0 {
			fmt.Printf("Reply Count: %v\n", parsed.ReplyCount)
		}

		if len(parsed.QuoteBlocks) > 0 {
			fmt.Printf("Green Text: %v\n", parsed.QuoteBlocks)
		}

		parsedPosts = append(parsedPosts, *parsed)
		relations := analyzer.ExtractRelations(parsed)
		allRelations = append(allRelations, relations...)
	}

	// Verify we have posts
	if len(parsedPosts) == 0 {
		t.Fatal("No posts were parsed successfully")
	}

	fmt.Printf("\n=== CONVERSATION ANALYSIS RESULTS ===\n")
	fmt.Printf("Total posts analyzed: %d\n", len(parsedPosts))
	fmt.Printf("Total relationships found: %d\n", len(allRelations))

	// Count relationship types
	relationTypes := make(map[string]int)
	for _, rel := range allRelations {
		relationTypes[rel.Type]++
	}

	fmt.Printf("\nRelationship types:\n")
	for relType, count := range relationTypes {
		fmt.Printf("  %s: %d\n", relType, count)
	}

	// Show conversation flow
	replyMap := analyzer.AnalyzeConversationFlow(parsedPosts)
	fmt.Printf("\nConversation flow (posts with replies):\n")
	for postNo, replies := range replyMap {
		if len(replies) > 0 {
			fmt.Printf("  Post %d ← %v\n", postNo, replies)
		}
	}

	// Find conversation chains
	chains := analyzer.FindConversationChains(parsedPosts)
	fmt.Printf("\nConversation chains found: %d\n", len(chains))
	for i, chain := range chains {
		if len(chain) > 1 {
			fmt.Printf("  Chain %d: %v (length: %d)\n", i+1, chain, len(chain))
		}
	}

	// Check for harassment patterns
	harassmentCount := 0
	emphasisCount := 0
	for _, rel := range allRelations {
		switch rel.Type {
		case "harassment":
			harassmentCount++
		case "emphasis":
			emphasisCount++
		}
	}

	fmt.Printf("\nInteraction patterns:\n")
	fmt.Printf("  Harassment patterns (4+ mentions): %d\n", harassmentCount)
	fmt.Printf("  Emphasis patterns (2-3 mentions): %d\n", emphasisCount)

	// Assertions
	if len(parsedPosts) < 5 {
		t.Errorf("Expected at least 5 parsed posts, got %d", len(parsedPosts))
	}

	if len(allRelations) == 0 {
		t.Error("Expected to find some reply relationships")
	}

	// Check that at least some posts have proper >> formatting
	foundProperReplyFormat := false
	for _, post := range parsedPosts {
		if len(post.ReplyLinks) > 0 && post.CleanText != "" {
			// Check if clean text contains >> followed by numbers
			if contains_reply_format(post.CleanText) {
				foundProperReplyFormat = true
				fmt.Printf("\n✓ Found proper reply formatting in post %d: %s\n",
					post.PostNo, truncate(post.CleanText, 100))
				break
			}
		}
	}

	if !foundProperReplyFormat {
		t.Error("No posts found with proper >> reply formatting - parser may not be working correctly")
	}

	fmt.Printf("\n=== TEST COMPLETED ===\n")
	fmt.Printf("✓ Conversation analysis appears to be working correctly\n")
}

// Helper function to truncate strings for display
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// Helper function to check if text contains proper reply formatting
func contains_reply_format(text string) bool {
	for i := 0; i < len(text)-1; i++ {
		if text[i] == '>' && i+1 < len(text) && text[i+1] == '>' {
			// Found >>, check if followed by digits
			j := i + 2
			digitFound := false
			for j < len(text) && text[j] >= '0' && text[j] <= '9' {
				digitFound = true
				j++
			}
			if digitFound {
				return true
			}
		}
	}
	return false
}
