package analysis

import (
	"html"
	"regexp"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// ParsedPost represents a parsed 4chan post with extracted relationships
type ParsedPost struct {
	PostNo      int64
	CleanText   string
	ReplyLinks  []int64       // Posts this post replies to
	Mentions    []int64       // Posts this post mentions
	QuoteBlocks []string      // Green text quotes
	CrossLinks  []string      // Cross-board/thread links (>>> format)
	ReplyCount  map[int64]int // Count of how many times each post is mentioned (harassment detection)
}

// ReplyRelation represents a relationship between two posts
type ReplyRelation struct {
	FromPost int64
	ToPost   int64
	Type     string // "direct", "emphasis", "harassment", "mention"
}

// PostAnalyzer handles parsing and analysis of 4chan posts
type PostAnalyzer struct {
	quotelinkRegex *regexp.Regexp
	greenTextRegex *regexp.Regexp
	htmlTagRegex   *regexp.Regexp
}

// NewPostAnalyzer creates a new post analyzer
func NewPostAnalyzer() *PostAnalyzer {
	return &PostAnalyzer{
		quotelinkRegex: regexp.MustCompile(`<a href="#p(\d+)" class="quotelink">&gt;&gt;(\d+)</a>`),
		greenTextRegex: regexp.MustCompile(`<span class="quote">([^<]+)</span>`),
		htmlTagRegex:   regexp.MustCompile(`<[^>]*>`),
	}
}

// ParsePost analyzes a 4chan post's HTML content and extracts relationships
func (pa *PostAnalyzer) ParsePost(postNo int64, htmlComment string) (*ParsedPost, error) {
	parsed := &ParsedPost{
		PostNo:      postNo,
		ReplyLinks:  []int64{},
		Mentions:    []int64{},
		QuoteBlocks: []string{},
		CrossLinks:  []string{},
		ReplyCount:  make(map[int64]int),
	}

	// Extract reply links (>>123456) - allow duplicates for harassment detection
	replyMatches := pa.quotelinkRegex.FindAllStringSubmatch(htmlComment, -1)
	for _, match := range replyMatches {
		if len(match) >= 3 {
			if replyTo, err := strconv.ParseInt(match[2], 10, 64); err == nil {
				parsed.ReplyLinks = append(parsed.ReplyLinks, replyTo)
				parsed.ReplyCount[replyTo]++
			}
		}
	}

	// Extract cross-board/thread links (>>>)
	crossBoardRegex := regexp.MustCompile(`<a href="[^"]*" class="quotelink">&gt;&gt;&gt;([^<]+)</a>`)
	crossMatches := crossBoardRegex.FindAllStringSubmatch(htmlComment, -1)
	for _, match := range crossMatches {
		if len(match) >= 2 {
			parsed.CrossLinks = append(parsed.CrossLinks, match[1])
		}
	}

	// Extract green text quotes
	quoteMatches := pa.greenTextRegex.FindAllStringSubmatch(htmlComment, -1)
	for _, match := range quoteMatches {
		if len(match) >= 2 {
			cleanQuote := html.UnescapeString(match[1])
			parsed.QuoteBlocks = append(parsed.QuoteBlocks, cleanQuote)
		}
	}

	// Clean HTML and get plain text - this will now properly preserve >> formatting
	parsed.CleanText = pa.stripHTML(htmlComment)

	// Look for additional mentions in the cleaned text (>>123456 that weren't in anchor tags)
	mentionRegex := regexp.MustCompile(`>>(\d+)`)
	mentionMatches := mentionRegex.FindAllStringSubmatch(parsed.CleanText, -1)
	for _, match := range mentionMatches {
		if len(match) >= 2 {
			if mention, err := strconv.ParseInt(match[1], 10, 64); err == nil {
				// Check if this mention is not already counted as a reply link
				found := false
				for _, replyTo := range parsed.ReplyLinks {
					if replyTo == mention {
						found = true
						break
					}
				}
				if !found {
					parsed.Mentions = append(parsed.Mentions, mention)
				}
			}
		}
	}

	return parsed, nil
}

// stripHTML removes HTML tags and cleans up text while preserving reply formatting
func (pa *PostAnalyzer) stripHTML(htmlText string) string {
	// First, handle line breaks
	htmlText = strings.ReplaceAll(htmlText, "<br>", "\n")
	htmlText = strings.ReplaceAll(htmlText, "<br/>", "\n")
	htmlText = strings.ReplaceAll(htmlText, "<br />", "\n")

	// Remove <wbr> tags that break URLs
	htmlText = regexp.MustCompile(`<wbr\s*/?>`).ReplaceAllString(htmlText, "")

	// CRITICAL: Preserve reply links in proper format before stripping HTML
	// Replace quotelink anchors with plain >> format
	quotelinkRegex := regexp.MustCompile(`<a href="#p(\d+)" class="quotelink">&gt;&gt;(\d+)</a>`)
	htmlText = quotelinkRegex.ReplaceAllString(htmlText, ">>$2")

	// Handle cross-board links (>>> format) - these link to other threads/boards
	crossBoardRegex := regexp.MustCompile(`<a href="[^"]*" class="quotelink">&gt;&gt;&gt;([^<]+)</a>`)
	htmlText = crossBoardRegex.ReplaceAllString(htmlText, ">>>$1")

	// Handle any remaining &gt;&gt; that weren't in anchor tags
	htmlText = strings.ReplaceAll(htmlText, "&gt;&gt;", ">>")
	htmlText = strings.ReplaceAll(htmlText, "&gt;", ">")

	// Parse with goquery for better HTML handling
	doc, err := goquery.NewDocumentFromReader(strings.NewReader("<div>" + htmlText + "</div>"))
	if err != nil {
		// Fallback to regex if parsing fails
		text := pa.htmlTagRegex.ReplaceAllString(htmlText, "")
		return html.UnescapeString(strings.TrimSpace(text))
	}

	// Extract text - reply links should already be in >> format
	text := doc.Find("div").First().Text()
	text = html.UnescapeString(text)
	text = strings.TrimSpace(text)

	// Clean up multiple newlines
	text = regexp.MustCompile(`\n\s*\n`).ReplaceAllString(text, "\n\n")

	return text
}

// ExtractRelations extracts all reply relationships from a parsed post
func (pa *PostAnalyzer) ExtractRelations(parsed *ParsedPost) []ReplyRelation {
	var relations []ReplyRelation

	// Direct replies (quotelinks) - detect harassment patterns
	for replyTo, count := range parsed.ReplyCount {
		relationType := "direct"
		if count > 3 {
			relationType = "harassment" // Multiple mentions of same post (possible harassment)
		} else if count > 1 {
			relationType = "emphasis" // Post mentioned multiple times for emphasis
		}

		relations = append(relations, ReplyRelation{
			FromPost: parsed.PostNo,
			ToPost:   replyTo,
			Type:     relationType,
		})
	}

	// Mentions (non-linked references)
	for _, mention := range parsed.Mentions {
		relations = append(relations, ReplyRelation{
			FromPost: parsed.PostNo,
			ToPost:   mention,
			Type:     "mention",
		})
	}

	return relations
}

// AnalyzeConversationFlow analyzes the conversation flow in a thread
func (pa *PostAnalyzer) AnalyzeConversationFlow(posts []ParsedPost) map[int64][]int64 {
	replyMap := make(map[int64][]int64)

	for _, post := range posts {
		for _, replyTo := range post.ReplyLinks {
			if replyMap[replyTo] == nil {
				replyMap[replyTo] = []int64{}
			}
			replyMap[replyTo] = append(replyMap[replyTo], post.PostNo)
		}
	}

	return replyMap
}

// FindConversationChains finds conversation chains starting from root posts
func (pa *PostAnalyzer) FindConversationChains(posts []ParsedPost) [][]int64 {
	replyMap := pa.AnalyzeConversationFlow(posts)
	visited := make(map[int64]bool)
	var chains [][]int64

	// Find root posts (posts that are not replies to anything in this thread)
	postNos := make(map[int64]bool)
	for _, post := range posts {
		postNos[post.PostNo] = true
	}

	for _, post := range posts {
		if visited[post.PostNo] {
			continue
		}

		// Check if this post is a root (doesn't reply to any post in this thread)
		isRoot := true
		for _, replyTo := range post.ReplyLinks {
			if postNos[replyTo] {
				isRoot = false
				break
			}
		}

		if isRoot {
			chain := pa.buildChain(post.PostNo, replyMap, visited)
			if len(chain) > 1 { // Only include chains with responses
				chains = append(chains, chain)
			}
		}
	}

	return chains
}

// buildChain recursively builds a conversation chain
func (pa *PostAnalyzer) buildChain(postNo int64, replyMap map[int64][]int64, visited map[int64]bool) []int64 {
	if visited[postNo] {
		return []int64{}
	}

	visited[postNo] = true
	chain := []int64{postNo}

	// Add all replies to this post
	if replies, exists := replyMap[postNo]; exists {
		for _, reply := range replies {
			subChain := pa.buildChain(reply, replyMap, visited)
			chain = append(chain, subChain...)
		}
	}

	return chain
}

// GetTopPosters analyzes posting patterns and returns top contributors
func (pa *PostAnalyzer) GetTopPosters(posts []ParsedPost, userMap map[int64]string) []UserStats {
	userStats := make(map[string]*UserStats)

	for _, post := range posts {
		userID := userMap[post.PostNo]
		if userID == "" {
			userID = "Anonymous"
		}

		if userStats[userID] == nil {
			userStats[userID] = &UserStats{
				UserID:        userID,
				PostCount:     0,
				TotalReplies:  0,
				TotalMentions: 0,
				Posts:         []int64{},
			}
		}

		stats := userStats[userID]
		stats.PostCount++
		stats.Posts = append(stats.Posts, post.PostNo)
		stats.TotalReplies += len(post.ReplyLinks)
		stats.TotalMentions += len(post.Mentions)
	}

	// Convert to slice and sort
	var result []UserStats
	for _, stats := range userStats {
		result = append(result, *stats)
	}

	// Sort by post count (descending)
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].PostCount > result[i].PostCount {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
}

// UserStats represents statistics for a user in a thread
type UserStats struct {
	UserID        string
	PostCount     int
	TotalReplies  int
	TotalMentions int
	Posts         []int64
}

// contains checks if a slice contains a value
func contains(slice []int64, value int64) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}
