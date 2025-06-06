package analysis

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/davidroman0O/4chan-archiver/internal/database"
)

// ConversationAnalyzer handles conversation flow analysis for 4chan threads
type ConversationAnalyzer struct {
	db       *database.Queries
	dbConn   *sql.DB
	parser   *PostAnalyzer
	board    string
	threadID string
}

// ChanPost represents a 4chan post from the API
type ChanPost struct {
	No          int64  `json:"no"`
	Now         string `json:"now"`
	Name        string `json:"name"`
	ID          string `json:"id,omitempty"`           // User ID (pol, etc.)
	Country     string `json:"country,omitempty"`      // Country code
	CountryName string `json:"country_name,omitempty"` // Country name
	Flag        string `json:"flag,omitempty"`         // Custom flag
	FlagName    string `json:"flag_name,omitempty"`    // Custom flag name
	Sub         string `json:"sub,omitempty"`          // Subject
	Com         string `json:"com,omitempty"`          // Comment (HTML)
	Filename    string `json:"filename,omitempty"`     // Original filename
	Ext         string `json:"ext,omitempty"`          // File extension
	W           int    `json:"w,omitempty"`            // Image width
	H           int    `json:"h,omitempty"`            // Image height
	TnW         int    `json:"tn_w,omitempty"`         // Thumbnail width
	TnH         int    `json:"tn_h,omitempty"`         // Thumbnail height
	Tim         int64  `json:"tim,omitempty"`          // Timestamp (for filename)
	Time        int64  `json:"time"`                   // Unix timestamp
	MD5         string `json:"md5,omitempty"`          // MD5 hash
	Fsize       int    `json:"fsize,omitempty"`        // File size
	Resto       int64  `json:"resto"`                  // Thread ID (0 if OP)
}

// ThreadData represents a complete thread with posts
type ThreadData struct {
	Posts []ChanPost `json:"posts"`
}

// ConversationStats represents conversation statistics for a thread
type ConversationStats struct {
	TotalPosts      int                              `json:"total_posts"`
	UniqueUsers     int                              `json:"unique_users"`
	ReplyChains     int                              `json:"reply_chains"`
	MostActiveUser  string                           `json:"most_active_user"`
	MostRepliedPost int64                            `json:"most_replied_post"`
	UserStats       map[string]ConversationUserStats `json:"user_stats"`
	PostStats       map[int64]PostStats              `json:"post_stats"`
}

// ConversationUserStats represents statistics for a specific user in conversation analysis
type ConversationUserStats struct {
	PostCount   int    `json:"post_count"`
	ReplyCount  int    `json:"reply_count"`
	RepliedTo   int    `json:"replied_to"`
	Country     string `json:"country,omitempty"`
	CountryName string `json:"country_name,omitempty"`
	FirstPost   int64  `json:"first_post"`
	LastPost    int64  `json:"last_post"`
}

// PostStats represents statistics for a specific post
type PostStats struct {
	ReplyCount int  `json:"reply_count"`
	HasMedia   bool `json:"has_media"`
	IsOP       bool `json:"is_op"`
	TextLength int  `json:"text_length"`
}

// NewConversationAnalyzer creates a new conversation analyzer
func NewConversationAnalyzer(dbConn *sql.DB, board, threadID string) *ConversationAnalyzer {
	return &ConversationAnalyzer{
		db:       database.New(dbConn),
		dbConn:   dbConn,
		parser:   NewPostAnalyzer(),
		board:    board,
		threadID: threadID,
	}
}

// AnalyzeThread processes a complete thread and extracts conversation data
func (ca *ConversationAnalyzer) AnalyzeThread(threadData ThreadData) error {
	if len(threadData.Posts) == 0 {
		return fmt.Errorf("no posts in thread data")
	}

	// Begin transaction
	tx, err := ca.dbConn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	qtx := ca.db.WithTx(tx)

	// Create or update thread record
	op := threadData.Posts[0]
	now := time.Now()

	_, err = qtx.CreateThread(context.Background(), database.CreateThreadParams{
		ThreadID:    ca.threadID,
		Board:       ca.board,
		Subject:     sql.NullString{String: op.Sub, Valid: op.Sub != ""},
		CreatedAt:   now,
		LastUpdated: now,
		PostsCount:  sql.NullInt64{Int64: int64(len(threadData.Posts)), Valid: true},
		MediaCount:  sql.NullInt64{Int64: ca.countMediaPosts(threadData.Posts), Valid: true},
		Status:      sql.NullString{String: "active", Valid: true},
	})
	if err != nil {
		// Thread might already exist, try to update
		err = qtx.UpdateThread(context.Background(), database.UpdateThreadParams{
			LastUpdated: now,
			PostsCount:  sql.NullInt64{Int64: int64(len(threadData.Posts)), Valid: true},
			MediaCount:  sql.NullInt64{Int64: ca.countMediaPosts(threadData.Posts), Valid: true},
			Status:      sql.NullString{String: "active", Valid: true},
			ThreadID:    ca.threadID,
			Board:       ca.board,
		})
		if err != nil {
			return fmt.Errorf("failed to create/update thread: %w", err)
		}
	}

	// Process each post
	for i, post := range threadData.Posts {
		err = ca.processPost(qtx, post, i == 0)
		if err != nil {
			return fmt.Errorf("failed to process post %d: %w", post.No, err)
		}
	}

	// Extract and store conversation relationships
	err = ca.extractConversationRelationships(qtx, threadData.Posts)
	if err != nil {
		return fmt.Errorf("failed to extract conversations: %w", err)
	}

	// Commit transaction
	return tx.Commit()
}

// processPost stores a single post and updates user information
func (ca *ConversationAnalyzer) processPost(qtx *database.Queries, post ChanPost, isOP bool) error {
	cleanText := ca.parser.stripHTML(post.Com)

	// Create post record
	_, err := qtx.CreatePost(context.Background(), database.CreatePostParams{
		ThreadID:        ca.threadID,
		Board:           ca.board,
		PostNo:          post.No,
		Timestamp:       post.Time,
		Name:            post.Name,
		UserID:          sql.NullString{String: post.ID, Valid: post.ID != ""},
		Country:         sql.NullString{String: post.Country, Valid: post.Country != ""},
		CountryName:     sql.NullString{String: post.CountryName, Valid: post.CountryName != ""},
		Flag:            sql.NullString{String: post.Flag, Valid: post.Flag != ""},
		FlagName:        sql.NullString{String: post.FlagName, Valid: post.FlagName != ""},
		Subject:         sql.NullString{String: post.Sub, Valid: post.Sub != ""},
		Comment:         sql.NullString{String: post.Com, Valid: post.Com != ""},
		CleanText:       sql.NullString{String: cleanText, Valid: cleanText != ""},
		Filename:        sql.NullString{String: post.Filename, Valid: post.Filename != ""},
		FileExt:         sql.NullString{String: post.Ext, Valid: post.Ext != ""},
		FileSize:        sql.NullInt64{Int64: int64(post.Fsize), Valid: post.Fsize > 0},
		ImageWidth:      sql.NullInt64{Int64: int64(post.W), Valid: post.W > 0},
		ImageHeight:     sql.NullInt64{Int64: int64(post.H), Valid: post.H > 0},
		ThumbnailWidth:  sql.NullInt64{Int64: int64(post.TnW), Valid: post.TnW > 0},
		ThumbnailHeight: sql.NullInt64{Int64: int64(post.TnH), Valid: post.TnH > 0},
		Md5Hash:         sql.NullString{String: post.MD5, Valid: post.MD5 != ""},
		IsOp:            sql.NullBool{Bool: isOP, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("failed to create post: %w", err)
	}

	// Update user information if user ID is available
	if post.ID != "" {
		err = qtx.UpsertUser(context.Background(), database.UpsertUserParams{
			Board:       ca.board,
			UserID:      post.ID,
			Name:        post.Name,
			Country:     sql.NullString{String: post.Country, Valid: post.Country != ""},
			CountryName: sql.NullString{String: post.CountryName, Valid: post.CountryName != ""},
			Flag:        sql.NullString{String: post.Flag, Valid: post.Flag != ""},
			FlagName:    sql.NullString{String: post.FlagName, Valid: post.FlagName != ""},
			FirstSeen:   time.Unix(post.Time, 0),
			LastSeen:    time.Unix(post.Time, 0),
		})
		if err != nil {
			return fmt.Errorf("failed to upsert user: %w", err)
		}
	}

	return nil
}

// extractConversationRelationships parses posts to find reply relationships
func (ca *ConversationAnalyzer) extractConversationRelationships(qtx *database.Queries, posts []ChanPost) error {
	for _, post := range posts {
		if post.Com == "" {
			continue
		}

		// Parse reply links from HTML
		replyLinks := ca.parseReplyLinks(post.Com)

		for _, replyTo := range replyLinks {
			// Verify the target post exists in this thread
			if ca.postExistsInThread(posts, replyTo) {
				err := qtx.CreateReply(context.Background(), database.CreateReplyParams{
					ThreadID:  ca.threadID,
					Board:     ca.board,
					FromPost:  post.No,
					ToPost:    replyTo,
					ReplyType: "direct",
				})
				if err != nil {
					// Ignore duplicate key errors
					if !strings.Contains(err.Error(), "UNIQUE constraint failed") {
						return fmt.Errorf("failed to create reply relationship: %w", err)
					}
				}
			}
		}
	}

	return nil
}

// parseReplyLinks extracts reply post numbers from HTML comment
func (ca *ConversationAnalyzer) parseReplyLinks(htmlComment string) []int64 {
	var replyLinks []int64

	// Parse HTML to find quotelinks
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlComment))
	if err != nil {
		// Fallback to regex if HTML parsing fails
		return ca.parseReplyLinksRegex(htmlComment)
	}

	doc.Find("a.quotelink").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists {
			return
		}

		// Extract post number from href like "#p935426252"
		re := regexp.MustCompile(`#p(\d+)`)
		matches := re.FindStringSubmatch(href)
		if len(matches) > 1 {
			if postNo, err := strconv.ParseInt(matches[1], 10, 64); err == nil {
				replyLinks = append(replyLinks, postNo)
			}
		}
	})

	return replyLinks
}

// parseReplyLinksRegex is a fallback regex-based parser
func (ca *ConversationAnalyzer) parseReplyLinksRegex(htmlComment string) []int64 {
	var replyLinks []int64

	// Match patterns like >>935426252
	re := regexp.MustCompile(`&gt;&gt;(\d+)`)
	matches := re.FindAllStringSubmatch(htmlComment, -1)

	for _, match := range matches {
		if len(match) > 1 {
			if postNo, err := strconv.ParseInt(match[1], 10, 64); err == nil {
				replyLinks = append(replyLinks, postNo)
			}
		}
	}

	return replyLinks
}

// postExistsInThread checks if a post number exists in the current thread
func (ca *ConversationAnalyzer) postExistsInThread(posts []ChanPost, postNo int64) bool {
	for _, post := range posts {
		if post.No == postNo {
			return true
		}
	}
	return false
}

// countMediaPosts counts posts with media attachments
func (ca *ConversationAnalyzer) countMediaPosts(posts []ChanPost) int64 {
	count := int64(0)
	for _, post := range posts {
		if post.Filename != "" && post.Ext != "" {
			count++
		}
	}
	return count
}

// GetConversationStats generates comprehensive conversation statistics
func (ca *ConversationAnalyzer) GetConversationStats() (*ConversationStats, error) {
	// Get conversation tree
	conversations, err := ca.db.GetConversationTree(context.Background(), database.GetConversationTreeParams{
		ThreadID: ca.threadID,
		Board:    ca.board,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get conversation tree: %w", err)
	}

	// Get thread posts
	posts, err := ca.db.GetPostsByThread(context.Background(), database.GetPostsByThreadParams{
		ThreadID: ca.threadID,
		Board:    ca.board,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get posts: %w", err)
	}

	// Get most replied posts
	mostReplied, err := ca.db.GetMostRepliedPosts(context.Background(), database.GetMostRepliedPostsParams{
		ThreadID: ca.threadID,
		Board:    ca.board,
		Limit:    10,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get most replied posts: %w", err)
	}

	// Build statistics
	stats := &ConversationStats{
		TotalPosts: len(posts),
		UserStats:  make(map[string]ConversationUserStats),
		PostStats:  make(map[int64]PostStats),
	}

	// Count unique users and build user stats
	uniqueUsers := make(map[string]bool)
	for _, post := range posts {
		if post.UserID.Valid {
			userID := post.UserID.String
			uniqueUsers[userID] = true

			userStat, exists := stats.UserStats[userID]
			if !exists {
				userStat = ConversationUserStats{
					Country:     post.Country.String,
					CountryName: post.CountryName.String,
					FirstPost:   post.Timestamp,
					LastPost:    post.Timestamp,
				}
			}

			userStat.PostCount++
			if post.Timestamp > userStat.LastPost {
				userStat.LastPost = post.Timestamp
			}
			if post.Timestamp < userStat.FirstPost {
				userStat.FirstPost = post.Timestamp
			}

			stats.UserStats[userID] = userStat
		}

		// Build post stats
		postStat := PostStats{
			HasMedia:   post.Filename.Valid,
			IsOP:       post.IsOp.Bool,
			TextLength: len(post.CleanText.String),
		}
		stats.PostStats[post.PostNo] = postStat
	}

	stats.UniqueUsers = len(uniqueUsers)

	// Count reply relationships and find most active user
	replyCount := make(map[string]int)
	repliedToCount := make(map[int64]int)

	for _, conv := range conversations {
		if conv.FromUserID.Valid {
			replyCount[conv.FromUserID.String]++
		}
		repliedToCount[conv.ToPost]++
	}

	// Update user stats with reply counts
	for userID, count := range replyCount {
		if userStat, exists := stats.UserStats[userID]; exists {
			userStat.ReplyCount = count
			stats.UserStats[userID] = userStat
		}
	}

	// Update post stats with reply counts
	for postNo, count := range repliedToCount {
		if postStat, exists := stats.PostStats[postNo]; exists {
			postStat.ReplyCount = count
			stats.PostStats[postNo] = postStat
		}
	}

	// Find most active user
	maxPosts := 0
	for userID, userStat := range stats.UserStats {
		if userStat.PostCount > maxPosts {
			maxPosts = userStat.PostCount
			stats.MostActiveUser = userID
		}
	}

	// Find most replied post
	if len(mostReplied) > 0 {
		stats.MostRepliedPost = mostReplied[0].ToPost
	}

	stats.ReplyChains = len(conversations)

	return stats, nil
}
