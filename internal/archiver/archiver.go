package archiver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/corpix/uarand"
	"github.com/davidroman0O/4chan-archiver/internal/analysis"
	"github.com/davidroman0O/4chan-archiver/internal/database"
	"github.com/davidroman0O/4chan-archiver/internal/metadata"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sethvargo/go-retry"
	"golang.org/x/time/rate"
)

// Source types
const (
	SourceFourChan    = "4chan"
	SourceArchivedMoe = "archived.moe"
	Source4Plebs      = "4plebs"
	SourceAuto        = "auto"
)

// Base URLs for different sources
const (
	FourChanAPIBaseURL   = "https://a.4cdn.org"
	FourChanMediaBaseURL = "https://i.4cdn.org"
	ArchivedMoeBaseURL   = "https://archived.moe"
	FourPlebsBaseURL     = "https://archive.4plebs.org"
)

// Database modes
const (
	DatabaseModeMemory = "memory"
	DatabaseModeFile   = "file"
	DatabaseModeAuto   = "auto"
)

// Board names
const (
	BoardPol = "pol"
	BoardB   = "b"
)

// File names
const (
	ThreadJSONFileName     = "thread.json"
	PostsJSONFileName      = "posts.json"
	ThreadDBFileName       = "thread.db"
	MetadataFileName       = ".metadata.json"
	ThreadMarkdownFileName = "thread.md"
	MediaDirName           = "media"
)

// Default configuration values
const (
	DefaultDatabaseMode = DatabaseModeFile
	DefaultBoard        = BoardPol
	DefaultSource       = SourceAuto
)

// Config holds configuration for the archiver
type Config struct {
	Board           string
	OutputDir       string
	RateLimitMs     int
	MaxRetries      int
	UserAgent       string
	Verbose         bool
	IncludeContent  bool
	IncludeMedia    bool
	IncludePosts    bool
	IncludeMarkdown bool
	MaxConcurrency  int
	SkipExisting    bool

	// Database configuration
	DatabaseMode string // DatabaseModeMemory, DatabaseModeFile, or DatabaseModeAuto (auto detects test mode)

	// Source configuration
	Source string // SourceFourChan, SourceArchivedMoe, Source4Plebs, or SourceAuto
}

// ArchiveResult represents the result of archiving a single thread
type ArchiveResult struct {
	ThreadID        string
	MediaDownloaded int
	PostsSaved      int
	Error           error
}

// MonitorConfig holds configuration for thread monitoring
type MonitorConfig struct {
	ThreadIDs        []string // Changed from ThreadID to ThreadIDs to support multiple threads
	Interval         time.Duration
	MaxDuration      time.Duration
	StopOnArchive    bool
	StopOnInactivity time.Duration
}

// MonitorState tracks the current state of thread monitoring
type MonitorState struct {
	LastPostNo     int64
	LastPostTime   int64
	LastCheckTime  time.Time
	TotalChecks    int
	NewPostsFound  int
	NewMediaFound  int
	IsActive       bool
	ThreadArchived bool
	LastActivity   time.Time
}

// Archiver handles the archiving process
type Archiver struct {
	config          *Config
	client          *http.Client
	limiter         *rate.Limiter
	metadataManager *metadata.Manager

	// Monitoring state
	stopMonitoring int64 // atomic flag
	monitorState   *MonitorState
	monitorMutex   sync.RWMutex
}

// Thread represents a 4chan thread JSON structure
type Thread struct {
	Posts []Post `json:"posts"`
}

// Post represents a single post in a thread
type Post struct {
	No       int64  `json:"no"`
	Time     int64  `json:"time"`
	Name     string `json:"name,omitempty"`
	Trip     string `json:"trip,omitempty"`
	ID       string `json:"id,omitempty"`
	Subject  string `json:"sub,omitempty"`
	Comment  string `json:"com,omitempty"`
	Tim      int64  `json:"tim,omitempty"`
	Filename string `json:"filename,omitempty"`
	Ext      string `json:"ext,omitempty"`
	Fsize    int    `json:"fsize,omitempty"`
	MD5      string `json:"md5,omitempty"`
	W        int    `json:"w,omitempty"`
	H        int    `json:"h,omitempty"`

	// Additional fields for archived.moe support
	ArchivedMoeURL string `json:"archived_moe_url,omitempty"`

	// Country and flag information
	Country     string `json:"country,omitempty"`
	CountryName string `json:"country_name,omitempty"`
	Flag        string `json:"flag,omitempty"`
	FlagName    string `json:"flag_name,omitempty"`
}

// New creates a new archiver instance
func New(config *Config) (*Archiver, error) {
	if config.Board == "" {
		return nil, fmt.Errorf("board is required")
	}
	if config.OutputDir == "" {
		return nil, fmt.Errorf("output directory is required")
	}

	// Create cookie jar for session management
	jar, err := cookiejar.New(&cookiejar.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to create cookie jar: %w", err)
	}

	// Create HTTP client with timeout, redirect handling, and cookie jar
	client := &http.Client{
		Timeout: 30 * time.Second,
		Jar:     jar, // Enable cookie management
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Allow up to 10 redirects (archived.moe uses redirects for media)
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			// Copy headers to the redirect request
			for key, val := range via[0].Header {
				req.Header[key] = val
			}
			return nil
		},
	}

	// Create rate limiter (requests per second)
	rps := 1000.0 / float64(config.RateLimitMs) // Convert ms to requests per second
	limiter := rate.NewLimiter(rate.Limit(rps), 1)

	// Create metadata manager
	metadataManager := metadata.NewManager(config.OutputDir)

	return &Archiver{
		config:          config,
		client:          client,
		limiter:         limiter,
		metadataManager: metadataManager,
	}, nil
}

// ArchiveThreads archives multiple threads concurrently
func (a *Archiver) ArchiveThreads(threadIDs []string) (map[string]ArchiveResult, error) {
	results := make(map[string]ArchiveResult)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Create semaphore for limiting concurrency
	sem := make(chan struct{}, a.config.MaxConcurrency)

	for _, threadID := range threadIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			result := a.archiveThread(id)

			mu.Lock()
			results[id] = result
			mu.Unlock()
		}(threadID)
	}

	wg.Wait()
	return results, nil
}

// archiveThread archives a single thread
func (a *Archiver) archiveThread(threadID string) ArchiveResult {
	result := ArchiveResult{ThreadID: threadID}

	// Always show basic progress
	fmt.Printf("Archiving thread %s from /%s/...\n", threadID, a.config.Board)

	// Load metadata
	meta, err := a.metadataManager.LoadMetadata(a.config.Board, threadID)
	if err != nil {
		result.Error = fmt.Errorf("failed to load metadata: %w", err)
		return result
	}

	// Fetch thread data
	thread, err := a.fetchThread(threadID)
	if err != nil {
		meta.SetError(err)
		a.metadataManager.SaveMetadata(meta)
		result.Error = fmt.Errorf("failed to fetch thread: %w", err)
		return result
	}

	// Create thread directory
	threadDir := filepath.Join(a.config.OutputDir, a.config.Board, threadID)
	if err := os.MkdirAll(threadDir, 0755); err != nil {
		result.Error = fmt.Errorf("failed to create thread directory: %w", err)
		return result
	}

	// Archive posts
	if a.config.IncludePosts {
		if err := a.savePosts(thread, threadDir, meta); err != nil {
			if a.config.Verbose {
				fmt.Printf("Failed to save posts for thread %s: %v\n", threadID, err)
			}
		}
	}

	// Archive media
	if a.config.IncludeMedia {
		// Create media subdirectory
		mediaDir := filepath.Join(threadDir, MediaDirName)
		os.MkdirAll(mediaDir, 0755)

		mediaCount, err := a.downloadMediaToSubdir(thread, mediaDir, meta)
		if err != nil {
			if a.config.Verbose {
				fmt.Printf("Failed to download some media for thread %s: %v\n", threadID, err)
			}
		}
		result.MediaDownloaded = mediaCount
	}

	result.PostsSaved = len(thread.Posts)

	// Create thread.json for analysis compatibility
	if a.config.IncludeContent {
		threadJSONPath := filepath.Join(threadDir, ThreadJSONFileName)
		if err := os.WriteFile(threadJSONPath, []byte("{}"), 0644); err == nil {
			// Convert thread data and save as thread.json
			threadData, err := json.MarshalIndent(thread, "", "  ")
			if err == nil {
				os.WriteFile(threadJSONPath, threadData, 0644)
			}
		}
	}

	// Create database and perform conversation analysis using proper sqlc
	if a.config.IncludePosts {
		err = a.performSqlcConversationAnalysis(thread, threadDir, threadID)
		if err != nil {
			if a.config.Verbose {
				fmt.Printf("Failed to perform conversation analysis for thread %s: %v\n", threadID, err)
			}
		}
	}

	// Create metadata file
	if a.config.IncludeContent {
		metadataPath := filepath.Join(threadDir, MetadataFileName)
		metadata := map[string]interface{}{
			"thread_id":        threadID,
			"board":            a.config.Board,
			"archived_at":      time.Now().UTC(),
			"posts_saved":      result.PostsSaved,
			"media_downloaded": result.MediaDownloaded,
			"archiver_version": "1.0.0",
		}
		if data, err := json.MarshalIndent(metadata, "", "  "); err == nil {
			os.WriteFile(metadataPath, data, 0644)
		}
	}

	// Create markdown export if requested using enhanced database approach
	if a.config.IncludeMarkdown {
		markdownPath := filepath.Join(threadDir, ThreadMarkdownFileName)

		// Use the enhanced database-driven markdown generation
		err := a.createEnhancedMarkdownExport(threadID, threadDir, markdownPath)
		if err != nil {
			if a.config.Verbose {
				fmt.Printf("Failed to create enhanced markdown export for thread %s: %v\n", threadID, err)
			}
			// Fallback to basic markdown if enhanced fails
			if err := a.createMarkdownExport(thread, markdownPath, threadID); err != nil {
				if a.config.Verbose {
					fmt.Printf("Failed to create fallback markdown export for thread %s: %v\n", threadID, err)
				}
			}
		}
	}

	meta.SetStatus("completed")

	// Save final metadata
	if err := a.metadataManager.SaveMetadata(meta); err != nil {
		fmt.Printf("Warning: failed to save metadata for thread %s: %v\n", threadID, err)
	}

	// Always show completion
	fmt.Printf("✓ Completed thread %s: %d posts, %d media files\n", threadID, result.PostsSaved, result.MediaDownloaded)

	return result
}

// fetchThread retrieves thread data using source-specific parsers with proper fallback
func (a *Archiver) fetchThread(threadID string) (*Thread, error) {
	if a.config.Verbose {
		fmt.Printf("Fetching thread %s with source strategy: %s\n", threadID, a.config.Source)
	}

	switch a.config.Source {
	case SourceFourChan:
		if a.config.Verbose {
			fmt.Printf("Using dedicated 4chan fetcher and parser\n")
		}
		return a.fetchFromFourChan(threadID)

	case SourceArchivedMoe:
		if a.config.Verbose {
			fmt.Printf("Using dedicated archived.moe fetcher and parser\n")
		}
		return a.fetchFromArchivedMoe(threadID)

	case Source4Plebs:
		if a.config.Verbose {
			fmt.Printf("Using dedicated 4plebs fetcher and parser\n")
		}
		return a.fetchFrom4Plebs(threadID)

	case SourceAuto:
		// Smart fallback with proper source tracking
		if a.config.Verbose {
			fmt.Printf("Auto-detection mode: trying sources in order: 4chan -> archived.moe -> 4plebs\n")
		}

		// Try 4chan first (fastest and most reliable)
		if a.config.Verbose {
			fmt.Printf("🔍 [Auto 1/3] Trying 4chan API...\n")
		}
		thread, err := a.fetchFromFourChan(threadID)
		if err == nil {
			if a.config.Verbose {
				fmt.Printf("✅ [Auto] Success with 4chan API (%d posts)\n", len(thread.Posts))
			}
			// Update config to remember successful source for this session
			a.config.Source = SourceFourChan
			return thread, nil
		}

		// Check if it's a permanent 4chan error (thread doesn't exist there)
		errorStr := strings.ToLower(err.Error())
		shouldTryArchives := strings.Contains(errorStr, "thread not found (404)") ||
			strings.Contains(errorStr, "http 403") ||
			strings.Contains(errorStr, "http 404") ||
			strings.Contains(errorStr, "forbidden") ||
			strings.Contains(errorStr, "not found") ||
			strings.Contains(errorStr, "thread does not exist") ||
			strings.Contains(errorStr, "thread has no posts") ||
			strings.Contains(errorStr, "received html error page")

		if shouldTryArchives {
			if a.config.Verbose {
				fmt.Printf("❌ [Auto] 4chan failed (%v), trying archived.moe...\n", err)
			}

			// Try archived.moe second
			if a.config.Verbose {
				fmt.Printf("🔍 [Auto 2/3] Trying archived.moe...\n")
			}
			thread, err = a.fetchFromArchivedMoe(threadID)
			if err == nil {
				if a.config.Verbose {
					fmt.Printf("✅ [Auto] Success with archived.moe (%d posts)\n", len(thread.Posts))
				}
				// Update config to remember successful source
				a.config.Source = SourceArchivedMoe
				return thread, nil
			}

			if a.config.Verbose {
				fmt.Printf("❌ [Auto] archived.moe failed (%v), trying 4plebs...\n", err)
			}

			// Try 4plebs last
			if a.config.Verbose {
				fmt.Printf("🔍 [Auto 3/3] Trying 4plebs...\n")
			}
			thread, err = a.fetchFrom4Plebs(threadID)
			if err == nil {
				if a.config.Verbose {
					fmt.Printf("✅ [Auto] Success with 4plebs (%d posts)\n", len(thread.Posts))
				}
				// Update config to remember successful source
				a.config.Source = Source4Plebs
				return thread, nil
			}

			if a.config.Verbose {
				fmt.Printf("❌ [Auto] All sources failed. Last error from 4plebs: %v\n", err)
			}
			return nil, fmt.Errorf("thread %s not found on any source (4chan, archived.moe, 4plebs)", threadID)
		} else {
			// For non-permanent 4chan errors, just return the 4chan error
			return nil, fmt.Errorf("4chan error: %w", err)
		}

	default:
		// Default to 4chan if unknown source
		if a.config.Verbose {
			fmt.Printf("Unknown source '%s', defaulting to 4chan\n", a.config.Source)
		}
		return a.fetchFromFourChan(threadID)
	}
}

// fetchFromFourChan retrieves thread data from 4chan API
func (a *Archiver) fetchFromFourChan(threadID string) (*Thread, error) {
	// Wait for rate limiter
	a.limiter.Wait(context.Background())

	url := fmt.Sprintf("%s/%s/thread/%s.json", FourChanAPIBaseURL, a.config.Board, threadID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Set user agent
	userAgent := a.config.UserAgent
	if userAgent == "" {
		userAgent = uarand.GetRandom()
	}
	req.Header.Set("User-Agent", userAgent)

	var thread Thread
	for attempt := 0; attempt < a.config.MaxRetries; attempt++ {
		resp, err := a.client.Do(req)
		if err != nil {
			if attempt == a.config.MaxRetries-1 {
				return nil, err
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil, fmt.Errorf("thread not found (404)")
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			if attempt == a.config.MaxRetries-1 {
				return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		// Read the response body to check for error messages
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			if attempt == a.config.MaxRetries-1 {
				return nil, err
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		// Check for common 4chan error responses
		bodyStr := string(body)
		if strings.Contains(bodyStr, "Nothing Found") ||
			strings.Contains(bodyStr, "404 Not Found") ||
			strings.Contains(bodyStr, "Thread not found") ||
			strings.Contains(bodyStr, "Thread does not exist") ||
			len(bodyStr) < 10 { // Empty or very short response
			return nil, fmt.Errorf("thread not found - response: %s", bodyStr[:min(100, len(bodyStr))])
		}

		// Try to decode JSON
		err = json.Unmarshal(body, &thread)
		if err != nil {
			// If JSON decode fails, it might be an HTML error page
			if strings.Contains(bodyStr, "<html>") || strings.Contains(bodyStr, "<!DOCTYPE") {
				return nil, fmt.Errorf("received HTML error page instead of JSON - thread likely doesn't exist")
			}

			if attempt == a.config.MaxRetries-1 {
				return nil, fmt.Errorf("JSON decode error: %w - response: %s", err, bodyStr[:min(200, len(bodyStr))])
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		// Validate that we got actual thread data
		if len(thread.Posts) == 0 {
			return nil, fmt.Errorf("thread has no posts - likely deleted or archived")
		}

		break
	}

	return &thread, nil
}

// fetchFromArchivedMoe retrieves thread data from archived.moe by scraping HTML
func (a *Archiver) fetchFromArchivedMoe(threadID string) (*Thread, error) {
	// Wait for rate limiter
	a.limiter.Wait(context.Background())

	url := fmt.Sprintf("%s/%s/thread/%s", ArchivedMoeBaseURL, a.config.Board, threadID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Set user agent
	userAgent := a.config.UserAgent
	if userAgent == "" {
		userAgent = uarand.GetRandom()
	}
	req.Header.Set("User-Agent", userAgent)

	for attempt := 0; attempt < a.config.MaxRetries; attempt++ {
		resp, err := a.client.Do(req)
		if err != nil {
			if attempt == a.config.MaxRetries-1 {
				return nil, err
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusForbidden {
			resp.Body.Close()
			if attempt == a.config.MaxRetries-1 {
				return nil, fmt.Errorf("archived.moe returned 403 Forbidden")
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			retryAfter := resp.Header.Get("Retry-After")
			if retryAfter != "" {
				if seconds, err := strconv.Atoi(retryAfter); err == nil {
					time.Sleep(time.Duration(seconds) * time.Second)
					continue
				}
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil, fmt.Errorf("thread not found on archived.moe (404)")
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			if attempt == a.config.MaxRetries-1 {
				return nil, fmt.Errorf("archived.moe returned HTTP %d", resp.StatusCode)
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		// Parse HTML using goquery
		doc, err := goquery.NewDocumentFromReader(resp.Body)
		resp.Body.Close()
		if err != nil {
			if attempt == a.config.MaxRetries-1 {
				return nil, fmt.Errorf("failed to parse archived.moe HTML: %w", err)
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		// Convert archived.moe HTML to Thread structure
		thread, err := a.parseArchivedMoeHTML(doc, threadID)
		if err != nil {
			if attempt == a.config.MaxRetries-1 {
				return nil, fmt.Errorf("failed to parse archived.moe thread: %w", err)
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		return thread, nil
	}

	return nil, fmt.Errorf("max retries reached for archived.moe")
}

// parseArchivedMoeHTML converts archived.moe HTML to Thread structure
func (a *Archiver) parseArchivedMoeHTML(doc *goquery.Document, threadID string) (*Thread, error) {
	if a.config.Verbose {
		fmt.Printf("Parsing archived.moe HTML for thread %s\n", threadID)
	}

	// archived.moe uses a similar structure to 4plebs, so we can leverage the unified parser
	// But first, let's try to detect the actual HTML structure

	// Check if this looks like 4plebs/archived.moe format (article.post elements)
	if doc.Find("article.post, article.thread").Length() > 0 {
		if a.config.Verbose {
			fmt.Printf("Detected 4plebs-like format in archived.moe, using 4plebs parser\n")
		}
		return a.Parse4PlebsHTML(doc, fmt.Sprintf("archived.moe://%s/thread/%s", a.config.Board, threadID))
	}

	// Check if this looks like 4chan format (div.postContainer elements)
	if doc.Find("div.postContainer").Length() > 0 {
		if a.config.Verbose {
			fmt.Printf("Detected 4chan-like format in archived.moe, using 4chan parser\n")
		}
		return a.Parse4ChanHTML(doc, fmt.Sprintf("archived.moe://%s/thread/%s", a.config.Board, threadID))
	}

	// If we can't detect the format, try to parse any post-like elements
	thread := &Thread{Posts: []Post{}}

	// Look for common post patterns
	postSelectors := []string{
		"article.post",
		"article.thread",
		"div.postContainer",
		"div.post",
		".post",
		"[id^='p']", // Elements with IDs starting with 'p' (4chan style)
	}

	postsFound := false
	for _, selector := range postSelectors {
		elements := doc.Find(selector)
		if elements.Length() > 0 {
			if a.config.Verbose {
				fmt.Printf("Found %d elements with selector '%s'\n", elements.Length(), selector)
			}

			elements.Each(func(i int, s *goquery.Selection) {
				var post *Post

				// Try different parsing methods based on the element structure
				if strings.Contains(selector, "article") {
					post = a.parse4PlebsPost(s, i == 0)
				} else {
					post = a.parse4ChanPost(s, i == 0)
				}

				if post != nil {
					thread.Posts = append(thread.Posts, *post)
					postsFound = true
				}
			})

			if postsFound {
				break // Use the first selector that found posts
			}
		}
	}

	if !postsFound {
		return nil, fmt.Errorf("no posts found in archived.moe page - unknown HTML structure")
	}

	if len(thread.Posts) == 0 {
		return nil, fmt.Errorf("no valid posts extracted from archived.moe page")
	}

	if a.config.Verbose {
		fmt.Printf("Successfully parsed %d posts from archived.moe\n", len(thread.Posts))
	}

	return thread, nil
}

// savePosts saves thread posts to a JSON file
func (a *Archiver) savePosts(thread *Thread, threadDir string, meta *metadata.ThreadMetadata) error {
	postsFile := filepath.Join(threadDir, PostsJSONFileName)

	// Check if we should skip existing
	if a.config.SkipExisting {
		if _, err := os.Stat(postsFile); err == nil {
			if a.config.Verbose {
				fmt.Printf("Posts file already exists, skipping: %s\n", postsFile)
			}
			return nil
		}
	}

	data, err := json.MarshalIndent(thread, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(postsFile, data, 0644); err != nil {
		return err
	}

	// Update metadata
	for _, post := range thread.Posts {
		postID := strconv.FormatInt(post.No, 10)
		hasMedia := post.Tim != 0 && post.Ext != ""
		meta.AddSavedPost(postID, post.Time, hasMedia)
	}

	return nil
}

// downloadFile downloads a single file with proper retry and backoff logic
func (a *Archiver) downloadFile(url, localPath string) (int64, error) {
	var finalSize int64
	var finalErr error
	var attemptCount int

	// Use exponential backoff with proper retry logic
	backoff := retry.WithMaxRetries(uint64(a.config.MaxRetries), retry.NewExponential(2*time.Second))

	err := retry.Do(context.Background(), backoff, func(ctx context.Context) error {
		attemptCount++

		// Wait for rate limiter before each attempt
		if err := a.limiter.Wait(ctx); err != nil {
			return retry.RetryableError(fmt.Errorf("rate limiter wait failed: %w", err))
		}

		// Create request with realistic browser headers
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return fmt.Errorf("error creating request: %w", err)
		}

		// Generate realistic browser headers after multiple failures
		a.setRealisticHeaders(req, attemptCount)

		if a.config.Verbose {
			fmt.Printf("Attempting download: %s\n", url)
		}

		resp, err := a.client.Do(req)
		if err != nil {
			return retry.RetryableError(fmt.Errorf("request failed: %w", err))
		}
		defer resp.Body.Close()

		// Handle different HTTP status codes
		switch resp.StatusCode {
		case http.StatusTooManyRequests:
			retryAfter := resp.Header.Get("Retry-After")
			if retryAfter != "" {
				if seconds, err := strconv.Atoi(retryAfter); err == nil {
					time.Sleep(time.Duration(seconds) * time.Second)
				}
			}
			return retry.RetryableError(fmt.Errorf("rate limited (429)"))

		case http.StatusForbidden:
			// 403 could be temporary blocking, retry with new UA and extra delay
			if a.config.Verbose {
				body, _ := io.ReadAll(resp.Body)
				fmt.Printf("Received 403 for %s (attempt %d), body: %s\n", url, attemptCount, string(body)[:min(100, len(body))])
			}

			// Add extra delay for repeated 403s to avoid further triggering rate limits
			if attemptCount >= 3 {
				extraDelay := time.Duration(attemptCount*2) * time.Second
				if a.config.Verbose {
					fmt.Printf("Adding extra delay of %v after %d 403 responses\n", extraDelay, attemptCount)
				}
				time.Sleep(extraDelay)
			}

			return retry.RetryableError(fmt.Errorf("forbidden (403), retrying with enhanced headers"))

		case http.StatusNotFound:
			// 404 is permanent, don't retry
			return fmt.Errorf("not found (404)")

		case http.StatusOK:
			// Success case, continue processing
		default:
			// Other errors are retryable
			return retry.RetryableError(fmt.Errorf("HTTP %d", resp.StatusCode))
		}

		// Check if we got HTML redirect page (archived.moe meta refresh)
		contentType := resp.Header.Get("Content-Type")
		if strings.Contains(contentType, "text/html") || resp.ContentLength < 1000 {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return retry.RetryableError(fmt.Errorf("error reading response: %w", err))
			}

			// Check for meta refresh redirect (archived.moe pattern)
			bodyStr := string(body)
			if strings.Contains(bodyStr, "meta http-equiv=\"Refresh\"") {
				start := strings.Index(bodyStr, "url=")
				if start != -1 {
					start += 4
					end := strings.Index(bodyStr[start:], "\"")
					if end != -1 {
						actualURL := strings.TrimSpace(bodyStr[start : start+end])
						if a.config.Verbose {
							fmt.Printf("Following meta refresh redirect from %s to %s\n", url, actualURL)
						}
						// Recursively download from the actual URL
						size, err := a.downloadFile(actualURL, localPath)
						if err != nil {
							return err
						}
						finalSize = size
						return nil
					}
				}
			}

			// Not a redirect, probably an error page
			return retry.RetryableError(fmt.Errorf("received HTML content: %s", bodyStr[:min(200, len(bodyStr))]))
		}

		// Create the file for media content
		out, err := os.Create(localPath)
		if err != nil {
			return fmt.Errorf("error creating file: %w", err)
		}
		defer out.Close()

		// Copy response body to file
		size, err := io.Copy(out, resp.Body)
		if err != nil {
			return retry.RetryableError(fmt.Errorf("error copying file: %w", err))
		}

		// Validate downloaded content isn't corrupted HTML
		if size < 1000 {
			if data, err := os.ReadFile(localPath); err == nil {
				contentType := http.DetectContentType(data)
				if strings.Contains(contentType, "text/") || strings.Contains(contentType, "html") {
					os.Remove(localPath) // Remove corrupted file
					return retry.RetryableError(fmt.Errorf("downloaded HTML/text instead of media (size: %d)", size))
				}
			}
		}

		if a.config.Verbose {
			fmt.Printf("Successfully downloaded %s (%d bytes)\n", url, size)
		}

		finalSize = size
		return nil
	})

	if err != nil {
		finalErr = err
	}

	return finalSize, finalErr
}

// setRealisticHeaders sets realistic browser headers, escalating after failures
func (a *Archiver) setRealisticHeaders(req *http.Request, attemptCount int) {
	// Always get a fresh random user agent for each request
	freshUA := uarand.GetRandom()

	// Base headers that work well for most sites
	headers := map[string]string{
		"User-Agent":                freshUA,
		"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Accept-Language":           "en-US,en;q=0.5",
		"Accept-Encoding":           "gzip, deflate, br",
		"Connection":                "keep-alive",
		"Upgrade-Insecure-Requests": "1",
		"Cache-Control":             "max-age=0",
		"DNT":                       "1",
	}

	// Add some randomization to make each request unique
	if rand.Intn(2) == 0 {
		headers["Accept-Language"] = "en-US,en;q=0.9"
	}
	if rand.Intn(3) == 0 {
		headers["Cache-Control"] = "no-cache"
	}

	// Vary headers based on attempt to evade detection
	switch attemptCount % 4 {
	case 0:
		// Chrome-like headers
		headers["Sec-Fetch-Dest"] = "document"
		headers["Sec-Fetch-Mode"] = "navigate"
		headers["Sec-Fetch-Site"] = "none"
		headers["Sec-Fetch-User"] = "?1"
		headers["Sec-Ch-Ua"] = `"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"`
		headers["Sec-Ch-Ua-Mobile"] = "?0"
		headers["Sec-Ch-Ua-Platform"] = `"macOS"`

	case 1:
		// Firefox-like headers
		headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
		headers["Accept-Language"] = "en-US,en;q=0.9"
		headers["Sec-Fetch-Dest"] = "document"
		headers["Sec-Fetch-Mode"] = "navigate"
		headers["Sec-Fetch-Site"] = "cross-site"

	case 2:
		// Safari-like headers
		headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
		headers["Accept-Language"] = "en-US,en;q=0.9"
		headers["Sec-Ch-Ua"] = `"Safari";v="17", "Not;A=Brand";v="8"`
		headers["Sec-Ch-Ua-Mobile"] = "?0"
		headers["Sec-Ch-Ua-Platform"] = `"macOS"`

	case 3:
		// Edge-like headers with some variation
		headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
		headers["Accept-Language"] = "en-US,en;q=0.9"
		headers["Sec-Ch-Ua"] = `"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"`
		headers["Sec-Ch-Ua-Mobile"] = "?0"
		headers["Sec-Ch-Ua-Platform"] = `"Windows"`
		headers["Sec-Fetch-Dest"] = "document"
		headers["Sec-Fetch-Mode"] = "navigate"
		headers["Sec-Fetch-Site"] = "none"
		headers["Sec-Fetch-User"] = "?1"
	}

	// For 4plebs specifically, add referrer after first attempt
	if strings.Contains(req.URL.Host, "4plebs") && attemptCount > 1 {
		headers["Referer"] = "https://archive.4plebs.org/"
		// Vary some 4plebs-specific headers with randomization
		if attemptCount%2 == 0 {
			headers["Accept-Language"] = "en-US,en;q=0.8,*;q=0.6"
			headers["Cache-Control"] = "no-cache"
			headers["Pragma"] = "no-cache"
		}
		// Occasionally add some additional headers to look more like a real session
		if rand.Intn(3) == 0 {
			headers["X-Forwarded-For"] = fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255))
		}
	}

	// For archived.moe
	if strings.Contains(req.URL.Host, "archived.moe") && attemptCount > 1 {
		headers["Referer"] = "https://archived.moe/"
	}

	// Apply all headers to the request
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	if a.config.Verbose && attemptCount > 1 {
		fmt.Printf("Attempt %d for %s: using rotated headers (type %d) with fresh UA\n", attemptCount, req.URL.String(), attemptCount%4)
	}
}

// downloadMediaToSubdir downloads media to the media subdirectory with proper filename format
func (a *Archiver) downloadMediaToSubdir(thread *Thread, mediaDir string, meta *metadata.ThreadMetadata) (int, error) {
	downloaded := 0

	// Count total media files to download
	totalMedia := 0
	for _, post := range thread.Posts {
		if post.Tim != 0 && post.Ext != "" {
			totalMedia++
		}
	}

	if totalMedia > 0 {
		fmt.Printf("Found %d media files to download\n", totalMedia)
	}

	currentFile := 0
	for _, post := range thread.Posts {
		if post.Tim == 0 || post.Ext == "" {
			continue // No media file
		}

		currentFile++
		var mediaURL string
		var filename string

		// Handle different sources
		if a.config.Source == SourceArchivedMoe || post.ArchivedMoeURL != "" {
			// For archived.moe, use the URL we extracted during HTML parsing
			if post.ArchivedMoeURL != "" {
				mediaURL = post.ArchivedMoeURL
			} else {
				// This shouldn't happen with the new parser, but fallback just in case
				mediaURL = fmt.Sprintf("%s/%s/%s%s", ArchivedMoeBaseURL, a.config.Board, post.Filename, post.Ext)
			}
			filename = fmt.Sprintf("%d_%s%s", post.No, post.Filename, post.Ext)
		} else {
			// 4chan URLs
			mediaURL = fmt.Sprintf("%s/%s/%d%s", FourChanMediaBaseURL, a.config.Board, post.Tim, post.Ext)
			filename = fmt.Sprintf("%d_%s%s", post.No, post.Filename, post.Ext)
		}

		localPath := filepath.Join(mediaDir, filename)

		// Check if already downloaded
		if meta.IsMediaDownloaded(mediaURL) && a.config.SkipExisting {
			if a.config.Verbose {
				fmt.Printf("Media already downloaded, skipping: %s\n", filename)
			}
			continue
		}

		// Check if file exists on disk
		if a.config.SkipExisting {
			if _, err := os.Stat(localPath); err == nil {
				if a.config.Verbose {
					fmt.Printf("Media file already exists, skipping: %s\n", filename)
				}
				continue
			}
		}

		// Show download progress with counter
		fmt.Printf("[%d/%d] Downloading: %s\n", currentFile, totalMedia, filename)

		size, err := a.downloadFile(mediaURL, localPath)
		if err != nil {
			fmt.Printf("Failed to download %s: %v\n", mediaURL, err)
			continue
		}

		meta.AddDownloadedMedia(mediaURL, filename, size)
		downloaded++
	}

	return downloaded, nil
}

// performSqlcConversationAnalysis performs conversation analysis using proper sqlc database approach
func (a *Archiver) performSqlcConversationAnalysis(thread *Thread, threadDir, threadID string) error {
	// Determine database mode
	var useMemoryDB bool
	switch a.config.DatabaseMode {
	case DatabaseModeMemory:
		useMemoryDB = true
	case DatabaseModeFile:
		useMemoryDB = false
	case DatabaseModeAuto:
		// Auto-detect test mode
		useMemoryDB = strings.Contains(threadDir, "_test_") || os.Getenv("GO_TEST_MODE") == "1"
	default:
		// Default to file-based
		useMemoryDB = false
	}

	// Set database path
	dbPath := filepath.Join(threadDir, ThreadDBFileName)
	if useMemoryDB {
		dbPath = ":memory:"
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	// Execute the schema from 001_init.sql
	// Try multiple possible paths to find the schema file
	schemaPaths := []string{
		"internal/database/schema/001_init.sql",
		"../database/schema/001_init.sql",
		"../../database/schema/001_init.sql",
		"database/schema/001_init.sql",
	}

	var schemaBytes []byte
	for _, path := range schemaPaths {
		if bytes, err := os.ReadFile(path); err == nil {
			schemaBytes = bytes
			break
		}
	}

	if schemaBytes == nil {
		return fmt.Errorf("failed to find schema file in any of the expected locations")
	}

	_, err = db.Exec(string(schemaBytes))
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Use sqlc queries to insert data
	queries := database.New(db)

	// Insert thread record
	_, err = queries.CreateThread(context.Background(), database.CreateThreadParams{
		ThreadID:    threadID,
		Board:       a.config.Board,
		Subject:     getThreadSubject(thread),
		CreatedAt:   time.Now(),
		LastUpdated: time.Now(),
		PostsCount:  sql.NullInt64{Valid: true, Int64: int64(len(thread.Posts))},
		MediaCount:  sql.NullInt64{Valid: true, Int64: int64(countMediaPosts(thread))},
		Status:      sql.NullString{Valid: true, String: "archived"},
	})
	if err != nil {
		// Ignore unique constraint errors
		if a.config.Verbose {
			fmt.Printf("Warning: Failed to insert thread record: %v\n", err)
		}
	}

	// Insert posts and analyze conversations
	for i, post := range thread.Posts {
		isOP := i == 0

		// Insert post using sqlc
		_, err = queries.CreatePost(context.Background(), database.CreatePostParams{
			ThreadID:        threadID,
			Board:           a.config.Board,
			PostNo:          post.No,
			Timestamp:       post.Time,
			Name:            post.Name,
			UserID:          stringToSqlNull(post.ID),
			Subject:         stringToSqlNull(post.Subject),
			Comment:         stringToSqlNull(post.Comment),
			CleanText:       stringToSqlNull(cleanHTMLText(post.Comment)),
			Filename:        stringToSqlNull(post.Filename),
			FileExt:         stringToSqlNull(post.Ext),
			FileSize:        intToSqlNull(int64(post.Fsize)),
			ImageWidth:      intToSqlNull(int64(post.W)),
			ImageHeight:     intToSqlNull(int64(post.H)),
			ThumbnailWidth:  sql.NullInt64{},
			ThumbnailHeight: sql.NullInt64{},
			Md5Hash:         stringToSqlNull(post.MD5),
			IsOp:            sql.NullBool{Valid: true, Bool: isOP},
		})
		if err != nil && a.config.Verbose {
			fmt.Printf("Warning: Failed to insert post %d: %v\n", post.No, err)
		}

		// Insert user if has ID (for /pol/)
		if post.ID != "" {
			err = queries.UpsertUser(context.Background(), database.UpsertUserParams{
				Board:     a.config.Board,
				UserID:    post.ID,
				Name:      post.Name,
				FirstSeen: time.Unix(post.Time, 0),
				LastSeen:  time.Unix(post.Time, 0),
			})
			if err != nil && a.config.Verbose {
				fmt.Printf("Warning: Failed to upsert user %s: %v\n", post.ID, err)
			}
		}

		// Parse reply relationships (>>123456)
		if post.Comment != "" {
			replyNumbers := extractReplyNumbers(post.Comment)
			for _, replyTo := range replyNumbers {
				// Check if reply already exists
				count, err := queries.ReplyExists(context.Background(), database.ReplyExistsParams{
					ThreadID:  threadID,
					Board:     a.config.Board,
					FromPost:  post.No,
					ToPost:    replyTo,
					ReplyType: "direct",
				})
				if err != nil && a.config.Verbose {
					fmt.Printf("Warning: Failed to check reply existence %d->%d: %v\n", post.No, replyTo, err)
					continue
				}

				// Only insert if it doesn't exist
				if count == 0 {
					err = queries.CreateReply(context.Background(), database.CreateReplyParams{
						ThreadID:  threadID,
						Board:     a.config.Board,
						FromPost:  post.No,
						ToPost:    replyTo,
						ReplyType: "direct",
					})
					if err != nil && a.config.Verbose {
						fmt.Printf("Warning: Failed to insert reply %d->%d: %v\n", post.No, replyTo, err)
					}
				}
			}
		}

		// Insert media record if post has media
		if post.Tim != 0 && post.Ext != "" {
			mediaFilename := fmt.Sprintf("%d_%s%s", post.No, post.Filename, post.Ext)
			localPath := filepath.Join(MediaDirName, mediaFilename)

			_, err = queries.CreateMedia(context.Background(), database.CreateMediaParams{
				ThreadID:         threadID,
				Board:            a.config.Board,
				PostNo:           post.No,
				Filename:         mediaFilename,
				OriginalFilename: stringToSqlNull(post.Filename + post.Ext),
				FileExt:          stringToSqlNull(post.Ext),
				FileSize:         intToSqlNull(int64(post.Fsize)),
				Width:            intToSqlNull(int64(post.W)),
				Height:           intToSqlNull(int64(post.H)),
				Md5Hash:          stringToSqlNull(post.MD5),
				LocalPath:        stringToSqlNull(localPath),
				DownloadStatus:   sql.NullString{Valid: true, String: "downloaded"},
			})
			if err != nil && a.config.Verbose {
				fmt.Printf("Warning: Failed to insert media record for post %d: %v\n", post.No, err)
			}
		}
	}

	// For memory databases: copy to disk for validation if needed
	if useMemoryDB && dbPath == ":memory:" {
		diskDBPath := filepath.Join(threadDir, ThreadDBFileName)
		err = copyMemoryDBToDisk(db, diskDBPath)
		if err != nil && a.config.Verbose {
			fmt.Printf("Warning: Failed to copy database to disk: %v\n", err)
		}
	}

	return nil
}

// copyMemoryDBToDisk copies an in-memory SQLite database to a disk file
func copyMemoryDBToDisk(memDB *sql.DB, diskPath string) error {
	// Create disk database
	diskDB, err := sql.Open("sqlite3", diskPath)
	if err != nil {
		return err
	}
	defer diskDB.Close()

	// Read schema from in-memory database and execute on disk database
	schemaBytes, err := os.ReadFile("internal/database/schema/001_init.sql")
	if err != nil {
		// Try alternative paths if the first one fails
		schemaPaths := []string{
			"../database/schema/001_init.sql",
			"../../database/schema/001_init.sql",
			"database/schema/001_init.sql",
		}
		for _, path := range schemaPaths {
			if bytes, err := os.ReadFile(path); err == nil {
				schemaBytes = bytes
				break
			}
		}
		if schemaBytes == nil {
			return fmt.Errorf("failed to find schema file for disk copy")
		}
	}

	_, err = diskDB.Exec(string(schemaBytes))
	if err != nil {
		return fmt.Errorf("failed to create schema on disk: %w", err)
	}

	// Copy data from memory to disk using ATTACH DATABASE
	attachSQL := fmt.Sprintf("ATTACH DATABASE '%s' AS disk", diskPath)
	_, err = memDB.Exec(attachSQL)
	if err != nil {
		return fmt.Errorf("failed to attach disk database: %w", err)
	}

	// Copy each table
	tables := []string{"threads", "posts", "users", "replies", "quotes", "media"}
	for _, table := range tables {
		copySQL := fmt.Sprintf("INSERT INTO disk.%s SELECT * FROM %s", table, table)
		_, err = memDB.Exec(copySQL)
		if err != nil {
			// Ignore errors for empty tables
			continue
		}
	}

	// Detach the disk database
	_, err = memDB.Exec("DETACH DATABASE disk")
	if err != nil {
		return fmt.Errorf("failed to detach disk database: %w", err)
	}

	return nil
}

// Helper functions for sqlc
func stringToSqlNull(s string) sql.NullString {
	if s == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}

func intToSqlNull(i int64) sql.NullInt64 {
	if i == 0 {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: i, Valid: true}
}

func getThreadSubject(thread *Thread) sql.NullString {
	if len(thread.Posts) > 0 && thread.Posts[0].Subject != "" {
		return sql.NullString{String: thread.Posts[0].Subject, Valid: true}
	}
	return sql.NullString{Valid: false}
}

func countMediaPosts(thread *Thread) int {
	count := 0
	for _, post := range thread.Posts {
		if post.Tim != 0 && post.Ext != "" {
			count++
		}
	}
	return count
}

func cleanHTMLText(html string) string {
	if html == "" {
		return ""
	}

	// Use the proper conversation analysis parser to preserve >> formatting
	analyzer := analysis.NewPostAnalyzer()
	parsed, err := analyzer.ParsePost(0, html) // PostNo doesn't matter for text cleaning
	if err != nil || parsed.CleanText == "" {
		// Fallback to basic cleaning if parser fails or returns empty
		text := html

		// Basic HTML tag removal and entity decoding
		replacements := []string{
			"<br>", "\n",
			"<br/>", "\n",
			"<br />", "\n",
			"</p>", "\n",
			"<p>", "",
			"</div>", "\n",
			"<div>", "",
			"&gt;", ">",
			"&lt;", "<",
			"&amp;", "&",
			"&quot;", "\"",
			"&nbsp;", " ",
			"&#39;", "'",
		}
		for i := 0; i < len(replacements); i += 2 {
			text = strings.Replace(text, replacements[i], replacements[i+1], -1)
		}

		// Remove any remaining HTML tags using a simple regex-like approach
		var result strings.Builder
		inTag := false
		for _, char := range text {
			if char == '<' {
				inTag = true
				continue
			}
			if char == '>' {
				inTag = false
				continue
			}
			if !inTag {
				result.WriteRune(char)
			}
		}

		// Clean up excessive whitespace
		cleaned := strings.TrimSpace(result.String())
		cleaned = strings.ReplaceAll(cleaned, "\n\n\n", "\n\n")

		return cleaned
	}

	return parsed.CleanText
}

func extractReplyNumbers(comment string) []int64 {
	var replyNumbers []int64

	// Look for patterns like href="#p123456" and &gt;&gt;123456
	for i := 0; i < len(comment)-8; i++ {
		// Check for href="#p123456" patterns
		if i+8 < len(comment) && comment[i:i+8] == `href="#p` {
			j := i + 8
			for j < len(comment) && comment[j] >= '0' && comment[j] <= '9' {
				j++
			}
			if j > i+8 {
				if replyTo, err := strconv.ParseInt(comment[i+8:j], 10, 64); err == nil {
					replyNumbers = append(replyNumbers, replyTo)
				}
			}
		}

		// Check for &gt;&gt;123456 patterns
		if i+8 < len(comment) && comment[i:i+8] == "&gt;&gt;" {
			j := i + 8
			for j < len(comment) && comment[j] >= '0' && comment[j] <= '9' {
				j++
			}
			if j > i+8 {
				if replyTo, err := strconv.ParseInt(comment[i+8:j], 10, 64); err == nil {
					replyNumbers = append(replyNumbers, replyTo)
				}
			}
		}
	}

	return replyNumbers
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MonitorThreads continuously monitors multiple 4chan threads for new posts and media
func (a *Archiver) MonitorThreads(config *MonitorConfig) error {
	if len(config.ThreadIDs) == 0 {
		return fmt.Errorf("at least one thread ID is required for monitoring")
	}

	// Validate threads exist before starting monitoring
	fmt.Printf("🔍 Validating %d thread(s) before starting monitoring...\n", len(config.ThreadIDs))
	validThreads := []string{}

	for i, threadID := range config.ThreadIDs {
		fmt.Printf("📋 [%d/%d] Checking thread %s...", i+1, len(config.ThreadIDs), threadID)

		_, err := a.fetchThread(threadID)
		if err != nil {
			fmt.Printf(" ❌ FAILED\n")
			fmt.Printf("   └─ %v\n", err)

			// Check if it's a "not found" error
			if strings.Contains(err.Error(), "not found") ||
				strings.Contains(err.Error(), "Nothing Found") ||
				strings.Contains(err.Error(), "404") ||
				strings.Contains(err.Error(), "doesn't exist") ||
				strings.Contains(err.Error(), "HTML error page") {
				fmt.Printf("   └─ 🗑️  Thread does not exist, skipping\n")
				continue
			}

			// For other errors, warn but continue (might be temporary)
			fmt.Printf("   └─ ⚠️  Error fetching thread (might be temporary), skipping\n")
			continue
		}

		fmt.Printf(" ✅ OK\n")
		validThreads = append(validThreads, threadID)
	}

	if len(validThreads) == 0 {
		return fmt.Errorf("no valid threads found to monitor")
	}

	if len(validThreads) != len(config.ThreadIDs) {
		fmt.Printf("⚠️  Will monitor %d out of %d threads (others were invalid)\n", len(validThreads), len(config.ThreadIDs))
	}

	// Update config with only valid threads
	config.ThreadIDs = validThreads

	// For single thread, use the existing MonitorThread method
	if len(config.ThreadIDs) == 1 {
		singleConfig := &MonitorConfig{
			ThreadIDs:        []string{config.ThreadIDs[0]},
			Interval:         config.Interval,
			MaxDuration:      config.MaxDuration,
			StopOnArchive:    config.StopOnArchive,
			StopOnInactivity: config.StopOnInactivity,
		}
		return a.monitorSingleThread(config.ThreadIDs[0], singleConfig)
	}

	// For multiple threads, monitor them concurrently
	fmt.Printf("🚀 Starting concurrent monitoring of %d valid threads\n", len(config.ThreadIDs))

	atomic.StoreInt64(&a.stopMonitoring, 0)

	// Create a wait group for all monitor goroutines
	var wg sync.WaitGroup
	errorChan := make(chan error, len(config.ThreadIDs))

	// Start monitoring each thread in its own goroutine
	for i, threadID := range config.ThreadIDs {
		wg.Add(1)
		go func(id string, index int) {
			defer wg.Done()

			fmt.Printf("📡 [Thread %d/%d] Starting monitor for thread %s\n", index+1, len(config.ThreadIDs), id)

			singleConfig := &MonitorConfig{
				ThreadIDs:        []string{id},
				Interval:         config.Interval,
				MaxDuration:      config.MaxDuration,
				StopOnArchive:    config.StopOnArchive,
				StopOnInactivity: config.StopOnInactivity,
			}

			err := a.monitorSingleThread(id, singleConfig)
			if err != nil {
				errorChan <- fmt.Errorf("thread %s: %w", id, err)
			} else {
				fmt.Printf("✅ [Thread %d/%d] Completed monitoring thread %s\n", index+1, len(config.ThreadIDs), id)
			}
		}(threadID, i)
	}

	// Wait for all monitoring to complete
	go func() {
		wg.Wait()
		close(errorChan)
	}()

	// Collect any errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		fmt.Printf("⚠️  %d thread(s) completed with errors:\n", len(errors))
		for _, err := range errors {
			fmt.Printf("  - %v\n", err)
		}
		return fmt.Errorf("monitoring completed with %d errors", len(errors))
	}

	fmt.Printf("🎉 All %d threads monitored successfully\n", len(config.ThreadIDs))
	return nil
}

// monitorSingleThread handles monitoring a single thread (refactored from MonitorThread)
func (a *Archiver) monitorSingleThread(threadID string, config *MonitorConfig) error {
	atomic.StoreInt64(&a.stopMonitoring, 0)

	// Validate interval
	if config.Interval <= 0 {
		config.Interval = 1 * time.Second // Default to 1 second if invalid
		if a.config.Verbose {
			fmt.Printf("⚠️  [%s] Invalid interval, using default 1 second\n", threadID)
		}
	}

	// Initialize monitoring state for this thread
	a.monitorMutex.Lock()
	a.monitorState = &MonitorState{
		LastCheckTime: time.Now(),
		IsActive:      true,
		LastActivity:  time.Now(),
	}
	a.monitorMutex.Unlock()

	// Load existing thread data to determine starting point
	existing, err := a.loadExistingThreadData(threadID)
	if err == nil && existing != nil {
		a.monitorMutex.Lock()
		a.monitorState.LastPostNo = a.getHighestPostNo(existing)
		a.monitorState.LastPostTime = a.getLatestPostTime(existing)
		a.monitorMutex.Unlock()
		if a.config.Verbose {
			fmt.Printf("📄 [%s] Resuming monitoring from post #%d\n", threadID, a.monitorState.LastPostNo)
		}
	} else {
		if a.config.Verbose {
			fmt.Printf("🆕 [%s] Starting fresh monitoring\n", threadID)
		}
	}

	// Create ticker for periodic checks
	ticker := time.NewTicker(config.Interval)
	defer ticker.Stop()

	// Start time for duration limiting
	startTime := time.Now()

	for {
		select {
		case <-ticker.C:
			// Check if we should stop monitoring
			if atomic.LoadInt64(&a.stopMonitoring) != 0 {
				if a.config.Verbose {
					fmt.Printf("🛑 [%s] Monitor stop requested\n", threadID)
				}
				return nil
			}

			// Check max duration limit
			if config.MaxDuration > 0 && time.Since(startTime) > config.MaxDuration {
				fmt.Printf("⏰ [%s] Maximum monitoring duration (%v) reached\n", threadID, config.MaxDuration)
				return nil
			}

			// Check inactivity timeout
			a.monitorMutex.RLock()
			lastActivity := a.monitorState.LastActivity
			threadArchived := a.monitorState.ThreadArchived
			a.monitorMutex.RUnlock()

			if config.StopOnInactivity > 0 && time.Since(lastActivity) > config.StopOnInactivity {
				fmt.Printf("💤 [%s] No new activity for %v, stopping monitor\n", threadID, config.StopOnInactivity)
				return nil
			}

			if threadArchived && config.StopOnArchive {
				fmt.Printf("📦 [%s] Thread has been archived, stopping monitor\n", threadID)
				return nil
			}

			// Perform monitoring check
			err := a.performSingleThreadCheck(threadID, config)
			if err != nil {
				// Check if it's a permanent error (thread doesn't exist)
				if strings.Contains(err.Error(), "thread no longer exists") {
					fmt.Printf("🗑️ [%s] Thread no longer exists, stopping monitor\n", threadID)
					return nil
				}

				if a.config.Verbose {
					fmt.Printf("❌ [%s] Monitoring check failed: %v\n", threadID, err)
				}

				// If thread returns 404, it's been deleted/archived
				if strings.Contains(err.Error(), "404") ||
					strings.Contains(err.Error(), "not found") ||
					strings.Contains(err.Error(), "Nothing Found") ||
					strings.Contains(err.Error(), "doesn't exist") ||
					strings.Contains(err.Error(), "HTML error page") {
					a.monitorMutex.Lock()
					a.monitorState.ThreadArchived = true
					a.monitorMutex.Unlock()

					if config.StopOnArchive {
						fmt.Printf("🗑️ [%s] Thread has been deleted/archived, stopping monitor\n", threadID)
						return nil
					}
				}

				// Continue monitoring despite other errors (might be temporary)
				continue
			}

		default:
			// Non-blocking check for stop signal
			if atomic.LoadInt64(&a.stopMonitoring) != 0 {
				if a.config.Verbose {
					fmt.Printf("🛑 [%s] Monitor stop requested\n", threadID)
				}
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// MonitorThread maintains backwards compatibility for single thread monitoring
func (a *Archiver) MonitorThread(config *MonitorConfig) error {
	if len(config.ThreadIDs) == 0 {
		return fmt.Errorf("thread ID is required for monitoring")
	}
	return a.monitorSingleThread(config.ThreadIDs[0], config)
}

// StopMonitoring signals the monitoring loop to stop gracefully
func (a *Archiver) StopMonitoring() {
	atomic.StoreInt64(&a.stopMonitoring, 1)
}

// loadExistingThreadData attempts to load existing thread data
func (a *Archiver) loadExistingThreadData(threadID string) (*Thread, error) {
	threadDir := filepath.Join(a.config.OutputDir, a.config.Board, threadID)
	postsFile := filepath.Join(threadDir, PostsJSONFileName)

	if _, err := os.Stat(postsFile); os.IsNotExist(err) {
		return nil, err
	}

	data, err := os.ReadFile(postsFile)
	if err != nil {
		return nil, err
	}

	var thread Thread
	err = json.Unmarshal(data, &thread)
	if err != nil {
		return nil, err
	}

	return &thread, nil
}

// findNewPosts returns posts with post numbers higher than lastPostNo
func (a *Archiver) findNewPosts(thread *Thread, lastPostNo int64) []Post {
	var newPosts []Post

	for _, post := range thread.Posts {
		if post.No > lastPostNo {
			newPosts = append(newPosts, post)
		}
	}

	return newPosts
}

// countNewMedia counts posts with media attachments
func (a *Archiver) countNewMedia(posts []Post) int {
	count := 0
	for _, post := range posts {
		if post.Tim != 0 && post.Ext != "" {
			count++
		}
	}
	return count
}

// getHighestPostNo returns the highest post number in the thread
func (a *Archiver) getHighestPostNo(thread *Thread) int64 {
	var highest int64
	for _, post := range thread.Posts {
		if post.No > highest {
			highest = post.No
		}
	}
	return highest
}

// getLatestPostTime returns the latest post timestamp in the thread
func (a *Archiver) getLatestPostTime(thread *Thread) int64 {
	var latest int64
	for _, post := range thread.Posts {
		if post.Time > latest {
			latest = post.Time
		}
	}
	return latest
}

// archiveNewContent archives only the new posts and their media
func (a *Archiver) archiveNewContent(threadID string, fullThread *Thread, newPosts []Post) error {
	if len(newPosts) == 0 {
		return nil
	}

	// Create thread directory if it doesn't exist
	threadDir := filepath.Join(a.config.OutputDir, a.config.Board, threadID)
	if err := os.MkdirAll(threadDir, 0755); err != nil {
		return fmt.Errorf("failed to create thread directory: %w", err)
	}

	// Load metadata
	meta, err := a.metadataManager.LoadMetadata(a.config.Board, threadID)
	if err != nil {
		meta, _ = a.metadataManager.LoadMetadata(a.config.Board, threadID) // Create new if doesn't exist
	}

	// Update posts file with full thread data (incremental update)
	if a.config.IncludePosts {
		if err := a.savePosts(fullThread, threadDir, meta); err != nil {
			fmt.Printf("Warning: Failed to save posts: %v\n", err)
		}
	}

	// Download new media files only
	if a.config.IncludeMedia {
		mediaDir := filepath.Join(threadDir, MediaDirName)
		os.MkdirAll(mediaDir, 0755)

		newMediaCount := 0
		for i, post := range newPosts {
			if post.Tim == 0 || post.Ext == "" {
				continue
			}

			fmt.Printf("📁 [%d/%d] Downloading new media: %s%s\n", i+1, len(newPosts), post.Filename, post.Ext)

			var mediaURL string
			var filename string

			if a.config.Source == SourceArchivedMoe || post.ArchivedMoeURL != "" {
				if post.ArchivedMoeURL != "" {
					mediaURL = post.ArchivedMoeURL
				} else {
					mediaURL = fmt.Sprintf("%s/%s/%s%s", ArchivedMoeBaseURL, a.config.Board, post.Filename, post.Ext)
				}
				filename = fmt.Sprintf("%d_%s%s", post.No, post.Filename, post.Ext)
			} else {
				mediaURL = fmt.Sprintf("%s/%s/%d%s", FourChanMediaBaseURL, a.config.Board, post.Tim, post.Ext)
				filename = fmt.Sprintf("%d_%s%s", post.No, post.Filename, post.Ext)
			}

			localPath := filepath.Join(mediaDir, filename)

			// Skip if already exists
			if _, err := os.Stat(localPath); err == nil && a.config.SkipExisting {
				continue
			}

			size, err := a.downloadFile(mediaURL, localPath)
			if err != nil {
				fmt.Printf("❌ [%s] Failed to download %s: %v\n", threadID, mediaURL, err)
				continue
			}

			meta.AddDownloadedMedia(mediaURL, filename, size)
			newMediaCount++
		}

		fmt.Printf("📥 [%s] Downloaded %d new media files\n", threadID, newMediaCount)
	}

	// Update database with new posts (incremental update)
	if a.config.IncludePosts {
		err = a.performSqlcConversationAnalysis(fullThread, threadDir, threadID)
		if err != nil {
			fmt.Printf("Warning: [%s] Failed to update conversation analysis: %v\n", threadID, err)
		}
	}

	// Save updated metadata
	if err := a.metadataManager.SaveMetadata(meta); err != nil {
		fmt.Printf("Warning: [%s] Failed to save metadata: %v\n", threadID, err)
	}

	return nil
}

// performSingleThreadCheck checks for new posts and downloads new content for a single thread
func (a *Archiver) performSingleThreadCheck(threadID string, config *MonitorConfig) error {
	a.monitorMutex.Lock()
	checkNum := a.monitorState.TotalChecks + 1
	a.monitorState.TotalChecks = checkNum
	a.monitorState.LastCheckTime = time.Now()
	a.monitorMutex.Unlock()

	if a.config.Verbose {
		fmt.Printf("🔍 [%s] [Check #%d] Fetching thread...\n", threadID, checkNum)
	}

	// Fetch current thread state
	thread, err := a.fetchThread(threadID)
	if err != nil {
		// Check if it's a permanent error (thread doesn't exist)
		if strings.Contains(err.Error(), "not found") ||
			strings.Contains(err.Error(), "Nothing Found") ||
			strings.Contains(err.Error(), "404") ||
			strings.Contains(err.Error(), "doesn't exist") ||
			strings.Contains(err.Error(), "HTML error page") ||
			strings.Contains(err.Error(), "no posts") {

			// Mark as archived and stop monitoring
			a.monitorMutex.Lock()
			a.monitorState.ThreadArchived = true
			a.monitorMutex.Unlock()

			return fmt.Errorf("thread no longer exists: %w", err)
		}

		// For other errors, return as retryable
		return fmt.Errorf("failed to fetch thread: %w", err)
	}

	// Find new posts since last check
	a.monitorMutex.RLock()
	lastPostNo := a.monitorState.LastPostNo
	a.monitorMutex.RUnlock()

	newPosts := a.findNewPosts(thread, lastPostNo)
	newMedia := a.countNewMedia(newPosts)

	if len(newPosts) == 0 {
		if a.config.Verbose {
			fmt.Printf("📭 [%s] No new posts found\n", threadID)
		}
		return nil
	}

	fmt.Printf("📬 [%s] Found %d new posts, %d with media\n", threadID, len(newPosts), newMedia)

	// Update monitoring state
	a.monitorMutex.Lock()
	a.monitorState.NewPostsFound += len(newPosts)
	a.monitorState.NewMediaFound += newMedia
	a.monitorState.LastActivity = time.Now()
	if len(newPosts) > 0 {
		a.monitorState.LastPostNo = a.getHighestPostNo(thread)
		a.monitorState.LastPostTime = a.getLatestPostTime(thread)
	}
	a.monitorMutex.Unlock()

	// Archive the new content incrementally
	err = a.archiveNewContent(threadID, thread, newPosts)
	if err != nil {
		return fmt.Errorf("failed to archive new content: %w", err)
	}

	fmt.Printf("✅ [%s] Archived %d new posts with %d media files\n", threadID, len(newPosts), newMedia)
	return nil
}

// fetchFrom4Plebs retrieves thread data from 4plebs by scraping HTML
func (a *Archiver) fetchFrom4Plebs(threadID string) (*Thread, error) {
	thread := &Thread{Posts: []Post{}}

	// First establish a session to look like a real user
	err := a.establish4PlebsSession()
	if err != nil {
		if a.config.Verbose {
			fmt.Printf("Session establishment failed, continuing anyway: %v\n", err)
		}
		// Continue even if session establishment fails
	}

	// Check if this is a chunk URL with pagination info
	if strings.Contains(threadID, "/chunk/") {
		return a.fetchFrom4PlebsChunked(threadID)
	}

	// Regular thread URL with human-like delay before main request
	time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)

	url := fmt.Sprintf("%s/%s/thread/%s", FourPlebsBaseURL, a.config.Board, threadID)
	return a.fetch4PlebsPage(url, thread)
}

// fetchFrom4PlebsChunked handles paginated 4plebs threads
func (a *Archiver) fetchFrom4PlebsChunked(chunkURL string) (*Thread, error) {
	thread := &Thread{Posts: []Post{}}

	// Parse chunk URL: /pol/chunk/507147249/500/22/
	// Extract thread ID, posts per page, and current page
	parts := strings.Split(strings.Trim(chunkURL, "/"), "/")
	if len(parts) < 5 {
		return nil, fmt.Errorf("invalid chunk URL format: %s", chunkURL)
	}

	actualThreadID := parts[2]
	postsPerPage, err := strconv.Atoi(parts[3])
	if err != nil {
		return nil, fmt.Errorf("invalid posts per page in chunk URL: %s", parts[3])
	}

	startPage, err := strconv.Atoi(parts[4])
	if err != nil {
		return nil, fmt.Errorf("invalid page number in chunk URL: %s", parts[4])
	}

	// Try to get the first page to see total posts
	firstPageURL := fmt.Sprintf("%s/%s/chunk/%s/%d/1/", FourPlebsBaseURL, a.config.Board, actualThreadID, postsPerPage)
	_, err = a.fetch4PlebsPage(firstPageURL, &Thread{Posts: []Post{}})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch first page: %w", err)
	}

	// Estimate total pages based on post count info from the page
	// For now, try to fetch pages starting from startPage until we get an error
	currentPage := startPage

	for {
		pageURL := fmt.Sprintf("%s/%s/chunk/%s/%d/%d/", FourPlebsBaseURL, a.config.Board, actualThreadID, postsPerPage, currentPage)

		if a.config.Verbose {
			fmt.Printf("Fetching 4plebs page %d: %s\n", currentPage, pageURL)
		}

		pageThread, err := a.fetch4PlebsPage(pageURL, &Thread{Posts: []Post{}})
		if err != nil {
			if currentPage == startPage {
				// If we can't even get the start page, return error
				return nil, fmt.Errorf("failed to fetch starting page %d: %w", startPage, err)
			}
			// If we can't get this page, we've reached the end
			break
		}

		// Add posts from this page
		thread.Posts = append(thread.Posts, pageThread.Posts...)

		// If this page has fewer posts than expected, we've reached the end
		if len(pageThread.Posts) < postsPerPage {
			break
		}

		currentPage++
	}

	if len(thread.Posts) == 0 {
		return nil, fmt.Errorf("no posts found in thread %s", actualThreadID)
	}

	return thread, nil
}

// fetch4PlebsPage fetches a single 4plebs page and parses the HTML
func (a *Archiver) fetch4PlebsPage(url string, thread *Thread) (*Thread, error) {
	// Wait for rate limiter
	a.limiter.Wait(context.Background())

	for attempt := 0; attempt < a.config.MaxRetries; attempt++ {
		// Add human-like delay between attempts (not just after failures)
		if attempt > 0 {
			// Exponential backoff with randomization for failed attempts
			baseDelay := time.Duration(3+attempt*3) * time.Second
			randomDelay := time.Duration(rand.Intn(2000)) * time.Millisecond
			totalDelay := baseDelay + randomDelay

			if a.config.Verbose {
				fmt.Printf("Waiting %v before attempt %d (base: %v + random: %v)\n",
					totalDelay, attempt+1, baseDelay, randomDelay)
			}
			time.Sleep(totalDelay)
		} else {
			// Small random delay even on first attempt to look more human
			initialDelay := time.Duration(200+rand.Intn(800)) * time.Millisecond
			time.Sleep(initialDelay)
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}

		// Set realistic headers with rotation - get a new random UA each time
		a.setRealisticHeaders(req, attempt+1)

		if a.config.Verbose {
			fmt.Printf("Attempting 4plebs fetch (attempt %d/%d): %s\n", attempt+1, a.config.MaxRetries, url)
		}

		resp, err := a.client.Do(req)
		if err != nil {
			if attempt == a.config.MaxRetries-1 {
				return nil, err
			}
			if a.config.Verbose {
				fmt.Printf("Request error (attempt %d): %v\n", attempt+1, err)
			}
			continue // The delay is handled at the top of the loop
		}

		switch resp.StatusCode {
		case http.StatusOK:
			// Success case
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				if attempt == a.config.MaxRetries-1 {
					return nil, err
				}
				continue
			}

			// Parse HTML
			doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(body)))
			if err != nil {
				if attempt == a.config.MaxRetries-1 {
					return nil, fmt.Errorf("failed to parse HTML: %w", err)
				}
				continue
			}

			return a.Parse4PlebsHTML(doc, url)

		case http.StatusForbidden:
			resp.Body.Close()
			if a.config.Verbose {
				fmt.Printf("403 Forbidden (attempt %d/%d) - will retry with different headers\n",
					attempt+1, a.config.MaxRetries)
			}

			if attempt == a.config.MaxRetries-1 {
				return nil, fmt.Errorf("4plebs returned HTTP 403 after %d attempts - blocked by anti-bot protection", a.config.MaxRetries)
			}
			continue // The delay is handled at the top of the loop

		case http.StatusTooManyRequests:
			resp.Body.Close()
			retryAfter := resp.Header.Get("Retry-After")
			if retryAfter != "" {
				if seconds, err := strconv.Atoi(retryAfter); err == nil {
					delay := time.Duration(seconds) * time.Second
					if a.config.Verbose {
						fmt.Printf("429 Rate Limited, waiting %v as requested\n", delay)
					}
					time.Sleep(delay)
					continue
				}
			}
			if a.config.Verbose {
				fmt.Printf("429 Rate Limited (attempt %d)\n", attempt+1)
			}
			continue // The delay is handled at the top of the loop

		case http.StatusNotFound:
			resp.Body.Close()
			return nil, fmt.Errorf("4plebs page not found (404): %s", url)

		default:
			resp.Body.Close()
			if attempt == a.config.MaxRetries-1 {
				return nil, fmt.Errorf("4plebs returned HTTP %d for %s", resp.StatusCode, url)
			}
			if a.config.Verbose {
				fmt.Printf("HTTP %d (attempt %d)\n", resp.StatusCode, attempt+1)
			}
			continue
		}
	}

	return thread, nil
}

// createMarkdownExport creates a markdown file with all thread posts
func (a *Archiver) createMarkdownExport(thread *Thread, markdownPath, threadID string) error {
	var builder strings.Builder

	// Write header
	builder.WriteString(fmt.Sprintf("# Thread %s\n\n", threadID))
	builder.WriteString(fmt.Sprintf("**Board:** /%s/\n", a.config.Board))
	builder.WriteString(fmt.Sprintf("**Archived:** %s\n", time.Now().UTC().Format("2006-01-02 15:04:05 UTC")))
	builder.WriteString(fmt.Sprintf("**Posts:** %d\n\n", len(thread.Posts)))

	builder.WriteString("---\n\n")

	// Process each post
	for i, post := range thread.Posts {
		isOP := i == 0

		// Post header
		if isOP {
			builder.WriteString("## Original Post\n\n")
		} else {
			builder.WriteString(fmt.Sprintf("## Post #%d\n\n", post.No))
		}

		// Post metadata
		builder.WriteString("**Post Info:**\n")
		builder.WriteString(fmt.Sprintf("- **Post Number:** %d\n", post.No))

		// Format timestamp
		if post.Time > 0 {
			timestamp := time.Unix(post.Time, 0).UTC()
			builder.WriteString(fmt.Sprintf("- **Date:** %s\n", timestamp.Format("2006-01-02 15:04:05 UTC")))
		}

		// Author information
		author := "Anonymous"
		if post.Name != "" && post.Name != "Anonymous" {
			author = post.Name
		}
		builder.WriteString(fmt.Sprintf("- **Author:** %s", author))

		if post.Trip != "" {
			builder.WriteString(fmt.Sprintf(" **%s**", post.Trip))
		}

		if post.ID != "" {
			builder.WriteString(fmt.Sprintf(" (ID: %s)", post.ID))
		}
		builder.WriteString("\n")

		// Subject line (usually only for OP)
		if post.Subject != "" {
			builder.WriteString(fmt.Sprintf("- **Subject:** %s\n", post.Subject))
		}

		// Media information and display
		if post.Filename != "" || post.ArchivedMoeURL != "" {
			builder.WriteString("- **Media:** ")
			if post.Filename != "" {
				builder.WriteString(fmt.Sprintf("`%s%s`", post.Filename, post.Ext))
				if post.Fsize > 0 {
					builder.WriteString(fmt.Sprintf(" (%s)", formatFileSize(post.Fsize)))
				}
				if post.W > 0 && post.H > 0 {
					builder.WriteString(fmt.Sprintf(" [%dx%d]", post.W, post.H))
				}
			} else {
				builder.WriteString("File attached")
			}
			builder.WriteString("\n")

			// Add image display in markdown if it's an image file
			if post.ArchivedMoeURL != "" && post.Ext != "" {
				imageExts := []string{".jpg", ".jpeg", ".png", ".gif", ".webp"}
				isImage := false
				for _, ext := range imageExts {
					if strings.ToLower(post.Ext) == ext {
						isImage = true
						break
					}
				}

				if isImage {
					builder.WriteString(fmt.Sprintf("\n![%s%s](%s)\n", post.Filename, post.Ext, post.ArchivedMoeURL))
				} else {
					// For non-image files, just show a link
					builder.WriteString(fmt.Sprintf("\n[📎 %s%s](%s)\n", post.Filename, post.Ext, post.ArchivedMoeURL))
				}
			}
		}

		builder.WriteString("\n")

		// Post content
		if post.Comment != "" {
			builder.WriteString("**Content:**\n\n")

			// Clean and format the comment text
			content := a.formatPostContentForMarkdown(post.Comment)
			builder.WriteString(content)
			builder.WriteString("\n\n")
		}

		// Separator between posts
		if i < len(thread.Posts)-1 {
			builder.WriteString("---\n\n")
		}
	}

	// Write to file
	return os.WriteFile(markdownPath, []byte(builder.String()), 0644)
}

// formatPostContentForMarkdown formats post content for markdown output
func (a *Archiver) formatPostContentForMarkdown(content string) string {
	// Convert line breaks
	content = strings.ReplaceAll(content, "\n", "\n\n")

	// Convert 4chan greentext to markdown quotes
	lines := strings.Split(content, "\n")
	var formattedLines []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			formattedLines = append(formattedLines, "")
			continue
		}

		// Format greentext
		if strings.HasPrefix(line, ">") && !strings.HasPrefix(line, ">>") {
			formattedLines = append(formattedLines, fmt.Sprintf("> %s", strings.TrimPrefix(line, ">")))
		} else if strings.HasPrefix(line, ">>") {
			// Format post references
			formattedLines = append(formattedLines, fmt.Sprintf("**%s**", line))
		} else {
			formattedLines = append(formattedLines, line)
		}
	}

	return strings.Join(formattedLines, "\n")
}

// formatFileSize formats file size in human-readable format
func formatFileSize(size int) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}

// establish4PlebsSession simulates real browsing by visiting the main page first
func (a *Archiver) establish4PlebsSession() error {
	if a.config.Verbose {
		fmt.Printf("Establishing 4plebs session...\n")
	}

	// First, visit the main 4plebs page
	mainReq, err := http.NewRequest("GET", "https://archive.4plebs.org/", nil)
	if err != nil {
		return err
	}
	a.setRealisticHeaders(mainReq, 1)

	resp, err := a.client.Do(mainReq)
	if err != nil {
		return err
	}
	resp.Body.Close()

	// Small delay like a real user
	time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)

	// Then visit the board page
	boardReq, err := http.NewRequest("GET", fmt.Sprintf("https://archive.4plebs.org/%s/", a.config.Board), nil)
	if err != nil {
		return err
	}
	a.setRealisticHeaders(boardReq, 1)

	resp, err = a.client.Do(boardReq)
	if err != nil {
		return err
	}
	resp.Body.Close()

	// Another small delay
	time.Sleep(time.Duration(800+rand.Intn(1500)) * time.Millisecond)

	if a.config.Verbose {
		fmt.Printf("4plebs session established\n")
	}

	return nil
}

// Parse4PlebsHTML parses 4plebs HTML and extracts posts (exported for testing)
func (a *Archiver) Parse4PlebsHTML(doc *goquery.Document, url string) (*Thread, error) {
	thread := &Thread{Posts: []Post{}}

	// Find the main thread post (OP)
	doc.Find("article.thread").Each(func(i int, s *goquery.Selection) {
		post := a.parse4PlebsPost(s, true)
		if post != nil {
			thread.Posts = append(thread.Posts, *post)
		}
	})

	// Find reply posts
	doc.Find("article.post").Each(func(i int, s *goquery.Selection) {
		post := a.parse4PlebsPost(s, false)
		if post != nil {
			thread.Posts = append(thread.Posts, *post)
		}
	})

	if len(thread.Posts) == 0 {
		return nil, fmt.Errorf("no posts found in 4plebs page: %s", url)
	}

	return thread, nil
}

// Parse4ChanHTML parses 4chan HTML and extracts posts (exported for testing)
func (a *Archiver) Parse4ChanHTML(doc *goquery.Document, url string) (*Thread, error) {
	thread := &Thread{Posts: []Post{}}

	// Find all post containers
	doc.Find("div.postContainer").Each(func(i int, s *goquery.Selection) {
		post := a.parse4ChanPost(s, i == 0)
		if post != nil {
			thread.Posts = append(thread.Posts, *post)
		}
	})

	if len(thread.Posts) == 0 {
		return nil, fmt.Errorf("no posts found in 4chan page: %s", url)
	}

	return thread, nil
}

// ParseHTML automatically detects the format and parses accordingly (exported for testing)
func (a *Archiver) ParseHTML(doc *goquery.Document, url string) (*Thread, error) {
	// Check if it's 4plebs format by looking for article.post or article.thread
	if doc.Find("article.post, article.thread").Length() > 0 {
		if a.config.Verbose {
			fmt.Printf("Detected 4plebs format\n")
		}
		return a.Parse4PlebsHTML(doc, url)
	}

	// Check if it's 4chan format by looking for div.postContainer
	if doc.Find("div.postContainer").Length() > 0 {
		if a.config.Verbose {
			fmt.Printf("Detected 4chan format\n")
		}
		return a.Parse4ChanHTML(doc, url)
	}

	return nil, fmt.Errorf("unknown HTML format for page: %s", url)
}

// parse4PlebsPost parses a single post from 4plebs HTML
func (a *Archiver) parse4PlebsPost(s *goquery.Selection, isOP bool) *Post {
	post := &Post{}

	// Check if this is actually a post element by looking for essential attributes
	id, hasId := s.Attr("id")
	classes, _ := s.Attr("class")

	// Skip non-post elements (like backlink containers, stubs, etc.)
	if !hasId || id == "" || strings.Contains(classes, "backlink_container") || strings.Contains(classes, "stub") {
		if a.config.Verbose && id != "" {
			fmt.Printf("Skipping non-post element: id='%s' class='%s'\n", id, classes)
		}
		return nil
	}

	// Debug: Check what element we're parsing
	if a.config.Verbose {
		tagName := goquery.NodeName(s)
		fmt.Printf("Parsing element: <%s> id='%s' class='%s'\n", tagName, id, classes)
	}

	// Get post number from id attribute - this is the REAL post number from 4chan
	if postNo, err := strconv.ParseInt(id, 10, 64); err == nil && postNo > 0 {
		post.No = postNo
		post.Tim = postNo // Use the real post number as Tim too
		if a.config.Verbose {
			fmt.Printf("Extracted post number from id: %d\n", postNo)
		}
	} else {
		if a.config.Verbose {
			fmt.Printf("Warning: Could not extract post number from element. ID: '%s'\n", id)
		}
		return nil
	}

	// Get author name - try multiple selectors but process only the FIRST one
	authorSelectors := []string{".post_author", ".author", ".name", ".nameBlock"}
	for _, selector := range authorSelectors {
		authorEl := s.Find(selector).First() // Take only the first match
		if authorEl.Length() > 0 {
			authorText := strings.TrimSpace(authorEl.Text())
			if authorText != "" && authorText != "Anonymous" {
				// Clean up any remaining concatenation issues
				if strings.Contains(authorText, "CIA OP") {
					authorText = "CIA OP"
				}
				post.Name = authorText
				break
			}
		}
	}
	if post.Name == "" {
		post.Name = "Anonymous"
	}

	// Get tripcode - only from the first matching element
	tripcodeSelectors := []string{".post_tripcode", ".postertrip", ".tripcode"}
	for _, selector := range tripcodeSelectors {
		tripcodeEl := s.Find(selector).First() // Take only the first match
		if tripcodeEl.Length() > 0 {
			tripcodeText := strings.TrimSpace(tripcodeEl.Text())
			if tripcodeText != "" {
				post.Trip = tripcodeText
				break
			}
		}
	}

	// Get user ID - only from the first matching element
	userIDSelectors := []string{".poster_hash", ".posteruid", ".hand", ".post_id"}
	for _, selector := range userIDSelectors {
		idEl := s.Find(selector).First() // Take only the first match
		if idEl.Length() > 0 {
			idText := strings.TrimSpace(idEl.Text())
			if idText != "" {
				post.ID = idText
				break
			}
		}
	}

	// Get timestamp
	timeSelectors := []string{"time", ".dateTime", ".timestamp"}
	for _, selector := range timeSelectors {
		timeEl := s.Find(selector).First() // Take only the first match
		if timeEl.Length() > 0 {
			if datetimeAttr, exists := timeEl.Attr("datetime"); exists {
				if timestamp, err := time.Parse(time.RFC3339, datetimeAttr); err == nil {
					post.Time = timestamp.Unix()
					break
				}
			}
		}
	}

	// Get post content - only from the first text div
	contentSelectors := []string{".text", ".postMessage", ".message", ".comment"}
	for _, selector := range contentSelectors {
		contentEl := s.Find(selector).First() // Take only the first match
		if contentEl.Length() > 0 {
			// Get both text and HTML content
			textContent := strings.TrimSpace(contentEl.Text())
			htmlContent, _ := contentEl.Html()

			if textContent != "" {
				post.Comment = htmlContent
				break
			}
		}
	}

	// Get media information - only from the first media element
	if s.HasClass("has_image") || s.Find(".post_file, .file, .fileThumb").Length() > 0 {
		mediaEl := s.Find(".post_file, .file, .fileThumb").First() // Take only the first match
		if mediaEl.Length() > 0 {
			// Get filename
			if link := mediaEl.Find("a").First(); link.Length() > 0 {
				if href, exists := link.Attr("href"); exists {
					post.Filename = filepath.Base(href)
				}

				// Get filename and extension from title if available
				if title, exists := link.Attr("title"); exists && title != "" {
					post.Filename = strings.TrimSpace(title)
				}
			}

			// Get thumbnail
			if img := mediaEl.Find("img").First(); img.Length() > 0 {
				if src, exists := img.Attr("src"); exists {
					post.ArchivedMoeURL = src // Store thumbnail URL here
				}
			}

			post.Ext = filepath.Ext(post.Filename)
			if post.Ext != "" {
				post.Fsize = 1 // We have a file
			}
		}
	}

	// Get country and flag information
	country, countryName := a.extractCountryInfo(s)
	flag, flagName := a.extractFlagInfo(s)
	post.Country = country
	post.CountryName = countryName
	post.Flag = flag
	post.FlagName = flagName

	// Get media information

	// Debug output with clean data
	if a.config.Verbose {
		fmt.Printf("✓ Parsed post #%d: author='%s', tripcode='%s', id='%s', content_length=%d, media=%t\n",
			post.No, post.Name, post.Trip, post.ID, len(post.Comment), post.Fsize > 0)
	}

	return post
}

// parse4ChanPost parses a single post from 4chan HTML
func (a *Archiver) parse4ChanPost(s *goquery.Selection, isOP bool) *Post {
	post := &Post{}

	// Debug: Check what element we're parsing
	if a.config.Verbose {
		tagName := goquery.NodeName(s)
		classes, _ := s.Attr("class")
		id, _ := s.Attr("id")
		fmt.Printf("Parsing 4chan element: <%s> id='%s' class='%s'\n", tagName, id, classes)
	}

	// Get the actual post div inside the container
	postDiv := s.Find("div.post")
	if postDiv.Length() == 0 {
		postDiv = s // Sometimes the selection itself is the post
	}

	// Get post number from id attribute
	if id, exists := postDiv.Attr("id"); exists && id != "" {
		// 4chan post IDs are usually in format "p123456" or "m123456"
		if strings.HasPrefix(id, "p") || strings.HasPrefix(id, "m") {
			numStr := id[1:] // Remove the prefix
			if postNo, err := strconv.ParseInt(numStr, 10, 64); err == nil && postNo > 0 {
				post.No = postNo
				post.Tim = postNo
				if a.config.Verbose {
					fmt.Printf("Extracted 4chan post number from id: %d\n", postNo)
				}
			}
		} else if postNo, err := strconv.ParseInt(id, 10, 64); err == nil && postNo > 0 {
			// Sometimes it's just the number
			post.No = postNo
			post.Tim = postNo
			if a.config.Verbose {
				fmt.Printf("Extracted 4chan post number from id: %d\n", postNo)
			}
		}
	}

	// If no post number found, try other methods
	if post.No == 0 {
		// Try to find post number in post info
		postDiv.Find(".postInfo, .post_info").Each(func(i int, infoEl *goquery.Selection) {
			// Look for post number in desktop format
			infoEl.Find(".postNum a").Each(func(j int, link *goquery.Selection) {
				href, _ := link.Attr("href")
				if strings.HasPrefix(href, "#p") {
					numStr := strings.TrimPrefix(href, "#p")
					if postNo, err := strconv.ParseInt(numStr, 10, 64); err == nil && postNo > 0 {
						post.No = postNo
						post.Tim = postNo
						if a.config.Verbose {
							fmt.Printf("Extracted 4chan post number from postNum link: %d\n", postNo)
						}
					}
				}
			})

			// Look for post number in mobile format
			if post.No == 0 {
				text := infoEl.Text()
				if match := regexp.MustCompile(`No\.(\d+)`).FindStringSubmatch(text); len(match) > 1 {
					if postNo, err := strconv.ParseInt(match[1], 10, 64); err == nil && postNo > 0 {
						post.No = postNo
						post.Tim = postNo
						if a.config.Verbose {
							fmt.Printf("Extracted 4chan post number from info text: %d\n", postNo)
						}
					}
				}
			}
		})
	}

	// Get timestamp
	postDiv.Find("span.dateTime").Each(func(i int, timeEl *goquery.Selection) {
		if unixtime, exists := timeEl.Attr("data-utc"); exists {
			if timestamp, err := strconv.ParseInt(unixtime, 10, 64); err == nil {
				post.Time = timestamp
			}
		}
	})

	// Get author name - try multiple selectors
	nameSelectors := []string{".name", ".nameBlock .name"}
	for _, selector := range nameSelectors {
		nameEl := postDiv.Find(selector)
		if nameEl.Length() > 0 {
			nameText := strings.TrimSpace(nameEl.Text())
			if nameText != "" {
				// Remove duplicates that sometimes occur in 4chan parsing
				if strings.Contains(nameText, nameText[:len(nameText)/2]) && len(nameText) > 10 {
					nameText = nameText[:len(nameText)/2]
				}
				post.Name = nameText
				break
			}
		}
	}
	if post.Name == "" {
		post.Name = "Anonymous"
	}

	// Get tripcode - try multiple selectors
	tripcodeSelectors := []string{".postertrip", ".nameBlock .postertrip"}
	for _, selector := range tripcodeSelectors {
		tripEl := postDiv.Find(selector)
		if tripEl.Length() > 0 {
			tripText := strings.TrimSpace(tripEl.Text())
			if tripText != "" {
				post.Trip = tripText
				break
			}
		}
	}

	// Get poster ID - try multiple selectors
	idSelectors := []string{".hand", ".posteruid", ".uid"}
	for _, selector := range idSelectors {
		idEl := postDiv.Find(selector)
		if idEl.Length() > 0 {
			idText := strings.TrimSpace(idEl.Text())
			// Clean up ID text - remove common patterns and duplicates
			idText = strings.TrimPrefix(idText, "ID:")
			idText = strings.TrimPrefix(idText, "(ID: ")
			idText = strings.TrimSuffix(idText, ")")
			idText = strings.TrimSpace(idText)

			// Remove duplicates that sometimes occur in 4chan parsing
			if len(idText) > 8 && strings.Contains(idText, idText[:len(idText)/2]) {
				idText = idText[:len(idText)/2]
			}

			if idText != "" {
				post.ID = idText
				break
			}
		}
	}

	// Get country flag
	flagSelectors := []string{".flag", ".countryFlag"}
	for _, selector := range flagSelectors {
		flagEl := postDiv.Find(selector)
		if flagEl.Length() > 0 {
			if title, exists := flagEl.Attr("title"); exists && title != "" {
				// Store country info, but don't overwrite existing ID
				if post.ID == "" {
					post.ID = title
				}
				break
			}
		}
	}

	// Get subject
	subjectSelectors := []string{".subject", ".filetitle"}
	for _, selector := range subjectSelectors {
		subjectEl := postDiv.Find(selector)
		if subjectEl.Length() > 0 {
			subjectText := strings.TrimSpace(subjectEl.Text())
			if subjectText != "" {
				post.Subject = subjectText
				break
			}
		}
	}

	// Get comment text
	commentSelectors := []string{".postMessage", "blockquote", ".comment"}
	for _, selector := range commentSelectors {
		commentEl := postDiv.Find(selector)
		if commentEl.Length() > 0 {
			html, _ := commentEl.Html()
			if html != "" {
				post.Comment = cleanHTMLText(html)
				break
			}
			textContent := strings.TrimSpace(commentEl.Text())
			if textContent != "" {
				post.Comment = textContent
				break
			}
		}
	}

	// Get media information
	fileSelectors := []string{".file", ".fileThumb"}
	for _, selector := range fileSelectors {
		fileDiv := postDiv.Find(selector)
		if fileDiv.Length() > 0 {
			// Get image link
			fileDiv.Find("a").Each(func(i int, link *goquery.Selection) {
				href, exists := link.Attr("href")
				if !exists || href == "" {
					return
				}

				// Convert relative URLs to absolute
				if strings.HasPrefix(href, "//") {
					href = "https:" + href
				} else if strings.HasPrefix(href, "/") {
					href = "https://i.4cdn.org" + href
				}

				post.ArchivedMoeURL = href

				// Get filename and extension
				if title, exists := link.Attr("title"); exists && title != "" {
					post.Filename = strings.TrimSpace(title)
				}

				// Extract extension from URL
				if idx := strings.LastIndex(href, "."); idx != -1 {
					ext := href[idx:]
					if qIdx := strings.Index(ext, "?"); qIdx != -1 {
						ext = ext[:qIdx]
					}
					post.Ext = ext
				}

				// Get file info
				fileText := fileDiv.Find(".fileText")
				if fileText.Length() > 0 {
					text := fileText.Text()
					// Extract file size
					if match := regexp.MustCompile(`(\d+(?:\.\d+)?)\s*([KMGT]?)B`).FindStringSubmatch(text); len(match) > 2 {
						if size, err := strconv.ParseFloat(match[1], 64); err == nil {
							multiplier := 1.0
							switch match[2] {
							case "K":
								multiplier = 1024
							case "M":
								multiplier = 1024 * 1024
							case "G":
								multiplier = 1024 * 1024 * 1024
							case "T":
								multiplier = 1024 * 1024 * 1024 * 1024
							}
							post.Fsize = int(size * multiplier)
						}
					}

					// Extract dimensions
					if match := regexp.MustCompile(`(\d+)x(\d+)`).FindStringSubmatch(text); len(match) > 2 {
						if w, err := strconv.Atoi(match[1]); err == nil {
							post.W = w
						}
						if h, err := strconv.Atoi(match[2]); err == nil {
							post.H = h
						}
					}
				}

				return // Only process the first valid file
			})

			if post.ArchivedMoeURL != "" {
				break // Found media, break out of selector loop
			}
		}
	}

	// Only return posts with valid post numbers
	if post.No == 0 {
		if a.config.Verbose {
			fmt.Printf("Warning: Could not extract post number from 4chan post\n")
		}
		return nil
	}

	if a.config.Verbose {
		fmt.Printf("✓ Parsed 4chan post #%d: author='%s', tripcode='%s', id='%s', content_length=%d, media=%v\n",
			post.No, post.Name, post.Trip, post.ID, len(post.Comment), post.ArchivedMoeURL != "")
	}

	return post
}

// ExtractToDatabase extracts thread data to database with enhanced parsing (exported for testing)
func (a *Archiver) ExtractToDatabase(ctx context.Context, queries *database.Queries, thread *Thread, threadID, board string) error {
	// Determine source based on config
	source := a.config.Source
	if source == SourceAuto {
		source = "auto"
	}

	// Insert thread record with enhanced fields
	_, err := queries.CreateThread(ctx, database.CreateThreadParams{
		ThreadID:    threadID,
		Board:       board,
		Subject:     getThreadSubject(thread),
		Source:      source,
		SourceUrl:   stringToSqlNull(fmt.Sprintf("https://example.com/%s/thread/%s", board, threadID)), // Placeholder
		CreatedAt:   time.Now(),
		LastUpdated: time.Now(),
		PostsCount:  sql.NullInt64{Valid: true, Int64: int64(len(thread.Posts))},
		MediaCount:  sql.NullInt64{Valid: true, Int64: int64(countMediaPosts(thread))},
		Status:      sql.NullString{Valid: true, String: "archived"},
	})
	if err != nil {
		// Ignore unique constraint errors
		if a.config.Verbose {
			fmt.Printf("Warning: Failed to insert thread record: %v\n", err)
		}
	}

	// Extract and insert posts with enhanced parsing
	for i, post := range thread.Posts {
		isOP := i == 0

		// Generate content hash for deduplication
		contentHash := a.generateContentHash(post.Comment)

		// Determine if media was successfully processed
		hasMediaProcessed := post.Tim != 0 && post.Ext != "" && (post.ArchivedMoeURL != "" || post.Filename != "")

		// Insert post using enhanced data (country/flag info already extracted during HTML parsing)
		_, err = queries.CreatePost(ctx, database.CreatePostParams{
			ThreadID:          threadID,
			Board:             board,
			PostNo:            post.No,
			Timestamp:         post.Time,
			Name:              post.Name,
			Tripcode:          stringToSqlNull(post.Trip),
			UserID:            stringToSqlNull(post.ID),
			Country:           stringToSqlNull(post.Country),
			CountryName:       stringToSqlNull(post.CountryName),
			Flag:              stringToSqlNull(post.Flag),
			FlagName:          stringToSqlNull(post.FlagName),
			Subject:           stringToSqlNull(post.Subject),
			Comment:           stringToSqlNull(post.Comment),
			CleanText:         stringToSqlNull(cleanHTMLText(post.Comment)),
			ContentHash:       stringToSqlNull(contentHash),
			Source:            source,
			ParsingStatus:     "success",
			Filename:          stringToSqlNull(post.Filename),
			FileExt:           stringToSqlNull(post.Ext),
			FileSize:          intToSqlNull(int64(post.Fsize)),
			ImageWidth:        intToSqlNull(int64(post.W)),
			ImageHeight:       intToSqlNull(int64(post.H)),
			ThumbnailWidth:    sql.NullInt64{},
			ThumbnailHeight:   sql.NullInt64{},
			Md5Hash:           stringToSqlNull(post.MD5),
			IsOp:              sql.NullBool{Valid: true, Bool: isOP},
			HasMediaProcessed: sql.NullBool{Valid: true, Bool: hasMediaProcessed},
		})
		if err != nil && a.config.Verbose {
			fmt.Printf("Warning: Failed to insert post %d: %v\n", post.No, err)
		}

		// Insert user if has ID with enhanced tracking
		if post.ID != "" {
			err = queries.UpsertUser(ctx, database.UpsertUserParams{
				Board:           board,
				UserID:          post.ID,
				Name:            post.Name,
				Tripcode:        stringToSqlNull(post.Trip),
				Country:         stringToSqlNull(post.Country),
				CountryName:     stringToSqlNull(post.CountryName),
				Flag:            stringToSqlNull(post.Flag),
				FlagName:        stringToSqlNull(post.FlagName),
				FirstSeen:       time.Unix(post.Time, 0),
				LastSeen:        time.Unix(post.Time, 0),
				TotalMediaPosts: intToSqlNull(int64(boolToInt(hasMediaProcessed))),
				AvgPostLength:   floatToSqlNull(float64(len(post.Comment))),
				MostCommonBoard: stringToSqlNull(board),
			})
			if err != nil && a.config.Verbose {
				fmt.Printf("Warning: Failed to upsert user %s: %v\n", post.ID, err)
			}
		}

		// Enhanced reply/quote extraction
		err = a.extractRepliesAndQuotes(ctx, queries, post, threadID, board)
		if err != nil && a.config.Verbose {
			fmt.Printf("Warning: Failed to extract replies for post %d: %v\n", post.No, err)
		}

		// Insert enhanced media record if post has media
		if post.Tim != 0 && post.Ext != "" {
			mediaFilename := fmt.Sprintf("%d_%s%s", post.No, post.Filename, post.Ext)
			localPath := filepath.Join(MediaDirName, mediaFilename)

			// Determine media type
			mediaType := a.determineMediaType(post.Ext)

			// Get source URL
			sourceURL := a.getMediaSourceURL(post, board)

			_, err = queries.CreateMedia(ctx, database.CreateMediaParams{
				ThreadID:            threadID,
				Board:               board,
				PostNo:              post.No,
				Filename:            mediaFilename,
				OriginalFilename:    stringToSqlNull(post.Filename + post.Ext),
				FileExt:             stringToSqlNull(post.Ext),
				FileSize:            intToSqlNull(int64(post.Fsize)),
				Width:               intToSqlNull(int64(post.W)),
				Height:              intToSqlNull(int64(post.H)),
				Md5Hash:             stringToSqlNull(post.MD5),
				MediaType:           stringToSqlNull(mediaType),
				SourceUrl:           stringToSqlNull(sourceURL),
				ThumbnailUrl:        stringToSqlNull(post.ArchivedMoeURL), // Use this for thumbnail
				LocalPath:           stringToSqlNull(localPath),
				DownloadStatus:      sql.NullString{Valid: true, String: "downloaded"},
				DownloadAttempts:    sql.NullInt64{Valid: true, Int64: 1},
				LastDownloadAttempt: sql.NullTime{Valid: true, Time: time.Now()},
			})
			if err != nil && a.config.Verbose {
				fmt.Printf("Warning: Failed to insert media record for post %d: %v\n", post.No, err)
			}
		}
	}

	return nil
}

// extractCountryInfo extracts country information from post HTML element
func (a *Archiver) extractCountryInfo(postElement *goquery.Selection) (country, countryName string) {
	// 4chan format: <span title="Germany" class="flag flag-de"></span>
	flagElement := postElement.Find("span.flag")
	if flagElement.Length() > 0 {
		if title, exists := flagElement.Attr("title"); exists && title != "" {
			// Extract country name from title
			countryName = strings.TrimSpace(title)
			// For 4plebs, remove the extra text
			if strings.Contains(countryName, ". Click here to search") {
				countryName = strings.Split(countryName, ".")[0]
			}
		}

		// Extract country code from class
		if class, exists := flagElement.Attr("class"); exists && class != "" {
			// Look for pattern like "flag flag-de" or "flag-pol2 flag-ct"
			classes := strings.Split(class, " ")
			for _, cls := range classes {
				if strings.HasPrefix(cls, "flag-") && len(cls) > 5 {
					// Remove "flag-" prefix to get country code
					code := strings.TrimPrefix(cls, "flag-")
					// Skip pol2 or other prefixes for troll flags
					if len(code) == 2 && !strings.Contains(code, "pol") {
						country = strings.ToUpper(code)
						break
					}
				}
			}
		}
	}

	// If we got country name but no code, try to derive code
	if countryName != "" && country == "" {
		country = a.deriveCountryCode(countryName)
	}

	return country, countryName
}

// extractFlagInfo extracts flag information from post HTML element (includes troll flags)
func (a *Archiver) extractFlagInfo(postElement *goquery.Selection) (flag, flagName string) {
	// Look for both regular flags and troll flags
	flagSelectors := []string{
		"span.flag",      // Regular flags
		"span.flag-pol2", // Troll flags on /pol/
	}

	for _, selector := range flagSelectors {
		flagElement := postElement.Find(selector)
		if flagElement.Length() > 0 {
			if title, exists := flagElement.Attr("title"); exists && title != "" {
				flagName = strings.TrimSpace(title)
				// Clean up 4plebs format
				if strings.Contains(flagName, ". Click here to search") {
					flagName = strings.Split(flagName, ".")[0]
				}
			}

			if class, exists := flagElement.Attr("class"); exists && class != "" {
				// Extract flag code from class
				classes := strings.Split(class, " ")
				for _, cls := range classes {
					if strings.HasPrefix(cls, "flag-") {
						code := strings.TrimPrefix(cls, "flag-")
						// Remove any additional prefixes
						if strings.HasPrefix(code, "pol2-") {
							code = strings.TrimPrefix(code, "pol2-")
						}
						flag = strings.ToUpper(code)
						break
					}
				}
			}
			break
		}
	}

	return flag, flagName
}

// deriveCountryCode attempts to map country names to ISO codes
func (a *Archiver) deriveCountryCode(countryName string) string {
	countryMap := map[string]string{
		"United States":  "US",
		"Germany":        "DE",
		"Netherlands":    "NL",
		"France":         "FR",
		"Canada":         "CA",
		"Australia":      "AU",
		"United Kingdom": "GB",
		"Poland":         "PL",
		"Israel":         "IL",
		"Egypt":          "EG",
		"China":          "CN",
		"Japan":          "JP",
		"Russia":         "RU",
		"Brazil":         "BR",
		"Mexico":         "MX",
		"India":          "IN",
		"South Korea":    "KR",
		"Italy":          "IT",
		"Spain":          "ES",
		"Sweden":         "SE",
		"Norway":         "NO",
		"Finland":        "FI",
		"Denmark":        "DK",
		"Czech Republic": "CZ",
		"Austria":        "AT",
		"Switzerland":    "CH",
		"Belgium":        "BE",
		"Portugal":       "PT",
		"Greece":         "GR",
		"Turkey":         "TR",
		"Saudi Arabia":   "SA",
		"Singapore":      "SG",
		"Ukraine":        "UA",
		"Romania":        "RO",
		"Hungary":        "HU",
		"Croatia":        "HR",
		"Slovakia":       "SK",
		"Slovenia":       "SI",
		"Bulgaria":       "BG",
		"Serbia":         "RS",
		"Lithuania":      "LT",
		"Latvia":         "LV",
		"Estonia":        "EE",
	}

	if code, exists := countryMap[countryName]; exists {
		return code
	}

	return ""
}

// extractRepliesAndQuotes extracts reply and quote relationships from post content
func (a *Archiver) extractRepliesAndQuotes(ctx context.Context, queries *database.Queries, post Post, threadID, board string) error {
	if post.Comment == "" {
		return nil
	}

	// Extract direct replies (>>123456 patterns)
	replyNumbers := extractReplyNumbers(post.Comment)
	for _, replyTo := range replyNumbers {
		// Check if reply already exists
		count, err := queries.ReplyExists(ctx, database.ReplyExistsParams{
			ThreadID:  threadID,
			Board:     board,
			FromPost:  post.No,
			ToPost:    replyTo,
			ReplyType: "direct",
		})
		if err != nil {
			continue
		}

		// Only insert if it doesn't exist
		if count == 0 {
			err = queries.CreateReply(ctx, database.CreateReplyParams{
				ThreadID:  threadID,
				Board:     board,
				FromPost:  post.No,
				ToPost:    replyTo,
				ReplyType: "direct",
			})
			if err != nil && a.config.Verbose {
				fmt.Printf("Warning: Failed to insert reply %d->%d: %v\n", post.No, replyTo, err)
			}
		}
	}

	// Extract quotes (greentext) - TODO: Add quotes.sql with CreateQuote method
	quotes := a.extractQuotes(post.Comment)
	for _, quote := range quotes {
		_, err := queries.CreateQuote(ctx, database.CreateQuoteParams{
			ThreadID:  threadID,
			Board:     board,
			PostNo:    post.No,
			QuoteText: quote.Text,
			QuoteType: quote.Type,
		})
		if err != nil && a.config.Verbose {
			fmt.Printf("Warning: Failed to insert quote for post %d: %v\n", post.No, err)
		}
	}

	return nil
}

// Quote represents a quote extracted from post content
type Quote struct {
	Text string
	Type string // "greentext", "regular"
}

// extractQuotes extracts greentext and other quotes from post content
func (a *Archiver) extractQuotes(content string) []Quote {
	var quotes []Quote

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, ">") && !strings.HasPrefix(line, ">>") {
			// It's greentext
			quoteText := strings.TrimPrefix(line, ">")
			quotes = append(quotes, Quote{
				Text: strings.TrimSpace(quoteText),
				Type: "greentext",
			})
		}
	}

	return quotes
}

// CreateMarkdownFromDatabase generates markdown from database data with clickable links (exported)
func (a *Archiver) CreateMarkdownFromDatabase(ctx context.Context, queries *database.Queries, threadID, board, outputPath string) error {
	// Get all posts for the thread
	posts, err := queries.GetPostsByThread(ctx, database.GetPostsByThreadParams{
		ThreadID: threadID,
		Board:    board,
	})
	if err != nil {
		return fmt.Errorf("failed to get posts: %w", err)
	}

	// Get conversation tree for the thread
	conversations, err := queries.GetConversationTree(ctx, database.GetConversationTreeParams{
		ThreadID: threadID,
		Board:    board,
	})
	if err != nil {
		return fmt.Errorf("failed to get conversation tree: %w", err)
	}

	// Build reply map for "Quoted By" sections
	quotedBy := make(map[int64][]int64)
	for _, conv := range conversations {
		quotedBy[conv.ToPost] = append(quotedBy[conv.ToPost], conv.FromPost)
	}

	var builder strings.Builder

	// Write header
	builder.WriteString(fmt.Sprintf("# Thread %s\n\n", threadID))
	builder.WriteString(fmt.Sprintf("**Board:** /%s/\n", board))
	builder.WriteString(fmt.Sprintf("**Archived:** %s\n", time.Now().UTC().Format("2006-01-02 15:04:05 UTC")))
	builder.WriteString(fmt.Sprintf("**Posts:** %d\n\n", len(posts)))
	builder.WriteString("---\n\n")

	// Process each post
	for i, post := range posts {
		isOP := post.IsOp.Bool

		// Post header with anchor
		if isOP {
			builder.WriteString(fmt.Sprintf("## <a name=\"%d\"></a>Original Post\n\n", post.PostNo))
		} else {
			builder.WriteString(fmt.Sprintf("## <a name=\"%d\"></a>Post #%d\n\n", post.PostNo, post.PostNo))
		}

		// Post metadata
		builder.WriteString("**Post Info:**\n")
		builder.WriteString(fmt.Sprintf("- **Post Number:** [%d](#%d)\n", post.PostNo, post.PostNo))

		// Format timestamp
		if post.Timestamp > 0 {
			timestamp := time.Unix(post.Timestamp, 0).UTC()
			builder.WriteString(fmt.Sprintf("- **Date:** %s\n", timestamp.Format("2006-01-02 15:04:05 UTC")))
		}

		// Author information with country/flag
		builder.WriteString(fmt.Sprintf("- **Author:** %s", post.Name))
		if post.Country.Valid && post.Country.String != "" {
			emoji := getCountryEmoji(post.Country.String)
			if post.CountryName.Valid {
				builder.WriteString(fmt.Sprintf(" %s %s", emoji, post.CountryName.String))
			} else {
				builder.WriteString(fmt.Sprintf(" %s %s", emoji, post.Country.String))
			}
		}
		if post.UserID.Valid && post.UserID.String != "" {
			builder.WriteString(fmt.Sprintf(" (ID: %s)", post.UserID.String))
		}
		builder.WriteString("\n")

		// Subject line
		if post.Subject.Valid && post.Subject.String != "" {
			builder.WriteString(fmt.Sprintf("- **Subject:** %s\n", post.Subject.String))
		}

		// Media information
		if post.Filename.Valid && post.Filename.String != "" {
			builder.WriteString("- **Media:** ")
			builder.WriteString(fmt.Sprintf("`%s%s`", post.Filename.String, post.FileExt.String))
			if post.FileSize.Valid && post.FileSize.Int64 > 0 {
				builder.WriteString(fmt.Sprintf(" (%s)", formatFileSize(int(post.FileSize.Int64))))
			}
			if post.ImageWidth.Valid && post.ImageHeight.Valid && post.ImageWidth.Int64 > 0 && post.ImageHeight.Int64 > 0 {
				builder.WriteString(fmt.Sprintf(" [%dx%d]", post.ImageWidth.Int64, post.ImageHeight.Int64))
			}
			builder.WriteString("\n")
		}

		// Quoted By section with clickable links
		if replies, exists := quotedBy[post.PostNo]; exists && len(replies) > 0 {
			builder.WriteString("- **Quoted By:** ")
			for i, replyPost := range replies {
				if i > 0 {
					builder.WriteString(", ")
				}
				builder.WriteString(fmt.Sprintf("[>>%d](#%d)", replyPost, replyPost))
			}
			builder.WriteString("\n")
		}

		builder.WriteString("\n")

		// Post content with clickable reply links
		if post.CleanText.Valid && post.CleanText.String != "" {
			builder.WriteString("**Content:**\n\n")
			content := a.formatPostContentWithLinks(post.CleanText.String)
			builder.WriteString(content)
			builder.WriteString("\n\n")
		}

		// Separator between posts
		if i < len(posts)-1 {
			builder.WriteString("---\n\n")
		}
	}

	// Write to file
	return os.WriteFile(outputPath, []byte(builder.String()), 0644)
}

// formatPostContentWithLinks formats post content with clickable reply links
func (a *Archiver) formatPostContentWithLinks(content string) string {
	// Convert >>123456 to clickable links
	content = strings.ReplaceAll(content, "&gt;&gt;", ">>")

	// Replace >>123456 with [>>123456](#123456)
	lines := strings.Split(content, "\n")
	var formattedLines []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			formattedLines = append(formattedLines, "")
			continue
		}

		// Format greentext
		if strings.HasPrefix(line, ">") && !strings.HasPrefix(line, ">>") {
			formattedLines = append(formattedLines, fmt.Sprintf("> %s", strings.TrimPrefix(line, ">")))
		} else {
			// Convert >>123456 to clickable links
			for i := 0; i < len(line)-2; i++ {
				if line[i:i+2] == ">>" {
					// Find the number
					j := i + 2
					for j < len(line) && line[j] >= '0' && line[j] <= '9' {
						j++
					}
					if j > i+2 {
						postNo := line[i+2 : j]
						replacement := fmt.Sprintf("[>>%s](#%s)", postNo, postNo)
						line = line[:i] + replacement + line[j:]
						i += len(replacement) - 1
					}
				}
			}
			formattedLines = append(formattedLines, line)
		}
	}

	return strings.Join(formattedLines, "\n")
}

// getCountryEmoji returns emoji for country codes
func getCountryEmoji(countryCode string) string {
	// Map common country codes to emojis
	countryEmojis := map[string]string{
		"US": "🇺🇸",
		"GB": "🇬🇧",
		"CA": "🇨🇦",
		"AU": "🇦🇺",
		"DE": "🇩🇪",
		"FR": "🇫🇷",
		"JP": "🇯🇵",
		"EG": "🇪🇬", // Egypt
		"CN": "🇨🇳",
		"RU": "🇷🇺",
		"BR": "🇧🇷",
		"MX": "🇲🇽",
		"IN": "🇮🇳",
		"KR": "🇰🇷",
		"IT": "🇮🇹",
		"ES": "🇪🇸",
		"NL": "🇳🇱",
		"SE": "🇸🇪",
		"NO": "🇳🇴",
		"FI": "🇫🇮",
		"DK": "🇩🇰",
		"PL": "🇵🇱",
		"CZ": "🇨🇿",
		"AT": "🇦🇹",
		"CH": "🇨🇭",
		"BE": "🇧🇪",
		"PT": "🇵🇹",
		"GR": "🇬🇷",
		"TR": "🇹🇷",
		"IL": "🇮🇱",
		"SA": "🇸🇦",
		"AE": "🇦🇪",
		"SG": "🇸🇬",
		"MY": "🇲🇾",
		"TH": "🇹🇭",
		"PH": "🇵🇭",
		"ID": "🇮🇩",
		"VN": "🇻🇳",
		"TW": "🇹🇼",
		"HK": "🇭🇰",
		"NZ": "🇳🇿",
		"ZA": "🇿🇦",
		"NG": "🇳🇬",
		"MA": "🇲🇦",
		"AR": "🇦🇷",
		"CL": "🇨🇱",
		"CO": "🇨🇴",
		"PE": "🇵🇪",
		"VE": "🇻🇪",
		"UY": "🇺🇾",
		"EC": "🇪🇨",
		"BO": "🇧🇴",
		"PY": "🇵🇾",
	}

	if emoji, exists := countryEmojis[strings.ToUpper(countryCode)]; exists {
		return emoji
	}
	return "🏳️" // Default flag
}

// Helper functions for enhanced database extraction

// generateContentHash creates a hash of the post content for deduplication
func (a *Archiver) generateContentHash(content string) string {
	if content == "" {
		return ""
	}
	// Simple hash based on content length and first/last characters
	// This is a placeholder - could be enhanced with proper cryptographic hash
	if len(content) < 10 {
		return fmt.Sprintf("hash_%d_%s", len(content), content)
	}
	return fmt.Sprintf("hash_%d_%c%c", len(content), content[0], content[len(content)-1])
}

// determineMediaType categorizes media files by extension
func (a *Archiver) determineMediaType(ext string) string {
	if ext == "" {
		return "other"
	}

	ext = strings.ToLower(ext)
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".avif":
		return "image"
	case ".mp4", ".webm", ".avi", ".mov", ".wmv", ".mkv":
		return "video"
	case ".mp3", ".wav", ".ogg", ".flac", ".aac", ".m4a":
		return "audio"
	case ".pdf", ".txt", ".doc", ".docx", ".rtf":
		return "document"
	case ".zip", ".rar", ".7z", ".tar", ".gz":
		return "archive"
	default:
		return "other"
	}
}

// getMediaSourceURL constructs the source URL for media files
func (a *Archiver) getMediaSourceURL(post Post, board string) string {
	if post.ArchivedMoeURL != "" {
		return post.ArchivedMoeURL
	}

	// Construct URL based on source
	switch a.config.Source {
	case SourceFourChan:
		if post.Tim != 0 && post.Ext != "" {
			return fmt.Sprintf("%s/%s/%d%s", FourChanMediaBaseURL, board, post.Tim, post.Ext)
		}
	case SourceArchivedMoe:
		if post.Filename != "" && post.Ext != "" {
			return fmt.Sprintf("%s/%s/%s%s", ArchivedMoeBaseURL, board, post.Filename, post.Ext)
		}
	case Source4Plebs:
		if post.Filename != "" && post.Ext != "" {
			return fmt.Sprintf("%s/%s/image/%s%s", FourPlebsBaseURL, board, post.Filename, post.Ext)
		}
	}

	return ""
}

// boolToInt converts boolean to integer (1 for true, 0 for false)
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// floatToSqlNull converts float64 to sql.NullFloat64
func floatToSqlNull(f float64) sql.NullFloat64 {
	if f == 0.0 {
		return sql.NullFloat64{Valid: false}
	}
	return sql.NullFloat64{Float64: f, Valid: true}
}

// createEnhancedMarkdownExport creates markdown using the enhanced database approach
func (a *Archiver) createEnhancedMarkdownExport(threadID, threadDir, markdownPath string) error {
	// Determine database mode
	var useMemoryDB bool
	switch a.config.DatabaseMode {
	case DatabaseModeMemory:
		useMemoryDB = true
	case DatabaseModeFile:
		useMemoryDB = false
	case DatabaseModeAuto:
		// Auto-detect test mode
		useMemoryDB = strings.Contains(threadDir, "_test_") || os.Getenv("GO_TEST_MODE") == "1"
	default:
		// Default to file-based
		useMemoryDB = false
	}

	// Set database path
	dbPath := filepath.Join(threadDir, ThreadDBFileName)
	if useMemoryDB {
		dbPath = ":memory:"
	}

	// Check if database exists (for file-based mode)
	if !useMemoryDB {
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return fmt.Errorf("database file not found: %s", dbPath)
		}
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Use sqlc queries to generate enhanced markdown
	queries := database.New(db)

	// Generate enhanced markdown from database
	err = a.CreateMarkdownFromDatabase(context.Background(), queries, threadID, a.config.Board, markdownPath)
	if err != nil {
		return fmt.Errorf("failed to create enhanced markdown: %w", err)
	}

	if a.config.Verbose {
		fmt.Printf("✅ Created enhanced markdown export: %s\n", markdownPath)
	}

	return nil
}
