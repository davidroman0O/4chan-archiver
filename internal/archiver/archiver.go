package archiver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/corpix/uarand"
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
	SourceAuto        = "auto"
)

// Base URLs for different sources
const (
	FourChanAPIBaseURL   = "https://a.4cdn.org"
	FourChanMediaBaseURL = "https://i.4cdn.org"
	ArchivedMoeBaseURL   = "https://archived.moe"
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
	ThreadJSONFileName = "thread.json"
	PostsJSONFileName  = "posts.json"
	ThreadDBFileName   = "thread.db"
	MetadataFileName   = ".metadata.json"
	MediaDirName       = "media"
)

// Default configuration values
const (
	DefaultDatabaseMode = DatabaseModeFile
	DefaultBoard        = BoardPol
	DefaultSource       = SourceFourChan
)

// Config holds configuration for the archiver
type Config struct {
	Board          string
	OutputDir      string
	RateLimitMs    int
	MaxRetries     int
	UserAgent      string
	Verbose        bool
	IncludeContent bool
	IncludeMedia   bool
	IncludePosts   bool
	MaxConcurrency int
	SkipExisting   bool

	// Database configuration
	DatabaseMode string // DatabaseModeMemory, DatabaseModeFile, or DatabaseModeAuto (auto detects test mode)

	// Source configuration
	Source string // SourceFourChan, SourceArchivedMoe, or SourceAuto
}

// ArchiveResult represents the result of archiving a single thread
type ArchiveResult struct {
	ThreadID        string
	MediaDownloaded int
	PostsSaved      int
	Error           error
}

// Archiver handles the archiving process
type Archiver struct {
	config          *Config
	client          *http.Client
	limiter         *rate.Limiter
	metadataManager *metadata.Manager
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
}

// New creates a new archiver instance
func New(config *Config) (*Archiver, error) {
	if config.Board == "" {
		return nil, fmt.Errorf("board is required")
	}
	if config.OutputDir == "" {
		return nil, fmt.Errorf("output directory is required")
	}

	// Create HTTP client with timeout and redirect handling (like the working code)
	client := &http.Client{
		Timeout: 30 * time.Second,
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

	if a.config.Verbose {
		fmt.Printf("Starting archive of thread %s from /%s/\n", threadID, a.config.Board)
	}

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

	meta.SetStatus("completed")

	// Save final metadata
	if err := a.metadataManager.SaveMetadata(meta); err != nil {
		fmt.Printf("Warning: failed to save metadata for thread %s: %v\n", threadID, err)
	}

	if a.config.Verbose {
		fmt.Printf("Completed archive of thread %s\n", threadID)
	}

	return result
}

// fetchThread retrieves thread data from either 4chan API or archived.moe
func (a *Archiver) fetchThread(threadID string) (*Thread, error) {
	switch a.config.Source {
	case SourceFourChan:
		return a.fetchFromFourChan(threadID)
	case SourceArchivedMoe:
		return a.fetchFromArchivedMoe(threadID)
	case SourceAuto:
		// Try 4chan first, then archived.moe if 404
		thread, err := a.fetchFromFourChan(threadID)
		if err != nil && strings.Contains(err.Error(), "thread not found (404)") {
			if a.config.Verbose {
				fmt.Printf("Thread %s not found on 4chan, trying archived.moe...\n", threadID)
			}
			return a.fetchFromArchivedMoe(threadID)
		}
		return thread, err
	default:
		// Default to 4chan
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

		err = json.NewDecoder(resp.Body).Decode(&thread)
		resp.Body.Close()
		if err != nil {
			if attempt == a.config.MaxRetries-1 {
				return nil, err
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
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
	thread := &Thread{Posts: []Post{}}
	postNum := int64(1)
	threadIDInt, _ := strconv.ParseInt(threadID, 10, 64)

	// Find all <a> elements that wrap an <img> element (like in the working code)
	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		if s.Find("img").Length() == 0 {
			return
		}

		href, exists := s.Attr("href")
		if !exists || href == "" {
			return
		}

		// Process links to various file types found in 4chan/archived threads
		lower := strings.ToLower(href)
		isMediaFile := strings.Contains(lower, ".jpg") ||
			strings.Contains(lower, ".jpeg") ||
			strings.Contains(lower, ".png") ||
			strings.Contains(lower, ".gif") ||
			strings.Contains(lower, ".webp") ||
			strings.Contains(lower, ".webm") ||
			strings.Contains(lower, ".mp4") ||
			strings.Contains(lower, ".pdf") ||
			strings.Contains(lower, ".txt") ||
			strings.Contains(lower, ".zip") ||
			strings.Contains(lower, ".rar") ||
			strings.Contains(lower, ".7z") ||
			strings.Contains(lower, ".mp3") ||
			strings.Contains(lower, ".wav") ||
			strings.Contains(lower, ".ogg") ||
			strings.Contains(lower, ".flac") ||
			strings.Contains(lower, ".swf") ||
			strings.Contains(lower, ".bmp") ||
			strings.Contains(lower, ".tiff") ||
			strings.Contains(lower, ".avif")

		if !isMediaFile {
			return
		}

		// Normalize the URL (exactly like the working code)
		if strings.HasPrefix(href, "//") {
			href = "https:" + href
		} else if strings.HasPrefix(href, "/") {
			href = "https://archived.moe" + href
		}

		// Extract filename from the URL
		parts := strings.Split(href, "/")
		filename := parts[len(parts)-1]
		ext := filepath.Ext(filename)
		filenameNoExt := strings.TrimSuffix(filename, ext)

		// Create a minimal post structure for this media
		post := Post{
			No:             postNum,
			Time:           time.Now().Unix(),
			Name:           "Anonymous",
			Tim:            postNum, // Use post number as tim
			Filename:       filenameNoExt,
			Ext:            ext,
			ArchivedMoeURL: href, // Store the actual archived.moe URL
		}

		thread.Posts = append(thread.Posts, post)
		postNum++
	})

	// If no media posts found, create at least one dummy post for the thread
	if len(thread.Posts) == 0 {
		thread.Posts = append(thread.Posts, Post{
			No:   threadIDInt,
			Time: time.Now().Unix(),
			Name: "Anonymous",
		})
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
	// Always set a user agent
	userAgent := a.config.UserAgent
	if userAgent == "" {
		userAgent = uarand.GetRandom()
	}
	req.Header.Set("User-Agent", userAgent)

	// After 2+ failures, add more realistic browser headers
	if attemptCount >= 2 {
		// Accept any content type - 4chan/archived threads can have PDFs, videos, archives, etc.
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		req.Header.Set("Accept-Encoding", "gzip, deflate, br")
		req.Header.Set("Cache-Control", "max-age=0")

		// Use appropriate Sec-Fetch headers for media downloads
		req.Header.Set("Sec-Fetch-Dest", "image") // Most downloads are images
		req.Header.Set("Sec-Fetch-Mode", "no-cors")
		req.Header.Set("Sec-Fetch-Site", "cross-site")
		req.Header.Set("Upgrade-Insecure-Requests", "1")
	}

	// After 3+ failures, add referrer and connection headers
	if attemptCount >= 3 {
		if strings.Contains(req.URL.Host, "thebarchive.com") {
			req.Header.Set("Referer", "https://archived.moe/")
		} else if strings.Contains(req.URL.Host, "archived.moe") {
			req.Header.Set("Referer", "https://archived.moe/")
		}
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("DNT", "1")
	}

	// After 4+ failures, randomize user agent again and add viewport hint
	if attemptCount >= 4 {
		req.Header.Set("User-Agent", uarand.GetRandom()) // Force new random UA
		req.Header.Set("Sec-Ch-Ua", `"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"`)
		req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
		req.Header.Set("Sec-Ch-Ua-Platform", `"macOS"`)
	}

	if a.config.Verbose && attemptCount > 1 {
		fmt.Printf("Attempt %d for %s: using enhanced headers\n", attemptCount, req.URL.String())
	}
}

// downloadMediaToSubdir downloads media to the media subdirectory with proper filename format
func (a *Archiver) downloadMediaToSubdir(thread *Thread, mediaDir string, meta *metadata.ThreadMetadata) (int, error) {
	downloaded := 0

	for _, post := range thread.Posts {
		if post.Tim == 0 || post.Ext == "" {
			continue // No media file
		}

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

		if a.config.Verbose {
			fmt.Printf("Downloading media: %s\n", mediaURL)
		}

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
	// Simple HTML tag removal - you can improve this
	text := html
	// Remove common HTML tags
	replacements := []string{
		"<br>", "\n",
		"<br/>", "\n",
		"<br />", "\n",
		"&gt;", ">",
		"&lt;", "<",
		"&amp;", "&",
		"&quot;", "\"",
	}
	for i := 0; i < len(replacements); i += 2 {
		text = strings.Replace(text, replacements[i], replacements[i+1], -1)
	}
	return text
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
