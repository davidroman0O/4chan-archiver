package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ThreadMetadata represents the state of a thread's archiving process
type ThreadMetadata struct {
	ThreadID        string               `json:"thread_id"`
	Board           string               `json:"board"`
	CreatedAt       time.Time            `json:"created_at"`
	LastUpdated     time.Time            `json:"last_updated"`
	PostsCount      int                  `json:"posts_count"`
	MediaCount      int                  `json:"media_count"`
	DownloadedMedia map[string]MediaInfo `json:"downloaded_media"`
	SavedPosts      map[string]PostInfo  `json:"saved_posts"`
	Status          string               `json:"status"` // "in_progress", "completed", "failed"
	LastError       string               `json:"last_error,omitempty"`
}

// MediaInfo tracks information about downloaded media files
type MediaInfo struct {
	Filename     string    `json:"filename"`
	URL          string    `json:"url"`
	Size         int64     `json:"size"`
	DownloadedAt time.Time `json:"downloaded_at"`
	Hash         string    `json:"hash,omitempty"`
}

// PostInfo tracks information about saved posts
type PostInfo struct {
	PostID    string    `json:"post_id"`
	Timestamp int64     `json:"timestamp"`
	SavedAt   time.Time `json:"saved_at"`
	HasMedia  bool      `json:"has_media"`
}

// Manager handles metadata operations for thread archiving
type Manager struct {
	outputDir string
}

// NewManager creates a new metadata manager
func NewManager(outputDir string) *Manager {
	return &Manager{
		outputDir: outputDir,
	}
}

// GetMetadataPath returns the path to the metadata file for a thread
func (m *Manager) GetMetadataPath(board, threadID string) string {
	return filepath.Join(m.outputDir, board, threadID, ".metadata.json")
}

// LoadMetadata loads existing metadata for a thread, or creates new if it doesn't exist
func (m *Manager) LoadMetadata(board, threadID string) (*ThreadMetadata, error) {
	metadataPath := m.GetMetadataPath(board, threadID)

	// Check if metadata file exists
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		// Create new metadata
		return &ThreadMetadata{
			ThreadID:        threadID,
			Board:           board,
			CreatedAt:       time.Now(),
			LastUpdated:     time.Now(),
			DownloadedMedia: make(map[string]MediaInfo),
			SavedPosts:      make(map[string]PostInfo),
			Status:          "in_progress",
		}, nil
	}

	// Load existing metadata
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	var metadata ThreadMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata file: %w", err)
	}

	return &metadata, nil
}

// SaveMetadata saves metadata to the thread's directory
func (m *Manager) SaveMetadata(metadata *ThreadMetadata) error {
	metadataPath := m.GetMetadataPath(metadata.Board, metadata.ThreadID)

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(metadataPath), 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Update last updated time
	metadata.LastUpdated = time.Now()

	// Marshal to JSON with pretty formatting
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write to file
	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	return nil
}

// IsMediaDownloaded checks if a media file has already been downloaded
func (metadata *ThreadMetadata) IsMediaDownloaded(url string) bool {
	_, exists := metadata.DownloadedMedia[url]
	return exists
}

// IsPostSaved checks if a post has already been saved
func (metadata *ThreadMetadata) IsPostSaved(postID string) bool {
	_, exists := metadata.SavedPosts[postID]
	return exists
}

// AddDownloadedMedia records a newly downloaded media file
func (metadata *ThreadMetadata) AddDownloadedMedia(url, filename string, size int64) {
	if metadata.DownloadedMedia == nil {
		metadata.DownloadedMedia = make(map[string]MediaInfo)
	}

	metadata.DownloadedMedia[url] = MediaInfo{
		Filename:     filename,
		URL:          url,
		Size:         size,
		DownloadedAt: time.Now(),
	}
	metadata.MediaCount = len(metadata.DownloadedMedia)
}

// AddSavedPost records a newly saved post
func (metadata *ThreadMetadata) AddSavedPost(postID string, timestamp int64, hasMedia bool) {
	if metadata.SavedPosts == nil {
		metadata.SavedPosts = make(map[string]PostInfo)
	}

	metadata.SavedPosts[postID] = PostInfo{
		PostID:    postID,
		Timestamp: timestamp,
		SavedAt:   time.Now(),
		HasMedia:  hasMedia,
	}
	metadata.PostsCount = len(metadata.SavedPosts)
}

// SetStatus updates the archiving status
func (metadata *ThreadMetadata) SetStatus(status string) {
	metadata.Status = status
	metadata.LastUpdated = time.Now()
}

// SetError sets an error status and message
func (metadata *ThreadMetadata) SetError(err error) {
	metadata.Status = "failed"
	metadata.LastError = err.Error()
	metadata.LastUpdated = time.Now()
}
