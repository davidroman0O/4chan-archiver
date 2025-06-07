package archiver

import (
	"testing"
	"time"
)

func TestFindNewPosts(t *testing.T) {
	// Create archiver instance
	config := &Config{
		Board:     "b",
		OutputDir: "/tmp/test",
	}
	archiver, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	// Create test thread with posts
	thread := &Thread{
		Posts: []Post{
			{No: 123456789, Time: 1640995200, Name: "Anonymous", Comment: "OP post"},
			{No: 123456790, Time: 1640995300, Name: "Anonymous", Comment: "Reply 1"},
			{No: 123456791, Time: 1640995400, Name: "Anonymous", Comment: "Reply 2"},
			{No: 123456792, Time: 1640995500, Name: "Anonymous", Comment: "Reply 3"},
			{No: 123456793, Time: 1640995600, Name: "Anonymous", Comment: "Reply 4"},
		},
	}

	tests := []struct {
		name             string
		lastPostNo       int64
		expectedNewPosts int
		expectedHighest  int64
	}{
		{
			name:             "No previous posts - all are new",
			lastPostNo:       0,
			expectedNewPosts: 5,
			expectedHighest:  123456793,
		},
		{
			name:             "Some previous posts - only new ones returned",
			lastPostNo:       123456791,
			expectedNewPosts: 2,
			expectedHighest:  123456793,
		},
		{
			name:             "All posts already seen - no new posts",
			lastPostNo:       123456793,
			expectedNewPosts: 0,
			expectedHighest:  123456793,
		},
		{
			name:             "Last post higher than all (edge case)",
			lastPostNo:       123456800,
			expectedNewPosts: 0,
			expectedHighest:  123456793,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newPosts := archiver.findNewPosts(thread, tt.lastPostNo)

			if len(newPosts) != tt.expectedNewPosts {
				t.Errorf("Expected %d new posts, got %d", tt.expectedNewPosts, len(newPosts))
			}

			highest := archiver.getHighestPostNo(thread)
			if highest != tt.expectedHighest {
				t.Errorf("Expected highest post no %d, got %d", tt.expectedHighest, highest)
			}

			// Verify all returned posts have higher numbers than lastPostNo
			for _, post := range newPosts {
				if post.No <= tt.lastPostNo {
					t.Errorf("Found post %d which is not higher than lastPostNo %d", post.No, tt.lastPostNo)
				}
			}
		})
	}
}

func TestCountNewMedia(t *testing.T) {
	// Create archiver instance
	config := &Config{
		Board:     "b",
		OutputDir: "/tmp/test",
	}
	archiver, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	posts := []Post{
		{No: 1, Tim: 0, Ext: "", Comment: "Text only post"},
		{No: 2, Tim: 1640995200, Ext: ".jpg", Filename: "image1", Comment: "Post with image"},
		{No: 3, Tim: 1640995300, Ext: ".png", Filename: "image2", Comment: "Post with PNG"},
		{No: 4, Tim: 0, Ext: "", Comment: "Another text post"},
		{No: 5, Tim: 1640995400, Ext: ".webm", Filename: "video1", Comment: "Post with video"},
	}

	mediaCount := archiver.countNewMedia(posts)
	expectedMediaCount := 3 // Posts 2, 3, and 5 have media

	if mediaCount != expectedMediaCount {
		t.Errorf("Expected %d media posts, got %d", expectedMediaCount, mediaCount)
	}
}

func TestGetLatestPostTime(t *testing.T) {
	// Create archiver instance
	config := &Config{
		Board:     "b",
		OutputDir: "/tmp/test",
	}
	archiver, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	thread := &Thread{
		Posts: []Post{
			{No: 1, Time: 1640995200},
			{No: 2, Time: 1640995100}, // Earlier time
			{No: 3, Time: 1640995300}, // Latest time
			{No: 4, Time: 1640995250},
		},
	}

	latest := archiver.getLatestPostTime(thread)
	expected := int64(1640995300)

	if latest != expected {
		t.Errorf("Expected latest time %d, got %d", expected, latest)
	}
}

func TestMonitorConfigValidation(t *testing.T) {
	// Create archiver instance
	config := &Config{
		Board:     "b",
		OutputDir: "/tmp/test",
		Source:    SourceFourChan,
	}
	archiver, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	tests := []struct {
		name          string
		monitorConfig *MonitorConfig
		expectError   bool
	}{
		{
			name: "Valid config",
			monitorConfig: &MonitorConfig{
				ThreadIDs:        []string{"123456789"},
				Interval:         5 * time.Minute,
				MaxDuration:      1 * time.Hour,
				StopOnArchive:    true,
				StopOnInactivity: 30 * time.Minute,
			},
			expectError: false,
		},
		{
			name: "Multiple thread IDs",
			monitorConfig: &MonitorConfig{
				ThreadIDs:        []string{"123456789", "987654321"},
				Interval:         5 * time.Minute,
				MaxDuration:      1 * time.Hour,
				StopOnArchive:    true,
				StopOnInactivity: 30 * time.Minute,
			},
			expectError: false,
		},
		{
			name: "Empty thread IDs",
			monitorConfig: &MonitorConfig{
				ThreadIDs:        []string{},
				Interval:         5 * time.Minute,
				MaxDuration:      1 * time.Hour,
				StopOnArchive:    true,
				StopOnInactivity: 30 * time.Minute,
			},
			expectError: true,
		},
		{
			name: "Nil thread IDs",
			monitorConfig: &MonitorConfig{
				ThreadIDs:        nil,
				Interval:         5 * time.Minute,
				MaxDuration:      1 * time.Hour,
				StopOnArchive:    true,
				StopOnInactivity: 30 * time.Minute,
			},
			expectError: true,
		},
		{
			name: "Zero interval (edge case)",
			monitorConfig: &MonitorConfig{
				ThreadIDs:        []string{"123456789"},
				Interval:         0,
				MaxDuration:      1 * time.Hour,
				StopOnArchive:    true,
				StopOnInactivity: 30 * time.Minute,
			},
			expectError: false, // Should work but will cause immediate rapid checking
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't easily test the full MonitorThreads function without mocking HTTP,
			// but we can test the validation logic by checking if the function
			// returns immediately with an error for invalid configs

			// Start monitoring in a goroutine with a timeout
			done := make(chan error, 1)
			go func() {
				err := archiver.MonitorThreads(tt.monitorConfig)
				done <- err
			}()

			// Stop monitoring immediately to test validation
			go func() {
				time.Sleep(100 * time.Millisecond)
				archiver.StopMonitoring()
			}()

			// Wait for completion with timeout
			select {
			case err := <-done:
				if tt.expectError && err == nil {
					t.Errorf("Expected error but got none")
				}
				if !tt.expectError && err != nil && err.Error() != "" {
					// Only fail if it's a validation error, not network/fetch errors
					if err.Error() == "at least one thread ID is required for monitoring" {
						t.Errorf("Unexpected validation error: %v", err)
					}
				}
			case <-time.After(2 * time.Second):
				t.Errorf("Test timed out")
				archiver.StopMonitoring()
			}
		})
	}
}
