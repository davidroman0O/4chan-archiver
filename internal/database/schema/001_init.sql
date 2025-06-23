-- Create tables for 4chan thread analysis

-- Threads table to track archived threads
CREATE TABLE threads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT NOT NULL,
    board TEXT NOT NULL,
    subject TEXT,
    source TEXT NOT NULL DEFAULT 'auto', -- '4chan', '4plebs', 'archived.moe', 'auto'
    source_url TEXT, -- Original URL where thread was found
    created_at DATETIME NOT NULL,
    last_updated DATETIME NOT NULL,
    posts_count INTEGER DEFAULT 0,
    media_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active', -- 'active', 'archived', 'dead'
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
    tripcode TEXT, -- Dedicated tripcode field (!!xyz)
    user_id TEXT, -- 4chan ID (null for boards like /b/)
    country TEXT, -- Country code (null for boards without flags)
    country_name TEXT, -- Full country name
    flag TEXT, -- Custom flag (for /pol/)
    flag_name TEXT, -- Custom flag name
    subject TEXT,
    comment TEXT, -- Raw HTML comment
    clean_text TEXT, -- Cleaned text without HTML
    content_hash TEXT, -- For deduplication and content verification
    source TEXT NOT NULL DEFAULT 'auto', -- Which source this post was parsed from
    parsing_status TEXT NOT NULL DEFAULT 'complete', -- 'partial', 'complete', 'failed'
    filename TEXT, -- Original filename if has media
    file_ext TEXT, -- File extension
    file_size INTEGER, -- File size in bytes
    image_width INTEGER,
    image_height INTEGER,
    thumbnail_width INTEGER,
    thumbnail_height INTEGER,
    md5_hash TEXT,
    is_op BOOLEAN DEFAULT FALSE, -- Is original post
    has_media_processed BOOLEAN DEFAULT FALSE, -- Whether media was successfully processed
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(thread_id, board, post_no)
);

-- Users table to track unique users (when IDs are available)
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    board TEXT NOT NULL,
    user_id TEXT NOT NULL, -- 4chan user ID
    name TEXT NOT NULL DEFAULT 'Anonymous',
    tripcode TEXT, -- Most commonly used tripcode
    country TEXT,
    country_name TEXT,
    flag TEXT,
    flag_name TEXT,
    first_seen DATETIME NOT NULL,
    last_seen DATETIME NOT NULL,
    post_count INTEGER DEFAULT 0,
    total_media_posts INTEGER DEFAULT 0,
    avg_post_length REAL DEFAULT 0.0,
    most_common_board TEXT,
    UNIQUE(board, user_id)
);

-- Replies table to track conversation relationships
CREATE TABLE replies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT NOT NULL,
    board TEXT NOT NULL,
    from_post INTEGER NOT NULL, -- Post that is replying
    to_post INTEGER NOT NULL, -- Post being replied to
    reply_type TEXT NOT NULL DEFAULT 'direct', -- 'direct', 'quote', 'mention'
    context TEXT, -- Surrounding context of the reply
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(thread_id, board, from_post, to_post, reply_type)
);

-- Quotes table to track greentext quotes
CREATE TABLE quotes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT NOT NULL,
    board TEXT NOT NULL,
    post_no INTEGER NOT NULL,
    quote_text TEXT NOT NULL,
    quote_type TEXT NOT NULL DEFAULT 'greentext', -- 'greentext', 'regular', 'reply'
    referenced_post_no INTEGER, -- Which post is being quoted (if identifiable)
    quote_context TEXT, -- Additional context around the quote
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Media table to track downloaded media files
CREATE TABLE media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT NOT NULL,
    board TEXT NOT NULL,
    post_no INTEGER NOT NULL,
    filename TEXT NOT NULL,
    original_filename TEXT,
    file_ext TEXT,
    file_size INTEGER,
    width INTEGER,
    height INTEGER,
    md5_hash TEXT,
    media_type TEXT, -- 'image', 'video', 'document', 'audio', 'other'
    source_url TEXT, -- Original media URL from source
    thumbnail_url TEXT, -- Thumbnail URL (if different from main)
    local_path TEXT, -- Path to downloaded file
    download_status TEXT DEFAULT 'pending', -- 'pending', 'downloaded', 'failed', 'skipped'
    download_attempts INTEGER DEFAULT 0,
    last_download_attempt DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(thread_id, board, post_no, filename)
);

-- Indexes for better query performance
CREATE INDEX idx_posts_thread_board ON posts(thread_id, board);
CREATE INDEX idx_posts_timestamp ON posts(timestamp);
CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_source ON posts(source);
CREATE INDEX idx_posts_parsing_status ON posts(parsing_status);
CREATE INDEX idx_posts_content_hash ON posts(content_hash);
CREATE INDEX idx_replies_from_post ON replies(from_post);
CREATE INDEX idx_replies_to_post ON replies(to_post);
CREATE INDEX idx_replies_thread_board ON replies(thread_id, board);
CREATE INDEX idx_users_board_user_id ON users(board, user_id);
CREATE INDEX idx_users_tripcode ON users(tripcode);
CREATE INDEX idx_media_thread_board ON media(thread_id, board);
CREATE INDEX idx_media_source_url ON media(source_url);
CREATE INDEX idx_media_download_status ON media(download_status);
CREATE INDEX idx_quotes_referenced_post ON quotes(referenced_post_no);
CREATE INDEX idx_threads_source ON threads(source); 