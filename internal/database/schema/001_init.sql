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
    user_id TEXT, -- 4chan ID (null for boards like /b/)
    country TEXT, -- Country code (null for boards without flags)
    country_name TEXT, -- Full country name
    flag TEXT, -- Custom flag (for /pol/)
    flag_name TEXT, -- Custom flag name
    subject TEXT,
    comment TEXT, -- Raw HTML comment
    clean_text TEXT, -- Cleaned text without HTML
    filename TEXT, -- Original filename if has media
    file_ext TEXT, -- File extension
    file_size INTEGER, -- File size in bytes
    image_width INTEGER,
    image_height INTEGER,
    thumbnail_width INTEGER,
    thumbnail_height INTEGER,
    md5_hash TEXT,
    is_op BOOLEAN DEFAULT FALSE, -- Is original post
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(thread_id, board, post_no)
);

-- Users table to track unique users (when IDs are available)
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    board TEXT NOT NULL,
    user_id TEXT NOT NULL, -- 4chan user ID
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
    from_post INTEGER NOT NULL, -- Post that is replying
    to_post INTEGER NOT NULL, -- Post being replied to
    reply_type TEXT NOT NULL DEFAULT 'direct', -- 'direct', 'quote', 'mention'
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
    quote_type TEXT NOT NULL DEFAULT 'greentext', -- 'greentext', 'regular'
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
    local_path TEXT, -- Path to downloaded file
    download_status TEXT DEFAULT 'pending', -- 'pending', 'downloaded', 'failed'
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(thread_id, board, post_no, filename)
);

-- Indexes for better query performance
CREATE INDEX idx_posts_thread_board ON posts(thread_id, board);
CREATE INDEX idx_posts_timestamp ON posts(timestamp);
CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_replies_from_post ON replies(from_post);
CREATE INDEX idx_replies_to_post ON replies(to_post);
CREATE INDEX idx_replies_thread_board ON replies(thread_id, board);
CREATE INDEX idx_users_board_user_id ON users(board, user_id);
CREATE INDEX idx_media_thread_board ON media(thread_id, board); 