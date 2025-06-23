-- name: CreateThread :one
INSERT INTO threads (
    thread_id, board, subject, source, source_url, created_at, last_updated, posts_count, media_count, status
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
) RETURNING *;

-- name: GetThread :one
SELECT * FROM threads 
WHERE thread_id = ? AND board = ?;

-- name: UpdateThread :exec
UPDATE threads 
SET last_updated = ?, posts_count = ?, media_count = ?, status = ?
WHERE thread_id = ? AND board = ?;

-- name: ListThreads :many
SELECT * FROM threads
WHERE board = ?
ORDER BY last_updated DESC;

-- name: GetThreadStats :one
SELECT 
    COUNT(*) as total_threads,
    SUM(posts_count) as total_posts,
    SUM(media_count) as total_media
FROM threads 
WHERE board = ?; 