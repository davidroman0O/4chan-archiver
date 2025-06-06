-- name: CreateMedia :one
INSERT INTO media (
    thread_id, board, post_no, filename, original_filename, file_ext, 
    file_size, width, height, md5_hash, local_path, download_status
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
) RETURNING *;

-- name: GetMedia :one
SELECT * FROM media 
WHERE thread_id = ? AND board = ? AND post_no = ? AND filename = ?;

-- name: GetMediaByThread :many
SELECT * FROM media 
WHERE thread_id = ? AND board = ?
ORDER BY post_no ASC;

-- name: GetMediaByPost :many
SELECT * FROM media 
WHERE thread_id = ? AND board = ? AND post_no = ?;

-- name: UpdateMediaStatus :exec
UPDATE media 
SET download_status = ?
WHERE thread_id = ? AND board = ? AND post_no = ? AND filename = ?;

-- name: CountMediaByThread :one
SELECT COUNT(*) as media_count FROM media 
WHERE thread_id = ? AND board = ?;

-- name: GetDownloadedMedia :many
SELECT * FROM media 
WHERE board = ? AND download_status = 'downloaded'
ORDER BY created_at DESC
LIMIT ?;

-- name: GetFailedDownloads :many
SELECT * FROM media 
WHERE board = ? AND download_status = 'failed'
ORDER BY created_at DESC; 