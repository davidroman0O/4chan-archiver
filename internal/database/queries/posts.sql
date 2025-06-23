-- name: CreatePost :one
INSERT INTO posts (
    thread_id, board, post_no, timestamp, name, tripcode, user_id, 
    country, country_name, flag, flag_name, subject, comment, clean_text,
    content_hash, source, parsing_status, filename, file_ext, file_size, 
    image_width, image_height, thumbnail_width, thumbnail_height, md5_hash, 
    is_op, has_media_processed
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
) RETURNING *;

-- name: GetPost :one
SELECT * FROM posts 
WHERE thread_id = ? AND board = ? AND post_no = ?;

-- name: GetPostsByThread :many
SELECT * FROM posts 
WHERE thread_id = ? AND board = ?
ORDER BY post_no ASC;

-- name: GetPostsByUser :many
SELECT * FROM posts 
WHERE board = ? AND user_id = ?
ORDER BY timestamp DESC;

-- name: SearchPosts :many
SELECT * FROM posts 
WHERE board = ? AND clean_text LIKE ?
ORDER BY timestamp DESC
LIMIT ?;

-- name: GetRecentPosts :many
SELECT * FROM posts 
WHERE board = ?
ORDER BY timestamp DESC
LIMIT ?;

-- name: CountPostsByThread :one
SELECT COUNT(*) as post_count FROM posts 
WHERE thread_id = ? AND board = ?;

-- name: GetOriginalPost :one
SELECT * FROM posts 
WHERE thread_id = ? AND board = ? AND is_op = TRUE; 