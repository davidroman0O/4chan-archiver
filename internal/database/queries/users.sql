-- name: CreateUser :one
INSERT INTO users (
    board, user_id, name, country, country_name, flag, flag_name,
    first_seen, last_seen, post_count
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
) RETURNING *;

-- name: GetUser :one
SELECT * FROM users 
WHERE board = ? AND user_id = ?;

-- name: UpdateUser :exec
UPDATE users 
SET last_seen = ?, post_count = ?
WHERE board = ? AND user_id = ?;

-- name: UpsertUser :exec
INSERT INTO users (
    board, user_id, name, country, country_name, flag, flag_name,
    first_seen, last_seen, post_count
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, 1
)
ON CONFLICT(board, user_id) DO UPDATE SET
    last_seen = excluded.last_seen,
    post_count = post_count + 1;

-- name: GetTopUsers :many
SELECT * FROM users 
WHERE board = ?
ORDER BY post_count DESC
LIMIT ?;

-- name: GetUsersByCountry :many
SELECT * FROM users 
WHERE board = ? AND country = ?
ORDER BY post_count DESC;

-- name: GetActiveUsers :many
SELECT u.*, p.timestamp as last_post_time
FROM users u
JOIN posts p ON u.user_id = p.user_id AND u.board = p.board
WHERE u.board = ? AND p.timestamp > ?
GROUP BY u.user_id
ORDER BY p.timestamp DESC; 