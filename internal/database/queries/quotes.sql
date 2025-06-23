-- name: CreateQuote :one
INSERT INTO quotes (
    thread_id, board, post_no, quote_text, quote_type
) VALUES (
    ?, ?, ?, ?, ?
) RETURNING *;

-- name: GetQuotesByPost :many
SELECT * FROM quotes 
WHERE thread_id = ? AND board = ? AND post_no = ?
ORDER BY id ASC;

-- name: GetQuotesByThread :many
SELECT * FROM quotes 
WHERE thread_id = ? AND board = ?
ORDER BY post_no ASC, id ASC;

-- name: SearchQuotes :many
SELECT q.*, p.name, p.user_id, p.timestamp 
FROM quotes q
JOIN posts p ON q.post_no = p.post_no AND q.thread_id = p.thread_id AND q.board = p.board
WHERE q.board = ? AND q.quote_text LIKE ?
ORDER BY q.post_no DESC
LIMIT ?;

-- name: GetGreentextByThread :many
SELECT q.*, p.name, p.user_id, p.timestamp 
FROM quotes q
JOIN posts p ON q.post_no = p.post_no AND q.thread_id = p.thread_id AND q.board = p.board
WHERE q.thread_id = ? AND q.board = ? AND q.quote_type = 'greentext'
ORDER BY q.post_no ASC;

-- name: CountQuotesByThread :one
SELECT COUNT(*) as quote_count FROM quotes 
WHERE thread_id = ? AND board = ?; 