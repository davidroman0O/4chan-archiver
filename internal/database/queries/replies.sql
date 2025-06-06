-- name: CreateReply :exec
INSERT INTO replies (
    thread_id, board, from_post, to_post, reply_type
) VALUES (
    ?, ?, ?, ?, ?
);

-- name: ReplyExists :one
SELECT COUNT(*) FROM replies 
WHERE thread_id = ? AND board = ? AND from_post = ? AND to_post = ? AND reply_type = ?;

-- name: GetRepliesTo :many
SELECT r.*, p.clean_text as from_text 
FROM replies r
JOIN posts p ON r.from_post = p.post_no AND r.thread_id = p.thread_id AND r.board = p.board
WHERE r.thread_id = ? AND r.board = ? AND r.to_post = ?
ORDER BY r.from_post ASC;

-- name: GetRepliesFrom :many
SELECT r.*, p.clean_text as to_text 
FROM replies r
JOIN posts p ON r.to_post = p.post_no AND r.thread_id = p.thread_id AND r.board = p.board
WHERE r.thread_id = ? AND r.board = ? AND r.from_post = ?
ORDER BY r.to_post ASC;

-- name: GetConversationTree :many
SELECT 
    r.from_post,
    r.to_post,
    r.reply_type,
    p1.clean_text as from_text,
    p1.user_id as from_user_id,
    p1.timestamp as from_timestamp,
    p2.clean_text as to_text,
    p2.user_id as to_user_id,
    p2.timestamp as to_timestamp
FROM replies r
JOIN posts p1 ON r.from_post = p1.post_no AND r.thread_id = p1.thread_id AND r.board = p1.board
JOIN posts p2 ON r.to_post = p2.post_no AND r.thread_id = p2.thread_id AND r.board = p2.board
WHERE r.thread_id = ? AND r.board = ?
ORDER BY r.from_post ASC;

-- name: GetMostRepliedPosts :many
SELECT 
    r.to_post,
    COUNT(*) as reply_count,
    p.clean_text,
    p.user_id,
    p.timestamp
FROM replies r
JOIN posts p ON r.to_post = p.post_no AND r.thread_id = p.thread_id AND r.board = p.board
WHERE r.thread_id = ? AND r.board = ?
GROUP BY r.to_post
ORDER BY reply_count DESC
LIMIT ?;

-- name: GetUserConversations :many
SELECT 
    r.from_post,
    r.to_post,
    r.reply_type,
    p1.clean_text as from_text,
    p2.clean_text as to_text,
    p2.user_id as conversation_partner
FROM replies r
JOIN posts p1 ON r.from_post = p1.post_no AND r.thread_id = p1.thread_id AND r.board = p1.board
JOIN posts p2 ON r.to_post = p2.post_no AND r.thread_id = p2.thread_id AND r.board = p2.board
WHERE r.thread_id = ? AND r.board = ? AND p1.user_id = ?
ORDER BY r.from_post ASC; 