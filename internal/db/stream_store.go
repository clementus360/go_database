package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// --- CHANNEL DB ACTIONS ---

func (c *Client) CreateChannel(ctx context.Context, userID int64, key string) (*ChannelDB, error) {
	query := `INSERT INTO channels (user_id, stream_key) VALUES ($1, $2) 
              RETURNING id, user_id, stream_key, title, description, is_live, created_at, updated_at`
	var ch ChannelDB
	err := c.pool.QueryRow(ctx, query, userID, key).Scan(
		&ch.ID, &ch.UserID, &ch.StreamKey, &ch.Title, &ch.Description, &ch.IsLive, &ch.CreatedAt, &ch.UpdatedAt,
	)
	return &ch, err
}

func (c *Client) GetChannel(ctx context.Context, id int32, userID int64, streamKey string) (*ChannelDB, error) {
	var query string
	var arg interface{}

	// Prioritize by most specific identifier
	if id != 0 {
		query = `SELECT id, user_id, stream_key, title, description, is_live, created_at, updated_at FROM channels WHERE id = $1`
		arg = id
	} else if userID != 0 {
		query = `SELECT id, user_id, stream_key, title, description, is_live, created_at, updated_at FROM channels WHERE user_id = $1`
		arg = userID
	} else if streamKey != "" {
		query = `SELECT id, user_id, stream_key, title, description, is_live, created_at, updated_at FROM channels WHERE stream_key = $1`
		arg = streamKey
	} else {
		return nil, errors.New("db: must provide channel id, user id, or stream key")
	}

	var ch ChannelDB
	err := c.pool.QueryRow(ctx, query, arg).Scan(
		&ch.ID, &ch.UserID, &ch.StreamKey, &ch.Title, &ch.Description, &ch.IsLive, &ch.CreatedAt, &ch.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("db: channel not found")
		}
		return nil, err
	}
	return &ch, nil
}

func (c *Client) UpdateChannel(ctx context.Context, id int32, updates map[string]interface{}) error {
	if len(updates) == 0 {
		return nil
	}
	setValues, args, argIdx := []string{}, []interface{}{id}, 2
	for col, val := range updates {
		setValues = append(setValues, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}
	query := fmt.Sprintf("UPDATE channels SET %s, updated_at = NOW() WHERE id = $1", strings.Join(setValues, ", "))
	_, err := c.pool.Exec(ctx, query, args...)
	return err
}

func (c *Client) DeleteChannel(ctx context.Context, id int32) error {
	_, err := c.pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", id)
	return err
}

func (c *Client) GetStreamKey(ctx context.Context, userID int64) (string, error) {
	var key string
	query := `SELECT stream_key FROM channels WHERE user_id = $1`

	err := c.pool.QueryRow(ctx, query, userID).Scan(&key)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("db: no channel found for user %d", userID)
		}
		return "", fmt.Errorf("db: failed to fetch stream key: %w", err)
	}

	return key, nil
}

func (c *Client) ResetStreamKey(ctx context.Context, userID int64, newKey string) error {
	query := `
        UPDATE channels 
        SET stream_key = $1, 
            updated_at = NOW() 
        WHERE user_id = $2`

	res, err := c.pool.Exec(ctx, query, newKey, userID)
	if err != nil {
		return fmt.Errorf("db: failed to reset stream key: %w", err)
	}

	if res.RowsAffected() == 0 {
		return fmt.Errorf("db: no channel found for user %d", userID)
	}

	return nil
}

// --- SESSION DB ACTIONS ---

func (c *Client) CreateSession(ctx context.Context, s *StreamSessionDB) (*StreamSessionDB, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// 1. Atomic Check & Lock
	var isLive bool
	err = tx.QueryRow(ctx, "SELECT is_live FROM channels WHERE id = $1 FOR UPDATE", s.ChannelID).Scan(&isLive)
	if err != nil {
		return nil, fmt.Errorf("channel not found")
	}
	if isLive {
		return nil, fmt.Errorf("channel already live")
	}

	// 2. Mark Channel Live
	_, err = tx.Exec(ctx, "UPDATE channels SET is_live = true WHERE id = $1", s.ChannelID)
	if err != nil {
		return nil, err
	}

	// 3. INSERT Session with Category
	query := `INSERT INTO stream_sessions (channel_id, status, resolution, bitrate_kbps, codec, category)
              VALUES ($1, 'LIVE', $2, $3, $4, $5) 
              RETURNING id, start_time`

	// Pass s.Category (from CreateSessionRequest) into the SQL executor
	err = tx.QueryRow(ctx, query, s.ChannelID, s.Resolution, s.Bitrate, s.Codec, s.Category).
		Scan(&s.ID, &s.StartTime)

	if err != nil {
		return nil, err
	}

	return s, tx.Commit(ctx)
}

func (c *Client) GetSession(ctx context.Context, sessionID int64) (*StreamSessionDB, error) {
	// We use COALESCE so that if Title or Description are NULL in the DB,
	// we get an empty string "" instead of a scan error.
	query := `
        SELECT 
            s.id, s.channel_id, s.status, s.resolution, s.bitrate_kbps, s.codec,
            s.view_count, s.start_time,
            COALESCE(c.title, '') as title, 
            COALESCE(c.description, '') as description, 
            c.user_id
        FROM stream_sessions s
        JOIN channels c ON s.channel_id = c.id
        WHERE s.id = $1`

	var s StreamSessionDB
	// Temporary variables for the joined Channel data
	var title, description string
	var userID int32

	// Map the scanner precisely to the struct pointers and local variables
	err := c.pool.QueryRow(ctx, query, sessionID).Scan(
		&s.ID,
		&s.ChannelID,
		&s.Status,
		&s.Resolution, // This is a *string, so Scan handles NULL automatically
		&s.Bitrate,    // This is a *int32, Scan handles NULL
		&s.Codec,      // This is a *string, Scan handles NULL
		&s.ViewCount,
		&s.StartTime,
		&title,
		&description,
		&userID,
	)

	if err != nil {
		return nil, err
	}

	// If you need to pass Title/Description back to the Manager,
	// you'll need to add them to your StreamSessionDB struct first.
	// s.Title = title
	// s.Description = description

	return &s, nil
}

func (c *Client) UpdateSession(ctx context.Context, id int64, updates map[string]interface{}, increment int32) error {
	setValues, args, argIdx := []string{}, []interface{}{id}, 2

	// 1. Handle Incremental Logic (view_count = view_count + X)
	// Only do this if "view_count" isn't already being SET to an absolute value
	if increment != 0 && updates["view_count"] == nil {
		setValues = append(setValues, fmt.Sprintf("view_count = view_count + $%d", argIdx))
		args = append(args, increment)
		argIdx++
	}

	// 2. Handle Absolute Logic (status = X, view_count = Y)
	for col, val := range updates {
		setValues = append(setValues, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	fmt.Printf("UpdateSession - setValues: %v, args: %v\n", setValues, args)

	if len(setValues) == 0 {
		return nil
	}

	query := fmt.Sprintf("UPDATE stream_sessions SET %s WHERE id = $1", strings.Join(setValues, ", "))
	_, err := c.pool.Exec(ctx, query, args...)
	return err
}

// ListSessions provides paginated history for a specific channel
func (c *Client) ListSessions(ctx context.Context, channelID int32, limit, offset int32) ([]*StreamSessionDB, int32, error) {
	var total int32
	// 1. Get total count for the pagination metadata
	countQuery := `SELECT COUNT(*) FROM stream_sessions WHERE channel_id = $1`
	err := c.pool.QueryRow(ctx, countQuery, channelID).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("db: failed to count sessions: %w", err)
	}

	// 2. Fetch the actual page of data
	dataQuery := `
        SELECT id, channel_id, status, resolution, bitrate_kbps, codec, view_count, start_time, end_time 
        FROM stream_sessions 
        WHERE channel_id = $1 
        ORDER BY start_time DESC 
        LIMIT $2 OFFSET $3`

	rows, err := c.pool.Query(ctx, dataQuery, channelID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("db: failed to query sessions: %w", err)
	}
	defer rows.Close()

	var sessions []*StreamSessionDB
	for rows.Next() {
		var s StreamSessionDB
		err := rows.Scan(
			&s.ID, &s.ChannelID, &s.Status, &s.Resolution,
			&s.Bitrate, &s.Codec, &s.ViewCount, &s.StartTime, &s.EndTime,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("db: scan session failed: %w", err)
		}
		sessions = append(sessions, &s)
	}

	return sessions, total, nil
}

func (c *Client) EndSession(ctx context.Context, id int64) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var chanID int32
	// IMPORTANT: 'WHERE status = LIVE' ensures we don't end an already ended session
	// Returning channel_id ensures we know which channel to unlock
	err = tx.QueryRow(ctx, `
        UPDATE stream_sessions 
        SET status = 'COMPLETE', end_time = NOW() 
        WHERE id = $1 AND status = 'LIVE'
        RETURNING channel_id`, id).Scan(&chanID)

	if err != nil {
		if err == pgx.ErrNoRows {
			// Either session doesn't exist OR it's already COMPLETE/ERROR
			return fmt.Errorf("session %d is not active or not found", id)
		}
		return err
	}

	// Unlock the channel
	_, err = tx.Exec(ctx, "UPDATE channels SET is_live = false WHERE id = $1", chanID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (c *Client) UpdateSessionHeartbeat(ctx context.Context, id int64) error {
	query := `
        UPDATE stream_sessions 
        SET last_heartbeat_at = NOW() 
        WHERE id = $1 AND status = 'LIVE'`

	result, err := c.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("db: failed to update heartbeat: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("db: session %d not found or not live", id)
	}
	return nil
}

// GetStaleSessions finds sessions that are LIVE but haven't checked in
func (c *Client) GetStaleSessions(ctx context.Context, thresholdSeconds int32) ([]int64, error) {
	query := `
        SELECT id 
        FROM stream_sessions 
        WHERE status = 'LIVE' 
        AND last_heartbeat_at < (NOW() - ($1 || ' seconds')::interval)`

	rows, err := c.pool.Query(ctx, query, thresholdSeconds)
	if err != nil {
		return nil, fmt.Errorf("db: failed to query stale sessions: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
