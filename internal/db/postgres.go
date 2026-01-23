package db

import (
	"context"
	"fmt"
	"time"

	"github.com/clementus360/go_database/internal/logger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

type Client struct {
	pool   *pgxpool.Pool
	logger *logrus.Logger
}

func NewClient(ctx context.Context, dsn string) (*Client, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	var pool *pgxpool.Pool
	const maxRetries = 5

	// 1. Connection Retry Loop
	for i := 1; i <= maxRetries; i++ {
		pool, err = pgxpool.NewWithConfig(ctx, config)
		if err == nil {
			// Verify the connection is actually usable
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err = pool.Ping(pingCtx)
			cancel()

			if err == nil {
				logger.Log.Info("Successfully connected to PostgreSQL.")

				client := &Client{pool: pool}

				// 2. Automated Database Reset (Development Feature)
				if err := client.ResetDatabase(ctx); err != nil {
					logger.Log.Warnf("Database reset failed: %v", err)
				} else {
					logger.Log.Info("Database wiped and sequences reset for a clean start.")
				}

				return client, nil
			}
		}

		if i < maxRetries {
			logger.Log.Warnf("Database connection failed (attempt %d/%d). Retrying in 3 seconds...", i, maxRetries)
			time.Sleep(3 * time.Second)
		}
	}

	return nil, fmt.Errorf("exhausted retries: failed to connect to database: %w", err)
}

// ResetDatabase wipes all user and stream data and restarts the BIGSERIAL IDs.
func (c *Client) ResetDatabase(ctx context.Context) error {
	query := `TRUNCATE TABLE stream_sessions RESTART IDENTITY CASCADE`
	_, err := c.pool.Exec(ctx, query)
	return err
}

func (c *Client) Close() {
	if c.pool != nil {
		c.pool.Close()
		logger.Log.Info("PostgreSQL connection pool closed.")
	}
}
