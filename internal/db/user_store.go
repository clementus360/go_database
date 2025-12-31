package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// CreateUser - Database Service handles the INSERT
func (c *Client) CreateUser(ctx context.Context, user *UserDB) (*UserDB, error) {
	query := `
        INSERT INTO users (username, email, password_hash, roles)
        VALUES ($1, $2, $3, COALESCE($4, '{}'::text[]))
        RETURNING id, created_at, status`

	err := c.pool.QueryRow(ctx, query,
		user.Username, user.Email, user.PasswordHash, user.Roles,
	).Scan(&user.ID, &user.CreatedAt, &user.Status)

	if err != nil {
		return nil, fmt.Errorf("db: failed to insert user: %w", err)
	}
	return user, nil
}

// GetUserByUsername - Used by Auth Service for login
func (c *Client) GetUserByUsername(ctx context.Context, username string) (*UserDB, error) {
	query := `
        SELECT id, username, email, password_hash, roles, status, created_at
        FROM users
        WHERE username = $1 AND deleted_at IS NULL`

	var u UserDB
	err := c.pool.QueryRow(ctx, query, username).Scan(
		&u.ID, &u.Username, &u.Email, &u.PasswordHash, &u.Roles, &u.Status, &u.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("db: user not found")
		}
		return nil, err
	}
	return &u, nil
}

// GetUserByID - Used for profile lookups
func (c *Client) GetUserByID(ctx context.Context, id int64) (*UserDB, error) {
	query := `
        SELECT id, username, email, password_hash, roles, status, created_at, is_verified
        FROM users
        WHERE id = $1 AND deleted_at IS NULL`

	var u UserDB
	err := c.pool.QueryRow(ctx, query, id).Scan(
		&u.ID, &u.Username, &u.Email, &u.PasswordHash, &u.Roles, &u.Status, &u.CreatedAt, &u.IsVerified,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("db: user not found")
		}
		return nil, err
	}
	return &u, nil
}

// UpdateUserPassword - "Dumb" update of the hash
func (c *Client) UpdateUserPassword(ctx context.Context, id int64, newHash string) error {
	query := `UPDATE users SET password_hash = $1, updated_at = NOW() WHERE id = $2 AND deleted_at IS NULL`
	res, err := c.pool.Exec(ctx, query, newHash, id)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("db: user not found or deleted")
	}
	return nil
}

// UpdateUserStatus - For banning or suspending
func (c *Client) UpdateUserStatus(ctx context.Context, id int64, status string) error {
	query := `UPDATE users SET status = $1, updated_at = NOW() WHERE id = $2 AND deleted_at IS NULL`
	res, err := c.pool.Exec(ctx, query, status, id)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("db: user not found or deleted")
	}
	return nil
}

// VerifyUser - Sets is_verified to true
func (c *Client) VerifyUser(ctx context.Context, id int64) error {
	query := `UPDATE users SET is_verified = TRUE, updated_at = NOW() WHERE id = $1 AND deleted_at IS NULL`
	res, err := c.pool.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("db: user not found")
	}
	return nil
}

// AddUserRole - Appends a role to a comma-separated TEXT column
func (c *Client) AddUserRole(ctx context.Context, id int64, role string) error {
	// 1. The Atomic Update attempt
	query := `
        UPDATE users 
        SET roles = array_append(COALESCE(roles, '{}'::text[]), $1),
            updated_at = NOW() 
        WHERE id = $2 
          AND NOT (COALESCE(roles, '{}'::text[]) @> ARRAY[$1]::text[])
          AND deleted_at IS NULL`

	res, err := c.pool.Exec(ctx, query, role, id)
	if err != nil {
		return fmt.Errorf("db: failed to execute role update: %w", err)
	}

	// 2. The Check: If no rows were changed, find out why
	if res.RowsAffected() == 0 {
		var exists bool
		var roles []string

		checkQuery := `SELECT TRUE, roles FROM users WHERE id = $1 AND deleted_at IS NULL`
		err := c.pool.QueryRow(ctx, checkQuery, id).Scan(&exists, &roles)

		if err != nil {
			// If the Scan fails, the user actually doesn't exist
			return fmt.Errorf("db: user with ID %d not found", id)
		}

		// If we reached here, the user exists, so the role must already be present
		for _, r := range roles {
			if r == role {
				return fmt.Errorf("db: user already has the role '%s'", role)
			}
		}
	}

	return nil
}

// DeleteUser - Soft delete
func (c *Client) DeleteUser(ctx context.Context, id int64) error {
	query := `UPDATE users SET deleted_at = NOW() WHERE id = $1`
	res, err := c.pool.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("db: user not found")
	}
	return nil
}
