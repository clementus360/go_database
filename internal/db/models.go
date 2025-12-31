package db

import (
	"time"

	pbStream "github.com/clementus360/go_database/internal/proto/stream"
	pbUser "github.com/clementus360/go_database/internal/proto/user"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- USER MODEL ---

type UserDB struct {
	ID           int64      `db:"id"`
	Username     string     `db:"username"`
	Email        string     `db:"email"`
	PasswordHash string     `db:"password_hash"`
	Roles        []string   `db:"roles"`
	Status       string     `db:"status"` // Added: Maps to user_status ENUM
	CreatedAt    time.Time  `db:"created_at"`
	UpdatedAt    time.Time  `db:"updated_at"`
	DeletedAt    *time.Time `db:"deleted_at"`
	IsVerified   bool       `db:"is_verified"` // Added: New field to track email verification
}

func ToProtoUser(u *UserDB) *pbUser.User {
	if u == nil {
		return nil
	}
	return &pbUser.User{
		UserId:     u.ID,
		Username:   u.Username,
		Email:      u.Email,
		Roles:      u.Roles,
		Status:     pbUser.UserStatus(pbUser.UserStatus_value[u.Status]), // Map string to Proto Enum
		CreatedAt:  timestamppb.New(u.CreatedAt),
		IsVerified: u.IsVerified,
	}
}

func ToProtoUserInternal(u *UserDB) *pbUser.UserInternal {
	if u == nil {
		return nil
	}
	return &pbUser.UserInternal{
		Profile:      ToProtoUser(u),
		PasswordHash: u.PasswordHash,
	}
}

// --- STREAM MODEL ---

type ChannelDB struct {
	ID          int32     `db:"id"`
	UserID      int64     `db:"user_id"`
	StreamKey   string    `db:"stream_key"`
	Title       *string   `db:"title"`
	Description *string   `db:"description"`
	IsLive      bool      `db:"is_live"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
}

type StreamSessionDB struct {
	ID         int64      `db:"id"`
	ChannelID  int32      `db:"channel_id"`
	Status     string     `db:"status"`
	Resolution *string    `db:"resolution"`
	Bitrate    *int32     `db:"bitrate_kbps"`
	Codec      *string    `db:"codec"`
	ViewCount  int32      `db:"view_count"`
	StartTime  time.Time  `db:"start_time"`
	EndTime    *time.Time `db:"end_time"`
}

func ToProtoChannel(c *ChannelDB) *pbStream.ChannelResponse {
	resp := &pbStream.ChannelResponse{
		Id:        c.ID,
		UserId:    c.UserID,
		StreamKey: c.StreamKey,
		IsLive:    c.IsLive,
	}
	if c.Title != nil {
		resp.Title = *c.Title
	}
	if c.Description != nil {
		resp.Description = *c.Description
	}
	return resp
}

func ToProtoSession(s *StreamSessionDB) *pbStream.SessionResponse {
	resp := &pbStream.SessionResponse{
		Id:        s.ID,
		ChannelId: s.ChannelID,
		Status:    pbStream.StreamStatus(pbStream.StreamStatus_value[s.Status]),
		ViewCount: s.ViewCount,
		StartTime: timestamppb.New(s.StartTime),
	}
	if s.Resolution != nil {
		resp.Resolution = *s.Resolution
	}
	if s.Bitrate != nil {
		resp.Bitrate = *s.Bitrate
	}
	if s.Codec != nil {
		resp.Codec = *s.Codec
	}
	if s.EndTime != nil {
		resp.EndTime = timestamppb.New(*s.EndTime)
	}
	return resp
}
