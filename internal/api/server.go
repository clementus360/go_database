package api

import (
	"context"
	"errors"
	"strings"

	"github.com/clementus360/go_database/internal/db"
	pbCommon "github.com/clementus360/go_database/internal/proto/stream"
	pbStream "github.com/clementus360/go_database/internal/proto/stream"
	pbUser "github.com/clementus360/go_database/internal/proto/user"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pbStream.UnimplementedStreamServiceServer
	pbUser.UnimplementedUserServiceServer

	DBClient *db.Client
	logger   *logrus.Logger
}

func NewServer(dbClient *db.Client, logger *logrus.Logger) *Server {
	return &Server{
		DBClient: dbClient,
		logger:   logger,
	}
}

// handleError maps internal/DB errors to gRPC status codes with descriptive messages.
func (s *Server) handleError(err error, msg string) error {
	if err == nil {
		return nil
	}

	if err.Error() == "channel already live" {
		return status.Errorf(codes.AlreadyExists, "%s: the channel is currently live and cannot start a new session", msg)
	}
	if err.Error() == "channel not found" {
		return status.Errorf(codes.NotFound, "%s: channel does not exist", msg)
	}

	// 1. Not Found
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Errorf(codes.NotFound, "%s: the requested resource does not exist", msg)
	}

	// 2. Database Constraint Violations
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return status.Errorf(codes.AlreadyExists, "%s: a record with these details already exists (conflict: %s)", msg, pgErr.ConstraintName)
		case "23503": // foreign_key_violation
			return status.Errorf(codes.FailedPrecondition, "%s: provided reference ID is invalid or does not exist", msg)
		case "23502": // not_null_violation
			return status.Errorf(codes.InvalidArgument, "%s: missing required fields", msg)
		}
	}

	// 3. Context Deadlines (Timeouts)
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Errorf(codes.DeadlineExceeded, "%s: database took too long to respond", msg)
	}

	// 4. Default Fallback (Actual Internal Errors)
	s.logger.WithFields(logrus.Fields{
		"error":   err.Error(),
		"context": msg,
	}).Error("Database operation failed")

	return status.Errorf(codes.Internal, "%s: an unexpected error occurred on our end", msg)
}

// ----------------------------------------------------------------------------
// USER SERVICE HANDLERS
// ----------------------------------------------------------------------------

func (s *Server) CreateUser(ctx context.Context, req *pbUser.CreateUserRequest) (*pbUser.User, error) {
	if req.GetUsername() == "" || req.GetEmail() == "" {
		return nil, status.Error(codes.InvalidArgument, "username and email are required")
	}

	userModel := &db.UserDB{
		Username:     req.GetUsername(),
		Email:        req.GetEmail(),
		PasswordHash: req.GetPasswordHash(),
		Roles:        req.GetRoles(),
	}

	createdUser, err := s.DBClient.CreateUser(ctx, userModel)
	if err != nil {
		return nil, s.handleError(err, "failed to create user")
	}

	s.logger.Infof("User created: ID=%d, Username=%s", createdUser.ID, createdUser.Username)
	return db.ToProtoUser(createdUser), nil
}

func (s *Server) GetUserByUsername(ctx context.Context, req *pbUser.GetUserByUsernameRequest) (*pbUser.UserInternal, error) {
	user, err := s.DBClient.GetUserByUsername(ctx, req.GetUsername())
	if err != nil {
		return nil, s.handleError(err, "failed to fetch user by username")
	}

	return db.ToProtoUserInternal(user), nil
}

func (s *Server) GetUserByID(ctx context.Context, req *pbUser.GetUserByIDRequest) (*pbUser.UserInternal, error) {
	if req.GetUserId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "a valid user ID is required")
	}

	user, err := s.DBClient.GetUserByID(ctx, req.GetUserId())
	if err != nil {
		// Now returns "NotFound" instead of "Internal" if ID is missing
		return nil, s.handleError(err, "fetching user")
	}

	return db.ToProtoUserInternal(user), nil
}

func (s *Server) UpdateUserPassword(ctx context.Context, req *pbUser.UpdateUserPasswordRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request body cannot be empty")
	}

	err := s.DBClient.UpdateUserPassword(ctx, req.GetUserId(), req.GetNewPasswordHash())
	if err != nil {
		return nil, s.handleError(err, "failed to update user password")
	}

	s.logger.Infof("Password updated for user ID=%d", req.GetUserId())
	return &emptypb.Empty{}, nil
}

func (s *Server) UpdateUserStatus(ctx context.Context, req *pbUser.UpdateUserStatusRequest) (*emptypb.Empty, error) {
	statusStr := req.GetStatus().String()
	err := s.DBClient.UpdateUserStatus(ctx, req.GetUserId(), statusStr)
	if err != nil {
		return nil, s.handleError(err, "failed to update user status")
	}

	s.logger.Infof("Status updated for user ID=%d to %s", req.GetUserId(), statusStr)
	return &emptypb.Empty{}, nil
}

func (s *Server) UpdateUserProfilePicture(ctx context.Context, req *pbUser.UpdateUserProfilePictureRequest) (*emptypb.Empty, error) {
	if req == nil || req.GetUserId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "a valid user_id is required")
	}

	err := s.DBClient.UpdateUserProfilePicture(ctx, req.GetUserId(), req.GetProfileImageUrl())
	if err != nil {
		return nil, s.handleError(err, "failed to update profile picture")
	}

	s.logger.Infof("Profile picture updated for user ID=%d", req.GetUserId())
	return &emptypb.Empty{}, nil
}

func (s *Server) VerifyUser(ctx context.Context, req *pbUser.VerifyUserRequest) (*emptypb.Empty, error) {
	if req.GetUserId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	err := s.DBClient.VerifyUser(ctx, req.GetUserId())
	if err != nil {
		return nil, s.handleError(err, "verifying user")
	}

	s.logger.Infof("User ID=%d is now verified", req.GetUserId())
	return &emptypb.Empty{}, nil
}

func (s *Server) AddUserRole(ctx context.Context, req *pbUser.AddUserRoleRequest) (*emptypb.Empty, error) {
	err := s.DBClient.AddUserRole(ctx, req.GetUserId(), req.GetRole())
	if err != nil {
		// Match the error strings from the DB layer to gRPC codes
		if strings.Contains(err.Error(), "not found") {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		if strings.Contains(err.Error(), "already has the role") {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Error(codes.Internal, "database error during role update")
	}

	s.logger.Infof("Added role '%s' to user ID %d", req.GetRole(), req.GetUserId())
	return &emptypb.Empty{}, nil
}

func (s *Server) DeleteUser(ctx context.Context, req *pbUser.DeleteUserRequest) (*emptypb.Empty, error) {
	err := s.DBClient.DeleteUser(ctx, req.GetUserId())
	if err != nil {
		return nil, s.handleError(err, "failed to delete user")
	}

	s.logger.Warnf("User deleted (Soft Delete): ID=%d", req.GetUserId())
	return &emptypb.Empty{}, nil
}

// ----------------------------------------------------------------------------
// STREAM SERVICE HANDLERS
// ----------------------------------------------------------------------------

// --- CHANNEL HANDLERS ---

func (s *Server) CreateChannel(ctx context.Context, req *pbStream.CreateChannelRequest) (*pbStream.ChannelResponse, error) {
	if req.UserId == 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	ch, err := s.DBClient.CreateChannel(ctx, req.UserId, req.StreamKey)
	if err != nil {
		return nil, s.handleError(err, "create channel")
	}
	return db.ToProtoChannel(ch), nil
}

func (s *Server) GetChannel(ctx context.Context, req *pbStream.GetChannelRequest) (*pbStream.ChannelResponse, error) {
	var id int32
	if req.Id != nil {
		id = *req.Id
	}
	ch, err := s.DBClient.GetChannel(ctx, id, req.GetUserId(), req.GetStreamKey())

	if err != nil {
		// Map common errors for the Auth service to understand
		if strings.Contains(err.Error(), "not found") {
			return nil, status.Error(codes.NotFound, "channel not found")
		}
		return nil, s.handleError(err, "get channel")
	}

	// Convert the DB struct back to the Protobuf message
	return db.ToProtoChannel(ch), nil
}

func (s *Server) UpdateChannel(ctx context.Context, req *pbStream.UpdateChannelRequest) (*pbStream.ChannelResponse, error) {
	updates := make(map[string]interface{})
	if req.Title != nil {
		updates["title"] = req.GetTitle()
	}
	if req.Description != nil {
		updates["description"] = req.GetDescription()
	}
	if req.StreamKey != nil {
		updates["stream_key"] = req.GetStreamKey()
	}

	if err := s.DBClient.UpdateChannel(ctx, req.Id, updates); err != nil {
		return nil, s.handleError(err, "update channel")
	}
	return s.GetChannel(ctx, &pbStream.GetChannelRequest{Id: &req.Id})
}

func (s *Server) GetStreamKey(ctx context.Context, req *pbStream.GetStreamKeyRequest) (*pbStream.GetStreamKeyResponse, error) {
	if req.GetUserId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	key, err := s.DBClient.GetStreamKey(ctx, req.GetUserId())
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, status.Error(codes.NotFound, "Channel not found. User may not be a streamer.")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pbStream.GetStreamKeyResponse{
		StreamKey: key,
	}, nil
}

func (s *Server) ResetStreamKey(ctx context.Context, req *pbStream.ResetStreamKeyRequest) (*pbStream.ChannelResponse, error) {
	err := s.DBClient.ResetStreamKey(ctx, req.GetUserId(), req.GetNewKey())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	// Return the updated channel info (re-use GetChannel logic or return simple response)
	return &pbStream.ChannelResponse{StreamKey: req.GetNewKey()}, nil
}

// --- SESSION HANDLERS ---

func (s *Server) CreateSession(ctx context.Context, req *pbStream.CreateSessionRequest) (*pbStream.SessionResponse, error) {
	session, err := s.DBClient.CreateSession(ctx, &db.StreamSessionDB{
		ChannelID:  req.ChannelId,
		Resolution: &req.Resolution,
		Bitrate:    &req.Bitrate,
		Codec:      &req.Codec,
		Category:   req.Category, // NEW: Capture from Ingest/Manager request
	})

	if err != nil {
		return nil, s.handleError(err, "create session")
	}

	return db.ToProtoSession(session), nil
}

func (s *Server) GetSession(ctx context.Context, req *pbStream.GetSessionRequest) (*pbStream.SessionResponse, error) {
	session, err := s.DBClient.GetSession(ctx, req.Id)
	if err != nil {
		return nil, s.handleError(err, "get session")
	}

	// Map string from DB to the specific Proto Enum
	var protoStatus pbStream.StreamStatus
	switch strings.ToUpper(session.Status) {
	case "LIVE":
		protoStatus = pbStream.StreamStatus_LIVE
	case "COMPLETE", "FINISHED": // Handle both just in case
		protoStatus = pbStream.StreamStatus_COMPLETE
	case "ERROR":
		protoStatus = pbStream.StreamStatus_ERROR
	default:
		protoStatus = pbStream.StreamStatus_OFFLINE
	}

	return &pbStream.SessionResponse{
		Id:         session.ID,
		ChannelId:  session.ChannelID,
		Status:     protoStatus,
		Resolution: *session.Resolution,
		Bitrate:    *session.Bitrate,
		Codec:      *session.Codec,
	}, nil
}

func (s *Server) UpdateSession(ctx context.Context, req *pbStream.UpdateSessionRequest) (*pbStream.SessionResponse, error) {
	// 1. Prepare the map for the generic DB UpdateSession function
	updates := make(map[string]interface{})

	// Check optional proto fields (using proto3 optional / getters)
	if req.Resolution != nil {
		updates["resolution"] = req.GetResolution()
	}
	if req.Bitrate != nil {
		// Map 'bitrate' from proto to 'bitrate_kbps' in DB
		updates["bitrate_kbps"] = req.GetBitrate()
	}
	if req.ViewCount != nil {
		updates["view_count"] = req.GetViewCount()
	}

	// 2. Call the DB Client (passing 0 for increment since this is a direct set)
	if err := s.DBClient.UpdateSession(ctx, req.Id, updates, 0); err != nil {
		return nil, s.handleError(err, "update session metrics")
	}

	// 3. Fetch the updated session to return the full SessionResponse
	updatedSession, err := s.DBClient.GetSession(ctx, req.Id)
	if err != nil {
		return nil, s.handleError(err, "fetch updated session")
	}

	// Use your existing helper to convert DB model to Proto response
	return db.ToProtoSession(updatedSession), nil
}

func (s *Server) UpdateProcessingStatus(ctx context.Context, req *pbStream.UpdateProcessingStatusRequest) (*emptypb.Empty, error) {
	updates := make(map[string]interface{})
	var inc int32

	if req.Status != nil {
		updates["status"] = req.GetStatus().String()
	}

	// New Absolute Logic: If current_view_count is provided, add it to the SET map
	if req.CurrentViewCount != nil {
		updates["view_count"] = req.GetCurrentViewCount()
	}

	// Legacy/Incremental Logic: Only used if specifically requested
	if req.ViewCountIncrement != nil {
		inc = req.GetViewCountIncrement()
	}

	if err := s.DBClient.UpdateSession(ctx, req.SessionId, updates, inc); err != nil {
		return nil, s.handleError(err, "status update")
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) ListSessions(ctx context.Context, req *pbStream.ListSessionsRequest) (*pbStream.ListSessionsResponse, error) {
	limit, page := req.PageSize, req.PageNumber
	if limit <= 0 {
		limit = 10
	}
	if page <= 0 {
		page = 1
	}

	sessions, total, err := s.DBClient.ListSessions(ctx, req.ChannelId, limit, (page-1)*limit)
	if err != nil {
		return nil, s.handleError(err, "list sessions")
	}

	var resp []*pbStream.SessionResponse
	for _, sess := range sessions {
		resp = append(resp, db.ToProtoSession(sess))
	}

	return &pbStream.ListSessionsResponse{
		Sessions: resp,
		MetaData: &pbCommon.PaginationMetadata{
			TotalCount:  total,
			PageSize:    limit,
			CurrentPage: page,
			TotalPages:  (total + limit - 1) / limit,
		},
	}, nil
}

func (s *Server) EndSession(ctx context.Context, req *pbStream.EndSessionRequest) (*emptypb.Empty, error) {
	if err := s.DBClient.EndSession(ctx, req.Id); err != nil {
		return nil, s.handleError(err, "end session")
	}
	return &emptypb.Empty{}, nil
}

// UpdateSessionHeartbeat is called by the Stream Service when a heartbeat arrives
func (s *Server) UpdateSessionHeartbeat(ctx context.Context, req *pbStream.UpdateSessionHeartbeatRequest) (*emptypb.Empty, error) {
	if err := s.DBClient.UpdateSessionHeartbeat(ctx, req.SessionId); err != nil {
		return nil, s.handleError(err, "update session heartbeat")
	}
	return &emptypb.Empty{}, nil
}

// GetStaleSessions is called by the Stream Service Watchdog
func (s *Server) GetStaleSessions(ctx context.Context, req *pbStream.GetStaleSessionsRequest) (*pbStream.GetStaleSessionsResponse, error) {
	ids, err := s.DBClient.GetStaleSessions(ctx, req.ThresholdSeconds)
	if err != nil {
		return nil, s.handleError(err, "get stale sessions")
	}

	return &pbStream.GetStaleSessionsResponse{
		SessionIds: ids,
	}, nil
}
