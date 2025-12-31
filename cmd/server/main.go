package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/clementus360/go_database/internal/api"
	"github.com/clementus360/go_database/internal/config"
	"github.com/clementus360/go_database/internal/db"
	"github.com/clementus360/go_database/internal/logger"
	pbStream "github.com/clementus360/go_database/internal/proto/stream"
	pbUser "github.com/clementus360/go_database/internal/proto/user"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {

	// Initialize Logrus first!
	logger.InitLogger()
	log := logger.Log

	// 1. Load Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("FATAL: Failed to load configuration: %v", err)
	}

	// 2. Load mTLS Credentials
	creds, err := api.LoadServerTLSCredentials()
	if err != nil {
		log.Fatalf("failed to load TLS credentials: %v", err)
	}

	// 3. Initialize Database Connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbClient, err := db.NewClient(ctx, cfg.DatabaseDSN)
	if err != nil {
		log.Fatalf("FATAL: Could not connect to database: %v", err)
	}
	defer dbClient.Close()

	// 4. Setup gRPC Server
	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("FATAL: Failed to listen on port %s: %v", cfg.GRPCPort, err)
	}

	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
	)

	// Create the server handler instance
	serverHandler := api.NewServer(dbClient, log)

	// Register our services
	// Note: RegisterUserServiceServer replaces RegisterAuthServiceServer
	pbStream.RegisterStreamServiceServer(grpcServer, serverHandler)
	pbUser.RegisterUserServiceServer(grpcServer, serverHandler)

	// Enable reflection for tools like Evans or Postman gRPC
	reflection.Register(grpcServer)

	log.Printf("INFO: gRPC Service running on port %s", cfg.GRPCPort)

	// 4. Start gRPC Server (Non-blocking)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("FATAL: gRPC server failed to serve: %v", err)
		}
	}()

	// 5. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("INFO: Shutting down server...")

	// GracefulStop allows existing RPCs to finish before closing
	grpcServer.GracefulStop()

	log.Println("INFO: Server stopped gracefully.")
}
