// internal/config/config.go
package config

import "os"

type Config struct {
	DatabaseDSN string
	GRPCPort    string
}

func LoadConfig() (*Config, error) {
	// Docker injects these. We provide defaults just in case.
	dsn := os.Getenv("DATABASE_DSN")
	if dsn == "" {
		dsn = "postgres://user:password@localhost:5432/stream_db?sslmode=disable"
	}

	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = ":50051"
	}

	return &Config{
		DatabaseDSN: dsn,
		GRPCPort:    port,
	}, nil
}
