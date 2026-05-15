package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"

	_ "github.com/ClickHouse/clickhouse-go/v2"

	pb "request-analytics-service/internal/pb"
)

type analyticsServer struct {
	pb.UnimplementedAnalyticsServiceServer

	db *sqlx.DB

	logger *slog.Logger
}

func (s *analyticsServer) LogEvent(
	ctx context.Context,
	req *pb.EventRequest) (*pb.EventResponse, error) {
	insertQuery := `
	INSERT INTO endpoint_analytics
	(id, endpoint, created_at)
	VALUES (?, ?, ?)`

	createdAt := time.Unix(req.Timestamp, 0)

	_, err := s.db.ExecContext(
		ctx,
		insertQuery,
		req.Id,
		req.Endpoint,
		createdAt,
	)

	if err != nil {

		s.logger.Error(
			"failed to insert analytics event",
			slog.Any("error", err),
		)

		return &pb.EventResponse{
			Success: false,
			Message: "insert failed",
		}, err
	}

	return &pb.EventResponse{
		Success: true,
		Message: "event stored",
	}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	slog.SetDefault(logger)

	dsn := "clickhouse://default:password@localhost:9000/analytics"

	db, err := sqlx.Connect("clickhouse", dsn)
	if err != nil {

		logger.Error(
			"failed to connect clickhouse",
			slog.Any("error", err),
		)

		os.Exit(1)
	}

	defer db.Close()

	logger.Info("connected to clickhouse")
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS endpoint_analytics
	(
		id UInt64,
		endpoint String,
		created_at DateTime
	)
	ENGINE = MergeTree()
	ORDER BY (created_at)
	`

	_, err = db.ExecContext(
		context.Background(),
		createTableQuery,
	)

	if err != nil {

		logger.Error(
			"failed to create table",
			slog.Any("error", err),
		)

		os.Exit(1)
	}

	logger.Info("analytics table initialized")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {

		logger.Error(
			"failed to listen grpc port",
			slog.Any("error", err),
		)

		os.Exit(1)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterAnalyticsServiceServer(
		grpcServer,
		&analyticsServer{
			db:     db,
			logger: logger,
		},
	)

	logger.Info(
		"grpc analytics server started",
		slog.String("addr", ":50051"),
	)

	if err := grpcServer.Serve(lis); err != nil {

		logger.Error(
			"grpc server failed",
			slog.Any("error", err),
		)

		os.Exit(1)
	}
}
