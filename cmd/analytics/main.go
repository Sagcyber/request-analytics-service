package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/ClickHouse/clickhouse-go/v2"

	pb "request-analytics-service/internal/pb"

	"google.golang.org/grpc"
)

const (
	batchSize = 1000

	flushInterval = 2 * time.Second
)

type AnalyticsEvent struct {
	ID        uint64
	Endpoint  string
	CreatedAt time.Time
}

type analyticsServer struct {
	pb.UnimplementedAnalyticsServiceServer

	db *sqlx.DB

	logger *slog.Logger

	events chan AnalyticsEvent
}

func (s *analyticsServer) LogEvent(
	ctx context.Context,
	req *pb.EventRequest,
) (*pb.EventResponse, error) {

	event := AnalyticsEvent{
		ID:        req.Id,
		Endpoint:  req.Endpoint,
		CreatedAt: time.Unix(req.Timestamp, 0),
	}

	select {

	case s.events <- event:

		return &pb.EventResponse{
			Success: true,
			Message: "event queued",
		}, nil

	default:

		s.logger.Warn("event channel is full")

		return &pb.EventResponse{
			Success: false,
			Message: "event dropped",
		}, nil
	}
}

func (s *analyticsServer) batchWorker() {

	ticker := time.NewTicker(flushInterval)

	defer ticker.Stop()

	batch := make([]AnalyticsEvent, 0, batchSize)

	var mu sync.Mutex

	for {

		select {

		case event := <-s.events:

			mu.Lock()

			batch = append(batch, event)

			if len(batch) >= batchSize {

				s.flushBatch(batch)

				batch = batch[:0]
			}

			mu.Unlock()

		case <-ticker.C:

			mu.Lock()

			if len(batch) > 0 {

				s.flushBatch(batch)

				batch = batch[:0]
			}

			mu.Unlock()
		}
	}
}

func (s *analyticsServer) flushBatch(
	events []AnalyticsEvent,
) {

	ctx := context.Background()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {

		s.logger.Error(
			"failed to begin transaction",
			slog.Any("error", err),
		)

		return
	}

	stmt, err := tx.PrepareContext(
		ctx,
		`
		INSERT INTO endpoint_analytics
		(id, endpoint, created_at)
		VALUES (?, ?, ?)
		`,
	)

	if err != nil {

		s.logger.Error(
			"failed to prepare statement",
			slog.Any("error", err),
		)

		_ = tx.Rollback()

		return
	}

	defer stmt.Close()

	for _, event := range events {

		_, err := stmt.ExecContext(
			ctx,
			event.ID,
			event.Endpoint,
			event.CreatedAt,
		)

		if err != nil {

			s.logger.Error(
				"failed to execute batch insert",
				slog.Any("error", err),
			)

			_ = tx.Rollback()

			return
		}
	}

	if err := tx.Commit(); err != nil {

		s.logger.Error(
			"failed to commit transaction",
			slog.Any("error", err),
		)

		return
	}

	s.logger.Info(
		"batch flushed",
		slog.Int("events_count", len(events)),
	)
}

func main() {

	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, nil),
	)

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

	server := &analyticsServer{
		db:     db,
		logger: logger,

		events: make(chan AnalyticsEvent, 10000),
	}

	go server.batchWorker()

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
		server,
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
