package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jmoiron/sqlx"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	dsn := "clickhouse://default:password@localhost:9000/analytics"

	db, err := sqlx.Connect("clickhouse", dsn)
	if err != nil {
		logger.Error("failed to connect clickhouse", slog.Any("error", err))
		os.Exit(1)
	}

	defer db.Close()

	logger.Info("connected to clickhouse")

	ctx := context.Background()

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

	_, err = db.ExecContext(ctx, createTableQuery)
	if err != nil {
		logger.Error("failed to create table", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("table initialized")

	r := chi.NewRouter()

	r.Get("/status", func(w http.ResponseWriter, r *http.Request) {
		requestID := uint64(time.Now().UnixNano())
		endpoint := r.URL.Path
		now := time.Now()

		insertQuery := `
		INSERT INTO endpoint_analytics
		(id, endpoint, created_at)
		VALUES (?, ?, ?)
		`
		_, err := db.ExecContext(
			context.Background(),
			insertQuery,
			requestID,
			endpoint,
			now,
		)

		if err != nil {
			logger.Error("failed to insert analytics", slog.Any("error", err))
		}

		w.Header().Set("Content-Type", "application/json")

		w.WriteHeader(http.StatusOK)

		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	server := http.Server{
		Addr:         ":8080",
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	logger.Info("server started", slog.String("addr", server.Addr))

	if err := server.ListenAndServe(); err != nil &&
		err != http.ErrServerClosed {
		logger.Error("server failed to start", slog.Any("error", err))
		os.Exit(1)
	}
}
