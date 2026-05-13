package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-chi/chi/v5"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},

		Auth: clickhouse.Auth{
			Database: "analytics",
			Username: "default",
			Password: "password",
		},
	})

	if err != nil {
		logger.Error("ClickHouse connection failed", slog.Any("error", err))
		os.Exit(1)
	}

	ctx := context.Background()

	if err := conn.Ping(ctx); err != nil {
		logger.Error("ClickHouse ping failed", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("connected to Clickhouse")

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

	if err := conn.Exec(ctx, createTableQuery); err != nil {
		logger.Error("failed to create table", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("created table successfully")

	r := chi.NewRouter()

	r.Get("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	server := http.Server{
		Addr:         ":8080",
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	logger.Info("server started", slog.String("addr", server.Addr))

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server failed to start", slog.Any("error", err))
		os.Exit(1)
	}
}
