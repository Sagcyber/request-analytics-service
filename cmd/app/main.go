package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"

	pb "request-analytics-service/internal/pb"

	"google.golang.org/grpc"

	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, nil),
	)

	slog.SetDefault(logger)

	conn, err := grpc.NewClient(
		"localhost:50051",

		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {

		logger.Error(
			"failed to connect grpc server",
			slog.Any("error", err),
		)

		os.Exit(1)
	}

	defer conn.Close()

	analyticsClient := pb.NewAnalyticsServiceClient(conn)

	r := chi.NewRouter()

	r.Get("/status", func(w http.ResponseWriter, r *http.Request) {

		ctx, cancel := context.WithTimeout(
			r.Context(),
			3*time.Second,
		)

		defer cancel()

		requestID := uint64(time.Now().UnixNano())

		grpcRequest := &pb.EventRequest{
			Id:        requestID,
			Endpoint:  r.URL.Path,
			Timestamp: time.Now().Unix(),
		}

		response, err := analyticsClient.LogEvent(
			ctx,
			grpcRequest,
		)

		if err != nil {

			logger.Error(
				"grpc request failed",
				slog.Any("error", err),
			)

		} else {

			logger.Info(
				"analytics event sent",
				slog.Bool("success", response.Success),
			)
		}

		w.Header().Set(
			"Content-Type",
			"application/json",
		)

		w.WriteHeader(http.StatusOK)

		_, _ = w.Write(
			[]byte(`{"status":"ok"}`),
		)
	})

	server := http.Server{
		Addr:    ":8080",
		Handler: r,

		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	logger.Info(
		"rest gateway started",
		slog.String("addr", ":8080"),
	)

	if err := server.ListenAndServe(); err != nil &&
		err != http.ErrServerClosed {

		logger.Error(
			"server failed",
			slog.Any("error", err),
		)

		os.Exit(1)
	}
}
