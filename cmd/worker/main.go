package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"rc-notifier/internal/config"
	"rc-notifier/internal/delivery"
	"rc-notifier/internal/store"
	"rc-notifier/internal/worker"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.LoadWorker()
	if err != nil {
		logger.Error("load configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("create database pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		logger.Error("connect to database", "error", err)
		os.Exit(1)
	}
	if err := store.Migrate(ctx, pool); err != nil {
		logger.Error("run migrations", "error", err)
		os.Exit(1)
	}

	client := delivery.NewClient(cfg.AllowPrivateDestinations, delivery.EnvSecretProvider{})
	defer client.CloseIdleConnections()

	runner := &worker.Runner{
		Repository:    store.New(pool),
		Deliverer:     client,
		Backoff:       delivery.NewBackoff(cfg.BackoffBase, cfg.BackoffMax),
		Logger:        logger,
		WorkerID:      cfg.WorkerID,
		Concurrency:   cfg.Concurrency,
		PollInterval:  cfg.PollInterval,
		LeaseDuration: cfg.LeaseDuration,
	}

	logger.Info("worker started",
		"worker_id", cfg.WorkerID,
		"concurrency", cfg.Concurrency,
	)
	if err := runner.Run(ctx); err != nil {
		logger.Error("run worker", "error", err)
		os.Exit(1)
	}
}
