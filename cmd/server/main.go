package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/VasuBhakt/vahak/config"
	"github.com/VasuBhakt/vahak/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"go.uber.org/zap"
)

func runMigrations(dbUrl string, logger *zap.Logger) {
	m, err := migrate.New("file://migrations", dbUrl)
	if err != nil {
		logger.Fatal("failed to init migrations", zap.Error(err))
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		logger.Fatal("failed to run migrations", zap.Error(err))
	}

	logger.Info("migrations applied successfully")
}

func main() {
	// load config
	cfg := config.Load()

	// init logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal("failed to init logger:", err)
	}
	defer logger.Sync()

	// init db pool
	pool, err := store.NewPool(cfg.DBPoolUrl)
	if err != nil {
		logger.Fatal("failed to init db pool", zap.Error(err))
	}
	defer pool.Close()
	logger.Info("connected to database")

	runMigrations(cfg.DBUrl, logger)

	// init chi router
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// health check
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// start server
	addr := fmt.Sprintf(":%s", cfg.Port)
	logger.Info("starting server", zap.String("addr", addr))

	if err := http.ListenAndServe(addr, r); err != nil {
		logger.Fatal("server failed", zap.Error(err))
	}
}
