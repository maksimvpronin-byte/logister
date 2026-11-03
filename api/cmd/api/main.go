package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/httpapi"
	"github.com/logister/logister/api/internal/ingest"
	"github.com/logister/logister/api/internal/redisingest"
	"github.com/logister/logister/api/internal/search"
	"github.com/logister/logister/api/internal/seed"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.FromEnv()
	ctx := context.Background()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()
	if err := db.UpgradeMetadata(ctx, pool); err != nil {
		log.Fatalf("metadata upgrade: %v", err)
	}

	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	err = redisClient.Ping(pingCtx).Err()
	pingCancel()
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	journal := redisingest.New(redisClient, redisingest.NewPGCatalog(pool))

	osClient, err := search.NewClient(cfg.OpenSearchURL)
	if err != nil {
		log.Fatalf("opensearch client: %v", err)
	}

	baseIndex := cfg.OpenSearchIndex
	prepareCtx, prepareCancel := context.WithTimeout(ctx, 15*time.Second)
	prepareErr := osClient.PrepareStorage(prepareCtx, baseIndex)
	if prepareErr == nil {
		prepareErr = osClient.EnsureReadAlias(prepareCtx, search.StorageIndex(baseIndex))
	}
	prepareCancel()
	if prepareErr != nil {
		log.Printf("search storage preparation deferred; durable intake remains available: %v", prepareErr)
	}
	cfg.OpenSearchIndex = search.StorageIndex(baseIndex)
	cfg.SearchReadIndex = search.ReadAlias(cfg.OpenSearchIndex)
	if err := journal.RestoreCounters(ctx, cfg.OpenSearchIndex); err != nil {
		log.Fatalf("redis counters: %v", err)
	}
	workerCtx, workerCancel := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		lifecycleDone := make(chan struct{})
		go func() {
			defer close(lifecycleDone)
			ingest.RunLifecycle(workerCtx, pool, osClient, cfg.OpenSearchIndex, journal)
		}()
		ingest.Run(workerCtx, journal, osClient, baseIndex)
		<-lifecycleDone
	}()
	defer func() { workerCancel(); <-workerDone }()

	if cfg.SeedSampleLogs && prepareErr == nil {
		if err := seed.SampleLogs(ctx, osClient, cfg.OpenSearchIndex); err != nil {
			log.Printf("seed warning: %v", err)
		}
	}

	srv := httpapi.NewServer(cfg, pool, osClient)
	srv.UseJournal(journal)
	if err := srv.ConfigureAuth(ctx); err != nil {
		log.Fatalf("authentication: %v", err)
	}
	kafka, err := srv.ConfigureKafka(ctx)
	if err != nil {
		log.Fatalf("Kafka configuration: %v", err)
	}
	if kafka != nil {
		done := make(chan struct{})
		go func() { defer close(done); kafka.Run(workerCtx) }()
		defer func() { workerCancel(); <-done; kafka.Close() }()
	}
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("logister api listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}
