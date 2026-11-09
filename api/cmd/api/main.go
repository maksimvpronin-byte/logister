package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/logister/logister/api/internal/config"
	"github.com/logister/logister/api/internal/db"
	"github.com/logister/logister/api/internal/httpapi"
	"github.com/logister/logister/api/internal/ingest"
	"github.com/logister/logister/api/internal/redisingest"
	"github.com/logister/logister/api/internal/search"
	"github.com/logister/logister/api/internal/seed"
	"github.com/logister/logister/api/internal/telemetry"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.FromEnv()
	if cfg.KafkaOnly() && strings.TrimSpace(cfg.KafkaBrokers) == "" {
		log.Fatal("KAFKA_BROKERS must be configured for Kafka Direct without Redis")
	}
	ctx := context.Background()

	database, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("mongo: %v", err)
	}
	defer database.Close()
	if err := db.UpgradeMetadata(ctx, database); err != nil {
		log.Fatalf("metadata upgrade: %v", err)
	}

	meter := telemetry.New()

	osClient, err := search.NewClient(cfg.OpenSearchURL)
	if err != nil {
		log.Fatalf("opensearch client: %v", err)
	}

	osClient.SetTelemetry(meter)
	osClient.ConfigureBulk(cfg.BulkWorkers, cfg.BulkBytes)
	osClient.ConfigureShards(cfg.IndexShards)
	if err := osClient.ConfigureCodec(cfg.IndexCodec, cfg.IndexCodecLevel); err != nil {
		log.Fatalf("opensearch codec: %v", err)
	}
	osClient.EnableBulkProfile(cfg.BulkProfile)
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
	workerCtx, workerCancel := context.WithCancel(ctx)
	srv := httpapi.NewServer(cfg, database, osClient)
	var lifecycleJournal ingest.StreamJournal
	if !cfg.KafkaOnly() {
		redisOptions, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			log.Fatalf("redis: %v", err)
		}
		redisClient := redis.NewClient(redisOptions)
		defer redisClient.Close()
		pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
		err = redisClient.Ping(pingCtx).Err()
		pingCancel()
		if err != nil && !cfg.KafkaDirect {
			log.Fatalf("redis: %v", err)
		}
		journal := redisingest.New(redisClient, redisingest.NewCatalog(database))
		journal.SetTelemetry(meter)
		journal.EnableProfile(cfg.StageProfile)
		journal.SetPublishMaxBytes(cfg.PublishBatchBytes)
		journal.SetOutputBufferBytes(cfg.OutputBufferBytes)
		journal.SetQueueMaxBytes(cfg.QueueMaxBytes)
		if cfg.KafkaDirect {
			journal.RequireRestore()
		}
		if err := journal.RestoreCounters(ctx, cfg.OpenSearchIndex); err != nil {
			if !cfg.KafkaDirect {
				log.Fatalf("redis counters: %v", err)
			}
			log.Printf("HTTP ingest journal unavailable; direct Kafka ingestion can continue: %v", err)
			go restoreJournal(workerCtx, journal, cfg.OpenSearchIndex)
		}
		srv.UseJournal(journal)
		lifecycleJournal = journal
		publisherDone := make(chan struct{})
		go func() {
			defer close(publisherDone)
			ingest.Run(workerCtx, journal, osClient, baseIndex)
		}()
		defer func() { workerCancel(); <-publisherDone }()
	} else if prepareErr != nil {
		preparationDone := make(chan struct{})
		go func() {
			defer close(preparationDone)
			ingest.Run(workerCtx, nil, osClient, baseIndex)
		}()
		defer func() { workerCancel(); <-preparationDone }()
	}
	lifecycleDone := make(chan struct{})
	go func() {
		defer close(lifecycleDone)
		ingest.RunLifecycle(workerCtx, database, osClient, cfg.OpenSearchIndex, lifecycleJournal)
	}()
	defer func() { workerCancel(); <-lifecycleDone }()

	if cfg.SeedSampleLogs && prepareErr == nil {
		if err := seed.SampleLogs(ctx, osClient, cfg.OpenSearchIndex); err != nil {
			log.Printf("seed warning: %v", err)
		}
	}

	srv.UseTelemetry(meter)
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
	srv.StartMonitoring(workerCtx)
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

func restoreJournal(ctx context.Context, journal *redisingest.Store, namespace string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := journal.RestoreCounters(ctx, namespace); err == nil {
				return
			}
		}
	}
}
