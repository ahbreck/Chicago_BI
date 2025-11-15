package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"github.com/ahbreck/Chicago_BI/shared"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading .env file: %v", err)
	}

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = shared.DefaultConnectionString
	}

	db, err := shared.OpenDatabase(connStr)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Print("ensuring spatial datasets are available")
	if _, err := shared.EnsureSpatialDatasets(ctx, shared.DefaultSpatialDatasets...); err != nil {
		log.Fatalf("failed to prepare spatial datasets: %v", err)
	}

	log.Print("waiting for source datasets before starting report refresh loop")
	if err := WaitForTablesReady(ctx, db, time.Minute, SourceTables...); err != nil {
		log.Fatalf("failed to verify disadvantaged report dependencies: %v", err)
	}

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Print("reports microservice shutting down")
			return
		default:
		}

		log.Print("building disadvantaged report")
		if err := CreateDisadvantagedReport(db); err != nil {
			log.Printf("failed to build disadvantaged report: %v", err)
		} else {
			log.Print("disadvantaged report refreshed")
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
