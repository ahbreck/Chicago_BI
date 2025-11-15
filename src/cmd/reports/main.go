package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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

	projectRoot, err := findProjectRoot()
	if err != nil {
		log.Fatalf("failed to determine project root: %v", err)
	}

	if err := ensureGeographyCrosswalks(projectRoot); err != nil {
		log.Fatalf("%v", err)
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

func findProjectRoot() (string, error) {
	start, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get working directory: %w", err)
	}

	dir := start
	for {
		spatialDir := filepath.Join(dir, "src", "data", "spatial")
		if info, err := os.Stat(spatialDir); err == nil && info.IsDir() {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("could not locate the project root containing 'src/data/spatial'")
}

func ensureGeographyCrosswalks(projectRoot string) error {
	required := []string{
		filepath.Join("src", "data", "census_tract_to_zip_code.csv"),
		filepath.Join("src", "data", "zip_code_to_community_area.csv"),
		filepath.Join("src", "data", "community_area_to_zip_code.csv"),
	}

	var missing []string
	for _, relPath := range required {
		absPath := filepath.Join(projectRoot, relPath)
		info, err := os.Stat(absPath)
		if err != nil || info.Size() == 0 {
			if rel, relErr := filepath.Rel(projectRoot, absPath); relErr == nil {
				missing = append(missing, rel)
			} else {
				missing = append(missing, absPath)
			}
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"required geography crosswalk files missing or empty: %s. run 'python scripts/build_geography_maps.py' to generate them",
			strings.Join(missing, ", "),
		)
	}

	return nil
}
