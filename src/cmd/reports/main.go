package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"errors"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"github.com/ahbreck/Chicago_BI/shared"
)

const (
	defaultStartupDelayMinutes = 4
	startupDelayEnvKey         = "STARTUP_DELAY_MINUTES"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading .env file: %v", err)
	}

	runOnce := strings.EqualFold(os.Getenv("RUN_ONCE"), "true")

	projectRoot, err := findProjectRoot()
	if err != nil {
		log.Fatalf("failed to determine project root: %v", err)
	}

	if err := ensureGeographyCrosswalks(projectRoot); err != nil {
		log.Fatalf("%v", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("defaulting to port %s", port)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startHTTPServer(ctx, port)

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = shared.DefaultConnectionString
	}

	db, err := shared.OpenDatabase(connStr)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	log.Print("ensuring spatial datasets are available")
	if _, err := shared.EnsureSpatialDatasets(ctx, shared.DefaultSpatialDatasets...); err != nil {
		log.Fatalf("failed to prepare spatial datasets: %v", err)
	}

	startupDelay := startupDelayDuration()
	log.Print("waiting for source datasets before starting report refresh loop")
	if err := WaitForTablesReady(ctx, db, startupDelay, time.Minute, SourceTables...); err != nil {
		log.Fatalf("failed to verify disadvantaged report dependencies: %v", err)
	}

	runReports := func() {
		log.Print("building covid category report")
		if err := CreateCovidCategoryReport(db); err != nil {
			log.Printf("failed to build covid category report: %v", err)
		} else {
			log.Print("covid category report refreshed")
		}

		log.Print("building disadvantaged report")
		if err := CreateDisadvantagedReport(db); err != nil {
			log.Printf("failed to build disadvantaged report: %v", err)
		} else {
			log.Print("disadvantaged report refreshed")
		}
	}

	if runOnce {
		runReports()
		log.Print("RUN_ONCE enabled; reports will remain idle until Cloud Run scales down the instance")
		select {}
	} else {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Print("reports microservice shutting down")
				return
			default:
			}

			runReports()

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}
}

func startHTTPServer(ctx context.Context, port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("reports service is running"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("reports http server shutdown error: %v", err)
		}
	}()

	go func() {
		log.Printf("reports HTTP server listening on :%s", port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("reports http server failed: %v", err)
		}
	}()
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

func startupDelayDuration() time.Duration {
	raw := strings.TrimSpace(os.Getenv(startupDelayEnvKey))
	if raw == "" {
		return time.Duration(defaultStartupDelayMinutes) * time.Minute
	}

	minutes, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("invalid %s value %q; defaulting to %d minutes", startupDelayEnvKey, raw, defaultStartupDelayMinutes)
		return time.Duration(defaultStartupDelayMinutes) * time.Minute
	}

	if minutes < 0 {
		log.Printf("%s is negative (%d); defaulting to %d minutes", startupDelayEnvKey, minutes, defaultStartupDelayMinutes)
		return time.Duration(defaultStartupDelayMinutes) * time.Minute
	}

	return time.Duration(minutes) * time.Minute
}
