package shared

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const DefaultConnectionString = "user=postgres dbname=chicago_business_intelligence password=sql host=localhost sslmode=disable port = 5432"

// OpenDatabase establishes a database connection and verifies connectivity with retries.
func OpenDatabase(connStr string) (*sql.DB, error) {
	if connStr == "" {
		return nil, errors.New("database connection string is required")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("could not open connection: %w", err)
	}

	const maxRetries = 10
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err = db.Ping(); err == nil {
			break
		}
		if attempt == maxRetries {
			break
		}
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		db.Close()
		return nil, fmt.Errorf("database not reachable after %d attempts: %w", maxRetries, err)
	}

	if err := EnsurePostGISExtension(db); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// EnsurePostGISExtension verifies that the PostGIS extension is available on the
// connected database instance and installs it if needed. This allows both local
// development setups and future containerized deployments to run reports that
// depend on spatial features without manual preparation.
func EnsurePostGISExtension(db *sql.DB) error {
	if db == nil {
		return errors.New("db connection is nil")
	}

	const availabilityQuery = `SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'postgis')`
	var available bool
	if err := db.QueryRow(availabilityQuery).Scan(&available); err != nil {
		return fmt.Errorf("failed to check for postgis extension: %w", err)
	}

	if !available {
		return errors.New("postgis extension is not available on this database instance; install PostGIS (e.g., add postgis packages to your Postgres server or container image) before running reports")
	}

	if _, err := db.Exec(`CREATE EXTENSION IF NOT EXISTS postgis`); err != nil {
		return fmt.Errorf("failed to enable postgis extension: %w", err)
	}

	return nil

}
