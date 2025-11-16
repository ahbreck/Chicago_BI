package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/kelvins/geocoder"
)

const (
	disadvantagedTable        = "disadvantaged"
	publichealthTable         = "public_health"
	buildingPermits           = "building_permits"
	disadvantagedPermitsTable = "report_7_disadv_perm"
	ccviTable                 = "ccvi"
	covidTable                = "covid"
	taxiTripsTable            = "taxi_trips"
)

// SourceTables lists all base datasets produced by collectors that reports may depend on.
var SourceTables = []string{
	buildingPermits,
	ccviTable,
	covidTable,
	publichealthTable,
	taxiTripsTable,
}

func CreateDisadvantagedReport(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db connection is nil")
	}

	useGeocoding := os.Getenv("USE_GEOCODING") == "true"
	if useGeocoding {
		geocoder.ApiKey = os.Getenv("API_KEY")
	}

	if err := ensureTableReady(db, publichealthTable); err != nil {
		return err
	}

	if err := ensureTableReady(db, buildingPermits); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start disadvantaged report transaction: %w", err)
	}

	targetIdent := quoteIdentifier(disadvantagedTable)
	baseIdent := quoteIdentifier(publichealthTable)
	buildingPermitsIdent := quoteIdentifier(buildingPermits)
	disadvantagedPermitsIdent := quoteIdentifier(disadvantagedPermitsTable)

	if err := ensurePostGISEnabled(tx); err != nil {
		tx.Rollback()
		return err
	}

	statements := []string{
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, disadvantagedPermitsIdent),
		fmt.Sprintf(`CREATE TABLE %s AS TABLE %s`, disadvantagedPermitsIdent, buildingPermitsIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN point geometry(Point, 4326)`, disadvantagedPermitsIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN zip_code VARCHAR(9) DEFAULT ''`, disadvantagedPermitsIdent),
		fmt.Sprintf(`UPDATE %s
		SET point = ST_SetSRID(ST_MakePoint("longitude", "latitude"), 4326)
		WHERE "longitude" IS NOT NULL AND "latitude" IS NOT NULL`, disadvantagedPermitsIdent),
		fmt.Sprintf(`ALTER TABLE %s
                        ADD COLUMN top_5_poverty BOOLEAN DEFAULT FALSE,
                        ADD COLUMN top_5_unemployment BOOLEAN DEFAULT FALSE,
                        ADD COLUMN disadvantaged BOOLEAN DEFAULT FALSE`, disadvantagedPermitsIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, targetIdent),
		fmt.Sprintf(`CREATE TABLE %s AS TABLE %s`, targetIdent, baseIdent),
		fmt.Sprintf(`ALTER TABLE %s
                        ADD COLUMN top_5_poverty BOOLEAN DEFAULT FALSE,
                        ADD COLUMN top_5_unemployment BOOLEAN DEFAULT FALSE,
                        ADD COLUMN disadvantaged BOOLEAN DEFAULT FALSE`, targetIdent),
		fmt.Sprintf(`UPDATE %s
                        SET top_5_poverty = TRUE
                        WHERE "community_area" IN (
                                SELECT "community_area"
                                FROM %s
                                ORDER BY "below_poverty_level" DESC
                                LIMIT 5
                        )`, targetIdent, targetIdent),
		fmt.Sprintf(`UPDATE %s
                        SET top_5_unemployment = TRUE
                        WHERE "community_area" IN (
                                SELECT "community_area"
                                FROM %s
                                ORDER BY "unemployment" DESC
                                LIMIT 5
                        )`, targetIdent, targetIdent),
		fmt.Sprintf(`UPDATE %s
                        SET disadvantaged = top_5_poverty OR top_5_unemployment`, targetIdent),
		fmt.Sprintf(`UPDATE %s dp
		SET top_5_poverty = d.top_5_poverty,
		    top_5_unemployment = d.top_5_unemployment,
		    disadvantaged = d.disadvantaged
		FROM %s d
		WHERE dp."community_area" = d."community_area"`, disadvantagedPermitsIdent, targetIdent),
		fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN disadvantaged TO waived_fee`, disadvantagedPermitsIdent),
	}

	for _, statement := range statements {
		if _, execErr := tx.Exec(statement); execErr != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute statement %q: %w", statement, execErr)
		}
	}

	if err := populatePermitZipCodes(tx, disadvantagedPermitsIdent, useGeocoding); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to populate zip codes: %w", err)
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit disadvantaged report transaction: %w", err)
	}

	return nil
}

func populatePermitZipCodes(tx *sql.Tx, tableIdent string, useGeocoding bool) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	clearStmt := fmt.Sprintf(`UPDATE %s SET zip_code = ''`, tableIdent)
	if _, err := tx.Exec(clearStmt); err != nil {
		return fmt.Errorf("failed to initialize zip codes: %w", err)
	}

	if !useGeocoding {
		return nil
	}

	rows, err := tx.Query(fmt.Sprintf(`SELECT "id", "latitude", "longitude" FROM %s WHERE "latitude" IS NOT NULL AND "longitude" IS NOT NULL`, tableIdent))
	if err != nil {
		return fmt.Errorf("failed to fetch permits for geocoding: %w", err)
	}
	defer rows.Close()

	updateStmtSQL := fmt.Sprintf(`UPDATE %s SET zip_code = $1 WHERE "id" = $2`, tableIdent)
	updateStmt, prepErr := tx.Prepare(updateStmtSQL)
	if prepErr != nil {
		return fmt.Errorf("failed to prepare zip code update statement: %w", prepErr)
	}
	defer updateStmt.Close()

	for rows.Next() {
		var (
			id        string
			latitude  sql.NullFloat64
			longitude sql.NullFloat64
		)

		if scanErr := rows.Scan(&id, &latitude, &longitude); scanErr != nil {
			return fmt.Errorf("failed to scan permit coordinates: %w", scanErr)
		}

		if !latitude.Valid || !longitude.Valid {
			continue
		}

		location := geocoder.Location{
			Latitude:  latitude.Float64,
			Longitude: longitude.Float64,
		}

		addresses, geoErr := geocoder.GeocodingReverse(location)
		if geoErr != nil {
			fmt.Printf("failed to reverse geocode permit %s: %v\n", id, geoErr)
			continue
		}

		zipCode := ""
		if len(addresses) > 0 {
			zipCode = addresses[0].PostalCode
		}

		if _, updateErr := updateStmt.Exec(zipCode, id); updateErr != nil {
			fmt.Printf("failed to update zip code for permit %s: %v\n", id, updateErr)
			continue
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error while iterating permit rows: %w", err)
	}

	return nil
}

func ensurePostGISEnabled(tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	const availabilityQuery = `SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'postgis')`
	var available bool
	if err := tx.QueryRow(availabilityQuery).Scan(&available); err != nil {
		return fmt.Errorf("failed to check for postgis extension: %w", err)
	}

	if !available {
		return fmt.Errorf("postgis extension is not available on this database instance; please install it before generating the disadvantaged report")
	}

	if _, err := tx.Exec(`CREATE EXTENSION IF NOT EXISTS postgis`); err != nil {
		return fmt.Errorf("failed to enable postgis extension: %w", err)
	}

	return nil
}

func ensureTableReady(db *sql.DB, tableName string) error {
	var regClass sql.NullString
	lookup := fmt.Sprintf("public.%s", quoteIdentifier(tableName))
	if err := db.QueryRow(`SELECT to_regclass($1)`, lookup).Scan(&regClass); err != nil {
		return fmt.Errorf("failed to verify presence of %s: %w", tableName, err)
	}

	if !regClass.Valid {
		return fmt.Errorf("required table %q does not exist", tableName)
	}

	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdentifier(tableName))
	var rowCount int
	if err := db.QueryRow(countQuery).Scan(&rowCount); err != nil {
		return fmt.Errorf("failed to count rows in %s: %w", tableName, err)
	}

	if rowCount == 0 {
		return fmt.Errorf("required table %q has no data to report on", tableName)
	}

	return nil
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func WaitForTablesReady(ctx context.Context, db *sql.DB, pollInterval time.Duration, tables ...string) error {
	if db == nil {
		return fmt.Errorf("db connection is nil")
	}

	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}

	if len(tables) == 0 {
		return nil
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	const statusLogInterval = 30 * time.Second
	lastStatusLog := time.Time{}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var lastErr error
		allReady := true
		for _, table := range tables {
			if err := ensureTableReady(db, table); err != nil {
				lastErr = err
				allReady = false
				break
			}
		}

		if allReady {
			return nil
		}

		// Log the latest readiness issue periodically so callers can see why we're still waiting.
		if lastErr != nil && (lastStatusLog.IsZero() || time.Since(lastStatusLog) >= statusLogInterval) {
			log.Printf("still waiting for source tables: %v", lastErr)
			lastStatusLog = time.Now()
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("context canceled while waiting for tables: %w", lastErr)
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
