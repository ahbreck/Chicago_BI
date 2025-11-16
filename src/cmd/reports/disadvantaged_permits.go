package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
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
		fmt.Sprintf(`UPDATE %s
		SET point = ST_SetSRID(ST_MakePoint("longitude", "latitude"), 4326)
		WHERE "longitude" IS NOT NULL AND "latitude" IS NOT NULL`, disadvantagedPermitsIdent),
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
	}

	for _, statement := range statements {
		if _, execErr := tx.Exec(statement); execErr != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute statement %q: %w", statement, execErr)
		}
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit disadvantaged report transaction: %w", err)
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
