package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
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
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN zip_code VARCHAR(9) DEFAULT ''`, targetIdent),
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

	if err := populateDisadvantagedZipCodes(tx, targetIdent); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to populate disadvantaged zip codes: %w", err)
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

func populateDisadvantagedZipCodes(tx *sql.Tx, tableIdent string) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	clearStmt := fmt.Sprintf(`UPDATE %s SET zip_code = ''`, tableIdent)
	if _, err := tx.Exec(clearStmt); err != nil {
		return fmt.Errorf("failed to initialize disadvantaged zip codes: %w", err)
	}

	communityZipMap, err := loadCommunityAreaZipCodes()
	if err != nil {
		return err
	}

	if len(communityZipMap) == 0 {
		return fmt.Errorf("no community area to zip code mappings were loaded")
	}

	values := make([]string, 0, len(communityZipMap))
	for communityArea, zip := range communityZipMap {
		escapedZip := strings.ReplaceAll(zip, `'`, `''`)
		values = append(values, fmt.Sprintf("('%d', '%s')", communityArea, escapedZip))
	}

	updateStmt := fmt.Sprintf(`UPDATE %s d
SET zip_code = mapping.zip_code
FROM (VALUES %s) AS mapping(community_area, zip_code)
WHERE d."community_area"::text = mapping.community_area`, tableIdent, strings.Join(values, ","))

	if _, err := tx.Exec(updateStmt); err != nil {
		return fmt.Errorf("failed to populate disadvantaged zip codes from community area mapping: %w", err)
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
		communityZipMap, err := loadCommunityAreaZipCodes()
		if err != nil {
			return err
		}

		if len(communityZipMap) == 0 {
			return fmt.Errorf("no community area to zip code mappings were loaded")
		}

		values := make([]string, 0, len(communityZipMap))
		for communityArea, zip := range communityZipMap {
			escapedZip := strings.ReplaceAll(zip, `'`, `''`)
			values = append(values, fmt.Sprintf("('%d', '%s')", communityArea, escapedZip))
		}

		updateStmt := fmt.Sprintf(`UPDATE %s bp
SET zip_code = mapping.zip_code
FROM (VALUES %s) AS mapping(community_area, zip_code)
WHERE bp."community_area"::text = mapping.community_area`, tableIdent, strings.Join(values, ","))

		if _, err := tx.Exec(updateStmt); err != nil {
			return fmt.Errorf("failed to populate zip codes from community area mapping: %w", err)
		}

		return nil
	}

	rows, err := tx.Query(fmt.Sprintf(`SELECT "id", "latitude", "longitude" FROM %s WHERE "latitude" IS NOT NULL AND "longitude" IS NOT NULL`, tableIdent))
	if err != nil {
		return fmt.Errorf("failed to fetch permits for geocoding: %w", err)
	}
	defer rows.Close()

	type permitLocation struct {
		id        string
		latitude  float64
		longitude float64
	}

	var permits []permitLocation
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

		permits = append(permits, permitLocation{
			id:        id,
			latitude:  latitude.Float64,
			longitude: longitude.Float64,
		})
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error while reading permit rows: %w", err)
	}

	updateStmtSQL := fmt.Sprintf(`UPDATE %s SET zip_code = $1 WHERE "id" = $2`, tableIdent)
	updateStmt, prepErr := tx.Prepare(updateStmtSQL)
	if prepErr != nil {
		return fmt.Errorf("failed to prepare zip code update statement: %w", prepErr)
	}
	defer updateStmt.Close()

	for _, permit := range permits {
		location := geocoder.Location{
			Latitude:  permit.latitude,
			Longitude: permit.longitude,
		}

		addresses, geoErr := geocoder.GeocodingReverse(location)
		if geoErr != nil {
			fmt.Printf("failed to reverse geocode permit %s: %v\n", permit.id, geoErr)
			continue
		}

		zipCode := ""
		if len(addresses) > 0 {
			zipCode = addresses[0].PostalCode
		}

		if _, updateErr := updateStmt.Exec(zipCode, permit.id); updateErr != nil {
			fmt.Printf("failed to update zip code for permit %s: %v\n", permit.id, updateErr)
			continue
		}
	}

	return nil
}

func loadCommunityAreaZipCodes() (map[int]string, error) {
	projectRoot, err := findProjectRoot()
	if err != nil {
		return nil, fmt.Errorf("failed to locate project root while loading community area mapping: %w", err)
	}

	mappingPath := filepath.Join(projectRoot, "src", "data", "community_area_to_zip_code.csv")
	file, err := os.Open(mappingPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open community area zip code mapping %s: %w", mappingPath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read community area zip code mapping from %s: %w", mappingPath, err)
	}

	areaZipMap := make(map[int]string, len(records))
	for i, record := range records {
		if len(record) < 2 {
			return nil, fmt.Errorf("invalid row %d in %s: expected community_area and zip_code", i+1, mappingPath)
		}

		communityAreaRaw := strings.TrimSpace(record[0])
		zipCode := strings.TrimSpace(record[1])

		if i == 0 && strings.EqualFold(communityAreaRaw, "community_area") {
			continue
		}

		if communityAreaRaw == "" || zipCode == "" {
			return nil, fmt.Errorf("missing community_area or zip_code at row %d in %s", i+1, mappingPath)
		}

		communityArea, err := strconv.Atoi(communityAreaRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid community_area %q at row %d in %s: %w", communityAreaRaw, i+1, mappingPath, err)
		}

		areaZipMap[communityArea] = zipCode
	}

	if len(areaZipMap) == 0 {
		return nil, fmt.Errorf("community area mapping file %s contained no data rows", mappingPath)
	}

	return areaZipMap, nil
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
