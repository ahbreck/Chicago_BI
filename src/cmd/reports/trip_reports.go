package main

import (
	"database/sql"
	"fmt"
)

const (
	covidRepCatsTable    = "covid_rep_cats"
	covidAlertsTable     = "req_1a_covid_alerts_drivers"
	covidAlertsResidents = "req_1b_covid_alerts_residents"
	reqAirportTripsTable = "req_2_airport_trips"
	CCVITable            = "req_3_ccvi_trips"
	dailyTripsTable      = "req_4_daily_trips"
	weeklyTripsTable     = "req_4_weekly_trips"
	monthlyTripsTable    = "req_4_monthly_trips"
	weeklyPickupTable    = "weekly_trips_by_pickup_and_zip"
	weeklyDropoffTable   = "weekly_trips_by_dropoff_and_zip"
)

// CreateCovidCategoryReport builds covid_rep_cats with covid_cat buckets based on case_rate_weekly.
func CreateCovidCategoryReport(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db connection is nil")
	}

	if err := ensureTableReady(db, covidTable); err != nil {
		return err
	}

	if err := ensureTableReady(db, taxiTripsTable); err != nil {
		return err
	}

	if err := ensureTableReady(db, ccviTable); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start covid category report transaction: %w", err)
	}

	sourceIdent := quoteIdentifier(covidTable)
	targetIdent := quoteIdentifier(covidRepCatsTable)
	alertsIdent := quoteIdentifier(covidAlertsTable)
	alertsResidentsIdent := quoteIdentifier(covidAlertsResidents)
	reqAirportTripsIdent := quoteIdentifier(reqAirportTripsTable)
	reqAirportTripsSortedIdent := quoteIdentifier(reqAirportTripsTable + "_sorted")
	ccviIdent := quoteIdentifier(ccviTable)
	CCVIIdent := quoteIdentifier(CCVITable)
	CCVISortedIdent := quoteIdentifier(CCVITable + "_sorted")
	dailyIdent := quoteIdentifier(dailyTripsTable)
	weeklyIdent := quoteIdentifier(weeklyTripsTable)
	monthlyIdent := quoteIdentifier(monthlyTripsTable)
	weeklyPickupIdent := quoteIdentifier(weeklyPickupTable)
	weeklyDropoffIdent := quoteIdentifier(weeklyDropoffTable)
	tripsIdent := quoteIdentifier(taxiTripsTable)

	statements := []string{
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, targetIdent),
		fmt.Sprintf(`CREATE TABLE %s AS TABLE %s`, targetIdent, sourceIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN covid_cat VARCHAR(6)`, targetIdent),
		fmt.Sprintf(`UPDATE %s
			SET covid_cat = CASE
				WHEN "case_rate_weekly" < 50 THEN 'low'
				WHEN "case_rate_weekly" >= 50 AND "case_rate_weekly" < 100 THEN 'medium'
				WHEN "case_rate_weekly" >= 100 THEN 'high'
			END`, targetIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, alertsIdent),
		fmt.Sprintf(`CREATE TABLE %s AS TABLE %s`, alertsIdent, tripsIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN airport_dropoff BOOLEAN DEFAULT false`, alertsIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN airport_pickup BOOLEAN DEFAULT false`, alertsIdent),
		fmt.Sprintf(`UPDATE %s
			SET airport_dropoff = true
			WHERE "dropoff_zip_code" IN ('60666', '60656', '60665', '60638')`, alertsIdent),
		fmt.Sprintf(`UPDATE %s
			SET airport_pickup = true
			WHERE "pickup_zip_code" IN ('60666', '60656', '60665', '60638')`, alertsIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN day DATE`, alertsIdent),
		fmt.Sprintf(`UPDATE %s SET day = "trip_start_timestamp"::date`, alertsIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN week_start DATE`, alertsIdent),
		fmt.Sprintf(`UPDATE %s SET week_start = (DATE_TRUNC('week', "trip_start_timestamp") - INTERVAL '1 day')::date`, alertsIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN month_start DATE`, alertsIdent),
		fmt.Sprintf(`UPDATE %s SET month_start = DATE_TRUNC('month', "trip_start_timestamp")::date`, alertsIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, reqAirportTripsIdent),
		fmt.Sprintf(`CREATE TABLE %s AS TABLE %s`, reqAirportTripsIdent, targetIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN trips_to_airport INTEGER DEFAULT 0`, reqAirportTripsIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN trips_from_airport INTEGER DEFAULT 0`, reqAirportTripsIdent),
		fmt.Sprintf(`UPDATE %s cat
			SET trips_to_airport = airport_counts.trips_to_airport
			FROM (
				SELECT "pickup_zip_code" AS zip_code, week_start, COUNT(*) AS trips_to_airport
				FROM %s
				WHERE airport_dropoff = true
				GROUP BY "pickup_zip_code", week_start
			) AS airport_counts
			WHERE cat."zip_code" = airport_counts.zip_code
				AND cat."week_start" = airport_counts.week_start`, reqAirportTripsIdent, alertsIdent),
		fmt.Sprintf(`UPDATE %s cat
			SET trips_from_airport = airport_counts.trips_from_airport
			FROM (
				SELECT "dropoff_zip_code" AS zip_code, week_start, COUNT(*) AS trips_from_airport
				FROM %s
				WHERE airport_pickup = true
				GROUP BY "dropoff_zip_code", week_start
			) AS airport_counts
			WHERE cat."zip_code" = airport_counts.zip_code
				AND cat."week_start" = airport_counts.week_start`, reqAirportTripsIdent, alertsIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, reqAirportTripsSortedIdent),
		fmt.Sprintf(`CREATE TABLE %s AS
			SELECT *
			FROM %s
			ORDER BY "zip_code", "week_start"`, reqAirportTripsSortedIdent, reqAirportTripsIdent),
		fmt.Sprintf(`DROP TABLE %s`, reqAirportTripsIdent),
		fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, reqAirportTripsSortedIdent, reqAirportTripsIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN pickup_covid_cat VARCHAR(6)`, alertsIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN dropoff_covid_cat VARCHAR(6)`, alertsIdent),
		fmt.Sprintf(`UPDATE %s t
			SET pickup_covid_cat = c.covid_cat
			FROM %s c
			WHERE t."pickup_zip_code" = c."zip_code"
				AND t."week_start" = c."week_start"`, alertsIdent, targetIdent),
		fmt.Sprintf(`UPDATE %s t
			SET dropoff_covid_cat = c.covid_cat
			FROM %s c
			WHERE t."dropoff_zip_code" = c."zip_code"
				AND t."week_start" = c."week_start"`, alertsIdent, targetIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, weeklyPickupIdent),
		fmt.Sprintf(`CREATE TABLE %s AS
			SELECT week_start, "pickup_zip_code", COUNT(*) AS weekly_pickups
			FROM %s
			GROUP BY week_start, "pickup_zip_code"`, weeklyPickupIdent, alertsIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, weeklyDropoffIdent),
		fmt.Sprintf(`CREATE TABLE %s AS
			SELECT week_start, "dropoff_zip_code", COUNT(*) AS weekly_dropoffs
			FROM %s
			GROUP BY week_start, "dropoff_zip_code"`, weeklyDropoffIdent, alertsIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, alertsResidentsIdent),
		fmt.Sprintf(`CREATE TABLE %s AS TABLE %s`, alertsResidentsIdent, targetIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN weekly_dropoffs INTEGER DEFAULT 0`, alertsResidentsIdent),
		fmt.Sprintf(`UPDATE %s r
			SET weekly_dropoffs = wd.weekly_dropoffs
			FROM %s wd
			WHERE r."zip_code" = wd."dropoff_zip_code"
				AND r."week_start" = wd."week_start"`, alertsResidentsIdent, weeklyDropoffIdent),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN weekly_pickups INTEGER DEFAULT 0`, alertsResidentsIdent),
		fmt.Sprintf(`UPDATE %s r
			SET weekly_pickups = wp.weekly_pickups
			FROM %s wp
			WHERE r."zip_code" = wp."pickup_zip_code"
				AND r."week_start" = wp."week_start"`, alertsResidentsIdent, weeklyPickupIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, dailyIdent),
		fmt.Sprintf(`CREATE TABLE %s AS
			WITH daily_counts AS (
				SELECT "dropoff_zip_code", day, COUNT(*) AS trips_per_day
				FROM %s
				GROUP BY "dropoff_zip_code", day
			),
			next_day AS (
				SELECT (MAX(day) + INTERVAL '1 day')::date AS day_value FROM %s
			)
			SELECT dc."dropoff_zip_code" AS zip_code, nd.day_value AS day, AVG(dc.trips_per_day) AS trips
			FROM daily_counts dc
			CROSS JOIN next_day nd
			GROUP BY dc."dropoff_zip_code", nd.day_value`, dailyIdent, alertsIdent, alertsIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, weeklyIdent),
		fmt.Sprintf(`CREATE TABLE %s AS
			WITH weekly_counts AS (
				SELECT "dropoff_zip_code", week_start, COUNT(*) AS trips_per_week
				FROM %s
				GROUP BY "dropoff_zip_code", week_start
			),
			next_week AS (
				SELECT (MAX(week_start) + INTERVAL '1 week')::date AS week_value FROM %s
			)
			SELECT wc."dropoff_zip_code" AS zip_code, nw.week_value AS week_start, AVG(wc.trips_per_week) AS trips
			FROM weekly_counts wc
			CROSS JOIN next_week nw
			GROUP BY wc."dropoff_zip_code", nw.week_value`, weeklyIdent, alertsIdent, alertsIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, CCVIIdent),
		fmt.Sprintf(`CREATE TABLE %s AS
			WITH weekly_trips AS (
				SELECT week_start, "pickup_zip_code" AS zip_code, COUNT(*) AS trips
				FROM %s
				GROUP BY week_start, "pickup_zip_code"
				UNION ALL
				SELECT week_start, "dropoff_zip_code" AS zip_code, COUNT(*) AS trips
				FROM %s
				GROUP BY week_start, "dropoff_zip_code"
			)
			SELECT c.*, wt.week_start, SUM(wt.trips) AS weekly_trips
			FROM %s c
			JOIN weekly_trips wt ON wt.zip_code = c."community_area_or_zip"
			WHERE c."ccvi_category" = 'HIGH'
				AND c."geography_type" = 'ZIP'
			GROUP BY c."id", c."geography_type", c."community_area_or_zip", c."community_area_name", c."ccvi_score", c."ccvi_category", wt.week_start`, CCVIIdent, alertsIdent, alertsIdent, ccviIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, CCVISortedIdent),
		fmt.Sprintf(`CREATE TABLE %s AS
			SELECT *
			FROM %s
			ORDER BY "community_area_or_zip", "week_start"`, CCVISortedIdent, CCVIIdent),
		fmt.Sprintf(`DROP TABLE %s`, CCVIIdent),
		fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, CCVISortedIdent, CCVIIdent),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, monthlyIdent),
		fmt.Sprintf(`CREATE TABLE %s AS
			WITH monthly_counts AS (
				SELECT "dropoff_zip_code", month_start, COUNT(*) AS trips_per_month
				FROM %s
				GROUP BY "dropoff_zip_code", month_start
			),
			next_month AS (
				SELECT (MAX(month_start) + INTERVAL '1 month')::date AS month_value FROM %s
			)
			SELECT mc."dropoff_zip_code" AS zip_code, nm.month_value AS month_start, AVG(mc.trips_per_month) AS trips
			FROM monthly_counts mc
			CROSS JOIN next_month nm
			GROUP BY mc."dropoff_zip_code", nm.month_value`, monthlyIdent, alertsIdent, alertsIdent),
	}

	for _, stmt := range statements {
		if _, execErr := tx.Exec(stmt); execErr != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute statement %q: %w", stmt, execErr)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit covid category report transaction: %w", err)
	}

	return nil
}
