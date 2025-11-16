package main

import (
    "database/sql"
    "fmt"
)

const covidRepCatsTable = "covid_rep_cats"

// CreateCovidCategoryReport builds covid_rep_cats with covid_cat buckets based on case_rate_weekly.
func CreateCovidCategoryReport(db *sql.DB) error {
    if db == nil {
        return fmt.Errorf("db connection is nil")
    }

    if err := ensureTableReady(db, covidTable); err != nil {
        return err
    }

    tx, err := db.Begin()
    if err != nil {
        return fmt.Errorf("failed to start covid category report transaction: %w", err)
    }

    sourceIdent := quoteIdentifier(covidTable)
    targetIdent := quoteIdentifier(covidRepCatsTable)

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
