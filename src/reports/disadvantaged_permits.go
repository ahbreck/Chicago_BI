package reports

import (
	"database/sql"
	"fmt"
)

func CreateDisadvantagedReport(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db connection is nil")
	}

	statements := []string{
		`DROP TABLE IF EXISTS "disadvantaged"`,
		`CREATE TABLE "disadvantaged" AS TABLE "public_health"`,
		`ALTER TABLE "disadvantaged"
                        ADD COLUMN top_5_poverty BOOLEAN DEFAULT FALSE,
                        ADD COLUMN top_5_unemployment BOOLEAN DEFAULT FALSE,
                        ADD COLUMN disadvantaged BOOLEAN DEFAULT FALSE`,
		`UPDATE "disadvantaged"
                        SET top_5_poverty = TRUE
                        WHERE "community_area" IN (
                                SELECT "community_area"
                                FROM "disadvantaged"
                                ORDER BY below_poverty_level DESC
                                LIMIT 5
                        )`,
		`UPDATE "disadvantaged"
                        SET top_5_unemployment = TRUE
                        WHERE "community_area" IN (
                                SELECT "community_area"
                                FROM "disadvantaged"
                                ORDER BY unemployment DESC
                                LIMIT 5
                        )`,
		`UPDATE "disadvantaged"
                        SET disadvantaged = top_5_poverty OR top_5_unemployment`,
	}

	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return fmt.Errorf("failed to execute statement %q: %w", statement, err)
		}
	}

	return nil
}
