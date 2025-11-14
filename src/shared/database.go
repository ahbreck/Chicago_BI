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
			return db, nil
		}
		if attempt == maxRetries {
			break
		}
		time.Sleep(5 * time.Second)
	}

	db.Close()
	return nil, fmt.Errorf("database not reachable after %d attempts: %w", maxRetries, err)
}
