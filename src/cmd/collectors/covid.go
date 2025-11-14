package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"

	_ "github.com/lib/pq"

	"github.com/ahbreck/Chicago_BI/shared"
)

type CovidRecords []struct {
	ZIP                            string  `json:"zip_code"`
	Week_start                     string  `json:"week_start"`
	Week_end                       string  `json:"week_end"`
	Case_rate_weekly               float64 `json:"case_rate_weekly,string"`
	Percent_tested_positive_weekly float64 `json:"percent_tested_positive_weekly,string"`
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetCovidDetails(db *sql.DB) {
	fmt.Println("GetCovidDetails: Collecting weekly COVID data")

	drop_table := `drop table if exists covid`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "covid" (
    "id" SERIAL PRIMARY KEY,
    "zip_code" VARCHAR(9) NOT NULL,
    "week_start" DATE NOT NULL,
    "week_end" DATE NOT NULL,
    "case_rate_weekly" FLOAT8,
    "percent_tested_positive_weekly" FLOAT8,
    CONSTRAINT covid_unique_zip_week UNIQUE ("zip_code", "week_start", "week_end")
);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Unemployment")

	var url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$select=zip_code,week_start,week_end,case_rate_weekly,percent_tested_positive_weekly&$limit=1"

	//testing url: "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=1"

	res, err := shared.FetchFastAPI(url)
	if err != nil {
		panic(err)
	}
	// adding the below statement to ensure closure in case of early return
	defer res.Body.Close()

	fmt.Println("Received data from SODA REST API for COVID weekly")

	body, _ := io.ReadAll(res.Body)
	var covid_data_list CovidRecords
	json.Unmarshal(body, &covid_data_list)

	s := fmt.Sprintf("\n\n Number of COVID weekly SODA records received = %d\n\n", len(covid_data_list))
	io.WriteString(os.Stdout, s)

	sql := `INSERT INTO covid ("zip_code", "week_start", "week_end", "case_rate_weekly", "percent_tested_positive_weekly")
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT ("zip_code", "week_start", "week_end") DO UPDATE 
			SET case_rate_weekly = EXCLUDED.case_rate_weekly,
				percent_tested_positive_weekly = EXCLUDED.percent_tested_positive_weekly;`

	insertedCount := 0
	skippedCount := 0

	for _, record := range covid_data_list {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		if record.ZIP == "" ||
			record.Week_start == "" ||
			record.Week_end == "" ||
			record.Case_rate_weekly < 0 ||
			record.Percent_tested_positive_weekly < 0 {
			skippedCount++
			continue
		}

		_, err = db.Exec(sql,
			record.ZIP,
			record.Week_start,
			record.Week_end,
			record.Case_rate_weekly,
			record.Percent_tested_positive_weekly,
		)

		if err != nil {
			panic(err)
		}
		insertedCount++
	}
	fmt.Printf("Completed inserting %d rows into the covid table. Skipped %d records due to data quality issues.\n", insertedCount, skippedCount)

}
