package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	_ "github.com/lib/pq"

	"github.com/ahbreck/Chicago_BI/shared"
)

type UnemploymentJsonRecords []struct {
	Community_area      string  `json:"community_area"`
	Below_poverty_level float64 `json:"below_poverty_level,string"`
	Unemployment        float64 `json:"unemployment,string"`
	Per_capita_income   float64 `json:"per_capita_income,string"`
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetUnemploymentRates(db *sql.DB) {
	fmt.Println("GetUnemploymentRates: Collecting Unemployment Rates Data")

	drop_table := `drop table if exists public_health`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "public_health" (
		"community_area" VARCHAR(255) PRIMARY KEY,
		"below_poverty_level" FLOAT8,
		"unemployment" FLOAT8,
		"per_capita_income" FLOAT8
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Public Health Data")

	// There are 77 known community areas in the data set
	// So, set limit to 100.
	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$select=community_area,below_poverty_level,unemployment,per_capita_income&$limit=100"

	res, err := shared.FetchFastAPI(url)
	if err != nil {
		panic(err)
	}
	// adding the below statement to ensure closure in case of early return
	defer res.Body.Close()

	fmt.Println("Received data from SODA REST API for Public Health")

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_data_list UnemploymentJsonRecords
	json.Unmarshal(body, &unemployment_data_list)

	s := fmt.Sprintf("\n\n Community Areas number of SODA records received = %d\n\n", len(unemployment_data_list))
	io.WriteString(os.Stdout, s)

	sql := `INSERT INTO public_health ("community_area", "below_poverty_level", "unemployment", "per_capita_income")
			VALUES ($1, $2, $3, $4)
			ON CONFLICT ("community_area") DO UPDATE 
			SET below_poverty_level = EXCLUDED.below_poverty_level,
				unemployment = EXCLUDED.unemployment,
				per_capita_income = EXCLUDED.per_capita_income;`

	insertedCount := 0
	skippedCount := 0

	for _, record := range unemployment_data_list {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		if record.Community_area == "" ||
			record.Below_poverty_level < 0 ||
			record.Unemployment < 0 ||
			record.Per_capita_income < 0 {
			skippedCount++
			continue
		}

		_, err = db.Exec(sql,
			record.Community_area,
			record.Below_poverty_level,
			record.Unemployment,
			record.Per_capita_income,
		)

		if err != nil {
			panic(err)
		}
		insertedCount++
	}
	fmt.Printf("Completed inserting %d rows into the public_health table. Skipped %d records due to data quality issues.\n", insertedCount, skippedCount)

}
