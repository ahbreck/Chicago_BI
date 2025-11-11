package collectors

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	_ "github.com/lib/pq"
)

type UnemploymentJsonRecords []struct {
	Community_area      string `json:"community_area"`
	Below_poverty_level string `json:"below_poverty_level"`
	Unemployment        string `json:"unemployment"`
	Per_capita_income   string `json:"per_capita_income"`
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetUnemploymentRates(db *sql.DB) {
	fmt.Println("GetUnemploymentRates: Collecting Unemployment Rates Data")

	drop_table := `drop table if exists unemployment`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "unemployment" (
		"id" SERIAL PRIMARY KEY,
		"community_area" VARCHAR(255) UNIQUE,
		"below_poverty_level" VARCHAR(255),
		"unemployment" VARCHAR(255),
		"per_capita_income" VARCHAR(255)
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Unemployment")

	// There are 77 known community areas in the data set
	// So, set limit to 100.
	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$select=community_area,below_poverty_level,unemployment,per_capita_income&$limit=1"

	res, err := fetchFastAPI(url)
	if err != nil {
		panic(err)
	}
	// adding the below statement to ensure closure in case of early return
	defer res.Body.Close()

	fmt.Println("Received data from SODA REST API for Unemployment")

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_data_list UnemploymentJsonRecords
	json.Unmarshal(body, &unemployment_data_list)

	s := fmt.Sprintf("\n\n Community Areas number of SODA records received = %d\n\n", len(unemployment_data_list))
	io.WriteString(os.Stdout, s)

	sql := `INSERT INTO unemployment ("community_area", "below_poverty_level", "unemployment", "per_capita_income")
			VALUES ($1, $2, $3, $4)
			ON CONFLICT ("community_area") DO UPDATE 
			SET below_poverty_level = EXCLUDED.below_poverty_level,
				unemployment = EXCLUDED.unemployment,
				per_capita_income = EXCLUDED.per_capita_income;`

	for _, record := range unemployment_data_list {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		if record.Community_area == "" ||
			record.Below_poverty_level == "" ||
			record.Unemployment == "" ||
			record.Per_capita_income == "" {
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

	}

	fmt.Println("Completed Inserting Rows into the unemployment table")

}
