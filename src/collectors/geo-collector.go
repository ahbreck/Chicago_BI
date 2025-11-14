package collectors

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"

	_ "github.com/lib/pq"

	"github.com/ahbreck/Chicago_BI/shared"
)

type CCVIRecords []struct {
	Geography_type        string  `json:"geography_type"`
	Community_area_or_zip string  `json:"community_area_or_zip"`
	Community_area_name   string  `json:"community_area_name"`
	CCVI_score            float64 `json:"ccvi_score"`
	CCVI_category         string  `json:"ccvi_category"`
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetCCVIDetails(db *sql.DB) {
	fmt.Println("GetCCVIDetails: Collecting data on Chicago Community Vulnerability Index")

	drop_table := `drop table if exists ccvi`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "ccvi" (
    "id" SERIAL PRIMARY KEY,
    "geography_type" VARCHAR(3),
    "community_area_or_zip" VARCHAR(9) UNIQUE,
    "community_area_name" VARCHAR(255),
    "ccvi_score" FLOAT8,
    "ccvi_category" VARCHAR(6)
);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for CCVI")

	var url = "https://data.cityofchicago.org/resource/xhc6-88s9.json?$select=geography_type,community_area_or_zip,community_area_name,ccvi_score,ccvi_category&$limit=500"

	//testing url: "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=1"

	res, err := shared.FetchFastAPI(url)
	if err != nil {
		panic(err)
	}
	// adding the below statement to ensure closure in case of early return
	defer res.Body.Close()

	fmt.Println("Received data from SODA REST API for CCVI")

	body, _ := io.ReadAll(res.Body)
	var ccvi_data_list CCVIRecords
	json.Unmarshal(body, &ccvi_data_list)

	s := fmt.Sprintf("\n\n Number of CCVI SODA records received = %d\n\n", len(ccvi_data_list))
	io.WriteString(os.Stdout, s)

	sql := `INSERT INTO ccvi ("geography_type", "community_area_or_zip", "community_area_name", "ccvi_score", "ccvi_category")
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT ("community_area_or_zip") DO UPDATE 
			SET geography_type = EXCLUDED.geography_type,
				community_area_name = EXCLUDED.community_area_name,
				ccvi_score = EXCLUDED.ccvi_score,
				ccvi_category = EXCLUDED.ccvi_category;`

	insertedCount := 0
	skippedCount := 0

	for _, record := range ccvi_data_list {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		if record.Geography_type == "" ||
			record.Community_area_or_zip == "" ||
			//record.Community_area_name == "" ||
			record.CCVI_score < 0 ||
			record.CCVI_category == "" {
			skippedCount++
			continue
		}

		_, err = db.Exec(sql,
			record.Geography_type,
			record.Community_area_or_zip,
			record.Community_area_name,
			record.CCVI_score,
			record.CCVI_category,
		)

		if err != nil {
			panic(err)
		}
		insertedCount++
	}
	fmt.Printf("Completed inserting %d rows into the ccvi table. Skipped %d records due to data quality issues.\n", insertedCount, skippedCount)

}
