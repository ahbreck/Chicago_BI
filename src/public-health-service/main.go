package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	_ "github.com/lib/pq"
)

type UnemploymentJsonRecords []struct {
	Community_area      string `json:"community_area"`
	Below_poverty_level string `json:"below_poverty_level"`
	Unemployment        string `json:"unemployment"`
	Per_capita_income   string `json:"per_capita_income"`
}

func main() {

	// Establish connection to Postgres Database

	// OPTION 2
	// Docker container for the Postgres microservice - uncomment when deploy with host.docker.internal
	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=postgresdb sslmode=disable port=5432"

	// OPTION 3
	// Docker container for the Postgress microservice - uncomment when deploy with IP address of the container
	// To find your Postgres container IP, use the command with your network name listed in the docker compose file as follows:
	// docker network inspect cbi_backend
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=172.19.0.2 sslmode=disable port = 5433"

	db, err := sql.Open("postgres", db_connection)
	if err != nil {
		panic(err)
	}

	// Test the database connection
	maxRetries := 10
	for i := 1; i <= maxRetries; i++ {
		err = db.Ping()
		if err == nil {
			fmt.Println("Connected to database successfully")
			break
		}
		fmt.Printf("Attempt %d/%d: Couldn't connect to database (%v)\n", i, maxRetries, err)
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		panic(fmt.Sprintf("Database not reachable after %d attempts: %v", maxRetries, err))
	}

	// Spin in a loop and pull data from the city of chicago data portal
	// Once every hour, day, week, etc.
	// Though, please note that Not all datasets need to be pulled on daily basis
	// fine-tune the following code-snippet as you see necessary
	for {
		fmt.Println("Connected to database successfully")
		GetUnemploymentRates(db)
		fmt.Println("Finished weekly update, sleeping for 7 days...")
		time.Sleep(365 * 24 * time.Hour) // sleep for one year because this dataset does not change frequently
	}

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

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$select=community_area,below_poverty_level,unemployment,per_capita_income&$limit=1"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	// adding the below statement to ensure closure in case of early return
	defer res.Body.Close()

	fmt.Println("Received data from SODA REST API for Unemployment")

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_data_list UnemploymentJsonRecords
	json.Unmarshal(body, &unemployment_data_list)

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

}
