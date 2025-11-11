package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"database/sql"
	"encoding/json"

	_ "github.com/lib/pq"
)

type BuildingPermitsJsonRecords []struct {
	Id            string `json:"id"`
	Permit_       string `json:"permit_"`
	Permit_type   string `json:"permit_type"`
	Issue_date    string `json:"issue_date"`
	Street_number string `json:"street_number"`
	Street_name   string `json:"street_name"`
	Latitude      string `json:"latitude"`
	Longitude     string `json:"longitude"`
	//Location       string `json:"location"`
	Community_area string `json:"community_area"`
	Census_tract   string `json:"census_tract"`
}

func main() {

	// Establish connection to Postgres Database

	// OPTION 1
	// Establish connection to Postgres Database
	db_connection := "user=postgres dbname=chicago_business_intelligence password=sql host=localhost sslmode=disable"

	// OPTION 2
	// Docker container for the Postgres microservice - uncomment when deploy with host.docker.internal
	// db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=postgresdb sslmode=disable port=5432"

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
	// Once daily
	for {
		fmt.Println("Connected to database successfully")
		GetBuildingPermits(db)
		fmt.Println("Finished weekly update, sleeping for 1 day...")
		time.Sleep(24 * time.Hour)
	}

}

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")

	drop_table := `drop table if exists building_permits`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permits" (
		"id" VARCHAR(255) PRIMARY KEY,
		"permit_id" VARCHAR(255) UNIQUE,
		"permit_type" VARCHAR(255),
		"issue_date"      VARCHAR(255),
		"street_number"      VARCHAR(255),
		"street_name"      VARCHAR(255),
		"latitude"      DOUBLE PRECISION ,
		"longitude"      DOUBLE PRECISION,
		"community_area" VARCHAR(255),
		"census_tract" VARCHAR(255)
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Building Permits")

	var url = "https://data.cityofchicago.org/resource/building-permits.json?$select=id,permit_,permit_type,issue_date,street_number,street_name,latitude,longitude,community_area,census_tract&$limit=100"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	// adding the below statement to ensure closure in case of early return
	defer res.Body.Close()

	fmt.Println("Received data from SODA REST API for Building Permits")

	body, _ := ioutil.ReadAll(res.Body)
	var building_data_list BuildingPermitsJsonRecords
	json.Unmarshal(body, &building_data_list)

	for _, record := range building_data_list {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		if record.Id == "" ||
			record.Permit_ == "" ||
			record.Permit_type == "" ||
			record.Issue_date == "" ||
			record.Street_number == "" ||
			record.Street_name == "" ||
			record.Latitude == "" ||
			record.Longitude == "" ||
			//.Location == "" ||
			record.Community_area == "" ||
			record.Census_tract == "" {
			fmt.Printf("Skipping record due to missing fields: %+v\n", record)
			continue
		}

		sql := `INSERT INTO building_permits ("id", "permit_id", "permit_type", "issue_date", "street_number", "street_name", "latitude", "longitude", "community_area", "census_tract")
		values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

		lat, _ := strconv.ParseFloat(record.Latitude, 64)
		lon, _ := strconv.ParseFloat(record.Longitude, 64)

		_, err := db.Exec(
			sql,
			record.Id,
			record.Permit_,
			record.Permit_type,
			record.Issue_date,
			record.Street_number,
			record.Street_name,
			lat,
			lon,
			//record.Location,
			record.Community_area,
			record.Census_tract)

		if err != nil {
			panic(err)
		}

	}
}
