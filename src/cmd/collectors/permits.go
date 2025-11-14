package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"database/sql"
	"encoding/json"

	_ "github.com/lib/pq"

	"github.com/ahbreck/Chicago_BI/shared"
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

	res, err := shared.FetchFastAPI(url)
	if err != nil {
		panic(err)
	}

	// adding the below statement to ensure closure in case of early return
	defer res.Body.Close()

	fmt.Println("Received data from SODA REST API for Building Permits")

	body, _ := ioutil.ReadAll(res.Body)
	var building_data_list BuildingPermitsJsonRecords
	json.Unmarshal(body, &building_data_list)

	s := fmt.Sprintf("\n\n Building Permits: number of SODA records received = %d\n\n", len(building_data_list))
	io.WriteString(os.Stdout, s)

	insertedCount := 0
	skippedCount := 0

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
			//fmt.Printf("Skipping record due to missing fields: %+v\n", record)
			skippedCount++
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
		insertedCount++

	}

	fmt.Printf("Completed Inserting %d rows into the Building Permits Table. Skipped %d records due to data quality issues.\n", insertedCount, skippedCount)
}
