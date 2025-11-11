package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"database/sql"
	"encoding/json"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type TaxiTripsJsonRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func main() {

	// Establish connection to Postgres Database

	// OPTION 1 - Postgress application running on localhost
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

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

func GetTaxiTrips(db *sql.DB) {

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// You need to complete the implmentation and add the data source: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	geocoder.ApiKey = "insert_key_here"

	drop_table := `drop table if exists taxi_trips`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "taxi_trips" (
						"id"   SERIAL , 
						"trip_id" VARCHAR(255) UNIQUE, 
						"trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
						"trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
						"pickup_centroid_latitude" DOUBLE PRECISION, 
						"pickup_centroid_longitude" DOUBLE PRECISION, 
						"dropoff_centroid_latitude" DOUBLE PRECISION, 
						"dropoff_centroid_longitude" DOUBLE PRECISION, 
						"pickup_zip_code" VARCHAR(255), 
						"dropoff_zip_code" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Taxi Trips")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Taxi Trips")

	body, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list TaxiTripsJsonRecords
	json.Unmarshal(body, &taxi_trips_list)

	for i := 0; i < len(taxi_trips_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := taxi_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := taxi_trips_list[i].Pickup_centroid_longitude
		//pickup_centroid_longitude := taxi_trips_list[i].PICKUP_LONG

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude
		//dropoff_centroid_latitude := taxi_trips_list[i].DROPOFF_LAT

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude
		//dropoff_centroid_longitude := taxi_trips_list[i].DROPOFF_LONG

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		// Comment the following line while not unit-testing
		fmt.Println(pickup_location)

		pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		pickup_address := pickup_address_list[0]
		pickup_zip_code := pickup_address.PostalCode

		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}

		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)
		dropoff_address := dropoff_address_list[0]
		dropoff_zip_code := dropoff_address.PostalCode

		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code)

		if err != nil {
			panic(err)
		}

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

////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu/data

	// Data Collection needed from data source:
	// https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu/data

	drop_table := `drop table if exists building_permits`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permits" (
						"id"   SERIAL , 
						"permit_id" VARCHAR(255) UNIQUE, 
						"permit_code" VARCHAR(255), 
						"permit_type" VARCHAR(255),  
						"review_type"      VARCHAR(255), 
						"application_start_date"      VARCHAR(255), 
						"issue_date"      VARCHAR(255), 
						"processing_time"      VARCHAR(255), 
						"street_number"      VARCHAR(255), 
						"street_direction"      VARCHAR(255), 
						"street_name"      VARCHAR(255), 
						"suffix"      VARCHAR(255), 
						"work_description"      TEXT, 
						"building_fee_paid"      VARCHAR(255), 
						"zoning_fee_paid"      VARCHAR(255), 
						"other_fee_paid"      VARCHAR(255), 
						"subtotal_paid"      VARCHAR(255), 
						"building_fee_unpaid"      VARCHAR(255), 
						"zoning_fee_unpaid"      VARCHAR(255), 
						"other_fee_unpaid"      VARCHAR(255), 
						"subtotal_unpaid"      VARCHAR(255), 
						"building_fee_waived"      VARCHAR(255), 
						"zoning_fee_waived"      VARCHAR(255), 
						"other_fee_waived"      VARCHAR(255), 
						"subtotal_waived"      VARCHAR(255), 
						"total_fee"      VARCHAR(255), 
						"contact_1_type"      VARCHAR(255), 
						"contact_1_name"      VARCHAR(255), 
						"contact_1_city"      VARCHAR(255), 
						"contact_1_state"      VARCHAR(255), 
						"contact_1_zipcode"      VARCHAR(255), 
						"reported_cost"      VARCHAR(255), 
						"pin1"      VARCHAR(255), 
						"pin2"      VARCHAR(255), 
						"community_area"      VARCHAR(255), 
						"census_tract"      VARCHAR(255), 
						"ward"      VARCHAR(255), 
						"xcoordinate"      DOUBLE PRECISION ,
						"ycoordinate"      DOUBLE PRECISION ,
						"latitude"      DOUBLE PRECISION ,
						"longitude"      DOUBLE PRECISION,
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Building Permits")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/building-permits.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Building Permits")

	body, _ := ioutil.ReadAll(res.Body)
	var building_data_list BuildingPermitsJsonRecords
	json.Unmarshal(body, &building_data_list)

	for i := 0; i < len(building_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		permit_id := building_data_list[i].Id
		if permit_id == "" {
			continue
		}

		permit_code := building_data_list[i].Permit_Code
		if permit_code == "" {
			continue
		}

		permit_type := building_data_list[i].Permit_type
		if permit_type == "" {
			continue
		}

		review_type := building_data_list[i].Review_type
		if review_type == "" {
			continue
		}
		application_start_date := building_data_list[i].Application_start_date
		if application_start_date == "" {
			continue
		}
		issue_date := building_data_list[i].Issue_date
		if issue_date == "" {
			continue
		}
		processing_time := building_data_list[i].Processing_time
		if processing_time == "" {
			continue
		}

		street_number := building_data_list[i].Street_number
		if street_number == "" {
			continue
		}
		street_direction := building_data_list[i].Street_direction
		if street_direction == "" {
			continue
		}
		street_name := building_data_list[i].Street_name
		if street_name == "" {
			continue
		}
		suffix := building_data_list[i].Suffix
		if suffix == "" {
			continue
		}
		work_description := building_data_list[i].Work_description
		if work_description == "" {
			continue
		}
		building_fee_paid := building_data_list[i].Building_fee_paid
		if building_fee_paid == "" {
			continue
		}
		zoning_fee_paid := building_data_list[i].Zoning_fee_paid
		if zoning_fee_paid == "" {
			continue
		}
		other_fee_paid := building_data_list[i].Other_fee_paid
		if other_fee_paid == "" {
			continue
		}
		subtotal_paid := building_data_list[i].Subtotal_paid
		if subtotal_paid == "" {
			continue
		}
		building_fee_unpaid := building_data_list[i].Building_fee_unpaid
		if building_fee_unpaid == "" {
			continue
		}
		zoning_fee_unpaid := building_data_list[i].Zoning_fee_unpaid
		if zoning_fee_unpaid == "" {
			continue
		}
		other_fee_unpaid := building_data_list[i].Other_fee_unpaid
		if other_fee_unpaid == "" {
			continue
		}
		subtotal_unpaid := building_data_list[i].Subtotal_unpaid
		if subtotal_unpaid == "" {
			continue
		}
		building_fee_waived := building_data_list[i].Building_fee_waived
		if building_fee_waived == "" {
			continue
		}
		zoning_fee_waived := building_data_list[i].Zoning_fee_waived
		if zoning_fee_waived == "" {
			continue
		}
		other_fee_waived := building_data_list[i].Other_fee_waived
		if other_fee_waived == "" {
			continue
		}

		subtotal_waived := building_data_list[i].Subtotal_waived
		if subtotal_waived == "" {
			continue
		}
		total_fee := building_data_list[i].Total_fee
		if total_fee == "" {
			continue
		}

		contact_1_type := building_data_list[i].Contact_1_type
		if contact_1_type == "" {
			continue
		}

		contact_1_name := building_data_list[i].Contact_1_name
		if contact_1_name == "" {
			continue
		}

		contact_1_city := building_data_list[i].Contact_1_city
		if contact_1_city == "" {
			continue
		}
		contact_1_state := building_data_list[i].Contact_1_state
		if contact_1_state == "" {
			continue
		}

		contact_1_zipcode := building_data_list[i].Contact_1_zipcode
		if contact_1_zipcode == "" {
			continue
		}

		reported_cost := building_data_list[i].Reported_cost
		if reported_cost == "" {
			continue
		}

		pin1 := building_data_list[i].Pin1
		if pin1 == "" {
			continue
		}

		pin2 := building_data_list[i].Pin2
		if pin2 == "" {
			continue
		}

		community_area := building_data_list[i].Community_area

		// if community_area == "" {
		// 	continue
		// }

		census_tract := building_data_list[i].Census_tract
		if census_tract == "" {
			continue
		}

		ward := building_data_list[i].Ward
		if ward == "" {
			continue
		}

		xcoordinate := building_data_list[i].Xcoordinate
		if xcoordinate == "" {
			continue
		}

		ycoordinate := building_data_list[i].Ycoordinate
		if ycoordinate == "" {
			continue
		}

		latitude := building_data_list[i].Latitude
		if latitude == "" {
			continue
		}

		longitude := building_data_list[i].Longitude
		if longitude == "" {
			continue
		}

		sql := `INSERT INTO building_permits ("permit_id", "permit_code", "permit_type","review_type",
		"application_start_date",
		"issue_date",
		"processing_time",
		"street_number",
		"street_direction",
		"street_name",
		"suffix",
		"work_description",
		"building_fee_paid",
		"zoning_fee_paid",
		"other_fee_paid",
		"subtotal_paid",
		"building_fee_unpaid",
		"zoning_fee_unpaid",
		"other_fee_unpaid",
		"subtotal_unpaid",
		"building_fee_waived",
		"zoning_fee_waived",
		"other_fee_waived",
		"subtotal_waived",
		"total_fee",
		"contact_1_type",
		"contact_1_name",
		"contact_1_city",
		"contact_1_state",
		"contact_1_zipcode",
		"reported_cost",
		"pin1",
		"pin2",
		"community_area",
		"census_tract",
		"ward",
		"xcoordinate",
		"ycoordinate",
		"latitude",
		"longitude" )
		values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12, $13, $14, $15,$16, $17, $18, $19, $20,$21, $22, $23, $24, $25,$26, $27, $28, $29,$30,$31, $32, $33, $34, $35,$36, $37, $38, $39, $40)`

		_, err = db.Exec(
			sql,
			permit_id,
			permit_code,
			permit_type,
			review_type,
			application_start_date,
			issue_date,
			processing_time,
			street_number,
			street_direction,
			street_name,
			suffix,
			work_description,
			building_fee_paid,
			zoning_fee_paid,
			other_fee_paid,
			subtotal_paid,
			building_fee_unpaid,
			zoning_fee_unpaid,
			other_fee_unpaid,
			subtotal_unpaid,
			building_fee_waived,
			zoning_fee_waived,
			other_fee_waived,
			subtotal_waived,
			total_fee,
			contact_1_type,
			contact_1_name,
			contact_1_city,
			contact_1_state,
			contact_1_zipcode,
			reported_cost,
			pin1,
			pin2,
			community_area,
			census_tract,
			ward,
			xcoordinate,
			ycoordinate,
			latitude,
			longitude)

		if err != nil {
			panic(err)
		}

	}
}
