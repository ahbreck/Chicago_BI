package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"database/sql"
	"encoding/json"

	"github.com/joho/godotenv"
	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type TripRecord struct {
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

	// Load environment variables first
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Read USE_GEOCODING flag from environment
	useGeocoding := os.Getenv("USE_GEOCODING") == "true"

	// Establish connection to Postgres Database

	// OPTION 1 - Postgress application running on localhost
	db_connection := "user=postgres dbname=chicago_business_intelligence password=sql host=localhost sslmode=disable"

	// OPTION 2
	// Docker container for the Postgres microservice - uncomment when deploy with host.docker.internal
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=postgresdb sslmode=disable port=5432"

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
		fmt.Println("Starting data collection cycle...")

		drop_table := `drop table if exists taxi_trips`
		_, err = db.Exec(drop_table)
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
							"trip_type" VARCHAR(50),
							PRIMARY KEY ("id") 
						);`

		_, _err := db.Exec(create_table)
		if _err != nil {
			panic(_err)
		}

		start := time.Now()

		/*
			// Run both API pulls concurrently ---
			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				GetTrips(db, "taxi", "wrvz-psew", 10, useGeocoding)
			}()

			go func() {
				defer wg.Done()
				GetTrips(db, "tnp", "m6dm-c72p", 10, useGeocoding)
			}()

			wg.Wait()
		*/
		// Just running sequentially works better in this case rather than using goroutines.
		GetTrips(db, "taxi", "wrvz-psew", 10, useGeocoding)
		GetTrips(db, "tnp", "m6dm-c72p", 10, useGeocoding)
		duration := time.Since(start)
		fmt.Printf("Time to pull:   %v\n", duration)

		fmt.Println("Finished daily update, sleeping for 1 day...")
		time.Sleep(24 * time.Hour) // sleep for one day
	}

}

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

func GetTrips(db *sql.DB, tripType string, apiCode string, limit int, useGeocoding bool) {

	fmt.Printf("Collecting %s trip data...\n", tripType)

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	if useGeocoding {
		geocoder.ApiKey = os.Getenv("API_KEY")
	}

	// Build API URL dynamically
	url := fmt.Sprintf("https://data.cityofchicago.org/resource/%s.json?$limit=%d", apiCode, limit)

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list []TripRecord
	json.Unmarshal(body, &taxi_trips_list)

	insertedCount := 0
	skippedCount := 0

	for _, record := range taxi_trips_list {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table
		fmt.Printf("record: %+v\n", record)

		if record.Trip_id == "" ||
			// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
			// skip this record
			len(record.Trip_start_timestamp) < 23 ||
			len(record.Trip_end_timestamp) < 23 { //||
			//record.Pickup_centroid_latitude == "" ||
			//record.Pickup_centroid_longitude == "" ||
			//record.Dropoff_centroid_latitude == "" ||
			//record.Dropoff_centroid_longitude == "" {
			fmt.Printf("Skipping record due to missing fields: %+v\n", record)
			skippedCount++
			continue
		}

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(record.Pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(record.Pickup_centroid_longitude, 64)
		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(record.Dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(record.Dropoff_centroid_longitude, 64)

		// Default ZIPs to empty strings
		pickup_zip_code := ""
		dropoff_zip_code := ""

		if useGeocoding {

			pickup_location := geocoder.Location{
				Latitude:  pickup_centroid_latitude_float,
				Longitude: pickup_centroid_longitude_float,
			}

			dropoff_location := geocoder.Location{
				Latitude:  dropoff_centroid_latitude_float,
				Longitude: dropoff_centroid_longitude_float,
			}

			pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)

			dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)

			if len(pickup_address_list) > 0 {
				pickup_zip_code = pickup_address_list[0].PostalCode
			}
			if len(dropoff_address_list) > 0 {
				dropoff_zip_code = dropoff_address_list[0].PostalCode
			}
		}

		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code", "trip_type") values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (trip_id) DO NOTHING`

		_, err = db.Exec(
			sql,
			record.Trip_id,
			record.Trip_start_timestamp,
			record.Trip_end_timestamp,
			pickup_centroid_latitude_float,
			pickup_centroid_longitude_float,
			dropoff_centroid_latitude_float,
			dropoff_centroid_longitude_float,
			pickup_zip_code,
			dropoff_zip_code,
			tripType)

		if err != nil {
			fmt.Printf("Error inserting %s trip %s: %v\n", tripType, record.Trip_id, err)
			continue
		}
		insertedCount++

	}
	fmt.Printf("Finished inserting %d %s trips (%d skipped).\n", insertedCount, tripType, skippedCount)

}
