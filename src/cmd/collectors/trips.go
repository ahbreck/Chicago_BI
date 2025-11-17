package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"database/sql"
	"encoding/csv"
	"encoding/json"
	"math/rand"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"

	"github.com/ahbreck/Chicago_BI/shared"
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

func GetTaxiTrips(db *sql.DB) {

	// Read USE_GEOCODING flag from environment
	useGeocoding := os.Getenv("USE_GEOCODING") == "true"

	fmt.Println("Collecting trips data...")

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
						"pickup_zip_code" VARCHAR(9), 
						"dropoff_zip_code" VARCHAR(9), 
						"trip_type" VARCHAR(50),
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	start := time.Now()

	// Just running sequentially works better in this case rather than using goroutines.
	GetTrips(db, "taxi", "wrvz-psew", 500, useGeocoding)
	GetTrips(db, "tnp", "m6dm-c72p", 500, useGeocoding)
	duration := time.Since(start)
	fmt.Printf("Time to pull:   %v\n", duration)

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
	// For testing purposes, time range filter is set to limit data to Jan through March of 2022
	url := fmt.Sprintf("https://data.cityofchicago.org/resource/%s.json?$limit=%d&$where=trip_start_timestamp%%20between%%20'2022-01-01T00:00:00'%%20and%%20'2022-03-31T23:59:59'", apiCode, limit)

	res, err := shared.FetchSlowAPI(url)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list []TripRecord
	json.Unmarshal(body, &taxi_trips_list)

	insertedCount := 0
	skippedCount := 0
	var zipOptions []string

	if !useGeocoding {
		var err error
		zipOptions, err = loadZipCodeOptions()
		if err != nil {
			fmt.Printf("Unable to load ZIP code options, defaulting to empty values: %v\n", err)
		}
	}

	randomZipPicker := rand.New(rand.NewSource(time.Now().UnixNano()))

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
			//fmt.Printf("Skipping record due to missing fields: %+v\n", record)
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
		} else if len(zipOptions) > 0 {
			pickup_zip_code = zipOptions[randomZipPicker.Intn(len(zipOptions))]
			dropoff_zip_code = zipOptions[randomZipPicker.Intn(len(zipOptions))]
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

// findZipCodeDataPath walks up from the current working directory until it finds the zip code CSV.
func findZipCodeDataPath() (string, error) {
	relPath := filepath.Join("src", "data", "zip_code_to_community_area.csv")

	seen := map[string]struct{}{}
	searchFrom := func(start string) (string, bool) {
		if start == "" {
			return "", false
		}
		if _, ok := seen[start]; ok {
			return "", false
		}
		seen[start] = struct{}{}

		dir := start
		for {
			candidate := filepath.Join(dir, relPath)
			if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
				return candidate, true
			}

			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}

		return "", false
	}

	if cwd, err := os.Getwd(); err == nil {
		if path, ok := searchFrom(cwd); ok {
			return path, nil
		}
	}

	if exe, err := os.Executable(); err == nil {
		if path, ok := searchFrom(filepath.Dir(exe)); ok {
			return path, nil
		}
	}

	return "", fmt.Errorf("could not locate %s", relPath)
}

// loadZipCodeOptions reads the zip codes from the crosswalk CSV and returns the list of possible values.
func loadZipCodeOptions() ([]string, error) {
	csvPath, err := findZipCodeDataPath()
	if err != nil {
		return nil, err
	}

	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open zip code file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read zip code file: %w", err)
	}

	var zips []string
	for i, row := range records {
		if len(row) == 0 {
			continue
		}
		if i == 0 && row[0] == "zip_code" {
			continue
		}
		zips = append(zips, row[0])
	}

	if len(zips) == 0 {
		return nil, fmt.Errorf("no zip codes found in %s", csvPath)
	}

	return zips, nil
}
