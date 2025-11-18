package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"

	"github.com/ahbreck/Chicago_BI/shared"
)

type TripRecord struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_community_area      string `json:"pickup_community_area"`
	Dropoff_community_area     string `json:"dropoff_community_area"`
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
						"pickup_community_area" VARCHAR(2),
						"dropoff_community_area" VARCHAR(2),
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
	url := fmt.Sprintf("https://data.cityofchicago.org/resource/%s.json?$select=trip_id,trip_start_timestamp,trip_end_timestamp,pickup_community_area,dropoff_community_area,pickup_centroid_latitude,pickup_centroid_longitude,dropoff_centroid_latitude,dropoff_centroid_longitude&$limit=%d&$where=trip_start_timestamp%%20between%%20'2022-01-01T00:00:00'%%20and%%20'2022-03-31T23:59:59'", apiCode, limit)

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
	var communityZipMap map[string]string

	if !useGeocoding {
		var err error
		communityZipMap, err = loadCommunityAreaZipCodes()
		if err != nil {
			fmt.Printf("Unable to load community area ZIP code mapping, defaulting to empty values: %v\n", err)
		}
	}

	for _, record := range taxi_trips_list {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table
		fmt.Printf("record: %+v\n", record)

		pickupCommunityRaw := strings.TrimSpace(record.Pickup_community_area)
		dropoffCommunityRaw := strings.TrimSpace(record.Dropoff_community_area)

		if record.Trip_id == "" ||
			// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
			// skip this record
			len(record.Trip_start_timestamp) < 23 ||
			len(record.Trip_end_timestamp) < 23 ||
			(pickupCommunityRaw == "" && dropoffCommunityRaw == "") { //||
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

		pickupCommunityArea := sql.NullString{}
		if pickupCommunityRaw != "" {
			pickupCommunityArea = sql.NullString{String: pickupCommunityRaw, Valid: true}
		}

		dropoffCommunityArea := sql.NullString{}
		if dropoffCommunityRaw != "" {
			dropoffCommunityArea = sql.NullString{String: dropoffCommunityRaw, Valid: true}
		}

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
		} else if len(communityZipMap) > 0 {
			if pickupCommunityArea.Valid {
				if zip, ok := communityZipMap[pickupCommunityArea.String]; ok {
					pickup_zip_code = zip
				}
			}
			if dropoffCommunityArea.Valid {
				if zip, ok := communityZipMap[dropoffCommunityArea.String]; ok {
					dropoff_zip_code = zip
				}
			}
		}

		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_community_area", "dropoff_community_area", "pickup_zip_code", 
			"dropoff_zip_code", "trip_type") values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
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
			pickupCommunityArea,
			dropoffCommunityArea,
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

// findCommunityZipDataPath walks up from the current working directory until it finds the community area to ZIP code CSV.
func findCommunityZipDataPath() (string, error) {
	relPath := filepath.Join("src", "data", "community_area_to_zip_code.csv")

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

// loadCommunityAreaZipCodes reads the community area to ZIP code mapping.
func loadCommunityAreaZipCodes() (map[string]string, error) {
	csvPath, err := findCommunityZipDataPath()
	if err != nil {
		return nil, err
	}

	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open community area zip code file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read community area zip code file: %w", err)
	}

	areaZipMap := make(map[string]string, len(records))
	for i, row := range records {
		if len(row) < 2 {
			continue
		}
		communityArea := strings.TrimSpace(row[0])
		zip := strings.TrimSpace(row[1])

		if i == 0 && strings.EqualFold(communityArea, "community_area") {
			continue
		}

		if communityArea == "" || zip == "" {
			continue
		}

		areaZipMap[communityArea] = zip
	}

	if len(areaZipMap) == 0 {
		return nil, fmt.Errorf("no community area zip codes found in %s", csvPath)
	}

	return areaZipMap, nil
}
