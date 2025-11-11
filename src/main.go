package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"github.com/ahbreck/Chicago_BI/collectors"
)

// Declare database connection
var db *sql.DB

// Declare transports and clients once for better performance and stability

// Shared simple client (for fast APIs)
var simpleTransport = &http.Transport{
	MaxIdleConns:    10,
	IdleConnTimeout: 300 * time.Second,
}

var simpleClient = &http.Client{
	Transport: simpleTransport,
	Timeout:   10 * time.Second,
}

// Shared extended-timeout client (for slow APIs, i.e., trips datasets)
var slowTransport = &http.Transport{
	MaxIdleConns:          10,
	IdleConnTimeout:       1000 * time.Second,
	TLSHandshakeTimeout:   1000 * time.Second,
	ExpectContinueTimeout: 1000 * time.Second,
	DisableCompression:    true,
	Dial: (&net.Dialer{
		Timeout:   1000 * time.Second,
		KeepAlive: 1000 * time.Second,
	}).Dial,
	ResponseHeaderTimeout: 1000 * time.Second,
}

var slowClient = &http.Client{
	Transport: slowTransport,
	Timeout:   1200 * time.Second,
}

// API fetch functions
func fetchFastAPI(url string) (*http.Response, error) {
	res, err := simpleClient.Get(url)
	if err != nil {
		log.Printf("Error fetching %s: %v", url, err)
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		log.Printf("Unexpected status: %d", res.StatusCode)
	}
	return res, nil
}

func fetchSlowAPI(url string) (*http.Response, error) {
	res, err := slowClient.Get(url)
	if err != nil {
		log.Printf("Error fetching %s: %v", url, err)
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		log.Printf("Unexpected status: %d", res.StatusCode)
	}
	return res, nil
}

func init() {
	var err error

	fmt.Println("Initializing the DB connection")

	// Establish connection to Postgres Database

	// OPTION 1 - Postgress application running on localhost
	db_connection := "user=postgres dbname=chicago_business_intelligence password=sql host=localhost sslmode=disable port = 5432"

	// OPTION 2
	// Docker container for the Postgres microservice - uncomment when deploy with host.docker.internal
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=host.docker.internal sslmode=disable port = 5433"

	// OPTION 3
	// Docker container for the Postgress microservice - uncomment when deploy with IP address of the container
	// To find your Postgres container IP, use the command with your network name listed in the docker compose file as follows:
	// docker network inspect cbi_backend
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=162.123.0.9 sslmode=disable port = 5433"

	//Option 4
	//Database application running on Google Cloud Platform.
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/ADD_YOUR_CONNECTION_NAME_FROM_GCP sslmode=disable port = 5432"

	db, err = sql.Open("postgres", db_connection)
	if err != nil {
		log.Fatal(fmt.Println("Couldn't Open Connection to database"))
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
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func main() {

	// Load environment variables first
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Spin in a loop and pull data from the city of chicago data portal
	// Once every hour, day, week, etc.
	// Though, please note that Not all datasets need to be pulled on daily basis
	// fine-tune the following code-snippet as you see necessary

	// For now while you are doing protyping and unit-testing,
	// it is a good idea to use Cloud Run and start an HTTP server, and manually you kick-start
	// the microservices (goroutines) for data collection from the different sources
	// Once you are done with protyping and unit-testing,
	// you could port your code Cloud Run to  Compute Engine, App Engine, Kubernetes Engine, Google Functions, etc.

	for {

		// While using Cloud Run for instrumenting/prototyping/debugging use the server
		// to trace the state of you running data collection services
		// Navigate to Cloud Run services and find the URL of your service
		// An example of your services URL: https://go-microservice-23zzuv4hksp-uc.a.run.app
		// Use the browser and navigate to your service URL to to kick-start your service

		log.Print("starting CBI Microservices ...")

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		// This code snippet is only for prototypying and unit-testing

		// build and fine-tune the functions to pull data from the different data sources
		// The following code snippets show you how to pull data from different data sources

		go collectors.GetUnemploymentRates(db) // could probably sleep for one year because this dataset does not change frequently
		go collectors.GetBuildingPermits(db)
		go collectors.GetTaxiTrips(db)

		// go GetCovidDetails(db)
		// go GetCCVIDetails(db)

		http.HandleFunc("/", handler)

		// Determine port for HTTP service.
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
			log.Printf("defaulting to port %s", port)
		}

		// Start HTTP server.
		log.Printf("listening on port %s", port)
		log.Print("Navigate to Cloud Run services and find the URL of your service")
		log.Print("Use the browser and navigate to your service URL to to check your service has started")

		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatal(err)
		}
		log.Print("Finished daily update, sleeping for 1 day...")
		time.Sleep(24 * time.Hour)
	}

}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("PROJECT_ID")
	if name == "" {
		name = "CBI-Project"
	}

	fmt.Fprintf(w, "CBI data collection microservices' goroutines have started for %s!\n", name)
}

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
//Sample dataset reviewed:
//"zip_code":"60602",
//"week_number":"35",
//"week_start":"2021-08-29T00:00:00.000",
//"week_end":"2021-09-04T00:00:00.000",
//"cases_weekly":"2",
//"cases_cumulative":"123",
//"case_rate_weekly":"160.8",
//"case_rate_cumulative":"9887.5",
//"tests_weekly":"92",
//"tests_cumulative":"3970",
//"test_rate_weekly":"7395.5",
//"test_rate_cumulative":"319131.8",
//"percent_tested_positive_weekly":"0.022",
//"percent_tested_positive_cumulative":"0.035",
//"deaths_weekly":"0",
//"deaths_cumulative":"2",
//"death_rate_weekly":"0",
//"death_rate_cumulative":"160.8",
//"population":"1244",
//"row_id":"60602-2021-35",
//"zip_code_location":{"type":"Point",
//						"coordinates":
//							0 -87.628309
//							1  41.883136
//":@computed_region_rpca_8um6":"41",
//":@computed_region_vrxf_vc4k":"38",
//":@computed_region_6mkv_f3dw":"14310",
//":@computed_region_bdys_3d7i":"92",
//":@computed_region_43wa_7qmu":"36"
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

func GetCovidDetails(db *sql.DB) {

	fmt.Println("ADD-YOUR-CODE-HERE - To Implement GetCovidDetails")

}

// //////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////
// Sample dataset reviewed:
// "geography_type":"CA",
// "community_area_or_zip":"70",
// "community_area_name":"Ashburn",
// "ccvi_score":"45.1",
// "ccvi_category":"MEDIUM",
// "rank_socioeconomic_status":"34",
// "rank_household_composition":"32",
// "rank_adults_no_pcp":"28",
// "rank_cumulative_mobility_ratio":"45",
// "rank_frontline_essential_workers":"48",
// "rank_age_65_plus":"29",
// "rank_comorbid_conditions":"33",
// "rank_covid_19_incidence_rate":"59",
// "rank_covid_19_hospital_admission_rate":"66",
// "rank_covid_19_crude_mortality_rate":"39",
// "location":{"type":"Point",
//
//	"coordinates":
//			0	-87.7083657043
//			1	41.7457577128
//
// ":@computed_region_rpca_8um6":"8",
// ":@computed_region_vrxf_vc4k":"69",
// ":@computed_region_6mkv_f3dw":"4300",
// ":@computed_region_bdys_3d7i":"199",
// ":@computed_region_43wa_7qmu":"30"
// //////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////
func GetCCVIDetails(db *sql.DB) {

	fmt.Println("ADD-YOUR-CODE-HERE - To Implement GetCCVIDetails")

}
