package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"github.com/ahbreck/Chicago_BI/shared"
)

func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("PROJECT_ID")
	if name == "" {
		name = "CBI-Project"
	}

	w.Write([]byte("CBI data collection microservices' goroutines have started for " + name + "!\n"))
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading .env file: %v", err)
	}

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = shared.DefaultConnectionString
	}

	db, err := shared.OpenDatabase(connStr)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	http.HandleFunc("/", handler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("defaulting to port %s", port)
	}

	go func() {
		log.Printf("listening on port %s", port)
		log.Print("Navigate to Cloud Run services and find the URL of your service")
		log.Print("Use the browser and navigate to your service URL to to check your service has started")
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("collector server failed: %v", err)
		}
	}()

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		log.Print("starting CBI collector microservices ...")

		go GetUnemploymentRates(db)
		go GetBuildingPermits(db)
		go GetTaxiTrips(db)
		go GetCovidDetails(db)
		go GetCCVIDetails(db)

		log.Print("finished daily update, waiting for next run in 24 hours")
		<-ticker.C
	}
}
