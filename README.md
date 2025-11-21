# Chicago_BI

Available at https://github.com/ahbreck/Chicago_BI

Chicago_BI is a collection of Go microservices (collectors + reports) and a Flask frontend that downloads public data from the
City of Chicago, populates a data lake, builds report tables, and lets you browse them in a browser.

The report tables satisfy 6 requirements as follows:
- "req_1a_covid_alerts_drivers"
- "req_1b_covid_alerts_residents"
- "req_2_airport_trips"
- "req_3_ccvi_trips"
- "req_4_daily_trips"
- "req_4_weekly_trips"
- "req_4_monthly_trips"
- "req_5_disadv_perm"
- "req_6_loan_elig_permits"

You can run the stack locally with Docker for development, or deploy it to Google Cloud with Cloud Build, Cloud Run, Cloud SQL, and Cloud Scheduler.

## Cloud deployment (Cloud Build + Cloud Run)

The file `src/cloudbuild.yaml` builds/pushes the Go backend image, the Flask frontend image, and the pgAdmin helper, then deploys four Cloud Run services (collectors, reports, frontend, pg-admin). Those services attach to a Cloud SQL instance via the connection name you configure in the YAML.

### Prerequisites

- A Google Cloud project (set with `gcloud config set project <PROJECT_ID>`)
- Cloud SQL Postgres instance (e.g., `gcloud sql instances create mypostgres --database-version=POSTGRES_14 --cpu=2 --memory=7680MB --region=us-central1`)
- Cloud Build enabled
- Cloud Run API enabled
- (Recommended) A dedicated service account for Cloud Scheduler to call Cloud Run (`roles/run.invoker`)

Secrets such as database passwords or API keys should be stored in Secret Manager or substituted into `src/cloudbuild.yaml` before running a build.

### Deploy via Cloud Build

From the repository root (so `src/cloudbuild.yaml` is accessible), run:

```bash
gcloud builds submit --config src/cloudbuild.yaml src
```

This command uses the `src` directory as the build context, creates/pushes the images, and updates the Cloud Run services with `--min-instances 0` so they scale to zero when idle.

If you connect Cloud Build to GitHub, set the trigger’s build-file location to `src/cloudbuild.yaml`.

### Schedule collectors and reports

Collectors and reports now support a `RUN_ONCE=true` mode. Create Cloud Scheduler jobs that invoke the service URLs on the cadence you prefer (example: collectors at 09:00, reports at 10:00 Central):

```bash
gcloud scheduler jobs create http collectors-daily \
  --schedule="0 9 * * *" \
  --uri="https://<collectors-service-url>/" \
  --http-method=GET \
  --oidc-service-account-email="scheduler-invoker@PROJECT_ID.iam.gserviceaccount.com"

gcloud scheduler jobs create http reports-daily \
  --schedule="0 10 * * *" \
  --uri="https://<reports-service-url>/" \
  --http-method=GET \
  --oidc-service-account-email="scheduler-invoker@PROJECT_ID.iam.gserviceaccount.com"
```

Replace `PROJECT_ID` and the URLs with values from `gcloud run services describe <service> --region us-central1 --format="value(status.url)"`.

### Frontend service

The Flask UI lives in `src/web` and is deployed as the `frontend` Cloud Run service. It waits for the collectors/reports tables to exist, exposes `/` for browsing tables, `/healthz` for liveness, and `/readyz` for readiness. The service uses the same Cloud SQL connection string as the Go services.

## Local Docker workflow (fallback)

You can still run everything locally without installing Go or Python dependencies, which is useful for debugging or when you want a fully offline setup.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) 4.19 or newer (or the Docker Engine/CLI + Docker Compose plugin)
- At least 4 GB of available RAM

### Running the stack locally

1. Copy `src/docker/.env.docker.example` as `src/docker/.env.docker` and adjust any values you want to override
   (database credentials, exposed ports, project name, etc.). See "environment files and examples" section below.
2. Start all services from the repository root:

   ```bash
   docker compose -f src/docker/compose.yaml up --build
   ```

3. Navigate to http://localhost:8080 to confirm that the collectors microservice is running. The reports microservice runs in the
   background, rebuilding the disadvantaged report every 24 hours after the collectors finish populating their tables.

4. Follow the logs to watch the data ingestion pipeline:

   ```bash
   docker compose -f src/docker/compose.yaml logs -f docker-collectors-1
   docker compose -f src/docker/compose.yaml logs -f docker-reports-1
   ```

5. Navigate to http://localhost:8085 to access PgAdmin4 and log in with user@gmail.com and SuperSecret (or whatever other credentials you set in the compose.yaml file). Then register the server to view the tables.

6. Stop and remove the containers when you are finished:

   ```bash
   docker compose -f src/docker/compose.yaml down
   ```

   Use `docker compose -f src/docker/compose.yaml down -v` if you also want to remove the persisted database volume.

7. To browse the generated report tables in a browser, open http://localhost:8081. The Flask frontend lists the four report tables and lets you page through their rows.

### Included services (Docker)

| Service     | Description                                                                                  | Ports         |
|-------------|----------------------------------------------------------------------------------------------|---------------|
| `db`        | PostgreSQL 16 with the PostGIS 3.4 extension pre-installed. Data files live in the named     | `5432` (host) |
|             | volume `postgres-data`.                                                                      |               |
| `collectors`| Go service that orchestrates all dataset collectors and exposes a health/status endpoint.    | `8080`        |
| `reports`   | Go service that waits for fresh source tables and rebuilds the disadvantaged report daily.   | N/A           |
| `pgadmin4`  | PgAdmin4 web UI for viewing/managing the Postgres instance.                                  | `8085`        |
| `frontend`  | Flask UI for browsing the report tables listed above.                                        | `8081`        |

The collectors and reports Go services share the same image (see `Dockerfile`) and store spatial assets inside the named volume `spatial-data` mounted at `/app/data`.

### Useful commands

- Run only the collectors:

  ```bash
  docker compose -f src/docker/compose.yaml up --build collectors
  ```

- Run only the reports service (for example, after the collectors have filled the database):

  ```bash
  docker compose -f src/docker/compose.yaml up --build reports
  ```

- Connect to Postgres via `psql`:

  ```bash
  docker compose -f src/docker/compose.yaml exec db psql -U postgres -d chicago_business_intelligence
  ```

- Rebuild the application after editing Go code:

  ```bash
  docker compose -f src/docker/compose.yaml build collectors
  ```

## Configuration reference

Configuration is provided through the `src/docker/.env.docker` file, which is mounted into the runtime containers so the services can satisfy their `godotenv` requirement. The Compose file still lists the database environment variables.

### Environment files and examples

The tracked example files show the required environment variables and suggested defaults while keeping secrets
out of Git (like the geocoding API key):

- Use `src/docker/.env.docker.example` as the template for Docker runs. Copy it to `src/docker/.env.docker` and fill in your values:

  ```bash
  cp src/docker/.env.docker.example src/docker/.env.docker
  ```

- Use `src/.env.example` as the template for running the Go services directly
  on your machine. Copy it to `src/.env` and update the values:

  ```bash
  cp src/.env.example src/.env
  ```

Both `src/docker/.env.docker` and `src/.env` are listed in `.gitignore` so api keys stay private, while the example files remain in version control for reference.

| Variable            | Description                                                                      |
|---------------------|----------------------------------------------------------------------------------|
| `PROJECT_ID`        | Human-friendly name printed by the collectors HTTP endpoint.                     |
| `PORT`              | Port exposed by the collectors HTTP server.                                      |
| `DATABASE_URL`      | Connection string used by both Go services.                                      |
| `SPATIAL_DATA_DIR`  | Directory where downloaded GeoJSON files are cached.                             |
| `POSTGRES_*`        | Standard PostgreSQL username, password, and database name for the PostGIS image. |

The existing `src/.env.example` continues to serve as a template for
non-containerized workflows - copy it to `src/.env` if you need a separate set of
values when running the Go binaries directly. Keeping the Docker-specific
settings in `src/docker/.env.docker` prevents two different `.env` files from
coexisting in the same directory.

## Repository layout

```
.
`-- src                     # Go source, Docker assets, and the Flask frontend
    |-- cmd                 # Collectors and reports entrypoints
    |-- data                # Spatial data/location lookup files
    |-- docker              # Docker Compose stack and Postgres init
    |-- shared              # Shared Go packages
    `-- web                 # Flask frontend for browsing report tables
```
