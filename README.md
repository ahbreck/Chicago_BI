# Chicago_BI

Available at [https://github.com/ahbreck/Chicago_BI](https://github.com/ahbreck/Chicago_BI)

Chicago_BI is a collection of Go microservices that downloads public data from the
City of Chicago, populates a data lake, and then builds report tables using those source tables.

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

This repository is set up to run with Docker so you can run it without installing any
development dependencies on your machine.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) 4.19 or
  newer (or the Docker Engine/CLI + Docker Compose plugin)
- At least 4 GB of available RAM

## Running the stack

1. Copy `src/docker/.env.docker.example` as `src/docker/.env.docker` and adjust any values you want to override
   (database credentials, exposed ports, project name, etc.). See "environment files and examples" section below.
2. Start all services from the repository root:

   ```bash
   docker compose -f src/docker/compose.yaml up --build
   ```

3. Navigate to [http://localhost:8080](http://localhost:8080) to confirm that
   the collectors microservice is running. The reports microservice runs in the
   background, rebuilding the disadvantaged report every 24 hours after the
   collectors finish populating their tables.

4. Follow the logs to watch the data ingestion pipeline:

   ```bash
   docker compose -f src/docker/compose.yaml logs -f docker-collectors-1
   docker compose -f src/docker/compose.yaml logs -f docker-reports-1
   ```

5. Navigate to [http://localhost:8085](http://localhost:8085) to access PgAdmin4 and log in with user@gmail.com
  and SuperSecret (or whatever other credentials you set in the compose.yaml file). Then register the server to 
  view the tables.

6. Stop and remove the containers when you are finished:

   ```bash
   docker compose -f src/docker/compose.yaml down
   ```

   Use `docker compose -f src/docker/compose.yaml down -v` if you also want to
   remove the persisted database volume.

### Included services

| Service     | Description                                                                                  | Ports        |
|-------------|----------------------------------------------------------------------------------------------|--------------|
| `db`        | PostgreSQL 16 with the PostGIS 3.4 extension pre-installed. Data files live in the named     | `5432` (host) |
|             | volume `postgres-data`.                                                                      |              |
| `collectors`| Go service that orchestrates all dataset collectors and exposes a health/status endpoint.    | `8080`        |
| `reports`   | Go service that waits for fresh source tables and rebuilds the disadvantaged report daily.   | N/A           |
| `pgadmin4`  | PgAdmin4 web UI for viewing/managing the Postgres instance.                                  | `8085`        |

The collectors and reports Go services share the same image (see `Dockerfile`) and store spatial
assets inside the named volume `spatial-data` mounted at `/app/data`.

### Useful commands

- Run only the collectors:

  ```bash
  docker compose -f src/docker/compose.yaml up --build collectors
  ```

- Run only the reports service (for example, after the collectors have filled
  the database):

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

Configuration is provided through the `src/docker/.env.docker` file, which is
mounted into the runtime containers so the services can satisfy their
`godotenv` requirement. The Compose file still lists the database environment
variables.

### Environment files and examples

The tracked example files show the required environment variables and suggested defaults while keeping secrets
out of Git (like the geocoding API key):

- Use `src/docker/.env.docker.example` as the template for Docker runs. Copy it
  to `src/docker/.env.docker` and fill in your values:

  ```bash
  cp src/docker/.env.docker.example src/docker/.env.docker
  ```

- Use `src/.env.example` as the template for running the Go services directly
  on your machine. Copy it to `src/.env` and update the values:

  ```bash
  cp src/.env.example src/.env
  ```

Both `src/docker/.env.docker` and `src/.env` are listed in `.gitignore` so api keys stay private, 
while the example files remain in version control for reference.

| Variable            | Description                                                                      |
|---------------------|----------------------------------------------------------------------------------|
| `PROJECT_ID`        | Human-friendly name printed by the collectors HTTP endpoint.                     |
| `PORT`              | Port exposed by the collectors HTTP server.                                      |
| `DATABASE_URL`      | Connection string used by both Go services.                                      |
| `SPATIAL_DATA_DIR`  | Directory where downloaded GeoJSON files are cached.                             |
| `POSTGRES_*`        | Standard PostgreSQL username, password, and database name for the PostGIS image. |

The existing `src/.env.example` continues to serve as a template for
non-containerized workflows—copy it to `src/.env` if you need a separate set of
values when running the Go binaries directly. Keeping the Docker-specific
settings in `src/docker/.env.docker` prevents two different `.env` files from
coexisting in the same directory.

## Repository layout

```
.
├── src                    # Original Go source code, including collectors and reports
│   ├── Dockerfile         # Multi-stage build that compiles collectors and reports binaries
│   ├── .dockerignore      # Build context exclusions for the Go image
│   └── docker/            # Docker runtime assets that sit alongside the Go sources
│       ├── compose.yaml   # Docker Compose stack with the database and Go services
│       ├── .env.docker    # Default configuration used by dockerized services
│       └── postgres/      # Postgres initialization scripts (PostGIS extension)
└── ...                    # Repository metadata (LICENSE, README, etc.)
```

