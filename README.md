# Chicago_BI

Chicago_BI is a collection of Go microservices that download public data from the
City of Chicago, hydrate a PostGIS enabled PostgreSQL database, and build a
disadvantaged business report on top of those curated datasets.

This repository now ships with Docker assets so you can run the collectors,
report generator, and their backing Postgres instance without installing any
development dependencies on your machine.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) 4.19 or
  newer (or the Docker Engine/CLI + Docker Compose plugin)
- At least 4 GB of available RAM – the collectors and PostGIS database routinely
  hold large datasets in memory while ingesting records.

## Running the stack

1. Review `src/docker/.env.docker` and adjust any values you want to override
   (database credentials, exposed ports, project name, etc.). Alternatively,
   you can set environment variables inline when invoking `docker compose`.
2. Start all services from the repository root:

   ```bash
   docker compose -f src/docker/compose.yaml up --build
   ```

   The first build compiles the Go services and downloads all Go modules.
   Subsequent `up` calls reuse the cached image.

3. Navigate to [http://localhost:8080](http://localhost:8080) to confirm that
   the collectors microservice is running. The reports microservice runs in the
   background, rebuilding the disadvantaged report every 24 hours after the
   collectors finish populating their tables.

4. Follow the logs to watch the data ingestion pipeline:

   ```bash
   docker compose -f src/docker/compose.yaml logs -f collectors
   docker compose -f src/docker/compose.yaml logs -f reports
   ```

5. Stop and remove the containers when you are finished:

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

Both Go services share the same image (see `Dockerfile`) and store spatial
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
`godotenv` requirement. The Compose file still enumerates the database
variables so Postgres receives the same credentials, but all of those values
originate from this single Docker-specific source. Override any of the
variables by editing that file or defining them inline when calling
`docker compose -f src/docker/compose.yaml ...`.

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

Feel free to adapt these defaults for production deployments (for example,
externalizing the Postgres connection string, pointing to managed storage, or
adding monitoring sidecars).