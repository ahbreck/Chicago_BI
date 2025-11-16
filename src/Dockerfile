# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.22

FROM golang:${GO_VERSION}-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/collectors ./cmd/collectors
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/reports ./cmd/reports

FROM debian:bookworm-slim AS runner
RUN useradd --create-home --uid 1000 appuser \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /out/collectors /usr/local/bin/collectors
COPY --from=builder /out/reports /usr/local/bin/reports
COPY data ./src/data
COPY .env .env
RUN mkdir -p data/spatial && chown -R appuser:appuser /app
USER appuser
ENV PORT=8080 \
    PROJECT_ID=Chicago-BI \
    SPATIAL_DATA_DIR=/app/data/spatial
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/collectors"]
CMD []