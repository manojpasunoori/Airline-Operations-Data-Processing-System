\# Airline Operations Data Processing System



Production-style backend system to ingest airline operational datasets, run transformation/ETL, compute KPIs, and expose metrics via REST APIs.



\## Architecture

\- \*\*Postgres\*\*: staging + facts + KPI tables

\- \*\*Ingestion Service (Spring Boot)\*\*: `POST /ingest/flights` (CSV upload → `stg\_flights`)

\- \*\*Transform Job (Python)\*\*: `stg\_flights` → `fact\_flight\_leg` → KPI tables

\- \*\*KPI API (Spring Boot)\*\*: serves KPIs via REST



\## Services

\- `postgres` (5432)

\- `ingestion-service` (8081)

\- `kpi-api` (8080)

\- `transform` (batch job)



\## Run locally

```bash

docker compose up -d --build

Run locally (Docker Compose)
# Start services
docker compose up --build -d

# Ingest sample CSV (PowerShell)
curl.exe -X POST "http://localhost:8081/ingest/flights" -F "file=@data/sample/flights.csv"

# Run batch transform job (builds fact + KPI tables)
docker compose --profile jobs run --rm transform

# Query KPIs
curl.exe "http://localhost:8080/kpis/on-time-by-airport?date=2026-01-15"
curl.exe "http://localhost:8080/kpis/delay-by-route?date=2026-01-15"

Observability (Logs)

Spring Boot services emit structured JSON logs with request tracing:

request_start / request_end, durationMs

correlationId, requestId, path, httpMethod, clientIp

Python batch transform emits JSON logs with:

jobRunId per run

step-level timing (durationMs) and row counts (rows)

Quick log checks:

docker logs --tail 80 ops-ingestion
docker logs --tail 80 ops-kpi-api
docker compose --profile jobs run --rm transform



