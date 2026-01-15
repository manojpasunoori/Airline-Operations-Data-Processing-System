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



