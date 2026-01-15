import os
from sqlalchemy import create_engine, text

# 1) Build DB connection string from environment variables
def db_url():
    return (
        f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASS']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )

engine = create_engine(db_url())

# 2) Create/refresh clean fact table from staging
def refresh_fact(conn):
    conn.execute(text("""
        INSERT INTO fact_flight_leg (
          flight_date, airline, flight_num, origin, destination,
          sched_dep_ts, actual_dep_ts, sched_arr_ts, actual_arr_ts,
          dep_delay_min, arr_delay_min
        )
        SELECT
          flight_date, airline, flight_num, origin, destination,
          sched_dep_ts, actual_dep_ts, sched_arr_ts, actual_arr_ts,
          CASE
            WHEN sched_dep_ts IS NULL OR actual_dep_ts IS NULL THEN NULL
            ELSE ROUND(EXTRACT(EPOCH FROM (actual_dep_ts - sched_dep_ts))/60)::INT
          END AS dep_delay_min,
          CASE
            WHEN sched_arr_ts IS NULL OR actual_arr_ts IS NULL THEN NULL
            ELSE ROUND(EXTRACT(EPOCH FROM (actual_arr_ts - sched_arr_ts))/60)::INT
          END AS arr_delay_min
        FROM stg_flights
        ON CONFLICT (flight_date, airline, flight_num, origin, destination)
        DO UPDATE SET
          sched_dep_ts = EXCLUDED.sched_dep_ts,
          actual_dep_ts = EXCLUDED.actual_dep_ts,
          sched_arr_ts = EXCLUDED.sched_arr_ts,
          actual_arr_ts = EXCLUDED.actual_arr_ts,
          dep_delay_min = EXCLUDED.dep_delay_min,
          arr_delay_min = EXCLUDED.arr_delay_min
    """))

# 3) Compute KPI tables (fast reads for API/dashboard)
def refresh_kpis(conn):
    # KPI 1: Delay by route/day
    conn.execute(text("TRUNCATE kpi_delay_by_route_day"))
    conn.execute(text("""
      INSERT INTO kpi_delay_by_route_day (flight_date, route, flights, avg_dep_delay_min, p90_dep_delay_min)
      SELECT
        flight_date,
        origin || '-' || destination AS route,
        COUNT(*) AS flights,
        ROUND(AVG(COALESCE(dep_delay_min,0))::numeric, 2) AS avg_dep_delay_min,
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY COALESCE(dep_delay_min,0))::numeric, 2) AS p90_dep_delay_min
      FROM fact_flight_leg
      GROUP BY flight_date, origin, destination
    """))

    # KPI 2: On-time % by airport/day (<= 15 min dep delay)
    conn.execute(text("TRUNCATE kpi_on_time_by_airport_day"))
    conn.execute(text("""
      INSERT INTO kpi_on_time_by_airport_day (flight_date, airport, flights, on_time_pct)
      SELECT
        flight_date,
        origin AS airport,
        COUNT(*) AS flights,
        ROUND(
          100.0 * SUM(CASE WHEN COALESCE(dep_delay_min,0) <= 15 THEN 1 ELSE 0 END)::numeric
          / COUNT(*)::numeric,
          2
        ) AS on_time_pct
      FROM fact_flight_leg
      GROUP BY flight_date, origin
    """))

def main():
    # engine.begin() makes a transaction; it auto-commits if success, rolls back if failure.
    with engine.begin() as conn:
        refresh_fact(conn)
        refresh_kpis(conn)

    print("✅ Transform complete: fact_flight_leg + KPI tables refreshed.")

if __name__ == "__main__":
    main()
