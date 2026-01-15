import os
import sys
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from logging_utils import setup_logger

log, jobRunId = setup_logger()
t0 = time.time()


def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def db_url() -> str:
    # Build DB connection string from environment variables
    user = require_env("DB_USER")
    password = require_env("DB_PASS")
    host = require_env("DB_HOST")
    port = require_env("DB_PORT")
    db = require_env("DB_NAME")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def refresh_fact(conn) -> int:
    """
    Upsert into fact_flight_leg from stg_flights.
    Returns affected row count if available (may be -1 depending on driver).
    """
    sql = text("""
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
    """)
    res = conn.execute(sql)
    return getattr(res, "rowcount", -1)


def refresh_kpi_delay_by_route_day(conn) -> int:
    conn.execute(text("TRUNCATE kpi_delay_by_route_day"))
    sql = text("""
      INSERT INTO kpi_delay_by_route_day (flight_date, route, flights, avg_dep_delay_min, p90_dep_delay_min)
      SELECT
        flight_date,
        origin || '-' || destination AS route,
        COUNT(*) AS flights,
        ROUND(AVG(COALESCE(dep_delay_min,0))::numeric, 2) AS avg_dep_delay_min,
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY COALESCE(dep_delay_min,0))::numeric, 2) AS p90_dep_delay_min
      FROM fact_flight_leg
      GROUP BY flight_date, origin, destination
    """)
    res = conn.execute(sql)
    return getattr(res, "rowcount", -1)


def refresh_kpi_on_time_by_airport_day(conn) -> int:
    conn.execute(text("TRUNCATE kpi_on_time_by_airport_day"))
    sql = text("""
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
    """)
    res = conn.execute(sql)
    return getattr(res, "rowcount", -1)


def main() -> int:
    log.info("transform_start", extra={"jobRunId": jobRunId})

    try:
        engine = create_engine(db_url(), pool_pre_ping=True)

        # engine.begin() starts a transaction; commits on success, rolls back on error.
        with engine.begin() as conn:

            # Step: refresh_fact
            s = time.time()
            log.info("step_start", extra={"jobRunId": jobRunId, "step": "refresh_fact"})
            fact_rows = refresh_fact(conn)
            log.info(
                "step_end",
                extra={
                    "jobRunId": jobRunId,
                    "step": "refresh_fact",
                    "rows": fact_rows,
                    "durationMs": int((time.time() - s) * 1000),
                },
            )

            # Step: kpi_delay_by_route_day
            s = time.time()
            log.info("step_start", extra={"jobRunId": jobRunId, "step": "kpi_delay_by_route_day"})
            kpi1_rows = refresh_kpi_delay_by_route_day(conn)
            log.info(
                "step_end",
                extra={
                    "jobRunId": jobRunId,
                    "step": "kpi_delay_by_route_day",
                    "rows": kpi1_rows,
                    "durationMs": int((time.time() - s) * 1000),
                },
            )

            # Step: kpi_on_time_by_airport_day
            s = time.time()
            log.info("step_start", extra={"jobRunId": jobRunId, "step": "kpi_on_time_by_airport_day"})
            kpi2_rows = refresh_kpi_on_time_by_airport_day(conn)
            log.info(
                "step_end",
                extra={
                    "jobRunId": jobRunId,
                    "step": "kpi_on_time_by_airport_day",
                    "rows": kpi2_rows,
                    "durationMs": int((time.time() - s) * 1000),
                },
            )

        log.info("transform_success", extra={"jobRunId": jobRunId})
        return 0

    except (RuntimeError, SQLAlchemyError, Exception):
        # Logs exception stack trace in JSON
        log.exception("transform_failed", extra={"jobRunId": jobRunId})
        return 1

    finally:
        log.info(
            "transform_end",
            extra={"jobRunId": jobRunId, "durationMs": int((time.time() - t0) * 1000)},
        )


if __name__ == "__main__":
    sys.exit(main())


