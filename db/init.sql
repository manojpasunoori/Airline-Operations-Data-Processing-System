-- ========== STAGING ==========
CREATE TABLE IF NOT EXISTS stg_flights (
  id BIGSERIAL PRIMARY KEY,
  flight_date DATE NOT NULL,
  airline VARCHAR(10) NOT NULL,
  flight_num VARCHAR(10) NOT NULL,
  origin VARCHAR(5) NOT NULL,
  destination VARCHAR(5) NOT NULL,
  sched_dep_ts TIMESTAMP,
  actual_dep_ts TIMESTAMP,
  sched_arr_ts TIMESTAMP,
  actual_arr_ts TIMESTAMP,
  tail_num VARCHAR(20),
  created_at TIMESTAMP DEFAULT NOW()
);

-- ========== CURATED ==========
CREATE TABLE IF NOT EXISTS fact_flight_leg (
  flight_date DATE NOT NULL,
  airline VARCHAR(10) NOT NULL,
  flight_num VARCHAR(10) NOT NULL,
  origin VARCHAR(5) NOT NULL,
  destination VARCHAR(5) NOT NULL,
  sched_dep_ts TIMESTAMP,
  actual_dep_ts TIMESTAMP,
  sched_arr_ts TIMESTAMP,
  actual_arr_ts TIMESTAMP,
  dep_delay_min INT,
  arr_delay_min INT,
  PRIMARY KEY (flight_date, airline, flight_num, origin, destination)
);

-- ========== METRICS ==========
CREATE TABLE IF NOT EXISTS kpi_delay_by_route_day (
  flight_date DATE NOT NULL,
  route VARCHAR(20) NOT NULL,
  flights INT NOT NULL,
  avg_dep_delay_min NUMERIC(10,2),
  p90_dep_delay_min NUMERIC(10,2),
  PRIMARY KEY (flight_date, route)
);

CREATE TABLE IF NOT EXISTS kpi_on_time_by_airport_day (
  flight_date DATE NOT NULL,
  airport VARCHAR(5) NOT NULL,
  flights INT NOT NULL,
  on_time_pct NUMERIC(5,2),
  PRIMARY KEY (flight_date, airport)
);

CREATE INDEX IF NOT EXISTS idx_stg_flights_date ON stg_flights(flight_date);
CREATE INDEX IF NOT EXISTS idx_stg_flights_route ON stg_flights(origin, destination);
