package com.ops.ingest;

import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/ingest")
public class FlightsIngestController {

    private final JdbcTemplate jdbcTemplate;

    public FlightsIngestController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostMapping(value = "/flights", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, Object> ingestFlights(@RequestPart("file") MultipartFile file) throws Exception {

        // Basic checks
        if (file.isEmpty()) {
            throw new IllegalArgumentException("Uploaded file is empty");
        }

        // We’ll do a simple, safe approach first: parse CSV lines and INSERT.
        // (Next upgrade: PostgreSQL COPY for 10x faster bulk load.)
        String csv = new String(file.getBytes(), StandardCharsets.UTF_8);
        String[] lines = csv.split("\\r?\\n");

        if (lines.length < 2) {
            throw new IllegalArgumentException("CSV must contain header + at least 1 data row");
        }

        // Clear staging before load (for MVP)
        jdbcTemplate.execute("TRUNCATE stg_flights");

        int rowsLoaded = 0;

        // Skip header (index 0)
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) continue;

            String[] cols = line.split(",", -1);
            if (cols.length < 10) {
                throw new IllegalArgumentException("Invalid row (expected 10 columns): " + line);
            }

            // Insert into staging
            jdbcTemplate.update(
                "INSERT INTO stg_flights (flight_date, airline, flight_num, origin, destination, sched_dep_ts, actual_dep_ts, sched_arr_ts, actual_arr_ts, tail_num) " +
                "VALUES (?::date, ?, ?, ?, ?, ?::timestamp, ?::timestamp, ?::timestamp, ?::timestamp, ?)",
                cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7], cols[8], cols[9]
            );

            rowsLoaded++;
        }

        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "OK");
        resp.put("file", file.getOriginalFilename());
        resp.put("rows_loaded", rowsLoaded);
        return resp;
    }
}
