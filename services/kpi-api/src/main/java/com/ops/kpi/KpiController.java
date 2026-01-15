package com.ops.kpi;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/kpis")
public class KpiController {

    private final JdbcTemplate jdbcTemplate;

    public KpiController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/delay-by-route")
    public List<Map<String, Object>> delayByRoute(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date
    ) {
        return jdbcTemplate.queryForList(
            "SELECT flight_date, route, flights, avg_dep_delay_min, p90_dep_delay_min " +
            "FROM kpi_delay_by_route_day WHERE flight_date = ? ORDER BY avg_dep_delay_min DESC",
            date
        );
    }

    @GetMapping("/on-time-by-airport")
    public List<Map<String, Object>> onTimeByAirport(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date
    ) {
        return jdbcTemplate.queryForList(
            "SELECT flight_date, airport, flights, on_time_pct " +
            "FROM kpi_on_time_by_airport_day WHERE flight_date = ? ORDER BY on_time_pct DESC",
            date
        );
    }
}
