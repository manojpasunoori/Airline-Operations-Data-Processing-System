package com.ops.ingest.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestLoggingFilter extends OncePerRequestFilter {

    private static final Logger httpLog = LoggerFactory.getLogger("http");
    private static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        Instant start = Instant.now();

        String correlationId = Optional.ofNullable(request.getHeader(CORRELATION_ID_HEADER))
                .filter(s -> !s.isBlank())
                .orElse(UUID.randomUUID().toString());

        String requestId = UUID.randomUUID().toString();

        MDC.put("correlationId", correlationId);
        MDC.put("requestId", requestId);
        MDC.put("path", request.getRequestURI());
        MDC.put("httpMethod", request.getMethod());
        MDC.put("clientIp", getClientIp(request));

        response.setHeader(CORRELATION_ID_HEADER, correlationId);

        try {
            httpLog.info("request_start contentType={} contentLength={}",
                    request.getContentType(),
                    request.getContentLengthLong());

            filterChain.doFilter(request, response);

        } finally {
            long ms = Duration.between(start, Instant.now()).toMillis();
            httpLog.info("request_end status={} durationMs={}",
                    response.getStatus(),
                    ms);
            MDC.clear();
        }
    }

    private String getClientIp(HttpServletRequest request) {
        String xff = request.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            return xff.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
