import json
import logging
import os
import sys
import time
import uuid

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": os.getenv("APP_NAME", "transform-service"),
            "env": os.getenv("APP_ENV", "local"),
        }

        for k in ("jobRunId", "step", "rows", "durationMs"):
            if hasattr(record, k):
                payload[k] = getattr(record, k)

        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)

        return json.dumps(payload)

def setup_logger(name: str = "transform-service"):
    logger = logging.getLogger(name)
    logger.setLevel(os.getenv("LOG_LEVEL_ROOT", "INFO"))

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    logger.handlers = [handler]
    logger.propagate = False

    job_run_id = os.getenv("JOB_RUN_ID") or str(uuid.uuid4())
    return logger, job_run_id
