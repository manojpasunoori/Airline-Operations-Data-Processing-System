FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py /app/app.py
COPY logging_utils.py /app/logging_utils.py

CMD ["python", "app.py"]
