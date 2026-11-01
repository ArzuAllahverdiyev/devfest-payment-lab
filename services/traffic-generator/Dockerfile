FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "psycopg[binary]>=3.2,<4"

COPY services/traffic-generator/runner.py /app/runner.py

CMD ["python", "-u", "runner.py"]
