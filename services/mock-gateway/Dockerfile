FROM python:3.11-slim

WORKDIR /app

COPY services/mock-gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/mock-gateway/main.py /app/main.py

COPY services/mock-gateway/idempotency.py /app/idempotency.py

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
