FROM python:3.11-slim

WORKDIR /app

COPY services/payment-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
COPY services/payment-api/main.py /app/main.py

COPY ui/index.html /app/ui/index.html

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
