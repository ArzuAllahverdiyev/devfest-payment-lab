FROM python:3.11-slim

WORKDIR /app

COPY services/payment-worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
COPY services/payment-worker/main.py /app/main.py

COPY services/payment-worker/inbox.py /app/inbox.py

CMD ["python", "-u", "main.py"]

COPY services/payment-worker/supervisor.py /app/supervisor.py
