FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
COPY consumer/ ./consumer/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "consumer.consumer"]