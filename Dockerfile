# Build stage
FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Runtime stage
FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY ip_relay.py main.py ./

ENV PORT=8080
EXPOSE 8080
CMD ["uvicorn", "ip_relay:app", "--host", "0.0.0.0", "--port", "8080"]
