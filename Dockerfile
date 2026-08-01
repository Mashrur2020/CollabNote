# ---------- builder stage: has gcc to compile aiokafka's C extension ----------
FROM python:3.13-slim AS builder

# Tools needed to build Python C extensions (aiokafka uses Cython)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libpq-dev \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Build wheels into /wheels so the runtime stage can install them offline
COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt


# ---------- runtime stage: slim, no compiler ----------
FROM python:3.13-slim

# libpq5 is needed at runtime by psycopg2-binary
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install prebuilt wheels first (no compiler in this stage)
COPY --from=builder /wheels /wheels
COPY requirements.txt .
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
    && rm -rf /wheels

# Default port; compose overrides per instance (api1 -> 8001, api2 -> 8002)
ENV PORT=8000
EXPOSE 8000

COPY . .

# Pick up $PORT from compose so each replica can listen on a different port
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]