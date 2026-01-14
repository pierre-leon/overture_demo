FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code and data
# Only copy public road data - events are uploaded via UI
COPY server/ ./server/
COPY data/overture_roads.parquet ./data/overture_roads.parquet

# Set env vars
ENV PYTHONPATH=/app
ENV ROADS_PATH=/app/data/overture_roads.parquet
# Events are uploaded via UI, not loaded from file

# Expose port (default for many cloud runners is 8000 or $PORT)
EXPOSE 8000

# Run from server directory
WORKDIR /app/server
# Use shell form to expand environment variable
CMD sh -c "python -m uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"
