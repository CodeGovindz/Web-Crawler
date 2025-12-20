# Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory
RUN mkdir -p /app/data

# Expose port (Railway sets PORT env var)
EXPOSE 8000

# Start command
CMD ["sh", "-c", "uvicorn web_api:app --host 0.0.0.0 --port ${PORT:-8000}"]
