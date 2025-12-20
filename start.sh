#!/bin/bash
# Start script for Railway deployment
PORT=${PORT:-8000}
echo "Starting SOTA Web Crawler on port $PORT"
exec uvicorn web_api:app --host 0.0.0.0 --port "$PORT"
