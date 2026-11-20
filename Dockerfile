FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY orchestrator/ ./orchestrator/
COPY tools/ ./tools/

# Default data directory for SQLite (mount a volume in production)
RUN mkdir -p /app/data

ENV ORCHESTRATOR_HOST=0.0.0.0
ENV ORCHESTRATOR_PORT=8000
ENV SQLITE_PATH=/app/data/orchestrator.db

EXPOSE 8000

# Entry point is wired up in main.py (build step 7)
CMD ["uvicorn", "orchestrator.main:app", "--host", "0.0.0.0", "--port", "8000"]