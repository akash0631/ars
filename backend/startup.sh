#!/bin/bash
set -e

echo "=== ARS Startup ==="

# 1. Install ODBC Driver 18 if missing
if ! odbcinst -q -d -n "ODBC Driver 18 for SQL Server" > /dev/null 2>&1; then
  echo "Installing ODBC Driver 18..."
  curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
  echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list
  apt-get update -qq
  ACCEPT_EULA=Y apt-get install -y -qq msodbcsql18 unixodbc-dev
fi

# 2. Install Python dependencies
cd /home/site/wwwroot/backend
echo "Installing Python dependencies..."
pip install --no-cache-dir -r requirements.txt 2>&1 | tail -10

# 3. Ensure directories exist
mkdir -p logs uploads exports

# 4. Start Gunicorn (4 workers, 300s timeout for long allocation runs)
echo "Starting ARS backend..."
gunicorn main:app \
  -w 4 \
  -k uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 300 \
  --graceful-timeout 30 \
  --access-logfile - \
  --error-logfile -
