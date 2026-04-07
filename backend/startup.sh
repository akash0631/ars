#!/bin/bash
echo "=== ARS Startup ==="

# 1. Install ODBC Driver 18 if missing (may fail without root — that's OK)
if ! odbcinst -q -d -n "ODBC Driver 18 for SQL Server" > /dev/null 2>&1; then
  echo "Installing ODBC Driver 18..."
  curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg 2>/dev/null || true
  echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list 2>/dev/null || true
  apt-get update -qq 2>/dev/null || true
  ACCEPT_EULA=Y apt-get install -y -qq msodbcsql18 unixodbc-dev 2>/dev/null || echo "ODBC install failed — will use Snowflake-only mode"
fi

# 2. Find the app directory (zip deploy may put it in different locations)
if [ -d /home/site/wwwroot/backend ]; then
  cd /home/site/wwwroot/backend
elif [ -d /home/site/wwwroot ]; then
  cd /home/site/wwwroot
fi
echo "Working directory: $(pwd)"

# 3. Install Python dependencies
echo "Installing Python dependencies..."
pip install --no-cache-dir -r requirements.txt 2>&1 | tail -5 || echo "pip install had warnings"

# 4. Ensure directories exist
mkdir -p logs uploads exports

# 5. Start Gunicorn (2 workers for B2 tier, 300s timeout)
echo "Starting ARS backend..."
exec gunicorn main:app \
  -w 2 \
  -k uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 300 \
  --graceful-timeout 30 \
  --access-logfile - \
  --error-logfile -
