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
echo "Files in working directory:"
ls -la

# 3. Install Python dependencies (split so pyodbc failure doesn't block everything)
echo "Installing Python dependencies..."

# Install pyodbc separately — it may fail if ODBC headers are missing, and that's OK
pip install --no-cache-dir pyodbc==5.1.0 2>&1 | tail -3 || echo "WARNING: pyodbc install failed — SQL Server features disabled, Snowflake-only mode"

# Install everything else (skip pyodbc since we already tried it)
pip install --no-cache-dir -r requirements.txt 2>&1 | tail -10 || echo "WARNING: pip install had errors"

# 4. Ensure directories exist
mkdir -p logs uploads exports

# 5. Verify key imports before starting
echo "Verifying Python imports..."
python3 -c "
import sys
try:
    import fastapi; print(f'fastapi OK: {fastapi.__version__}')
except ImportError as e:
    print(f'FATAL: fastapi not installed: {e}'); sys.exit(1)
try:
    import uvicorn; print('uvicorn OK')
except ImportError as e:
    print(f'FATAL: uvicorn not installed: {e}'); sys.exit(1)
try:
    import pyodbc; print('pyodbc OK')
except ImportError:
    print('WARNING: pyodbc not available — SQL Server disabled')
try:
    import snowflake.connector; print('snowflake OK')
except ImportError:
    print('WARNING: snowflake-connector not available')
print('Import check passed')
" || { echo "FATAL: Basic imports failed"; exit 1; }

# 6. Start Gunicorn (2 workers for B2 tier, 300s timeout)
echo "Starting ARS backend..."
exec gunicorn main:app \
  -w 2 \
  -k uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 300 \
  --graceful-timeout 30 \
  --access-logfile - \
  --error-logfile -
