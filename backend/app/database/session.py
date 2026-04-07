"""
Database Engine & Session Management for SQL Server
Dual Database Setup:
- System DB (Claude): RBAC, RLS, Audit, Table Metadata
- Data DB (Rep_data): Business data, dynamic tables, allocations

NOTE: pyodbc / ODBC Driver 18 may not be available on Azure App Service Linux.
This module MUST NOT crash on import — the app should start in degraded mode
so that Snowflake-only endpoints (allocation) still work.
"""
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from sqlalchemy.pool import QueuePool
from typing import Generator, Optional
from loguru import logger

from app.core.config import get_settings

settings = get_settings()

# ============================================================================
# Check if pyodbc / ODBC driver is available
# ============================================================================
_odbc_available = False
try:
    import pyodbc
    _odbc_available = True
    logger.info("pyodbc loaded successfully")
except ImportError:
    logger.warning("pyodbc not installed — SQL Server engines disabled (Snowflake-only mode)")
except Exception as e:
    logger.warning(f"pyodbc import failed: {e} — SQL Server engines disabled (Snowflake-only mode)")

# ============================================================================
# Lazy Engine Creation — prevents Azure cold-start crash if DB unreachable
# ============================================================================
_system_engine = None
_data_engine = None


def _create_engine_safe(url, label="DB") -> Optional[object]:
    """Create SQLAlchemy engine with error handling for cold start."""
    if not _odbc_available:
        logger.warning(f"{label} engine skipped — ODBC driver not available")
        return None
    try:
        eng = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=settings.DB_POOL_SIZE,
            max_overflow=settings.DB_MAX_OVERFLOW,
            pool_timeout=settings.DB_POOL_TIMEOUT,
            pool_recycle=settings.DB_POOL_RECYCLE,
            pool_pre_ping=getattr(settings, 'DB_POOL_PRE_PING', True),
            echo=settings.DEBUG,
            connect_args={"timeout": 10},  # 10s login timeout to avoid hanging
        )
        logger.info(f"{label} engine created: {url[:50]}...")
        return eng
    except Exception as e:
        logger.error(f"{label} engine creation failed: {e}")
        return None


def _get_system_engine():
    global _system_engine
    if _system_engine is None:
        _system_engine = _create_engine_safe(settings.DATABASE_URL, "System")
    return _system_engine


def _get_data_engine():
    global _data_engine
    if _data_engine is None:
        _data_engine = _create_engine_safe(settings.DATA_DATABASE_URL, "Data")
    return _data_engine


# Create eagerly but catch errors — app still starts if DB is down
try:
    system_engine = _create_engine_safe(settings.DATABASE_URL, "System")
except Exception as e:
    logger.error(f"System DB init failed: {e}")
    system_engine = None

try:
    data_engine = _create_engine_safe(settings.DATA_DATABASE_URL, "Data")
except Exception as e:
    logger.error(f"Data DB init failed: {e}")
    data_engine = None

# Alias for backward compatibility
engine = system_engine


# ============================================================================
# Session Factories
# ============================================================================
SystemSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)
if system_engine:
    SystemSessionLocal.configure(bind=system_engine)

DataSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)
if data_engine:
    DataSessionLocal.configure(bind=data_engine)

# Alias for backward compatibility
SessionLocal = SystemSessionLocal


# ============================================================================
# Declarative Bases
# ============================================================================
class Base(DeclarativeBase):
    """Base for system tables (RBAC, RLS, Audit)."""
    pass


class DataBase(DeclarativeBase):
    """Base for data tables (business data)."""
    pass


# ============================================================================
# Dependencies: Get DB Sessions (for FastAPI)
# ============================================================================
def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency for System DB (RBAC, RLS, Audit)."""
    db = SystemSessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_data_db() -> Generator[Session, None, None]:
    """FastAPI dependency for Data DB (business data, dynamic tables)."""
    db = DataSessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ============================================================================
# Raw Connection Helpers (for Pandas / bulk ops)
# ============================================================================
def get_raw_connection():
    """Get a raw DBAPI connection for System DB."""
    return system_engine.raw_connection()


def get_data_raw_connection():
    """Get a raw DBAPI connection for Data DB."""
    return data_engine.raw_connection()


def get_engine():
    """Get the System DB SQLAlchemy engine."""
    return system_engine


def get_system_engine():
    """Get the System DB SQLAlchemy engine (alias for get_engine)."""
    return system_engine


def get_data_engine():
    """Get the Data DB SQLAlchemy engine."""
    return data_engine


def get_system_db_url() -> str:
    """Get the System DB connection URL string (for background tasks)."""
    return str(settings.DATABASE_URL)


def get_data_db_url() -> str:
    """Get the Data DB connection URL string (for background tasks)."""
    return str(settings.DATA_DATABASE_URL)


# ============================================================================
# Enable Read Committed Snapshot Isolation (RCSI)
# ============================================================================
def enable_rcsi():
    """
    Enable READ_COMMITTED_SNAPSHOT on both databases.
    This is the #1 fix for 'DB locked during upsert' — readers use row versioning
    instead of shared locks, so they NEVER block writers and vice versa.
    This is a one-time DB setting that persists across restarts.
    """
    for label, eng in [("System", system_engine), ("Data", data_engine)]:
        try:
            db_name = None
            with eng.connect() as conn:
                db_name = conn.execute(text("SELECT DB_NAME()")).scalar()
                is_rcsi = conn.execute(text(
                    "SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = DB_NAME()"
                )).scalar()
                if is_rcsi:
                    logger.info(f"{label} DB [{db_name}]: RCSI already enabled")
                    continue

            # Must run ALTER DATABASE outside of a transaction on a separate connection
            # using autocommit mode
            raw = eng.raw_connection()
            raw.autocommit = True
            try:
                cursor = raw.cursor()
                cursor.execute(f"ALTER DATABASE [{db_name}] SET READ_COMMITTED_SNAPSHOT ON")
                cursor.close()
                logger.info(f"{label} DB [{db_name}]: RCSI enabled successfully")
            except Exception as e:
                logger.warning(f"{label} DB [{db_name}]: Could not enable RCSI: {e}")
            finally:
                raw.close()

        except Exception as e:
            logger.warning(f"{label} DB: RCSI check failed: {e}")


# ============================================================================
# Health Checks
# ============================================================================
def check_db_connection() -> bool:
    """Verify System DB connectivity."""
    try:
        with system_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"System DB connection failed: {e}")
        return False


def check_data_db_connection() -> bool:
    """Verify Data DB connectivity."""
    try:
        with data_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"Data DB connection failed: {e}")
        return False


# ============================================================================
# Event Listeners
# ============================================================================
if system_engine:
    @event.listens_for(system_engine, "connect")
    def set_system_connection_options(dbapi_connection, connection_record):
        pass

if data_engine:
    @event.listens_for(data_engine, "connect")
    def set_data_connection_options(dbapi_connection, connection_record):
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.close()
        except Exception:
            pass


