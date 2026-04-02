"""
Database Engine & Session Management for SQL Server
Dual Database Setup:
- System DB (Claude): RBAC, RLS, Audit, Table Metadata
- Data DB (Rep_data): Business data, dynamic tables, allocations
"""
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from sqlalchemy.pool import QueuePool
from typing import Generator
from loguru import logger

from app.core.config import get_settings

settings = get_settings()


# ============================================================================
# System Database Engine (Claude) - RBAC, RLS, Audit
# ============================================================================
system_engine = create_engine(
    settings.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_recycle=settings.DB_POOL_RECYCLE,
    pool_pre_ping=True,
    echo=settings.DEBUG,
    fast_executemany=True,
)

# Alias for backward compatibility
engine = system_engine


# ============================================================================
# Data Database Engine (Rep_data) - Business Data
# ============================================================================
data_engine = create_engine(
    settings.DATA_DATABASE_URL,
    poolclass=QueuePool,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_recycle=settings.DB_POOL_RECYCLE,
    pool_pre_ping=True,
    echo=settings.DEBUG,
    fast_executemany=True,
)


# ============================================================================
# Session Factories
# ============================================================================
SystemSessionLocal = sessionmaker(
    bind=system_engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)

DataSessionLocal = sessionmaker(
    bind=data_engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)

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
@event.listens_for(system_engine, "connect")
def set_system_connection_options(dbapi_connection, connection_record):
    """Set connection-level options for system DB."""
    pass


@event.listens_for(data_engine, "connect")
def set_data_connection_options(dbapi_connection, connection_record):
    """Set connection-level options for data DB.
    Use READ COMMITTED SNAPSHOT so readers never block writers and vice versa."""
    try:
        cursor = dbapi_connection.cursor()
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        cursor.close()
    except Exception:
        pass


@event.listens_for(system_engine, "checkout")
def system_checkout_listener(dbapi_connection, connection_record, connection_proxy):
    """Log when system connection is checked out."""
    logger.debug("System DB connection checked out from pool")


@event.listens_for(data_engine, "checkout")
def data_checkout_listener(dbapi_connection, connection_record, connection_proxy):
    """Log when data connection is checked out."""
    logger.debug("Data DB connection checked out from pool")
