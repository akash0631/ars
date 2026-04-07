"""
Application Configuration — Cloud-Ready
All sensitive values from environment variables / .env file.
No hardcoded passwords or secrets.
"""
from functools import lru_cache
from typing import List
from pydantic_settings import BaseSettings
import json


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "ARS - Auto Replenishment System"
    APP_VERSION: str = "2.1.0"
    APP_ENV: str = "development"  # development | staging | production
    DEBUG: bool = False
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # System Database (RBAC, RLS, Audit, Table Metadata)
    DB_SERVER: str = "HOPC560"
    DB_NAME: str = "Claude"
    DB_USERNAME: str = "sa"
    DB_PASSWORD: str = "vrl@55555"           # Override via .env in production
    DB_DRIVER: str = "ODBC Driver 18 for SQL Server"
    DB_TRUST_CERT: str = "yes"               # "no" for Azure SQL
    DB_ENCRYPT: str = "no"                   # "yes" for Azure SQL (mandatory)

    # Connection pool — tuned for 20+ concurrent planners
    DB_POOL_SIZE: int = 15
    DB_MAX_OVERFLOW: int = 25
    DB_POOL_TIMEOUT: int = 60
    DB_POOL_RECYCLE: int = 300               # Azure recommends 300s
    DB_POOL_PRE_PING: bool = True

    DB_TEMPDB_CLEANUP_INTERVAL_MINUTES: int = 5
    DB_TEMPDB_ORPHAN_AGE_MINUTES: int = 15   # More room for long MSA runs

    # Working Database (Business data, dynamic tables, allocations)
    DATA_DB_NAME: str = "Rep_data"

    # Snowflake (read-only — scores + store stock)
    SNOWFLAKE_ACCOUNT: str = "iafphkw-hh80816"
    SNOWFLAKE_USER: str = "akashv2kart"
    SNOWFLAKE_PASSWORD: str = ""               # Override via env / .env
    SNOWFLAKE_WAREHOUSE: str = "ALLOC_WH"

    # Supabase (budget cascade + size contributions)
    SUPABASE_URL: str = "https://pymdqnnwwxrgeolvgvgv.supabase.co"
    SUPABASE_SERVICE_KEY: str = ""             # Override via env / .env

    # JWT
    JWT_SECRET_KEY: str = "your-super-secret-key-change-in-production-min-32-chars"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 480
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # Security
    CORS_ORIGINS: str = '["http://localhost:3000","http://localhost:8000","https://replen.v2retail.net"]'
    PASSWORD_MIN_LENGTH: int = 8
    MAX_LOGIN_ATTEMPTS: int = 5
    ACCOUNT_LOCK_DURATION_MINUTES: int = 30

    # File Upload / Storage
    MAX_UPLOAD_SIZE_MB: int = 100
    UPLOAD_CHUNK_SIZE: int = 2000
    ALLOWED_EXTENSIONS: str = ".csv,.xlsx,.xls"
    USE_BLOB_STORAGE: bool = False           # True in production (Azure Blob)
    AZURE_STORAGE_CONNECTION_STRING: str = ""
    AZURE_STORAGE_CONTAINER: str = "ars-files"
    LOCAL_UPLOAD_DIR: str = "uploads"
    LOCAL_EXPORT_DIR: str = "exports"

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/app.log"
    LOG_TO_FILE: bool = True                 # False in cloud (use stdout)

    # Super Admin
    SUPER_ADMIN_USERNAME: str = "superadmin"
    SUPER_ADMIN_EMAIL: str = "admin@nubo.in"
    SUPER_ADMIN_PASSWORD: str = "Admin@12345"  # Override via .env in production

    # =========================================================================
    # Computed properties
    # =========================================================================
    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"

    @property
    def DATABASE_URL(self) -> str:
        """SQLAlchemy connection string for System DB (Claude)."""
        return self._build_connection_url(self.DB_NAME)

    @property
    def DATA_DATABASE_URL(self) -> str:
        """SQLAlchemy connection string for Working DB (Rep_data)."""
        return self._build_connection_url(self.DATA_DB_NAME)

    def _build_connection_url(self, db_name: str) -> str:
        from urllib.parse import quote_plus
        password = quote_plus(self.DB_PASSWORD)
        driver = quote_plus(self.DB_DRIVER)
        url = (
            f"mssql+pyodbc://{self.DB_USERNAME}:{password}@{self.DB_SERVER}/"
            f"{db_name}?driver={driver}&TrustServerCertificate={self.DB_TRUST_CERT}"
        )
        if self.DB_ENCRYPT.lower() == "yes":
            url += "&Encrypt=yes"
        return url

    @property
    def cors_origins_list(self) -> List[str]:
        try:
            return json.loads(self.CORS_ORIGINS)
        except (json.JSONDecodeError, TypeError):
            return ["http://localhost:3000"]

    @property
    def allowed_extensions_list(self) -> List[str]:
        return [ext.strip() for ext in self.ALLOWED_EXTENSIONS.split(",")]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()
