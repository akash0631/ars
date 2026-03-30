"""
Application Configuration - Loaded from .env file
"""
from functools import lru_cache
from typing import List
from pydantic_settings import BaseSettings
from pydantic import field_validator
import json


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "RetailAllocationSystem"
    APP_VERSION: str = "1.0.0"
    APP_ENV: str = "development"
    DEBUG: bool = False
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # System Database (RBAC, RLS, Audit, Table Metadata)
    DB_SERVER: str = "HOPC560"
    DB_NAME: str = "Claude"
    DB_USERNAME: str = "sa"
    DB_PASSWORD: str = "vrl@55555"
    DB_DRIVER: str = "ODBC Driver 18 for SQL Server"
    DB_TRUST_CERT: str = "yes"
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 30
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 1800

    # Working Database (Business data, dynamic tables, allocations)
    DATA_DB_NAME: str = "Rep_data"

    # JWT
    JWT_SECRET_KEY: str = "your-super-secret-key-change-in-production-min-32-chars"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 480
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # Security
    CORS_ORIGINS: str = '["http://localhost:3000","http://localhost:8000"]'
    PASSWORD_MIN_LENGTH: int = 8
    MAX_LOGIN_ATTEMPTS: int = 5
    ACCOUNT_LOCK_DURATION_MINUTES: int = 30

    # File Upload
    MAX_UPLOAD_SIZE_MB: int = 100
    UPLOAD_CHUNK_SIZE: int = 10000
    ALLOWED_EXTENSIONS: str = ".csv,.xlsx,.xls"

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/app.log"

    # Super Admin
    SUPER_ADMIN_USERNAME: str = "superadmin"
    SUPER_ADMIN_EMAIL: str = "admin@nubo.in"
    SUPER_ADMIN_PASSWORD: str = "Admin@12345"

    @property
    def DATABASE_URL(self) -> str:
        """Build SQLAlchemy connection string for System DB (Claude)."""
        from urllib.parse import quote_plus
        password = quote_plus(self.DB_PASSWORD)
        driver = quote_plus(self.DB_DRIVER)
        return (
            f"mssql+pyodbc://{self.DB_USERNAME}:{password}@{self.DB_SERVER}/"
            f"{self.DB_NAME}?driver={driver}&TrustServerCertificate={self.DB_TRUST_CERT}"
        )

    @property
    def DATA_DATABASE_URL(self) -> str:
        """Build SQLAlchemy connection string for Working DB (Rep_data)."""
        from urllib.parse import quote_plus
        password = quote_plus(self.DB_PASSWORD)
        driver = quote_plus(self.DB_DRIVER)
        return (
            f"mssql+pyodbc://{self.DB_USERNAME}:{password}@{self.DB_SERVER}/"
            f"{self.DATA_DB_NAME}?driver={driver}&TrustServerCertificate={self.DB_TRUST_CERT}"
        )

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
