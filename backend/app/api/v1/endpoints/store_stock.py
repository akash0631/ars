"""
Store Stock - SLOC Settings API
================================
Manages KPI labels and Status (Active / Inactive) for each distinct SLOC
value found in the ET_STORE_STOCK table (Data DB).

DB columns in ARS_SLOC_SETTINGS:
  - kpi    NVARCHAR(200)  : user-defined KPI label
  - status NVARCHAR(20)   : 'Active' or 'Inactive'
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, validator
from sqlalchemy import text

from app.database.session import get_data_engine, get_system_engine
from app.schemas.common import APIResponse
from app.security.dependencies import get_current_user
from app.models.rbac import User

router = APIRouter(prefix="/store-stock", tags=["Store Stock"])

VALID_STATUSES = {"Active", "Inactive"}


# ── Schemas ──────────────────────────────────────────────────────────────────

class SlocSetting(BaseModel):
    sloc: str
    kpi: Optional[str] = None
    status: str = "Active"

    @validator("status")
    def validate_status(cls, v):
        if v not in VALID_STATUSES:
            raise ValueError(f"status must be 'Active' or 'Inactive', got '{v}'")
        return v


class BulkUpdateItem(BaseModel):
    sloc: str
    kpi: Optional[str] = None
    status: str = "Active"

    @validator("status")
    def validate_status(cls, v):
        if v not in VALID_STATUSES:
            raise ValueError(f"status must be 'Active' or 'Inactive', got '{v}'")
        return v


class BulkUpdateRequest(BaseModel):
    items: List[BulkUpdateItem]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_table(system_engine):
    """Auto-create / auto-migrate ARS_SLOC_SETTINGS table."""
    ddl = """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = 'ARS_SLOC_SETTINGS'
    )
    BEGIN
        CREATE TABLE ARS_SLOC_SETTINGS (
            id         INT IDENTITY(1,1) PRIMARY KEY,
            sloc       NVARCHAR(50)  NOT NULL UNIQUE,
            kpi        NVARCHAR(200) NULL,
            status     NVARCHAR(20)  NOT NULL DEFAULT 'Active',
            created_at DATETIME      NOT NULL DEFAULT GETDATE(),
            updated_at DATETIME      NOT NULL DEFAULT GETDATE()
        );
        CREATE INDEX IX_ARS_SLOC_SETTINGS_sloc ON ARS_SLOC_SETTINGS(sloc);
    END

    -- Upgrade: rename is_active BIT -> status NVARCHAR if old schema exists
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'ARS_SLOC_SETTINGS' AND COLUMN_NAME = 'status'
    )
    BEGIN
        ALTER TABLE ARS_SLOC_SETTINGS ADD status NVARCHAR(20) NOT NULL DEFAULT 'Active';
        IF EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'ARS_SLOC_SETTINGS' AND COLUMN_NAME = 'is_active'
        )
        BEGIN
            UPDATE ARS_SLOC_SETTINGS
            SET status = CASE WHEN is_active = 1 THEN 'Active' ELSE 'Inactive' END;
            ALTER TABLE ARS_SLOC_SETTINGS DROP COLUMN is_active;
        END
    END
    """
    with system_engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()


def _fetch_distinct_slocs(data_engine) -> List[str]:
    sql = "SELECT DISTINCT sloc AS qty FROM ET_STORE_STOCK ORDER BY sloc ASC"
    with data_engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return [str(r[0]) for r in rows if r[0] is not None]


def _fetch_saved(system_engine) -> dict:
    sql = "SELECT id, sloc, kpi, status, created_at, updated_at FROM ARS_SLOC_SETTINGS"
    with system_engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return {
        str(r[1]): {
            "id": r[0], "sloc": str(r[1]), "kpi": r[2],
            "status": r[3] if r[3] in VALID_STATUSES else "Active",
            "created_at": r[4], "updated_at": r[5],
        }
        for r in rows
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/sloc-settings", response_model=APIResponse)
def get_sloc_settings(current_user: User = Depends(get_current_user)):
    se = get_system_engine()
    de = get_data_engine()
    _ensure_table(se)
    try:
        slocs = _fetch_distinct_slocs(de)
    except Exception as e:
        raise HTTPException(500, detail=f"Failed to read ET_STORE_STOCK: {e}")

    saved  = _fetch_saved(se)
    result = []
    for sloc in slocs:
        if sloc in saved:
            entry = dict(saved[sloc]); entry["is_new"] = False
        else:
            entry = {"id": None, "sloc": sloc, "kpi": None, "status": "Active",
                     "created_at": None, "updated_at": None, "is_new": True}
        result.append(entry)

    return APIResponse(success=True,
        message=f"Loaded {len(result)} SLOC entries ({sum(1 for r in result if r['is_new'])} new)",
        data={"items": result, "total": len(result)})


@router.post("/sync", response_model=APIResponse)
def sync_slocs(current_user: User = Depends(get_current_user)):
    se = get_system_engine(); de = get_data_engine()
    _ensure_table(se)
    try:
        slocs = _fetch_distinct_slocs(de)
    except Exception as e:
        raise HTTPException(500, detail=f"Failed to read ET_STORE_STOCK: {e}")

    saved     = _fetch_saved(se)
    new_slocs = [s for s in slocs if s not in saved]
    if new_slocs:
        sql = text("INSERT INTO ARS_SLOC_SETTINGS (sloc,kpi,status,created_at,updated_at) VALUES (:sloc,NULL,'Active',GETDATE(),GETDATE())")
        with se.connect() as conn:
            for s in new_slocs: conn.execute(sql, {"sloc": s})
            conn.commit()
    return APIResponse(success=True, message=f"Sync complete. {len(new_slocs)} new SLOC(s) added.",
                       data={"new_count": len(new_slocs), "new_slocs": new_slocs})


@router.put("/sloc-settings/{sloc}", response_model=APIResponse)
def update_sloc_setting(sloc: str, payload: SlocSetting, current_user: User = Depends(get_current_user)):
    se = get_system_engine(); _ensure_table(se)
    sql = text("""
        IF EXISTS (SELECT 1 FROM ARS_SLOC_SETTINGS WHERE sloc=:sloc)
            UPDATE ARS_SLOC_SETTINGS SET kpi=:kpi,status=:status,updated_at=GETDATE() WHERE sloc=:sloc
        ELSE
            INSERT INTO ARS_SLOC_SETTINGS(sloc,kpi,status,created_at,updated_at) VALUES(:sloc,:kpi,:status,GETDATE(),GETDATE())
    """)
    with se.connect() as conn:
        conn.execute(sql, {"sloc": sloc, "kpi": payload.kpi, "status": payload.status})
        conn.commit()
    return APIResponse(success=True, message=f"SLOC '{sloc}' updated.",
                       data={"sloc": sloc, "kpi": payload.kpi, "status": payload.status})


@router.put("/sloc-settings", response_model=APIResponse)
def bulk_update(payload: BulkUpdateRequest, current_user: User = Depends(get_current_user)):
    se = get_system_engine(); _ensure_table(se)
    sql = text("""
        IF EXISTS (SELECT 1 FROM ARS_SLOC_SETTINGS WHERE sloc=:sloc)
            UPDATE ARS_SLOC_SETTINGS SET kpi=:kpi,status=:status,updated_at=GETDATE() WHERE sloc=:sloc
        ELSE
            INSERT INTO ARS_SLOC_SETTINGS(sloc,kpi,status,created_at,updated_at) VALUES(:sloc,:kpi,:status,GETDATE(),GETDATE())
    """)
    with se.connect() as conn:
        for item in payload.items:
            conn.execute(sql, {"sloc": item.sloc, "kpi": item.kpi, "status": item.status})
        conn.commit()
    return APIResponse(success=True, message=f"{len(payload.items)} SLOC(s) updated.",
                       data={"updated_count": len(payload.items)})
