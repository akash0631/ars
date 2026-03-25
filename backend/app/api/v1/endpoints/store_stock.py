"""
Store Stock - SLOC Settings API
Table: ARS_STORE_SLOC_SETTINGS (System DB)
  id INT IDENTITY PK | sloc NVARCHAR(50) UNIQUE | kpi NVARCHAR(200) | status NVARCHAR(20) | created_at | updated_at
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, validator
from sqlalchemy import text
from loguru import logger

from app.database.session import get_data_engine
from app.schemas.common import APIResponse
from app.security.dependencies import get_current_user
from app.models.rbac import User

router       = APIRouter(prefix="/store-stock", tags=["Store Stock"])
TABLE        = "ARS_STORE_SLOC_SETTINGS"
OLD_TABLE    = "ARS_SLOC_SETTINGS"
VALID_STATUS = {"Active", "Inactive"}


# ── Schemas ──────────────────────────────────────────────────────────────────
class SlocSetting(BaseModel):
    sloc:   str
    kpi:    Optional[str] = None
    status: str = "Active"
    @validator("status")
    def _chk(cls, v):
        if v not in VALID_STATUS: raise ValueError("status must be Active or Inactive")
        return v

class BulkUpdateItem(BaseModel):
    sloc:   str
    kpi:    Optional[str] = None
    status: str = "Active"
    @validator("status")
    def _chk(cls, v):
        if v not in VALID_STATUS: raise ValueError("status must be Active or Inactive")
        return v

class BulkUpdateRequest(BaseModel):
    items: List[BulkUpdateItem]


# ── DDL helpers (each step in its own batch – SQL Server parse-time safety) ──
def _run(conn, sql, params=None):
    """Execute one SQL batch and commit."""
    if params:
        conn.execute(text(sql), params)
    else:
        conn.execute(text(sql))
    conn.commit()

def _ensure_table(engine):
    """
    Auto-create / auto-migrate ARS_STORE_SLOC_SETTINGS.
    Each ALTER/CREATE is a separate execute() so SQL Server never compiles
    a batch that references columns which don't exist yet.
    """
    with engine.connect() as c:

        # ── Step 1: rename old table (nested IF is required – SQL Server has no IF…AND) ──
        _run(c, f"""
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{OLD_TABLE}')
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{TABLE}')
                BEGIN
                    EXEC sp_rename '{OLD_TABLE}', '{TABLE}'
                END
            END
        """)

        # ── Step 2: create fresh if still missing ────────────────────────────
        _run(c, f"""
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{TABLE}')
            BEGIN
                CREATE TABLE {TABLE} (
                    id         INT IDENTITY(1,1) PRIMARY KEY,
                    sloc       NVARCHAR(50)  NOT NULL,
                    kpi        NVARCHAR(200) NULL,
                    status     NVARCHAR(20)  NOT NULL DEFAULT 'Active',
                    created_at DATETIME      NOT NULL DEFAULT GETDATE(),
                    updated_at DATETIME      NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT UQ_{TABLE}_sloc UNIQUE (sloc)
                )
            END
        """)

        # ── Step 3: add status column if missing (NULL first, set default after) ──
        _run(c, f"""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME='{TABLE}' AND COLUMN_NAME='status'
            )
            BEGIN
                ALTER TABLE {TABLE} ADD status NVARCHAR(20) NULL
            END
        """)

        # ── Step 4: copy is_active→status via dynamic SQL (avoids parse-time error) ──
        _run(c, f"""
            IF EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME='{TABLE}' AND COLUMN_NAME='is_active'
            )
            BEGIN
                EXEC('UPDATE {TABLE} SET status = CASE WHEN is_active=1 THEN ''Active'' ELSE ''Inactive'' END')
            END
        """)

        # ── Step 5: fill any remaining NULLs in status ───────────────────────
        _run(c, f"UPDATE {TABLE} SET status='Active' WHERE status IS NULL")

        # ── Step 6: drop DEFAULT constraint on is_active FIRST ───────────────
        # SQL Server blocks DROP COLUMN when a DEFAULT constraint exists.
        # Find the auto-generated constraint name and drop it via dynamic SQL.
        _run(c, f"""
            IF EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME='{TABLE}' AND COLUMN_NAME='is_active'
            )
            BEGIN
                DECLARE @con NVARCHAR(256)
                SELECT @con = dc.name
                FROM sys.default_constraints dc
                JOIN sys.columns col
                  ON dc.parent_object_id = col.object_id
                 AND dc.parent_column_id = col.column_id
                JOIN sys.tables t ON col.object_id = t.object_id
                WHERE t.name = '{TABLE}' AND col.name = 'is_active'
                IF @con IS NOT NULL
                    EXEC('ALTER TABLE {TABLE} DROP CONSTRAINT [' + @con + ']')
            END
        """)

        # ── Step 7: now drop the column safely ───────────────────────────────
        _run(c, f"""
            IF EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME='{TABLE}' AND COLUMN_NAME='is_active'
            )
            BEGIN
                ALTER TABLE {TABLE} DROP COLUMN is_active
            END
        """)


def _fetch_distinct_slocs(data_engine) -> List[str]:
    try:
        with data_engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT sloc FROM ET_STORE_STOCK ORDER BY sloc ASC"
            )).fetchall()
        return [str(r[0]) for r in rows if r[0] is not None]
    except Exception as e:
        logger.error(f"ET_STORE_STOCK query failed: {e}")
        raise


def _fetch_saved(engine) -> dict:
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT id, sloc, kpi, status, created_at, updated_at FROM {TABLE}"
        )).fetchall()
    return {
        str(r[1]): {
            "id": r[0], "sloc": str(r[1]), "kpi": r[2],
            "status": r[3] if r[3] in VALID_STATUS else "Active",
            "created_at": r[4], "updated_at": r[5],
        }
        for r in rows
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/sloc-settings", response_model=APIResponse)
def get_sloc_settings(current_user: User = Depends(get_current_user)):
    de = get_data_engine()   # ARS_STORE_SLOC_SETTINGS lives in Rep_data

    try:
        _ensure_table(de)
    except Exception as e:
        logger.error(f"_ensure_table failed: {e}")
        raise HTTPException(500, detail=f"DB schema setup failed: {e}")

    try:
        slocs = _fetch_distinct_slocs(de)
    except Exception as e:
        raise HTTPException(500, detail=f"Failed to read ET_STORE_STOCK: {e}")

    try:
        saved = _fetch_saved(de)
    except Exception as e:
        raise HTTPException(500, detail=f"Failed to read {TABLE}: {e}")

    result = []
    for sloc in slocs:
        if sloc in saved:
            result.append({**saved[sloc], "is_new": False})
        else:
            result.append({"id": None, "sloc": sloc, "kpi": None, "status": "Active",
                           "created_at": None, "updated_at": None, "is_new": True})

    new_count = sum(1 for r in result if r["is_new"])
    return APIResponse(success=True,
        message=f"Loaded {len(result)} SLOC entries ({new_count} new)",
        data={"items": result, "total": len(result)})


@router.post("/sync", response_model=APIResponse)
def sync_slocs(current_user: User = Depends(get_current_user)):
    de = get_data_engine()   # ARS_STORE_SLOC_SETTINGS lives in Rep_data
    try:
        _ensure_table(de)
    except Exception as e:
        raise HTTPException(500, detail=f"DB schema setup failed: {e}")

    try:
        slocs = _fetch_distinct_slocs(de)
    except Exception as e:
        raise HTTPException(500, detail=f"Failed to read ET_STORE_STOCK: {e}")

    saved     = _fetch_saved(de)
    new_slocs = [s for s in slocs if s not in saved]

    if new_slocs:
        sql = text(f"""
            INSERT INTO {TABLE} (sloc, kpi, status, created_at, updated_at)
            VALUES (:sloc, NULL, 'Active', GETDATE(), GETDATE())
        """)
        with de.connect() as conn:
            for s in new_slocs:
                conn.execute(sql, {"sloc": s})
            conn.commit()

    return APIResponse(success=True,
        message=f"Sync complete. {len(new_slocs)} new SLOC(s) added.",
        data={"new_count": len(new_slocs), "new_slocs": new_slocs})


@router.put("/sloc-settings/{sloc}", response_model=APIResponse)
def update_sloc_setting(sloc: str, payload: SlocSetting,
                        current_user: User = Depends(get_current_user)):
    de = get_data_engine()   # ARS_STORE_SLOC_SETTINGS lives in Rep_data
    try: _ensure_table(de)
    except Exception as e: raise HTTPException(500, detail=str(e))

    with de.connect() as conn:
        conn.execute(text(f"""
            IF EXISTS (SELECT 1 FROM {TABLE} WHERE sloc=:sloc)
                UPDATE {TABLE} SET kpi=:kpi, status=:status, updated_at=GETDATE() WHERE sloc=:sloc
            ELSE
                INSERT INTO {TABLE}(sloc,kpi,status,created_at,updated_at)
                VALUES(:sloc,:kpi,:status,GETDATE(),GETDATE())
        """), {"sloc": sloc, "kpi": payload.kpi, "status": payload.status})
        conn.commit()
    return APIResponse(success=True, message=f"SLOC '{sloc}' updated.",
                       data={"sloc": sloc, "kpi": payload.kpi, "status": payload.status})


@router.put("/sloc-settings", response_model=APIResponse)
def bulk_update(payload: BulkUpdateRequest, current_user: User = Depends(get_current_user)):
    de = get_data_engine()   # ARS_STORE_SLOC_SETTINGS lives in Rep_data
    try: _ensure_table(de)
    except Exception as e: raise HTTPException(500, detail=str(e))

    with de.connect() as conn:
        for item in payload.items:
            conn.execute(text(f"""
                IF EXISTS (SELECT 1 FROM {TABLE} WHERE sloc=:sloc)
                    UPDATE {TABLE} SET kpi=:kpi, status=:status, updated_at=GETDATE() WHERE sloc=:sloc
                ELSE
                    INSERT INTO {TABLE}(sloc,kpi,status,created_at,updated_at)
                    VALUES(:sloc,:kpi,:status,GETDATE(),GETDATE())
            """), {"sloc": item.sloc, "kpi": item.kpi, "status": item.status})
        conn.commit()
    return APIResponse(success=True, message=f"{len(payload.items)} SLOC(s) updated.",
                       data={"updated_count": len(payload.items)})
