"""
Lookup Art Master API
=====================
Upload an Excel file, select a join key column, pick columns from VW_MASTER_PRODUCT,
and get the LEFT JOIN result back.

Performance: only fetches rows from VW_MASTER_PRODUCT that match the uploaded keys
using SQL WHERE … IN (…) — avoids loading the entire view.
"""

import io
import json
from typing import Optional, List

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from loguru import logger

from app.database.session import get_data_engine
from app.schemas.common import APIResponse
from app.security.dependencies import get_current_user
from app.models.rbac import User

router = APIRouter(prefix="/lookup-art-master", tags=["Lookup Art Master"])


# ── Helpers ──────────────────────────────────────────────────────────────────

def _read_upload(content: bytes, filename: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    lower = filename.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(io.BytesIO(content))
    elif lower.endswith((".xlsx", ".xls")):
        kw = {"sheet_name": sheet_name} if sheet_name else {}
        return pd.read_excel(io.BytesIO(content), **kw)
    raise ValueError("Unsupported file type. Use .csv, .xlsx, or .xls")


def _get_vw_columns(engine) -> List[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'VW_MASTER_PRODUCT'
            ORDER BY ORDINAL_POSITION
        """)).fetchall()
    return [r[0] for r in rows]


def _do_lookup(df_upload: pd.DataFrame, join_column: str,
               master_column: str, sel_cols: List[str], engine) -> pd.DataFrame:
    """
    Fast lookup: fetch only matching rows from VW_MASTER_PRODUCT via SQL WHERE IN,
    then LEFT JOIN in pandas.
    """
    # Unique non-null keys from the uploaded file
    keys = df_upload[join_column].dropna().astype(str).unique().tolist()
    if not keys:
        # No keys → return upload with empty master columns
        for c in sel_cols:
            if c != join_column:
                df_upload[c] = None
        return df_upload

    fetch_cols = list(dict.fromkeys([master_column] + sel_cols))
    cols_sql = ", ".join(f"[{c}]" for c in fetch_cols)

    # Build batched WHERE IN to avoid SQL parameter limits (max ~2000)
    BATCH = 2000
    frames = []
    for i in range(0, len(keys), BATCH):
        batch = keys[i:i + BATCH]
        placeholders = ", ".join(f":k{j}" for j in range(len(batch)))
        params = {f"k{j}": v for j, v in enumerate(batch)}
        sql = f"SELECT DISTINCT {cols_sql} FROM dbo.VW_MASTER_PRODUCT WITH (NOLOCK) WHERE [{master_column}] IN ({placeholders})"
        df_batch = pd.read_sql(text(sql), engine, params=params)
        frames.append(df_batch)

    df_master = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=fetch_cols)

    # Ensure matching dtypes for merge
    df_upload[join_column] = df_upload[join_column].astype(str)
    df_master[master_column] = df_master[master_column].astype(str)

    df_result = df_upload.merge(
        df_master, left_on=join_column, right_on=master_column,
        how="left", suffixes=("", "_master"),
    )

    # Drop duplicate join key column from master side
    if master_column != join_column and master_column in df_result.columns:
        df_result.drop(columns=[master_column], inplace=True)

    return df_result


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/columns", response_model=APIResponse)
def get_master_columns(current_user: User = Depends(get_current_user)):
    """Return available columns from VW_MASTER_PRODUCT."""
    engine = get_data_engine()
    cols = _get_vw_columns(engine)
    return APIResponse(success=True,
        message=f"{len(cols)} columns available",
        data={"columns": cols})


@router.post("/preview", response_model=APIResponse)
async def preview_upload(
    file: UploadFile = File(...),
    sheet_name: Optional[str] = Form(None),
    current_user: User = Depends(get_current_user),
):
    """Preview uploaded file: return column names and row count."""
    content = await file.read()
    try:
        df = _read_upload(content, file.filename, sheet_name)
    except Exception as e:
        raise HTTPException(400, detail=f"Failed to read file: {e}")

    return APIResponse(success=True,
        message=f"{len(df)} rows, {len(df.columns)} columns",
        data={
            "columns": list(df.columns),
            "row_count": len(df),
            "sample": json.loads(df.head(5).to_json(orient="records", date_format="iso")),
        })


@router.post("/run", response_model=APIResponse)
async def run_lookup(
    file: UploadFile = File(...),
    join_column: str = Form(...),
    master_column: str = Form(...),
    select_columns: str = Form(...),
    sheet_name: Optional[str] = Form(None),
    current_user: User = Depends(get_current_user),
):
    """LEFT JOIN uploaded file with VW_MASTER_PRODUCT (filtered by uploaded keys)."""
    content = await file.read()
    try:
        df_upload = _read_upload(content, file.filename, sheet_name)
    except Exception as e:
        raise HTTPException(400, detail=f"Failed to read file: {e}")

    if join_column not in df_upload.columns:
        raise HTTPException(400, detail=f"Column '{join_column}' not found in uploaded file")

    try:
        sel_cols = json.loads(select_columns)
    except Exception:
        raise HTTPException(400, detail="select_columns must be a valid JSON array")

    if not sel_cols:
        raise HTTPException(400, detail="Select at least one column from VW_MASTER_PRODUCT")

    engine = get_data_engine()
    vw_cols = _get_vw_columns(engine)
    if master_column not in vw_cols:
        raise HTTPException(400, detail=f"Master column '{master_column}' not in VW_MASTER_PRODUCT")

    try:
        df_result = _do_lookup(df_upload, join_column, master_column, sel_cols, engine)
    except Exception as e:
        logger.error(f"Lookup failed: {e}")
        raise HTTPException(500, detail=f"Lookup failed: {e}")

    total   = len(df_result)
    matched = int(df_result[sel_cols[0]].notna().sum()) if sel_cols else 0

    preview = json.loads(
        df_result.head(500).to_json(orient="records", date_format="iso")
    )

    return APIResponse(success=True,
        message=f"Lookup complete: {matched}/{total} rows matched",
        data={
            "columns": list(df_result.columns),
            "total_rows": total,
            "matched_rows": matched,
            "preview": preview,
        })


@router.post("/download")
async def download_lookup(
    file: UploadFile = File(...),
    join_column: str = Form(...),
    master_column: str = Form(...),
    select_columns: str = Form(...),
    sheet_name: Optional[str] = Form(None),
    current_user: User = Depends(get_current_user),
):
    """Same as /run but returns the full result as an Excel download."""
    content = await file.read()
    try:
        df_upload = _read_upload(content, file.filename, sheet_name)
    except Exception as e:
        raise HTTPException(400, detail=f"Failed to read file: {e}")

    if join_column not in df_upload.columns:
        raise HTTPException(400, detail=f"Column '{join_column}' not found in uploaded file")

    sel_cols = json.loads(select_columns)
    engine = get_data_engine()

    df_result = _do_lookup(df_upload, join_column, master_column, sel_cols, engine)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_result.to_excel(writer, index=False, sheet_name="Lookup_Result")
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=lookup_result.xlsx"},
    )
