"""
BDC Creation API Endpoints
- Upload allocation quantity data (CSV/Excel)
- Process: join with VW_MASTER_PRODUCT, filter out hold/division/majcat exclusions
- Return BDC-format output ready for download
"""
import io
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from loguru import logger

from app.database.session import get_data_db, get_data_engine
from app.security.dependencies import get_current_user
from app.models.rbac import User

router = APIRouter(prefix="/bdc", tags=["BDC Creation"])


def _read_file_to_df(content: bytes, filename: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Read CSV or Excel file bytes into a DataFrame."""
    lower = filename.lower()
    if lower.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(content))
    elif lower.endswith((".xlsx", ".xls")):
        kwargs = {}
        if sheet_name:
            kwargs["sheet_name"] = sheet_name
        df = pd.read_excel(io.BytesIO(content), **kwargs)
    else:
        raise ValueError("Unsupported file format. Please upload CSV or Excel (.xlsx/.xls) files.")
    return df


def _process_bdc(df: pd.DataFrame, engine, allocation_no: str = "") -> dict:
    """
    BDC Processing Pipeline:
    1. Join uploaded data with VW_MASTER_PRODUCT on VAR-ART = ARTICLE_NUMBER
    2. Remove rows matching ARS_HOLD_ARTICLE_BDC (GEN_ART_NUMBER + CLR)
    3. Remove rows where store is in ARS_DIVISION_DELETE_BDC and DIV = 'KIDS'
    4. Remove rows where store + MAJ_CAT matches ARS_DIVISION_DELETE_ON_MAJ_CAT_BDC
    5. Build final BDC output format
    """
    stats = {
        "input_rows": len(df),
        "after_master_join": 0,
        "hold_article_removed": 0,
        "division_delete_removed": 0,
        "majcat_delete_removed": 0,
        "final_rows": 0,
    }

    # Clean input - drop fully empty rows
    df = df.dropna(subset=["VAR-ART"]).copy()
    df["VAR-ART"] = df["VAR-ART"].astype("int64")
    stats["input_rows"] = len(df)

    # Step 1: Join with VW_MASTER_PRODUCT to get ARTICLE_NUMBER, GEN_ART_NUMBER, DIV, MAJ_CAT, CLR
    article_numbers = df["VAR-ART"].unique().tolist()

    # Query in chunks to avoid SQL parameter limits
    chunk_size = 500
    master_parts = []
    with engine.connect() as conn:
        for i in range(0, len(article_numbers), chunk_size):
            chunk = article_numbers[i:i + chunk_size]
            placeholders = ",".join(str(int(a)) for a in chunk)
            query = text(f"""
                SELECT DISTINCT ARTICLE_NUMBER, GEN_ART_NUMBER, DIV, MAJ_CAT, CLR, MATNR
                FROM VW_MASTER_PRODUCT WITH (NOLOCK)
                WHERE ARTICLE_NUMBER IN ({placeholders})
            """)
            result = conn.execute(query)
            rows = result.fetchall()
            if rows:
                master_parts.append(pd.DataFrame(rows, columns=["ARTICLE_NUMBER", "GEN_ART_NUMBER", "DIV", "MAJ_CAT", "CLR", "MATNR"]))

    if not master_parts:
        raise ValueError("No matching articles found in VW_MASTER_PRODUCT for the uploaded data.")

    master_df = pd.concat(master_parts, ignore_index=True)

    # Merge: input + master product
    combined = df.merge(
        master_df,
        left_on="VAR-ART",
        right_on="ARTICLE_NUMBER",
        how="inner",
    )
    stats["after_master_join"] = len(combined)

    if combined.empty:
        raise ValueError("No matching articles found after joining with master product data.")

    # Step 2: Remove hold articles (ARS_HOLD_ARTICLE_BDC) by GEN_ART_NUMBER + CLR
    with engine.connect() as conn:
        result = conn.execute(text("SELECT GEN_ART_CLR, CLR FROM ARS_HOLD_ARTICLE_BDC WITH (NOLOCK)"))
        hold_rows = result.fetchall()

    if hold_rows:
        hold_df = pd.DataFrame(hold_rows, columns=["GEN_ART_CLR", "CLR_HOLD"])
        hold_df["GEN_ART_CLR"] = hold_df["GEN_ART_CLR"].astype(str).str.strip()
        hold_df["CLR_HOLD"] = hold_df["CLR_HOLD"].astype(str).str.strip()

        combined["_GEN_ART_STR"] = combined["GEN_ART_NUMBER"].astype(str).str.strip()
        combined["_CLR_STR"] = combined["CLR"].astype(str).str.strip()

        before = len(combined)
        combined = combined.merge(
            hold_df,
            left_on=["_GEN_ART_STR", "_CLR_STR"],
            right_on=["GEN_ART_CLR", "CLR_HOLD"],
            how="left",
            indicator=True,
        )
        combined = combined[combined["_merge"] == "left_only"].drop(columns=["GEN_ART_CLR", "CLR_HOLD", "_merge"])
        stats["hold_article_removed"] = before - len(combined)

    # Step 3: Remove KIDS division for stores in ARS_DIVISION_DELETE_BDC
    with engine.connect() as conn:
        result = conn.execute(text("SELECT STORE FROM ARS_DIVISION_DELETE_BDC WITH (NOLOCK)"))
        div_delete_rows = result.fetchall()

    if div_delete_rows:
        div_delete_stores = set(r[0].strip() for r in div_delete_rows)
        before = len(combined)
        mask = (combined["ST-CD"].str.strip().isin(div_delete_stores)) & (combined["DIV"].str.strip().str.upper() == "KIDS")
        combined = combined[~mask]
        stats["division_delete_removed"] = before - len(combined)

    # Step 4: Remove store + MAJ_CAT matches from ARS_DIVISION_DELETE_ON_MAJ_CAT_BDC
    with engine.connect() as conn:
        result = conn.execute(text("SELECT STORE, MAJCAT FROM ARS_DIVISION_DELETE_ON_MAJ_CAT_BDC WITH (NOLOCK)"))
        majcat_rows = result.fetchall()

    if majcat_rows:
        majcat_df = pd.DataFrame(majcat_rows, columns=["STORE", "MAJCAT"])
        majcat_df["STORE"] = majcat_df["STORE"].astype(str).str.strip()
        majcat_df["MAJCAT"] = majcat_df["MAJCAT"].astype(str).str.strip()

        before = len(combined)
        combined["_ST_CD_STR"] = combined["ST-CD"].astype(str).str.strip()
        combined["_MAJ_CAT_STR"] = combined["MAJ_CAT"].astype(str).str.strip()

        combined = combined.merge(
            majcat_df,
            left_on=["_ST_CD_STR", "_MAJ_CAT_STR"],
            right_on=["STORE", "MAJCAT"],
            how="left",
            indicator=True,
        )
        combined = combined[combined["_merge"] == "left_only"].drop(columns=["STORE", "MAJCAT", "_merge"])
        stats["majcat_delete_removed"] = before - len(combined)

    # Step 5: Remove duplicate rows
    before = len(combined)
    combined = combined.drop_duplicates(subset=["ALLOC-DATE", "RDC", "VAR-ART", "ST-CD", "ALLOC-QTY", "PICKING_DATE"])
    stats["duplicates_removed"] = before - len(combined)

    # Step 6: Build BDC output format
    combined = combined.reset_index(drop=True)
    combined["Serial No"] = range(1, len(combined) + 1)
    combined["Allocation Date"] = pd.to_datetime(combined["ALLOC-DATE"]).dt.strftime("%Y-%m-%d")
    combined["Allocation Number"] = allocation_no
    combined["VENDOR"] = combined["RDC"].astype(str).str.strip()
    combined["MATERIAL NO"] = combined["MATNR"].astype(str).str.strip().str.lstrip("0")
    combined["BDC-QTY"] = combined["ALLOC-QTY"].astype(int)
    combined["RECEIVING STORE"] = combined["ST-CD"].astype(str).str.strip()
    combined["Picking Date"] = pd.to_datetime(combined["PICKING_DATE"]).dt.strftime("%Y-%m-%d")
    combined["Remark"] = ""

    output = combined[["Serial No", "Allocation Date", "Allocation Number", "VENDOR", "MATERIAL NO", "BDC-QTY", "RECEIVING STORE", "Picking Date", "Remark"]].copy()

    stats["final_rows"] = len(output)

    preview = output.head(100).to_dict(orient="records")
    columns = list(output.columns)

    return {
        "success": True,
        "stats": stats,
        "total_rows": len(output),
        "columns": columns,
        "preview": preview,
        # Store full data for download
        "_full_data": output,
    }


@router.post("/upload")
async def upload_and_process_bdc(
    file: UploadFile = File(..., description="CSV or Excel file with allocation quantity data"),
    sheet_name: Optional[str] = Form(None, description="Excel sheet name (optional)"),
    allocation_no: str = Form(..., description="Allocation number (e.g. N345)"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_data_db),
):
    """
    Upload allocation quantity data, process through BDC pipeline, and return results.

    Expected input columns: ALLOC-DATE, RDC, VAR-ART, ST-CD, ALLOC-QTY

    Processing:
    1. Join with VW_MASTER_PRODUCT on VAR-ART = ARTICLE_NUMBER
    2. Remove hold articles (ARS_HOLD_ARTICLE_BDC)
    3. Remove KIDS division for excluded stores (ARS_DIVISION_DELETE_BDC)
    4. Remove store+MAJ_CAT exclusions (ARS_DIVISION_DELETE_ON_MAJ_CAT_BDC)
    5. Output BDC format
    """
    try:
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="File is empty")

        df = _read_file_to_df(content, file.filename, sheet_name)

        if df.empty:
            raise HTTPException(status_code=400, detail="File contains no data rows")

        # Validate required columns
        required = {"ALLOC-DATE", "RDC", "VAR-ART", "ST-CD", "ALLOC-QTY", "PICKING_DATE"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required columns: {', '.join(missing)}")

        engine = get_data_engine()
        result = _process_bdc(df, engine, allocation_no=allocation_no.strip())

        # Remove internal full data from response
        result.pop("_full_data", None)

        return result

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"BDC processing error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process BDC: {str(e)}")


@router.post("/download")
async def download_bdc(
    file: UploadFile = File(..., description="CSV or Excel file with allocation quantity data"),
    sheet_name: Optional[str] = Form(None, description="Excel sheet name (optional)"),
    allocation_no: str = Form(..., description="Allocation number (e.g. N345)"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_data_db),
):
    """Process BDC and return as downloadable Excel file."""
    try:
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="File is empty")

        df = _read_file_to_df(content, file.filename, sheet_name)

        if df.empty:
            raise HTTPException(status_code=400, detail="File contains no data rows")

        required = {"ALLOC-DATE", "RDC", "VAR-ART", "ST-CD", "ALLOC-QTY", "PICKING_DATE"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required columns: {', '.join(missing)}")

        engine = get_data_engine()
        result = _process_bdc(df, engine, allocation_no=allocation_no.strip())
        output_df = result["_full_data"]

        # Write to CSV in memory
        buffer = io.StringIO()
        output_df.to_csv(buffer, index=False)
        buffer.seek(0)

        return StreamingResponse(
            io.BytesIO(buffer.getvalue().encode("utf-8")),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=BDC_Output.csv"},
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"BDC download error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate BDC file: {str(e)}")


@router.post("/sheets")
async def get_excel_sheets(
    file: UploadFile = File(..., description="Excel file to extract sheet names"),
    current_user: User = Depends(get_current_user),
):
    """Return list of sheet names from an Excel file."""
    try:
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="File is empty")

        lower = file.filename.lower()
        if not lower.endswith((".xlsx", ".xls")):
            return {"sheets": []}

        xls = pd.ExcelFile(io.BytesIO(content))
        return {"sheets": xls.sheet_names}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"BDC sheets error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read sheets: {str(e)}")
