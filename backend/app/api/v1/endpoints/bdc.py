"""
BDC Creation API Endpoints
- Upload allocation quantity data (CSV/Excel)
- Process: join with VW_MASTER_PRODUCT, filter out hold/division/majcat exclusions
- Return BDC-format output ready for download
- Status upload: update ARS_ALLOCATION_MASTER with DELIVERY_ORDER column
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
    1. Aggregate PEND/NEW status rows: sum qty by VAR-ART + ST-CD + RDC, track pending qty
    2. Join uploaded data with VW_MASTER_PRODUCT on VAR-ART = ARTICLE_NUMBER
    3. Remove rows matching ARS_HOLD_ARTICLE_BDC (GEN_ART_NUMBER + CLR)
    4. Remove rows where store is in ARS_DIVISION_DELETE_BDC and DIV = 'KIDS'
    5. Remove rows where store + MAJ_CAT matches ARS_DIVISION_DELETE_ON_MAJ_CAT_BDC
    6. Build final BDC output format
    7. Build WITHOUT_PENDING data (total qty - pending qty)
    """
    stats = {
        "input_rows": len(df),
        "input_qty": 0,
        "after_master_join": 0,
        "after_master_join_qty": 0,
        "hold_article_removed": 0,
        "hold_article_removed_qty": 0,
        "division_delete_removed": 0,
        "division_delete_removed_qty": 0,
        "majcat_delete_removed": 0,
        "majcat_delete_removed_qty": 0,
        "final_rows": 0,
        "final_qty": 0,
    }

    # Clean input - drop fully empty rows
    df = df.dropna(subset=["VAR-ART"]).copy()
    df["VAR-ART"] = df["VAR-ART"].astype("int64")

    # Validate STATUS column (PEND / NEW)
    if "STATUS" in df.columns:
        df["STATUS"] = df["STATUS"].astype(str).str.strip().str.upper()
        invalid = df[~df["STATUS"].isin(["PEND", "NEW"])]
        if len(invalid) > 0:
            raise ValueError(f"STATUS must be PEND or NEW. Found: {invalid['STATUS'].unique().tolist()}")
    else:
        df["STATUS"] = "NEW"

    # Aggregate PEND and NEW rows: group by VAR-ART + ST-CD + RDC
    # Sum total qty and track pending qty separately
    group_cols = ["ALLOC-DATE", "RDC", "VAR-ART", "ST-CD", "PICKING_DATE"]

    # Calculate pending qty per group
    pend_agg = df[df["STATUS"] == "PEND"].groupby(group_cols, as_index=False)["ALLOC-QTY"].sum().rename(columns={"ALLOC-QTY": "PEND-QTY"})

    # Aggregate total qty (PEND + NEW combined) per group
    total_agg = df.groupby(group_cols, as_index=False)["ALLOC-QTY"].sum()

    # Left join total with pending to get PEND-QTY per group
    merged = total_agg.merge(pend_agg, on=group_cols, how="left")
    merged["PEND-QTY"] = merged["PEND-QTY"].fillna(0).astype(int)
    merged["VAR-ART"] = merged["VAR-ART"].astype("int64")

    df = merged.copy()

    stats["input_rows"] = len(df)
    stats["input_qty"] = int(df["ALLOC-QTY"].sum())

    logger.info(f"BDC after aggregation: {len(df)} rows, {int(df['ALLOC-QTY'].sum())} qty, PEND total: {int(df['PEND-QTY'].sum())}")

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
    master_df["ARTICLE_NUMBER"] = master_df["ARTICLE_NUMBER"].astype("int64")

    logger.info(f"BDC master lookup: {len(article_numbers)} unique articles, {len(master_df)} master matches")

    # Merge: input + master product
    combined = df.merge(
        master_df,
        left_on="VAR-ART",
        right_on="ARTICLE_NUMBER",
        how="inner",
    )
    stats["after_master_join"] = len(combined)
    stats["after_master_join_qty"] = int(combined["ALLOC-QTY"].sum())

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
        before_qty = int(combined["ALLOC-QTY"].sum())
        combined = combined.merge(
            hold_df,
            left_on=["_GEN_ART_STR", "_CLR_STR"],
            right_on=["GEN_ART_CLR", "CLR_HOLD"],
            how="left",
            indicator=True,
        )
        combined = combined[combined["_merge"] == "left_only"].drop(columns=["GEN_ART_CLR", "CLR_HOLD", "_merge"])
        stats["hold_article_removed"] = before - len(combined)
        stats["hold_article_removed_qty"] = before_qty - int(combined["ALLOC-QTY"].sum())

    # Step 3: Remove KIDS division for stores in ARS_DIVISION_DELETE_BDC
    with engine.connect() as conn:
        result = conn.execute(text("SELECT STORE FROM ARS_DIVISION_DELETE_BDC WITH (NOLOCK)"))
        div_delete_rows = result.fetchall()

    if div_delete_rows:
        div_delete_stores = set(r[0].strip() for r in div_delete_rows)
        before = len(combined)
        before_qty = int(combined["ALLOC-QTY"].sum())
        mask = (combined["ST-CD"].str.strip().isin(div_delete_stores)) & (combined["DIV"].str.strip().str.upper() == "KIDS")
        combined = combined[~mask]
        stats["division_delete_removed"] = before - len(combined)
        stats["division_delete_removed_qty"] = before_qty - int(combined["ALLOC-QTY"].sum())

    # Step 4: Remove store + MAJ_CAT matches from ARS_DIVISION_DELETE_ON_MAJ_CAT_BDC
    with engine.connect() as conn:
        result = conn.execute(text("SELECT STORE, MAJCAT FROM ARS_DIVISION_DELETE_ON_MAJ_CAT_BDC WITH (NOLOCK)"))
        majcat_rows = result.fetchall()

    if majcat_rows:
        majcat_df = pd.DataFrame(majcat_rows, columns=["STORE", "MAJCAT"])
        majcat_df["STORE"] = majcat_df["STORE"].astype(str).str.strip()
        majcat_df["MAJCAT"] = majcat_df["MAJCAT"].astype(str).str.strip()

        before = len(combined)
        before_qty = int(combined["ALLOC-QTY"].sum())
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
        stats["majcat_delete_removed_qty"] = before_qty - int(combined["ALLOC-QTY"].sum())

    # Step 5: Build BDC output format (total qty = PEND + NEW)
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
    stats["final_qty"] = int(output["BDC-QTY"].sum())

    # Step 6: Build WITHOUT_PENDING data (total qty - pending qty)
    wp = combined.copy()
    wp["PEND-QTY"] = wp["PEND-QTY"].fillna(0).astype(int)
    wp["BDC-QTY-WP"] = (wp["ALLOC-QTY"].astype(int) - wp["PEND-QTY"]).clip(lower=0)

    wp_output = wp[["Serial No", "Allocation Date", "Allocation Number", "VENDOR", "MATERIAL NO", "RECEIVING STORE", "Picking Date", "Remark"]].copy()
    wp_output["BDC-QTY"] = wp["BDC-QTY-WP"]
    wp_output = wp_output[["Serial No", "Allocation Date", "Allocation Number", "VENDOR", "MATERIAL NO", "BDC-QTY", "RECEIVING STORE", "Picking Date", "Remark"]]
    # Remove rows where qty became 0 after subtracting pending
    wp_output = wp_output[wp_output["BDC-QTY"] > 0].copy()
    wp_output = wp_output.reset_index(drop=True)
    wp_output["Serial No"] = range(1, len(wp_output) + 1)

    preview = output.head(100).to_dict(orient="records")
    columns = list(output.columns)

    return {
        "success": True,
        "stats": stats,
        "total_rows": len(output),
        "columns": columns,
        "preview": preview,
        "_full_data": output,
        "_full_data_without_pending": wp_output,
    }


def _get_next_allocation_no(engine) -> int:
    """Get the next allocation number by checking ARS_ALLOCATION_MASTER."""
    table_name = "ARS_ALLOCATION_MASTER"
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tbl"
        ), {"tbl": table_name})
        if result.scalar() == 0:
            return 1

        result = conn.execute(text(f"""
            SELECT MAX(CAST([Allocation Number] AS INT))
            FROM dbo.{table_name}
            WHERE ISNUMERIC([Allocation Number]) = 1
        """))
        max_no = result.scalar()
        return (max_no or 0) + 1


def _save_to_db(output_df: pd.DataFrame, engine):
    """Save BDC output to ARS_ALLOCATION_MASTER table."""
    table_name = "ARS_ALLOCATION_MASTER"

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tbl"
        ), {"tbl": table_name})
        table_exists = result.scalar() > 0

        if not table_exists:
            conn.execute(text(f"""
                CREATE TABLE dbo.{table_name} (
                    [Serial No]          INT,
                    [Allocation Date]    VARCHAR(20),
                    [Allocation Number]  VARCHAR(50),
                    [VENDOR]             VARCHAR(50),
                    [MATERIAL NO]        VARCHAR(50),
                    [BDC-QTY]            INT,
                    [RECEIVING STORE]    VARCHAR(20),
                    [Picking Date]       VARCHAR(20),
                    [Remark]             VARCHAR(200),
                    [CREATED_AT]         DATETIME2 DEFAULT GETDATE()
                )
            """))
            conn.commit()

    save_df = output_df.copy()
    save_df.to_sql(table_name, engine, if_exists="append", index=False, schema="dbo")
    return True


def _save_to_db_without_pending(output_df: pd.DataFrame, engine):
    """Save BDC output (total qty minus pending qty) to ARS_ALLOCATION_MASTER_WITHOUT_PENDING."""
    table_name = "ARS_ALLOCATION_MASTER_WITHOUT_PENDING"

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tbl"
        ), {"tbl": table_name})
        table_exists = result.scalar() > 0

        if not table_exists:
            conn.execute(text(f"""
                CREATE TABLE dbo.{table_name} (
                    [Serial No]          INT,
                    [Allocation Date]    VARCHAR(20),
                    [Allocation Number]  VARCHAR(50),
                    [VENDOR]             VARCHAR(50),
                    [MATERIAL NO]        VARCHAR(50),
                    [BDC-QTY]            INT,
                    [RECEIVING STORE]    VARCHAR(20),
                    [Picking Date]       VARCHAR(20),
                    [Remark]             VARCHAR(200),
                    [CREATED_AT]         DATETIME2 DEFAULT GETDATE()
                )
            """))
            conn.commit()

    save_df = output_df.copy()
    save_df.to_sql(table_name, engine, if_exists="append", index=False, schema="dbo")
    return True


def _ensure_delivery_order_column(conn, table_name: str):
    """Add DELIVERY_ORDER column to table if it doesn't exist."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :tbl AND COLUMN_NAME = 'DELIVERY_ORDER'
    """), {"tbl": table_name})
    if result.scalar() == 0:
        conn.execute(text(f"ALTER TABLE dbo.{table_name} ADD [DELIVERY_ORDER] VARCHAR(20) NULL"))
        conn.commit()
        logger.info(f"Added DELIVERY_ORDER column to {table_name}")


# ===================== BDC Upload Endpoints =====================

@router.post("/upload")
async def upload_and_process_bdc(
    file: UploadFile = File(..., description="CSV or Excel file with allocation quantity data"),
    sheet_name: Optional[str] = Form(None, description="Excel sheet name (optional)"),
    auto_save: str = Form("false", description="Auto-save to database"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_data_db),
):
    """Upload allocation quantity data, process through BDC pipeline, and return results."""
    try:
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="File is empty")

        df = _read_file_to_df(content, file.filename, sheet_name)

        if df.empty:
            raise HTTPException(status_code=400, detail="File contains no data rows")

        required = {"ALLOC-DATE", "RDC", "VAR-ART", "ST-CD", "ALLOC-QTY", "PICKING_DATE", "STATUS"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required columns: {', '.join(missing)}")

        engine = get_data_engine()
        is_auto_save = auto_save.lower() == "true"

        allocation_no = str(_get_next_allocation_no(engine)) if is_auto_save else ""
        result = _process_bdc(df, engine, allocation_no=allocation_no)

        saved = False
        if is_auto_save and result["total_rows"] > 0:
            _save_to_db(result["_full_data"], engine)
            wp_data = result["_full_data_without_pending"]
            if len(wp_data) > 0:
                _save_to_db_without_pending(wp_data, engine)
                logger.info(f"BDC saved {len(wp_data)} rows to ARS_ALLOCATION_MASTER_WITHOUT_PENDING")
            saved = True

        result.pop("_full_data", None)
        result.pop("_full_data_without_pending", None)
        result["saved"] = saved
        result["allocation_no"] = allocation_no

        return result

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"BDC processing error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process BDC: {str(e)}")


@router.post("/save")
async def save_bdc_to_db(
    file: UploadFile = File(..., description="CSV or Excel file with allocation quantity data"),
    sheet_name: Optional[str] = Form(None, description="Excel sheet name (optional)"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_data_db),
):
    """Re-process and save BDC results to ARS_ALLOCATION_MASTER table."""
    try:
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="File is empty")

        df = _read_file_to_df(content, file.filename, sheet_name)

        if df.empty:
            raise HTTPException(status_code=400, detail="File contains no data rows")

        required = {"ALLOC-DATE", "RDC", "VAR-ART", "ST-CD", "ALLOC-QTY", "PICKING_DATE", "STATUS"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required columns: {', '.join(missing)}")

        engine = get_data_engine()
        allocation_no = str(_get_next_allocation_no(engine))
        result = _process_bdc(df, engine, allocation_no=allocation_no)

        if result["total_rows"] == 0:
            raise HTTPException(status_code=400, detail="No rows to save after processing")

        _save_to_db(result["_full_data"], engine)
        wp_data = result["_full_data_without_pending"]
        if len(wp_data) > 0:
            _save_to_db_without_pending(wp_data, engine)

        return {"success": True, "saved_rows": result["total_rows"], "allocation_no": allocation_no}

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"BDC save error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save BDC: {str(e)}")


@router.post("/download")
async def download_bdc(
    file: UploadFile = File(..., description="CSV or Excel file with allocation quantity data"),
    sheet_name: Optional[str] = Form(None, description="Excel sheet name (optional)"),
    allocation_no: str = Form(..., description="Allocation number to use for download"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_data_db),
):
    """Process BDC and return as downloadable CSV file."""
    try:
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="File is empty")

        df = _read_file_to_df(content, file.filename, sheet_name)

        if df.empty:
            raise HTTPException(status_code=400, detail="File contains no data rows")

        required = {"ALLOC-DATE", "RDC", "VAR-ART", "ST-CD", "ALLOC-QTY", "PICKING_DATE", "STATUS"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required columns: {', '.join(missing)}")

        engine = get_data_engine()
        result = _process_bdc(df, engine, allocation_no=allocation_no.strip())
        output_df = result["_full_data"]

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


# ===================== Delivery Order Upload =====================

@router.post("/delivery-order-upload")
async def upload_delivery_order(
    file: UploadFile = File(..., description="CSV or Excel file with DELIVERY_ORDER status"),
    sheet_name: Optional[str] = Form(None, description="Excel sheet name (optional)"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_data_db),
):
    """
    Upload file to update DELIVERY_ORDER column in ARS_ALLOCATION_MASTER.
    File must have: VENDOR, RECEIVING STORE, MATERIAL NO, Allocation Number, DELIVERY_ORDER
    Matches rows and sets DELIVERY_ORDER. Column is auto-created if missing.
    """
    try:
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="File is empty")

        df = _read_file_to_df(content, file.filename, sheet_name)

        if df.empty:
            raise HTTPException(status_code=400, detail="File contains no data rows")

        required = {"VENDOR", "RECEIVING STORE", "MATERIAL NO", "Allocation Number", "DELIVERY_ORDER"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required columns: {', '.join(missing)}")

        df["DELIVERY_ORDER"] = df["DELIVERY_ORDER"].astype(str).str.strip()
        df["VENDOR"] = df["VENDOR"].astype(str).str.strip()
        df["RECEIVING STORE"] = df["RECEIVING STORE"].astype(str).str.strip()
        df["MATERIAL NO"] = df["MATERIAL NO"].astype(str).str.strip()
        df["Allocation Number"] = df["Allocation Number"].astype(str).str.strip()

        engine = get_data_engine()
        table_name = "ARS_ALLOCATION_MASTER"

        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tbl"
            ), {"tbl": table_name})
            if result.scalar() == 0:
                raise HTTPException(status_code=404, detail="ARS_ALLOCATION_MASTER table does not exist. Upload BDC data first.")

            _ensure_delivery_order_column(conn, table_name)

            updated_count = 0
            not_found_count = 0

            for _, row in df.iterrows():
                res = conn.execute(
                    text(f"""
                        UPDATE dbo.{table_name}
                        SET [DELIVERY_ORDER] = :delivery_order
                        WHERE [VENDOR] = :vendor
                          AND [RECEIVING STORE] = :store
                          AND [MATERIAL NO] = :material
                          AND [Allocation Number] = :alloc_no
                    """),
                    {
                        "delivery_order": row["DELIVERY_ORDER"],
                        "vendor": row["VENDOR"],
                        "store": row["RECEIVING STORE"],
                        "material": row["MATERIAL NO"],
                        "alloc_no": row["Allocation Number"],
                    }
                )
                if res.rowcount > 0:
                    updated_count += res.rowcount
                else:
                    not_found_count += 1

            conn.commit()

        logger.info(f"DELIVERY_ORDER upload: {updated_count} updated, {not_found_count} not matched")

        return {
            "success": True,
            "total_file_rows": len(df),
            "updated_rows": updated_count,
            "not_found_rows": not_found_count,
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"DELIVERY_ORDER upload error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update DELIVERY_ORDER: {str(e)}")


# ===================== Sequences =====================

@router.get("/sequences")
async def get_bdc_sequences(
    current_user: User = Depends(get_current_user),
    db=Depends(get_data_db),
):
    """Get all saved allocation sequences from ARS_ALLOCATION_MASTER."""
    try:
        engine = get_data_engine()
        table_name = "ARS_ALLOCATION_MASTER"

        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tbl"
            ), {"tbl": table_name})
            if result.scalar() == 0:
                return {"sequences": []}

            result = conn.execute(text(f"""
                SELECT
                    [Allocation Number],
                    MIN([Allocation Date]) AS alloc_date,
                    MIN([VENDOR]) AS vendor,
                    COUNT(*) AS total_rows,
                    SUM([BDC-QTY]) AS total_qty,
                    MIN([CREATED_AT]) AS created_at
                FROM dbo.{table_name}
                GROUP BY [Allocation Number]
                ORDER BY
                    CASE WHEN ISNUMERIC([Allocation Number]) = 1
                         THEN CAST([Allocation Number] AS INT)
                         ELSE 0 END DESC
            """))
            rows = result.fetchall()

            sequences = []
            for r in rows:
                sequences.append({
                    "allocation_no": r[0],
                    "alloc_date": str(r[1]) if r[1] else "",
                    "vendor": r[2] or "",
                    "total_rows": r[3],
                    "total_qty": r[4],
                    "created_at": str(r[5]) if r[5] else "",
                })

            return {"sequences": sequences}

    except Exception as e:
        logger.error(f"BDC sequences error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get sequences: {str(e)}")


@router.delete("/sequences/{allocation_no}")
async def delete_bdc_sequence(
    allocation_no: str,
    current_user: User = Depends(get_current_user),
    db=Depends(get_data_db),
):
    """Delete all rows for a given allocation number from both ARS_ALLOCATION_MASTER and ARS_ALLOCATION_MASTER_WITHOUT_PENDING."""
    try:
        engine = get_data_engine()
        table_name = "ARS_ALLOCATION_MASTER"
        wp_table = "ARS_ALLOCATION_MASTER_WITHOUT_PENDING"

        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tbl"
            ), {"tbl": table_name})
            if result.scalar() == 0:
                raise HTTPException(status_code=404, detail="Table does not exist")

            result = conn.execute(
                text(f"DELETE FROM dbo.{table_name} WHERE [Allocation Number] = :alloc_no"),
                {"alloc_no": allocation_no},
            )
            deleted = result.rowcount

            # Also delete from ARS_ALLOCATION_MASTER_WITHOUT_PENDING if it exists
            wp_exists = conn.execute(text(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tbl"
            ), {"tbl": wp_table}).scalar()
            wp_deleted = 0
            if wp_exists > 0:
                wp_result = conn.execute(
                    text(f"DELETE FROM dbo.{wp_table} WHERE [Allocation Number] = :alloc_no"),
                    {"alloc_no": allocation_no},
                )
                wp_deleted = wp_result.rowcount

            conn.commit()

        logger.info(f"Deleted allocation #{allocation_no}: {deleted} from MASTER, {wp_deleted} from WITHOUT_PENDING")

        return {"success": True, "deleted_rows": deleted, "deleted_rows_wp": wp_deleted, "allocation_no": allocation_no}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"BDC delete error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete sequence: {str(e)}")


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
