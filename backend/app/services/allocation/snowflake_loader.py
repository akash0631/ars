"""
Snowflake Data Loader — DIRECT connection to Snowflake.
Reads pre-computed scores, store stock, budget cascades.
This is the ONLY source of truth for Engines 1+2 data.

Architecture:
  Snowflake (246M scored pairs, 4.97M store stock) → Azure ARS reads via snowflake-connector-python

Lazy import: snowflake.connector is only imported when a query runs,
not at module load — this prevents Azure cold-start crashes.
"""
import logging
import os
import time
from typing import List, Dict, Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Snowflake connection params
SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "iafphkw-hh80816")
SF_USER = os.getenv("SNOWFLAKE_USER", "akashv2kart")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "SVXqEe5pDdamMb9")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "ALLOC_WH")
SF_DATABASE = "V2_ALLOCATION"
SF_SCHEMA = "RESULTS"


def _get_connection():
    """Create a fresh Snowflake connection. Lazy import to avoid cold-start crash."""
    import snowflake.connector
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
        login_timeout=30,
        network_timeout=60,
    )


def test_connection() -> Dict[str, Any]:
    """Test Snowflake connectivity and return row counts."""
    conn = _get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM ARTICLE_SCORES")
        scores = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM V2RETAIL.GOLD.FACT_STOCK_GENCOLOR")
        stock = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT MAJCAT) FROM ARTICLE_SCORES")
        majcats = cur.fetchone()[0]
        return {
            "status": "connected",
            "article_scores": scores,
            "store_stock": stock,
            "majcats": majcats,
        }
    finally:
        cur.close()
        conn.close()


def get_available_majcats() -> List[str]:
    """Get list of all MAJCATs with scored data."""
    conn = _get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT MAJCAT FROM ARTICLE_SCORES ORDER BY MAJCAT")
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def get_scored_pairs(majcat: str, top_n: int = 200) -> pd.DataFrame:
    """
    Get top-N scored article×store pairs per store for a MAJCAT.
    Returns ~91K rows for M_JEANS (455 stores × 200) in ~6s.

    Columns returned (lowercase for engine compatibility):
      st_cd, gen_art_color, gen_art, color, seg, total_score,
      dc_stock_qty, mrp, vendor_code, fabric, season,
      is_st_specific, priority_type
    """
    conn = _get_connection()
    cur = conn.cursor()
    t0 = time.time()
    try:
        # Load ALL scored pairs (no top-N limit) — every MSA article must be allocatable
        if top_n and top_n < 9999:
            qualify = f"QUALIFY ROW_NUMBER() OVER (PARTITION BY ST_CD ORDER BY TOTAL_SCORE DESC) <= {top_n}"
        else:
            qualify = ""
        cur.execute(f"""
            SELECT ST_CD, GEN_ART_COLOR, GEN_ART, COLOR, SEG, TOTAL_SCORE,
                   DC_STOCK_QTY, MRP, VENDOR_CODE, FABRIC, SEASON,
                   IS_ST_SPECIFIC, PRIORITY_TYPE
            FROM ARTICLE_SCORES
            WHERE MAJCAT = %s
            {qualify}
        """, (majcat,))
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        logger.info(f"[snowflake] scored_pairs({majcat}): {len(df):,} rows, "
                     f"{df['st_cd'].nunique()} stores in {time.time()-t0:.1f}s")
        return df
    finally:
        cur.close()
        conn.close()


def get_store_stock(gen_art_colors: List[str] = None, majcat: str = "") -> pd.DataFrame:
    """
    Get store stock for scored articles from FACT_STOCK_GENCOLOR.
    Uses temp table + JOIN for efficiency.
    Returns ~35K rows for M_JEANS in ~3s.

    Columns returned (lowercase):
      st_cd, gen_art_color, stock_qty
    """
    if not gen_art_colors:
        return pd.DataFrame(columns=["st_cd", "gen_art_color", "stock_qty"])

    conn = _get_connection()
    cur = conn.cursor()
    t0 = time.time()
    try:
        cur.execute("CREATE OR REPLACE TEMPORARY TABLE tmp_gacs (gac VARCHAR(100))")
        batch_size = 500
        for i in range(0, len(gen_art_colors), batch_size):
            batch = gen_art_colors[i:i + batch_size]
            values = ",".join(f"('{g}')" for g in batch)
            cur.execute(f"INSERT INTO tmp_gacs VALUES {values}")

        cur.execute("""
            SELECT f.STORE_CODE AS ST_CD, f.GENCOLOR_KEY AS GEN_ART_COLOR, f.STK_QTY AS STOCK_QTY
            FROM V2RETAIL.GOLD.FACT_STOCK_GENCOLOR f
            JOIN tmp_gacs t ON f.GENCOLOR_KEY = t.gac
            WHERE f.STK_QTY > 0
        """)
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        logger.info(f"[snowflake] store_stock({majcat}): {len(df):,} rows, "
                     f"{df['st_cd'].nunique()} stores in {time.time()-t0:.1f}s")
        return df
    finally:
        cur.close()
        conn.close()


def get_budget_cascade(majcat: str) -> pd.DataFrame:
    """
    Get budget cascade for a MAJCAT from SCORING.BUDGET_CASCADE (236 MAJCATs, 396K rows).
    Has real SEG values (E, V, P) and all budget metrics.

    Columns returned (lowercase):
      st_cd, majcat, seg, opt_count, mbq, bgt_disp_q, bgt_sales_per_day, priority_rank, opt_density
    """
    conn = _get_connection()
    cur = conn.cursor()
    t0 = time.time()
    try:
        cur.execute("""
            SELECT ST_CD, MAJ_CAT AS MAJCAT, SEG, OPT_COUNT, MBQ, BGT_DISP_Q,
                   BGT_SALES_PER_DAY, PRIORITY_RANK, OPT_DENSITY
            FROM V2_ALLOCATION.SCORING.BUDGET_CASCADE
            WHERE MAJ_CAT = %s AND OPT_COUNT > 0
        """, (majcat,))
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)

        # Dedup: keep one row per store-seg (Snowflake has near-identical dupes with float noise)
        raw_count = len(df)
        df = df.drop_duplicates(subset=["st_cd", "seg"], keep="first")

        logger.info(f"[snowflake] budget_cascade({majcat}): {raw_count} raw → "
                     f"{len(df)} deduped, {df['st_cd'].nunique()} stores, "
                     f"segs={df['seg'].unique().tolist()} in {time.time()-t0:.1f}s")
        return df
    finally:
        cur.close()
        conn.close()


def get_dc_variant_stock(gen_art_colors: List[str], majcat: str = "", rdc_code: str = "DH24") -> pd.DataFrame:
    """
    Get DC variant-level (size) stock from MSA_ARTICLES for a specific warehouse.

    MSA_ARTICLES has stock per DC as separate columns:
      V04 = DH24 (Delhi Hub), V01 = DW01 (West), etc.

    Args:
        gen_art_colors: list of GEN_ART_NUMBER + '_' + CLR keys to filter
        majcat: for logging
        rdc_code: 'DH24' uses V04 column, 'DW01' uses V01 column

    Columns returned (lowercase):
      gen_art_color, var_art, sz, stock_qty, mrp
    """
    if not gen_art_colors:
        return pd.DataFrame()

    # V01 column has the allocatable DC stock for both DH24 and DW01
    rdc_stock_col = "V01"

    conn = _get_connection()
    cur = conn.cursor()
    t0 = time.time()
    try:
        cur.execute("CREATE OR REPLACE TEMPORARY TABLE tmp_gacs_dc (gac VARCHAR(100))")
        # Extract gen_art from gen_art_color (before the underscore+color part)
        batch_size = 500
        for i in range(0, len(gen_art_colors), batch_size):
            batch = gen_art_colors[i:i + batch_size]
            values = ",".join(f"('{g}')" for g in batch)
            cur.execute(f"INSERT INTO tmp_gacs_dc VALUES {values}")

        cur.execute(f"""
            SELECT
                m.GEN_ART_NUMBER || '_' || m.CLR AS GEN_ART_COLOR,
                m.ARTICLE_NUMBER AS VAR_ART,
                m.SIZE AS SZ,
                m.{rdc_stock_col} AS STOCK_QTY,
                m.MRP
            FROM V2_ALLOCATION.RAW.MSA_ARTICLES m
            JOIN tmp_gacs_dc t ON (m.GEN_ART_NUMBER || '_' || m.CLR) = t.gac
            WHERE m.{rdc_stock_col} > 0
        """)
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        logger.info(f"[snowflake] dc_variant_stock({majcat}, {rdc_code}/{rdc_stock_col}): "
                     f"{len(df):,} rows in {time.time()-t0:.1f}s")
        return df
    finally:
        cur.close()
        conn.close()


def get_msa_dc_stock(majcat: str, rdc_code: str = "DH24") -> pd.DataFrame:
    """
    Get DC stock at gen_art_color level from MSA_ARTICLES (V01 column).
    This replaces the DC_STOCK_QTY from ARTICLE_SCORES which is total across all DCs.

    Returns: gen_art_color, dc_stock (summed across sizes for that article-color)
    """
    conn = _get_connection()
    cur = conn.cursor()
    t0 = time.time()
    try:
        cur.execute("""
            SELECT GEN_ART_NUMBER || '_' || CLR AS GEN_ART_COLOR,
                   SUM(V01) AS DC_STOCK
            FROM V2_ALLOCATION.RAW.MSA_ARTICLES
            WHERE MAJ_CAT = %s AND V01 > 0
            GROUP BY GEN_ART_NUMBER, CLR
        """, (majcat,))
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        logger.info(f"[snowflake] msa_dc_stock({majcat}, V01): {len(df):,} articles, "
                     f"{int(df['dc_stock'].sum()):,} units in {time.time()-t0:.1f}s")
        return df
    finally:
        cur.close()
        conn.close()


def reload_cache():
    """No-op for direct connection mode. Kept for API compatibility."""
    pass
