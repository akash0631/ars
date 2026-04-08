"""
Engine 4: Size Allocator
Breaks article-level assignments to size-level variants (SKUs).

Uses:
  - CONT_SZ from Snowflake: store-specific size contribution % per MAJCAT
  - MSA_ARTICLES size-level stock: DC V01 stock per article × size
  - Conservative rounding: only round up if fraction > 0.7

Input:  option_assignments DataFrame from GlobalGreedyFiller
Output: variant-level DataFrame: store × article × size × qty × mrp × value
"""
import logging
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def conservative_round(value: float) -> int:
    """Round up only if fractional part > 0.7, else round down."""
    frac = value - int(value)
    return int(value) + (1 if frac > 0.7 else 0)


def get_size_contributions(majcat: str) -> pd.DataFrame:
    """
    Get store-specific size contribution % from Snowflake CONT_SZ.
    Returns DataFrame: st_cd, sz, cont (percentage 0-1)
    """
    import snowflake.connector
    import os
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", "iafphkw-hh80816"),
        user=os.getenv("SNOWFLAKE_USER", "akashv2kart"),
        password=os.getenv("SNOWFLAKE_PASSWORD", "SVXqEe5pDdamMb9"),
        database="V2_ALLOCATION", schema="RAW",
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ALLOC_WH"),
    )
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT ST_CD, SZ, CONT
            FROM CONT_SZ
            WHERE MAJ_CAT = %s AND CONT > 0
        """, (majcat,))
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        logger.info(f"[size_alloc] CONT_SZ({majcat}): {len(df):,} rows, "
                     f"{df['st_cd'].nunique()} stores, sizes={sorted(df['sz'].unique())}")
        return df
    finally:
        cur.close()
        conn.close()


def get_dc_size_stock(majcat: str) -> pd.DataFrame:
    """
    Get DC stock at size level from MSA_ARTICLES V01.
    Returns DataFrame: gen_art_color, sz, dc_stock
    """
    import snowflake.connector
    import os
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", "iafphkw-hh80816"),
        user=os.getenv("SNOWFLAKE_USER", "akashv2kart"),
        password=os.getenv("SNOWFLAKE_PASSWORD", "SVXqEe5pDdamMb9"),
        database="V2_ALLOCATION", schema="RAW",
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ALLOC_WH"),
    )
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT GEN_ART_NUMBER || '_' || CLR AS GEN_ART_COLOR,
                   SIZE AS SZ,
                   ARTICLE_NUMBER AS VAR_ART,
                   COALESCE(V01, 0) AS DC_STOCK
            FROM MSA_ARTICLES
            WHERE MAJ_CAT = %s AND COALESCE(V01, 0) > 0
        """, (majcat,))
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        df['sz'] = df['sz'].fillna('').astype(str)
        logger.info(f"[size_alloc] DC size stock({majcat}): {len(df):,} rows, "
                     f"{df['gen_art_color'].nunique()} articles, sizes={sorted(df['sz'].unique())}")
        return df
    finally:
        cur.close()
        conn.close()


def allocate_sizes(
    assignments: pd.DataFrame,
    majcat: str,
) -> pd.DataFrame:
    """
    Break article-level assignments to size-level variants.

    For each assignment with disp_q > 0:
      1. Get store's size contribution % from CONT_SZ
      2. Split disp_q proportionally: sz_qty = disp_q × cont%
      3. Cap each size by DC size stock (global tracker)
      4. Conservative rounding (>0.7 rounds up)

    Returns DataFrame:
      st_cd, majcat, gen_art_color, gen_art, color, var_art, sz,
      alloc_qty, mrp, value, art_status, opt_no, dc_sz_stock_before, dc_sz_stock_after
    """
    if assignments.empty:
        return pd.DataFrame()

    # Only process assignments with dispatch
    dispatch_assignments = assignments[assignments['disp_q'] > 0].copy()
    if dispatch_assignments.empty:
        logger.info(f"[size_alloc] {majcat}: no dispatch assignments to break into sizes")
        return pd.DataFrame()

    # Load size data from Snowflake
    size_cont = get_size_contributions(majcat)
    dc_size_stock = get_dc_size_stock(majcat)

    if size_cont.empty:
        logger.warning(f"[size_alloc] {majcat}: no size contribution data")
        return pd.DataFrame()

    # Build size contribution lookup: {st_cd: {sz: cont%}}
    size_cont_map: Dict[str, Dict[str, float]] = {}
    for _, row in size_cont.iterrows():
        st = row['st_cd']
        if st not in size_cont_map:
            size_cont_map[st] = {}
        size_cont_map[st][row['sz']] = float(row['cont'])

    # Build DC size stock tracker: {(gen_art_color, sz): remaining_stock}
    dc_sz_tracker: Dict[Tuple[str, str], float] = {}
    for _, row in dc_size_stock.iterrows():
        key = (row['gen_art_color'], row['sz'])
        dc_sz_tracker[key] = dc_sz_tracker.get(key, 0) + float(row['dc_stock'])

    # Get default size distribution (company average) for stores without CONT_SZ
    all_sizes = sorted(size_cont['sz'].unique())
    default_cont = size_cont.groupby('sz')['cont'].mean().to_dict()
    total_default = sum(default_cont.values())
    if total_default > 0:
        default_cont = {sz: c / total_default for sz, c in default_cont.items()}

    # Process each dispatch assignment
    variants = []
    total_alloc = 0
    total_short = 0

    for _, asgn in dispatch_assignments.iterrows():
        st_cd = asgn['st_cd']
        gac = asgn['gen_art_color']
        gen_art = asgn.get('gen_art', '')
        color = asgn.get('color', '')
        disp_q = float(asgn['disp_q'])
        mrp = float(asgn.get('mrp', 0) or 0)
        art_status = asgn.get('art_status', '')
        opt_no = asgn.get('opt_no', 0)

        if disp_q <= 0:
            continue

        # Get store's size contribution
        st_sizes = size_cont_map.get(st_cd, default_cont)
        if not st_sizes:
            st_sizes = default_cont

        # Normalize contributions to sum to 1
        total_cont = sum(st_sizes.values())
        if total_cont <= 0:
            continue

        # Split dispatch by size
        for sz, cont in sorted(st_sizes.items(), key=lambda x: -x[1]):
            raw_qty = disp_q * (cont / total_cont)
            qty = conservative_round(raw_qty)

            if qty <= 0:
                continue

            # Cap by DC size stock
            dc_key = (gac, sz)
            dc_avail = dc_sz_tracker.get(dc_key, 0)

            actual_qty = min(qty, int(dc_avail))
            if actual_qty <= 0:
                total_short += qty
                continue

            dc_sz_tracker[dc_key] = max(0, dc_avail - actual_qty)

            # Find variant article number
            var_art_row = dc_size_stock[
                (dc_size_stock['gen_art_color'] == gac) &
                (dc_size_stock['sz'] == sz)
            ]
            var_art = var_art_row.iloc[0]['var_art'] if not var_art_row.empty else f"{gac}_{sz}"

            variants.append({
                'st_cd': st_cd,
                'majcat': majcat,
                'gen_art_color': gac,
                'gen_art': gen_art,
                'color': color,
                'var_art': var_art,
                'sz': sz,
                'alloc_qty': actual_qty,
                'mrp': mrp,
                'value': actual_qty * mrp,
                'art_status': art_status,
                'opt_no': opt_no,
                'dc_sz_stock_before': dc_avail,
                'dc_sz_stock_after': dc_sz_tracker.get(dc_key, 0),
            })
            total_alloc += actual_qty

    result = pd.DataFrame(variants) if variants else pd.DataFrame()

    if not result.empty:
        total_value = result['value'].sum()
        unique_sizes = result['sz'].nunique()
        unique_stores = result['st_cd'].nunique()
        logger.info(
            f"[size_alloc] {majcat}: {len(result):,} variant rows, "
            f"{total_alloc:,} units allocated, {total_short:,} short, "
            f"₹{total_value:,.0f} value, {unique_sizes} sizes, {unique_stores} stores"
        )

    return result
