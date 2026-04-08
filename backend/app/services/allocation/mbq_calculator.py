"""
MBQ (Minimum Buy Quantity) Calculator — V2 Retail ARS

Computes MBQ per store x article based on actual sales data.

Business rules:
  L articles (existing in store):
    MBQ = display_qty + (actual_sales_per_day x cover_days) + in_transit_buffer
  NEW_L articles (new from DC):
    MBQ = display_qty + (budget_majcat_sales_per_day x cover_days)
  MIX articles (dying, no DC stock):
    MBQ = display_qty + (actual_sales_per_day x 0.70 x cover_days)

The Excel system has 13 MBQ formula variants:
  DISP, B_MTH, SSN, DISP+B_MTH, DISP+SSN, DISP/B_MTH, DISP/SSN,
  MAX(DISP,B_MTH), MAX(DISP,SSN), MIN(DISP,B_MTH), MIN(DISP,SSN),
  MAX(ALL3), MIN(ALL3)

This module implements the primary formula and exposes a variant selector.

Data sources:
  1. V2RETAIL.GOLD.FACT_SALE_GENCOLOR — actual sales per store x article
  2. V2_ALLOCATION.SCORING.BUDGET_CASCADE — bgt_sales_per_day per store x MAJCAT
"""
import logging
import math
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_COVER_DAYS = 14
DEFAULT_IN_TRANSIT_DAYS = 3
MIX_DECAY_FACTOR = 0.70
DEFAULT_DISPLAY_QTY = 2          # floor display qty when budget has none
SALES_LOOKBACK_DAYS = 30         # average over last N days of sales
MIN_SALES_PER_DAY = 0.0          # zero is fine — display still needs filling


# ── MBQ Formula Variants ─────────────────────────────────────────────────────

def _mbq_disp(display_qty: float, **_kw) -> float:
    """DISP: just display quantity."""
    return display_qty


def _mbq_b_mth(sales_per_day: float, cover_days: float, **_kw) -> float:
    """B_MTH: sales-based monthly cover."""
    return sales_per_day * cover_days


def _mbq_ssn(sales_per_day: float, cover_days: float, **_kw) -> float:
    """SSN: seasonal cover — same formula, caller passes seasonal rate."""
    return sales_per_day * cover_days


def _mbq_disp_plus_b_mth(display_qty: float, sales_per_day: float,
                          cover_days: float, **_kw) -> float:
    """DISP+B_MTH: display + monthly sales cover."""
    return display_qty + sales_per_day * cover_days


def _mbq_disp_plus_ssn(display_qty: float, sales_per_day: float,
                        cover_days: float, **_kw) -> float:
    """DISP+SSN: display + seasonal sales cover."""
    return display_qty + sales_per_day * cover_days


def _mbq_disp_div_b_mth(display_qty: float, sales_per_day: float,
                         cover_days: float, **_kw) -> float:
    """DISP/B_MTH: average of display and monthly cover."""
    return (display_qty + sales_per_day * cover_days) / 2


def _mbq_disp_div_ssn(display_qty: float, sales_per_day: float,
                       cover_days: float, **_kw) -> float:
    """DISP/SSN: average of display and seasonal cover."""
    return (display_qty + sales_per_day * cover_days) / 2


def _mbq_max_disp_b_mth(display_qty: float, sales_per_day: float,
                         cover_days: float, **_kw) -> float:
    """MAX(DISP, B_MTH)."""
    return max(display_qty, sales_per_day * cover_days)


def _mbq_max_disp_ssn(display_qty: float, sales_per_day: float,
                       cover_days: float, **_kw) -> float:
    """MAX(DISP, SSN)."""
    return max(display_qty, sales_per_day * cover_days)


def _mbq_min_disp_b_mth(display_qty: float, sales_per_day: float,
                         cover_days: float, **_kw) -> float:
    """MIN(DISP, B_MTH)."""
    return min(display_qty, sales_per_day * cover_days)


def _mbq_min_disp_ssn(display_qty: float, sales_per_day: float,
                       cover_days: float, **_kw) -> float:
    """MIN(DISP, SSN)."""
    return min(display_qty, sales_per_day * cover_days)


def _mbq_max_all3(display_qty: float, sales_per_day: float,
                   cover_days: float, ssn_sales_per_day: float = 0, **_kw) -> float:
    """MAX(DISP, B_MTH, SSN)."""
    return max(display_qty, sales_per_day * cover_days,
               ssn_sales_per_day * cover_days)


def _mbq_min_all3(display_qty: float, sales_per_day: float,
                   cover_days: float, ssn_sales_per_day: float = 0, **_kw) -> float:
    """MIN(DISP, B_MTH, SSN)."""
    return min(display_qty, sales_per_day * cover_days,
               ssn_sales_per_day * cover_days)


MBQ_FORMULAS = {
    "DISP":           _mbq_disp,
    "B_MTH":          _mbq_b_mth,
    "SSN":            _mbq_ssn,
    "DISP+B_MTH":     _mbq_disp_plus_b_mth,
    "DISP+SSN":       _mbq_disp_plus_ssn,
    "DISP/B_MTH":     _mbq_disp_div_b_mth,
    "DISP/SSN":       _mbq_disp_div_ssn,
    "MAX_DISP_B_MTH": _mbq_max_disp_b_mth,
    "MAX_DISP_SSN":   _mbq_max_disp_ssn,
    "MIN_DISP_B_MTH": _mbq_min_disp_b_mth,
    "MIN_DISP_SSN":   _mbq_min_disp_ssn,
    "MAX_ALL3":       _mbq_max_all3,
    "MIN_ALL3":       _mbq_min_all3,
}

# Default formula: display + monthly sales cover (most common in V2 Excel system)
DEFAULT_FORMULA = "DISP+B_MTH"


# ── Snowflake Sales Loader ────────────────────────────────────────────────────

def _get_connection():
    """Reuse the same lazy-import pattern from snowflake_loader.py."""
    from .snowflake_loader import _get_connection as _sf_conn
    return _sf_conn()


def get_actual_sales(
    store_codes: List[str],
    gen_art_colors: List[str],
    lookback_days: int = SALES_LOOKBACK_DAYS,
) -> pd.DataFrame:
    """
    Query FACT_SALE_GENCOLOR for actual sales rates per store x article.

    Returns DataFrame with columns:
      st_cd, gen_art_color, total_sale_qty, sale_days, sales_per_day

    Uses DESCRIBE to discover schema at first call, then caches column names.
    """
    if not store_codes or not gen_art_colors:
        return pd.DataFrame(columns=["st_cd", "gen_art_color", "total_sale_qty",
                                      "sale_days", "sales_per_day"])

    conn = _get_connection()
    cur = conn.cursor()
    t0 = time.time()
    try:
        # Discover schema once — find the store code, gencolor key, qty, and date columns
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='GOLD' AND table_name='FACT_SALE_GENCOLOR' "
                    "ORDER BY ordinal_position")
        all_cols = [r[0] for r in cur.fetchall()]
        logger.info(f"[mbq] FACT_SALE_GENCOLOR columns: {all_cols}")

        # Identify key columns by convention (Gold layer naming)
        store_col = _find_col(all_cols, ["STORE_CODE", "ST_CD", "STORE_CD"])
        gac_col = _find_col(all_cols, ["GENCOLOR_KEY", "GEN_ART_COLOR", "GENCOLOR"])
        qty_col = _find_col(all_cols, ["SALE_QTY", "SL_QTY", "QTY", "SOLD_QTY",
                                        "NET_SALE_QTY", "TOTAL_QTY"])
        date_col = _find_col(all_cols, ["BILL_DATE", "SALE_DATE", "TXN_DATE",
                                         "TRANSACTION_DATE", "DATE"])

        if not all([store_col, gac_col, qty_col, date_col]):
            logger.error(f"[mbq] Cannot identify sale columns. Found: "
                         f"store={store_col}, gac={gac_col}, qty={qty_col}, date={date_col}")
            return pd.DataFrame(columns=["st_cd", "gen_art_color", "total_sale_qty",
                                          "sale_days", "sales_per_day"])

        # Load gen_art_colors into temp table for efficient JOIN
        cur.execute("CREATE OR REPLACE TEMPORARY TABLE tmp_mbq_gacs (gac VARCHAR(100))")
        batch_size = 500
        for i in range(0, len(gen_art_colors), batch_size):
            batch = gen_art_colors[i:i + batch_size]
            values = ",".join(f"('{g}')" for g in batch)
            cur.execute(f"INSERT INTO tmp_mbq_gacs VALUES {values}")

        # Load store codes into temp table
        cur.execute("CREATE OR REPLACE TEMPORARY TABLE tmp_mbq_stores (st VARCHAR(20))")
        for i in range(0, len(store_codes), batch_size):
            batch = store_codes[i:i + batch_size]
            values = ",".join(f"('{s}')" for s in batch)
            cur.execute(f"INSERT INTO tmp_mbq_stores VALUES {values}")

        # Aggregate sales per store x article over lookback window
        cur.execute(f"""
            SELECT
                f.{store_col}  AS ST_CD,
                f.{gac_col}    AS GEN_ART_COLOR,
                SUM(f.{qty_col})  AS TOTAL_SALE_QTY,
                COUNT(DISTINCT f.{date_col}) AS SALE_DAYS
            FROM V2RETAIL.GOLD.FACT_SALE_GENCOLOR f
            JOIN tmp_mbq_gacs   g ON f.{gac_col}   = g.gac
            JOIN tmp_mbq_stores s ON f.{store_col}  = s.st
            WHERE f.{date_col} >= DATEADD(day, -{lookback_days}, CURRENT_DATE())
            GROUP BY f.{store_col}, f.{gac_col}
        """)
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)

        # Compute sales_per_day = total_sale_qty / lookback_days
        # (Use full lookback window as denominator, not just sale_days, to avoid
        #  overestimating rate for articles that sold on only 1-2 days)
        df["sales_per_day"] = df["total_sale_qty"].astype(float) / lookback_days

        logger.info(f"[mbq] actual_sales: {len(df):,} store×article pairs, "
                     f"{df['st_cd'].nunique()} stores in {time.time()-t0:.1f}s")
        return df

    finally:
        cur.close()
        conn.close()


def _find_col(all_cols: List[str], candidates: List[str]) -> Optional[str]:
    """Find the first matching column name from candidates."""
    all_upper = {c.upper(): c for c in all_cols}
    for cand in candidates:
        if cand.upper() in all_upper:
            return all_upper[cand.upper()]
    return None


# ── Core MBQ Computation ─────────────────────────────────────────────────────

def compute_mbq(
    art_status: str,
    display_qty: float,
    actual_sales_per_day: float = 0.0,
    budget_sales_per_day: float = 0.0,
    cover_days: float = DEFAULT_COVER_DAYS,
    in_transit_days: float = DEFAULT_IN_TRANSIT_DAYS,
    formula: str = DEFAULT_FORMULA,
) -> float:
    """
    Compute MBQ for a single store x article.

    Args:
        art_status: 'L' (listed), 'NEW_L' (new from DC), 'MIX' (dying)
        display_qty: Display quantity for the article in this store
        actual_sales_per_day: From FACT_SALE_GENCOLOR (last 30d avg)
        budget_sales_per_day: From BUDGET_CASCADE (budget MAJCAT rate)
        cover_days: Number of days of sales to cover (default 14)
        in_transit_days: Buffer for goods in transit (default 3, L only)
        formula: One of the 13 MBQ formula variants

    Returns:
        MBQ quantity (float, rounded up to integer)
    """
    art_status = art_status.upper().strip()
    display_qty = max(display_qty, 0)

    # Select sales rate based on article status
    if art_status == "L":
        # L: use actual sales, add in-transit buffer
        spd = max(actual_sales_per_day, MIN_SALES_PER_DAY)
        effective_cover = cover_days + in_transit_days
    elif art_status == "NEW_L":
        # NEW_L: use budget MAJCAT rate, no in-transit (first dispatch)
        spd = max(budget_sales_per_day, MIN_SALES_PER_DAY)
        effective_cover = cover_days
    elif art_status == "MIX":
        # MIX: 70% of actual sales rate, no in-transit
        spd = max(actual_sales_per_day * MIX_DECAY_FACTOR, MIN_SALES_PER_DAY)
        effective_cover = cover_days
    else:
        # Unknown status — treat as NEW_L
        spd = max(budget_sales_per_day, MIN_SALES_PER_DAY)
        effective_cover = cover_days

    # Apply formula variant
    formula_fn = MBQ_FORMULAS.get(formula, MBQ_FORMULAS[DEFAULT_FORMULA])
    mbq_raw = formula_fn(
        display_qty=display_qty,
        sales_per_day=spd,
        cover_days=effective_cover,
        ssn_sales_per_day=spd,  # seasonal = same unless overridden
    )

    # Round up — you can't buy half an item
    return math.ceil(max(mbq_raw, display_qty))


# ── Batch MBQ for a full MAJCAT ──────────────────────────────────────────────

def compute_mbq_batch(
    assignments: pd.DataFrame,
    budget_cascade: pd.DataFrame,
    majcat: str,
    cover_days: float = DEFAULT_COVER_DAYS,
    in_transit_days: float = DEFAULT_IN_TRANSIT_DAYS,
    formula: str = DEFAULT_FORMULA,
    fetch_sales: bool = True,
) -> pd.DataFrame:
    """
    Compute MBQ for all store x article assignments in a MAJCAT.

    Enriches the assignments DataFrame with:
      - actual_sales_per_day (from Snowflake FACT_SALE_GENCOLOR)
      - mbq_computed (from the MBQ formula)

    Args:
        assignments: DataFrame with at least [st_cd, gen_art_color, art_status]
        budget_cascade: From snowflake_loader.get_budget_cascade(), has bgt_sales_per_day
        majcat: MAJCAT being processed (for logging)
        cover_days: Days of sales to cover
        in_transit_days: In-transit buffer (L articles only)
        formula: MBQ formula variant
        fetch_sales: If True, query Snowflake for actual sales. If False, skip.

    Returns:
        assignments DataFrame with added columns:
          actual_sales_per_day, mbq_computed
    """
    if assignments.empty:
        return assignments

    t0 = time.time()
    df = assignments.copy()

    # ── Build budget lookup: st_cd -> bgt_sales_per_day ──
    # Budget cascade has per-seg rows; aggregate to store level
    bgt_spd = {}
    if not budget_cascade.empty:
        store_bgt = budget_cascade.groupby("st_cd").agg(
            bgt_sales_per_day=("bgt_sales_per_day", "sum"),
            bgt_disp_q=("bgt_disp_q", "sum") if "bgt_disp_q" in budget_cascade.columns
                       else ("bgt_sales_per_day", "count"),
        ).reset_index()
        bgt_spd = dict(zip(store_bgt["st_cd"], store_bgt["bgt_sales_per_day"]))

    # ── Build display_qty lookup from budget cascade ──
    disp_lookup = {}
    if "bgt_disp_q" in budget_cascade.columns and not budget_cascade.empty:
        store_disp = budget_cascade.groupby("st_cd")["bgt_disp_q"].sum().to_dict()
        # bgt_disp_q is total display for the MAJCAT at that store
        # Per-option display = bgt_disp_q / opt_count
        store_opts = budget_cascade.groupby("st_cd")["opt_count"].sum().to_dict()
        for st, total_disp in store_disp.items():
            n_opts = max(store_opts.get(st, 1), 1)
            disp_lookup[st] = max(total_disp / n_opts, DEFAULT_DISPLAY_QTY)

    # ── Fetch actual sales from Snowflake ──
    actual_spd_map: Dict[Tuple[str, str], float] = {}
    if fetch_sales:
        store_codes = df["st_cd"].unique().tolist()
        # Only fetch sales for L and MIX articles (NEW_L uses budget rate)
        l_mix_mask = df["art_status"].str.upper().isin(["L", "MIX", "L_ONLY"])
        l_mix_gacs = df.loc[l_mix_mask, "gen_art_color"].unique().tolist()

        if l_mix_gacs:
            try:
                sales_df = get_actual_sales(store_codes, l_mix_gacs)
                if not sales_df.empty:
                    for _, r in sales_df.iterrows():
                        actual_spd_map[(r["st_cd"], r["gen_art_color"])] = float(
                            r["sales_per_day"])
                logger.info(f"[mbq] {majcat}: fetched {len(actual_spd_map):,} "
                             f"actual sales rates")
            except Exception as e:
                logger.warning(f"[mbq] {majcat}: failed to fetch sales, "
                                f"falling back to budget rates: {e}")

    # ── Compute MBQ per row ──
    mbq_values = []
    actual_spd_values = []

    for _, row in df.iterrows():
        st_cd = row["st_cd"]
        gac = row["gen_art_color"]
        status = str(row.get("art_status", "")).upper().strip()

        # Actual sales rate for this specific store x article
        a_spd = actual_spd_map.get((st_cd, gac), 0.0)

        # Budget sales rate for this store (MAJCAT level)
        b_spd = bgt_spd.get(st_cd, 0.0)

        # Display qty per option for this store
        dq = disp_lookup.get(st_cd, DEFAULT_DISPLAY_QTY)

        mbq = compute_mbq(
            art_status=status,
            display_qty=dq,
            actual_sales_per_day=a_spd,
            budget_sales_per_day=b_spd,
            cover_days=cover_days,
            in_transit_days=in_transit_days,
            formula=formula,
        )

        mbq_values.append(mbq)
        actual_spd_values.append(a_spd)

    df["actual_sales_per_day"] = actual_spd_values
    df["mbq_computed"] = mbq_values

    elapsed = time.time() - t0
    logger.info(
        f"[mbq] {majcat}: computed MBQ for {len(df):,} assignments "
        f"(formula={formula}, cover={cover_days}d, transit={in_transit_days}d) "
        f"in {elapsed:.1f}s — "
        f"avg MBQ={df['mbq_computed'].mean():.1f}, "
        f"median={df['mbq_computed'].median():.0f}, "
        f"max={df['mbq_computed'].max():.0f}"
    )

    return df


# ── Convenience: compute MBQ for a single store ──────────────────────────────

def compute_mbq_for_store(
    st_cd: str,
    majcat: str,
    assignments: pd.DataFrame,
    budget_cascade: pd.DataFrame,
    cover_days: float = DEFAULT_COVER_DAYS,
    formula: str = DEFAULT_FORMULA,
) -> pd.DataFrame:
    """
    Compute MBQ for all articles assigned to a single store.
    Convenience wrapper around compute_mbq_batch.
    """
    store_assignments = assignments[assignments["st_cd"] == st_cd].copy()
    if store_assignments.empty:
        logger.warning(f"[mbq] No assignments for store {st_cd} in {majcat}")
        return store_assignments

    return compute_mbq_batch(
        assignments=store_assignments,
        budget_cascade=budget_cascade[budget_cascade["st_cd"] == st_cd],
        majcat=majcat,
        cover_days=cover_days,
        formula=formula,
    )


# ── Test helper ───────────────────────────────────────────────────────────────

def test_mbq(
    majcat: str = "M_TEES_HS",
    st_cd: str = "HA10",
    cover_days: float = DEFAULT_COVER_DAYS,
    formula: str = DEFAULT_FORMULA,
) -> Dict:
    """
    End-to-end MBQ test for a specific store x MAJCAT.
    Loads data from Snowflake, runs the full pipeline, returns summary.

    Usage:
        from app.services.allocation.mbq_calculator import test_mbq
        result = test_mbq("M_TEES_HS", "HA10")
    """
    from .snowflake_loader import (
        get_scored_pairs, get_store_stock, get_budget_cascade, get_msa_dc_stock
    )

    logger.info(f"[mbq-test] Starting MBQ test: {majcat} / {st_cd}")

    # 1. Load data from Snowflake
    scored = get_scored_pairs(majcat, top_n=200)
    budget = get_budget_cascade(majcat)
    store_stock = get_store_stock(
        scored["gen_art_color"].unique().tolist(), majcat
    )
    msa_dc = get_msa_dc_stock(majcat)

    # 2. Filter to target store
    store_scored = scored[scored["st_cd"] == st_cd]
    store_budget = budget[budget["st_cd"] == st_cd]

    if store_scored.empty:
        return {"error": f"No scored pairs for {st_cd} in {majcat}"}
    if store_budget.empty:
        return {"error": f"No budget cascade for {st_cd} in {majcat}"}

    # 3. Run option filler to get assignments
    from .option_filler import GlobalGreedyFiller
    filler = GlobalGreedyFiller(settings={})
    assignments = filler.fill(
        scored_pairs=scored,
        budget_cascade=budget,
        majcat=majcat,
        store_stock_gencolor=store_stock,
        msa_dc_stock=msa_dc,
    )

    # Filter to target store
    store_assigns = assignments[assignments["st_cd"] == st_cd].copy()
    if store_assigns.empty:
        return {"error": f"No assignments for {st_cd} after option fill"}

    # 4. Compute MBQ
    result_df = compute_mbq_batch(
        assignments=store_assigns,
        budget_cascade=store_budget,
        majcat=majcat,
        cover_days=cover_days,
        formula=formula,
    )

    # 5. Summary
    summary = {
        "store": st_cd,
        "majcat": majcat,
        "formula": formula,
        "cover_days": cover_days,
        "total_assignments": len(result_df),
        "budget_mbq": float(store_budget["mbq"].sum()),
        "budget_bgt_sales_per_day": float(store_budget["bgt_sales_per_day"].sum()),
        "budget_bgt_disp_q": float(store_budget["bgt_disp_q"].sum()),
        "computed_mbq_avg": round(float(result_df["mbq_computed"].mean()), 2),
        "computed_mbq_median": round(float(result_df["mbq_computed"].median()), 2),
        "computed_mbq_total": int(result_df["mbq_computed"].sum()),
        "computed_mbq_max": int(result_df["mbq_computed"].max()),
        "l_articles": int((result_df["art_status"].str.upper() == "L").sum()),
        "mix_articles": int((result_df["art_status"].str.upper() == "MIX").sum()),
        "new_l_articles": int((result_df["art_status"].str.upper() == "NEW_L").sum()),
        "articles_with_sales": int((result_df["actual_sales_per_day"] > 0).sum()),
        "top_5": result_df.nlargest(5, "mbq_computed")[
            ["gen_art_color", "art_status", "actual_sales_per_day", "mbq_computed"]
        ].to_dict("records"),
    }

    logger.info(f"[mbq-test] {st_cd}/{majcat}: {summary['total_assignments']} articles, "
                 f"budget MBQ={summary['budget_mbq']}, "
                 f"computed total={summary['computed_mbq_total']}, "
                 f"avg={summary['computed_mbq_avg']}")

    return summary
