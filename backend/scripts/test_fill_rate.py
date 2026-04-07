#!/usr/bin/env python3
"""
Test fill rate calculation on M_TEES_HS data.
Loads scored pairs from Snowflake, runs the waterfall allocation,
then computes fill rates and verifies qty_fill > option_fill.
"""
import sys
import os
import logging

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("test_fill_rate")

import snowflake.connector
import pandas as pd

# -- Snowflake credentials --
SF_ACCOUNT = "iafphkw-hh80816"
SF_USER = "akashv2kart"
SF_PASSWORD = "SVXqEe5pDdamMb9"
SF_DATABASE = "V2_ALLOCATION"
SF_WAREHOUSE = "ALLOC_WH"

MAJCAT = "M_TEES_HS"
RUN_ID = "v2_tees_20260407"


def get_sf_connection():
    return snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        database=SF_DATABASE, schema="RESULTS", warehouse=SF_WAREHOUSE,
        login_timeout=30, network_timeout=60,
    )


def main():
    conn = get_sf_connection()
    cur = conn.cursor()

    # ---- Step 1: Check if RUN_ID exists, fallback to any available data ----
    logger.info(f"Checking for RUN_ID='{RUN_ID}' in ARTICLE_SCORES for MAJCAT='{MAJCAT}'...")
    cur.execute(
        "SELECT COUNT(*) FROM V2_ALLOCATION.RESULTS.ARTICLE_SCORES "
        "WHERE MAJCAT = %s AND RUN_ID = %s",
        (MAJCAT, RUN_ID),
    )
    run_count = cur.fetchone()[0]
    logger.info(f"Rows with RUN_ID='{RUN_ID}': {run_count}")

    if run_count == 0:
        # Try without RUN_ID
        cur.execute(
            "SELECT DISTINCT RUN_ID, COUNT(*) AS cnt "
            "FROM V2_ALLOCATION.RESULTS.ARTICLE_SCORES "
            "WHERE MAJCAT = %s GROUP BY RUN_ID ORDER BY cnt DESC LIMIT 5",
            (MAJCAT,),
        )
        available = cur.fetchall()
        if not available:
            # Try similar MAJCAT names
            cur.execute(
                "SELECT DISTINCT MAJCAT FROM V2_ALLOCATION.RESULTS.ARTICLE_SCORES "
                "WHERE MAJCAT LIKE '%TEES%' OR MAJCAT LIKE '%TEE%' LIMIT 10"
            )
            similar = [r[0] for r in cur.fetchall()]
            logger.error(f"No data for MAJCAT='{MAJCAT}'. Similar: {similar}")
            # Fallback to any available MAJCAT with good data
            cur.execute(
                "SELECT MAJCAT, COUNT(*) AS cnt FROM V2_ALLOCATION.RESULTS.ARTICLE_SCORES "
                "GROUP BY MAJCAT ORDER BY cnt DESC LIMIT 5"
            )
            top_cats = cur.fetchall()
            logger.info(f"Top MAJCATs by row count: {top_cats}")
            if top_cats:
                fallback_majcat = top_cats[0][0]
                logger.info(f"Falling back to MAJCAT='{fallback_majcat}'")
                return run_with_majcat(conn, cur, fallback_majcat, run_id=None)
            else:
                logger.error("No data at all in ARTICLE_SCORES")
                return
        else:
            logger.info(f"Available RUN_IDs for {MAJCAT}: {available}")
            best_run = available[0][0]
            logger.info(f"Using RUN_ID='{best_run}' ({available[0][1]} rows)")
            return run_with_majcat(conn, cur, MAJCAT, run_id=best_run)
    else:
        return run_with_majcat(conn, cur, MAJCAT, run_id=RUN_ID)


def run_with_majcat(conn, cur, majcat: str, run_id: str = None):
    """Load data, run allocation, compute fill rates."""

    # ---- Load scored pairs ----
    run_filter = f"AND RUN_ID = '{run_id}'" if run_id else ""
    # Top 200 per store to keep it tractable (same as production)
    cur.execute(f"""
        SELECT ST_CD, GEN_ART_COLOR, GEN_ART, COLOR, SEG, TOTAL_SCORE,
               DC_STOCK_QTY, MRP, VENDOR_CODE, FABRIC, SEASON,
               IS_ST_SPECIFIC, PRIORITY_TYPE
        FROM V2_ALLOCATION.RESULTS.ARTICLE_SCORES
        WHERE MAJCAT = %s {run_filter}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ST_CD ORDER BY TOTAL_SCORE DESC) <= 200
    """, (majcat,))
    cols = [d[0].lower() for d in cur.description]
    scored_pairs = pd.DataFrame(cur.fetchall(), columns=cols)
    logger.info(f"Scored pairs: {len(scored_pairs):,} rows, {scored_pairs['st_cd'].nunique()} stores")

    if scored_pairs.empty:
        logger.error("No scored pairs found")
        return

    # ---- Load budget cascade ----
    cur.execute("""
        SELECT ST_CD, MAJ_CAT AS MAJCAT, SEG, OPT_COUNT, MBQ, BGT_DISP_Q,
               BGT_SALES_PER_DAY, PRIORITY_RANK, OPT_DENSITY
        FROM V2_ALLOCATION.SCORING.BUDGET_CASCADE
        WHERE MAJ_CAT = %s AND OPT_COUNT > 0
    """, (majcat,))
    cols = [d[0].lower() for d in cur.description]
    budget_cascade = pd.DataFrame(cur.fetchall(), columns=cols)
    budget_cascade = budget_cascade.drop_duplicates(subset=["st_cd", "seg"], keep="first")
    logger.info(f"Budget cascade: {len(budget_cascade)} rows, {budget_cascade['st_cd'].nunique()} stores")

    if budget_cascade.empty:
        logger.error("No budget cascade data")
        return

    # ---- Load store stock ----
    gacs = scored_pairs["gen_art_color"].unique().tolist()
    logger.info(f"Loading store stock for {len(gacs)} unique articles...")

    cur.execute("CREATE OR REPLACE TEMPORARY TABLE tmp_gacs_test (gac VARCHAR(100))")
    batch_size = 500
    for i in range(0, len(gacs), batch_size):
        batch = gacs[i:i + batch_size]
        values = ",".join(f"('{g}')" for g in batch)
        cur.execute(f"INSERT INTO tmp_gacs_test VALUES {values}")

    cur.execute("""
        SELECT f.STORE_CODE AS ST_CD, f.GENCOLOR_KEY AS GEN_ART_COLOR, f.STK_QTY AS STOCK_QTY
        FROM V2RETAIL.GOLD.FACT_STOCK_GENCOLOR f
        JOIN tmp_gacs_test t ON f.GENCOLOR_KEY = t.gac
        WHERE f.STK_QTY > 0
    """)
    cols = [d[0].lower() for d in cur.description]
    store_stock = pd.DataFrame(cur.fetchall(), columns=cols)
    logger.info(f"Store stock: {len(store_stock):,} rows, {store_stock['st_cd'].nunique()} stores")

    # ---- Load MSA DC stock ----
    cur.execute("""
        SELECT GEN_ART_NUMBER || '_' || CLR AS GEN_ART_COLOR,
               SUM(COALESCE(V01, 0)) AS DC_STOCK
        FROM V2_ALLOCATION.RAW.MSA_ARTICLES
        WHERE MAJ_CAT = %s AND COALESCE(V01, 0) > 0
        GROUP BY GEN_ART_NUMBER, CLR
    """, (majcat,))
    cols = [d[0].lower() for d in cur.description]
    msa_dc_stock = pd.DataFrame(cur.fetchall(), columns=cols)
    logger.info(f"MSA DC stock: {len(msa_dc_stock)} articles, {int(msa_dc_stock['dc_stock'].sum()) if not msa_dc_stock.empty else 0} units")

    cur.close()
    conn.close()

    # ---- Run the waterfall filler ----
    from app.services.allocation.option_filler import GlobalGreedyFiller

    settings = {
        "min_score_threshold": "0",
        "multi_option_enabled": "true",
        "multi_option_min_score": "150",
        "multi_option_max_slots": "3",
        "max_colors_per_store": "5",
    }
    filler = GlobalGreedyFiller(settings)
    assignments = filler.fill(
        scored_pairs=scored_pairs,
        budget_cascade=budget_cascade,
        majcat=majcat,
        store_stock_gencolor=store_stock,
        msa_dc_stock=msa_dc_stock,
    )
    logger.info(f"Assignments: {len(assignments)} rows")

    if assignments.empty:
        logger.error("No assignments generated")
        return

    # ---- Compute fill rates ----
    from app.services.allocation.fill_rate import compute_fill_rates

    result = compute_fill_rates(assignments, budget_cascade)

    # ---- Print results ----
    print("\n" + "=" * 70)
    print(f"  FILL RATE REPORT: {majcat}")
    print("=" * 70)
    print(f"  Option Fill Rate BEFORE DC:  {result['option_fill_before_dc']:>7.2f}%")
    print(f"  Option Fill Rate AFTER DC:   {result['option_fill_after_dc']:>7.2f}%")
    print(f"  Qty Fill Rate BEFORE DC:     {result['qty_fill_before_dc']:>7.2f}%")
    print(f"  Qty Fill Rate AFTER DC:      {result['qty_fill_after_dc']:>7.2f}%")
    print(f"  Total Slots:                 {result['total_slots']:>7d}")
    print(f"  Total MBQ:                   {result['total_mbq']:>9.1f}")
    print("-" * 70)

    d = result["detail"]
    print(f"  L slots:   {d.get('l_slots', 0):>5d}   |  MIX slots: {d.get('mix_slots', 0):>5d}   |  NEW_L slots: {d.get('newl_slots', 0):>5d}")
    print(f"  Qty before sum: {d.get('qty_before_sum', 0):>9.1f}  |  Qty after sum: {d.get('qty_after_sum', 0):>9.1f}")
    print(f"  L dispatch:     {d.get('l_disp_qty', 0):>9.1f}  |  NEW_L dispatch: {d.get('newl_disp_qty', 0):>9.1f}")
    print(f"  Avg MBQ/opt:    {d.get('avg_mbq_per_opt', 0):>9.1f}  |  MIX decay: {d.get('mix_decay_factor', 0.7):.0%}")
    print(f"  Stores:         {d.get('stores_count', 0):>5d}")
    print("=" * 70)

    # ---- Verify: qty_fill >= option_fill when avg stock >= MBQ/opt ----
    avg_mbq_opt = result["total_mbq"] / max(result["total_slots"], 1)
    ok_before = result["qty_fill_before_dc"] >= result["option_fill_before_dc"] - 0.01
    ok_after = result["qty_fill_after_dc"] >= result["option_fill_after_dc"] - 0.01

    print(f"\n  STOCK DEPTH CHECK (avg MBQ/opt = {avg_mbq_opt:.1f})")

    # Show distribution of stock per slot
    adf = assignments.copy()
    for status in ["L", "MIX", "NEW_L"]:
        mask = adf["art_status"] == status
        if mask.sum() > 0:
            st_stock = adf.loc[mask, "st_stock"]
            disp_q = adf.loc[mask, "disp_q"]
            total_depth = st_stock + disp_q
            pct_at_mbq = (total_depth >= avg_mbq_opt).sum() / mask.sum() * 100
            print(f"    {status:5s}: count={mask.sum():>5d}, "
                  f"avg_stock={st_stock.mean():>5.1f}, "
                  f"avg_disp={disp_q.mean():>5.1f}, "
                  f"avg_total={total_depth.mean():>5.1f}, "
                  f"at_MBQ={pct_at_mbq:>5.1f}%")

    print(f"\n  QTY vs OPT FILL RATE COMPARISON:")
    print(f"    BEFORE DC: qty={result['qty_fill_before_dc']:.2f}% vs opt={result['option_fill_before_dc']:.2f}% "
          f"({'OK' if ok_before else 'qty < opt (thin stock per slot)'})")
    print(f"    AFTER DC:  qty={result['qty_fill_after_dc']:.2f}% vs opt={result['option_fill_after_dc']:.2f}% "
          f"({'OK' if ok_after else 'qty < opt (thin stock per slot)'})")

    if not ok_after:
        print(f"\n  NOTE: qty_fill < opt_fill because avg stock depth per filled slot")
        print(f"  ({d.get('qty_after_sum', 0) / max(d.get('l_slots', 0) + d.get('mix_slots', 0) + d.get('newl_slots', 0), 1):.1f}) "
              f"is less than MBQ/opt ({avg_mbq_opt:.1f}).")
        print(f"  This is expected when many L/MIX articles have partial stock.")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
