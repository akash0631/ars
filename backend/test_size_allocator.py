"""
Test script: SizeAllocator end-to-end with Snowflake + Supabase data.
Tests M_JEANS category through the full pipeline:
  1. Snowflake scored pairs → GlobalGreedyFiller → option_assignments
  2. Supabase size contribution %
  3. Snowflake DC variant stock (MSA_ARTICLES)
  4. SizeAllocator.allocate()
"""
import sys
import os
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('test_size_allocator')

import pandas as pd
import numpy as np

# ─── Config ───
MAJCAT = 'M_JEANS'
SUPABASE_URL = 'https://pymdqnnwwxrgeolvgvgv.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB5bWRxbm53d3hyZ2VvbHZndmd2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MzMzNTQ3NiwiZXhwIjoyMDY4OTExNDc2fQ.goCXYYC_gxKLm-1reF37I7Dpr-GA1fpMgDj4queofVY'

def main():
    t_start = time.time()

    # ═══════════════════════════════════════════════
    # STEP 1: Get scored pairs + budget + store stock from Snowflake
    # ═══════════════════════════════════════════════
    from app.services.allocation.snowflake_loader import (
        get_scored_pairs, get_budget_cascade, get_store_stock, get_dc_variant_stock
    )
    from app.services.allocation.option_filler import GlobalGreedyFiller
    from app.services.allocation.size_allocator import SizeAllocator

    logger.info("=" * 60)
    logger.info(f"STEP 1: Load Snowflake data for {MAJCAT}")
    logger.info("=" * 60)

    scored_pairs = get_scored_pairs(MAJCAT, top_n=200)
    logger.info(f"Scored pairs: {len(scored_pairs):,} rows, {scored_pairs['st_cd'].nunique()} stores")
    if scored_pairs.empty:
        logger.error("No scored pairs — aborting")
        return

    budget = get_budget_cascade(MAJCAT)
    logger.info(f"Budget cascade: {len(budget):,} rows, {budget['st_cd'].nunique()} stores")

    gacs = scored_pairs['gen_art_color'].unique().tolist()
    store_stock_gc = get_store_stock(gacs, majcat=MAJCAT)
    logger.info(f"Store stock (gencolor): {len(store_stock_gc):,} rows")

    # ═══════════════════════════════════════════════
    # STEP 2: Run GlobalGreedyFiller (Engine 3)
    # ═══════════════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STEP 2: Run GlobalGreedyFiller")
    logger.info("=" * 60)

    settings = {
        'multi_option_enabled': 'false',
        'max_colors_per_store': '5',
        'min_score_threshold': '0',
    }
    filler = GlobalGreedyFiller(settings)
    option_assignments = filler.fill(scored_pairs, budget, MAJCAT, store_stock_gencolor=store_stock_gc)
    logger.info(f"Option assignments: {len(option_assignments):,} rows")
    if option_assignments.empty:
        logger.error("No option assignments — aborting")
        return

    # Show filler summary
    if 'art_status' in option_assignments.columns:
        logger.info(f"Art status breakdown:\n{option_assignments['art_status'].value_counts().to_string()}")

    # ═══════════════════════════════════════════════
    # STEP 3: Get size contribution from Supabase
    # ═══════════════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STEP 3: Get size contribution from Supabase")
    logger.info("=" * 60)

    import httpx
    headers = {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}'}
    r = httpx.get(
        f'{SUPABASE_URL}/rest/v1/co_budget_company_major_category_size',
        headers=headers,
        params={'major_category': f'eq.{MAJCAT}', 'select': 'size,sale_q_feb_2026'},
        timeout=30,
    )
    r.raise_for_status()
    supabase_data = r.json()
    logger.info(f"Supabase returned {len(supabase_data)} size rows for {MAJCAT}")

    # Compute bgt_size DataFrame
    total_sales = sum(d.get('sale_q_feb_2026', 0) or 0 for d in supabase_data)
    bgt_rows = []
    for d in supabase_data:
        sz = d.get('size', '')
        sale_q = d.get('sale_q_feb_2026', 0) or 0
        pct = round(sale_q / total_sales, 4) if total_sales > 0 else 0
        bgt_rows.append({'majcat': MAJCAT, 'sz': sz, 'bgt_cont_pct': pct})
    bgt_size = pd.DataFrame(bgt_rows)
    bgt_size = bgt_size.sort_values('bgt_cont_pct', ascending=False)
    logger.info(f"Size contribution (total sales={total_sales:,}):")
    for _, row in bgt_size.iterrows():
        logger.info(f"  {row['sz']:>6s}: {row['bgt_cont_pct']*100:5.1f}%")

    # ═══════════════════════════════════════════════
    # STEP 4: Get DC variant stock from Snowflake
    # ═══════════════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STEP 4: Get DC variant stock from Snowflake MSA_ARTICLES")
    logger.info("=" * 60)

    assigned_gacs = option_assignments['gen_art_color'].unique().tolist()
    dc_variant_stock = get_dc_variant_stock(assigned_gacs, majcat=MAJCAT)
    logger.info(f"DC variant stock: {len(dc_variant_stock):,} rows, "
                f"{dc_variant_stock['gen_art_color'].nunique() if not dc_variant_stock.empty else 0} articles")

    if not dc_variant_stock.empty:
        logger.info(f"DC variant stock columns: {list(dc_variant_stock.columns)}")
        logger.info(f"Sample DC variant data:\n{dc_variant_stock.head(10).to_string()}")
        logger.info(f"Sizes in DC stock: {sorted(dc_variant_stock['sz'].unique().tolist())}")

    # Check: SizeAllocator needs 'var_art' column but Snowflake MSA_ARTICLES may not have it
    # We need to construct var_art if missing
    if not dc_variant_stock.empty and 'var_art' not in dc_variant_stock.columns:
        logger.info("var_art column MISSING from dc_variant_stock — constructing it")
        # var_art = gen_art_color + "_" + sz (or similar pattern)
        # Let's check what columns we have and figure out the right construction
        logger.info(f"Available columns: {list(dc_variant_stock.columns)}")

        # Check if there's a separate article number or SLOC that could be var_art
        if 'sloc' in dc_variant_stock.columns:
            # SLOC might be the variant article number itself
            logger.info(f"Sample SLOC values: {dc_variant_stock['sloc'].head(10).tolist()}")
            # Use SLOC as var_art if it looks like an article number
            sample_sloc = str(dc_variant_stock['sloc'].iloc[0]) if len(dc_variant_stock) > 0 else ''
            sample_gac = str(dc_variant_stock['gen_art_color'].iloc[0]) if len(dc_variant_stock) > 0 else ''
            logger.info(f"Sample SLOC='{sample_sloc}', GAC='{sample_gac}'")

            # If SLOC is a storage location code (like '1000'), construct var_art from gen_art_color + sz
            if sample_sloc.isdigit() and len(sample_sloc) <= 5:
                logger.info("SLOC appears to be storage location, constructing var_art = gen_art_color + '_' + sz")
                dc_variant_stock['var_art'] = dc_variant_stock['gen_art_color'] + '_' + dc_variant_stock['sz'].astype(str)
            else:
                # SLOC might be the variant article itself
                dc_variant_stock['var_art'] = dc_variant_stock['sloc']
        else:
            dc_variant_stock['var_art'] = dc_variant_stock['gen_art_color'] + '_' + dc_variant_stock['sz'].astype(str)

        logger.info(f"Constructed var_art samples: {dc_variant_stock['var_art'].head(5).tolist()}")

    # ═══════════════════════════════════════════════
    # STEP 5: Prepare store_stock for size allocator
    # ═══════════════════════════════════════════════
    # SizeAllocator expects store_stock with [st_cd, var_art, stock_qty]
    # We don't have variant-level store stock from Snowflake (only gencolor level)
    # Pass empty DataFrame — store stock at variant level not available
    store_stock_variant = pd.DataFrame(columns=['st_cd', 'var_art', 'stock_qty'])
    logger.info("Store stock at variant level: empty (only gencolor level available)")

    # ═══════════════════════════════════════════════
    # STEP 6: Run SizeAllocator
    # ═══════════════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STEP 6: Run SizeAllocator.allocate()")
    logger.info("=" * 60)

    allocator = SizeAllocator(settings)
    t_alloc = time.time()
    result = allocator.allocate(
        option_assignments=option_assignments,
        dc_variant_stock=dc_variant_stock,
        bgt_size=bgt_size,
        store_stock=store_stock_variant,
        majcat=MAJCAT,
    )
    alloc_time = time.time() - t_alloc
    logger.info(f"Size allocation completed in {alloc_time:.1f}s")

    if result.empty:
        logger.error("Size allocation returned empty DataFrame!")
        return

    # ═══════════════════════════════════════════════
    # STEP 7: Report results
    # ═══════════════════════════════════════════════
    logger.info("=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)

    total_variants = len(result)
    total_qty = result['alloc_qty'].sum()
    total_value = (result['alloc_qty'] * result['mrp']).sum() if 'mrp' in result.columns else 0
    total_short = result['short_qty'].sum() if 'short_qty' in result.columns else 0

    logger.info(f"Total variant rows: {total_variants:,}")
    logger.info(f"Total allocated qty: {total_qty:,}")
    logger.info(f"Total value (MRP): Rs {total_value:,.0f}")
    logger.info(f"Total short qty: {total_short:,}")
    logger.info(f"Unique stores: {result['st_cd'].nunique()}")
    logger.info(f"Unique articles (gen_art_color): {result['gen_art_color'].nunique()}")
    logger.info(f"Unique variants (var_art): {result['var_art'].nunique()}")

    # Size breakdown
    logger.info("\n--- SIZE BREAKDOWN ---")
    size_summary = result.groupby('sz').agg(
        rows=('alloc_qty', 'count'),
        total_qty=('alloc_qty', 'sum'),
        avg_qty=('alloc_qty', 'mean'),
        total_short=('short_qty', 'sum'),
    ).sort_values('total_qty', ascending=False)
    logger.info(f"\n{size_summary.to_string()}")

    # Fill rate
    if 'fill_rate_pct' in result.columns:
        avg_fill = result['fill_rate_pct'].mean()
        logger.info(f"\nAverage fill rate: {avg_fill:.1f}%")

    # Sample store
    logger.info("\n--- SAMPLE STORE ---")
    sample_store = result['st_cd'].value_counts().index[0]  # store with most rows
    store_data = result[result['st_cd'] == sample_store].sort_values('sz')
    logger.info(f"Store {sample_store} ({len(store_data)} variant rows):")
    cols_to_show = ['gen_art_color', 'var_art', 'sz', 'alloc_qty', 'bgt_sz_cont_pct', 'dc_sz_stock', 'fill_rate_pct']
    cols_avail = [c for c in cols_to_show if c in store_data.columns]
    logger.info(f"\n{store_data[cols_avail].to_string()}")

    store_total = store_data['alloc_qty'].sum()
    store_value = (store_data['alloc_qty'] * store_data['mrp']).sum() if 'mrp' in store_data.columns else 0
    logger.info(f"Store {sample_store} total: {store_total} units, Rs {store_value:,.0f}")

    # Per-article size curve for one article
    logger.info("\n--- SAMPLE ARTICLE SIZE CURVE ---")
    sample_gac = store_data['gen_art_color'].iloc[0]
    art_data = result[result['gen_art_color'] == sample_gac].groupby('sz').agg(
        total_qty=('alloc_qty', 'sum'),
        stores=('st_cd', 'nunique'),
    ).sort_values('total_qty', ascending=False)
    logger.info(f"Article {sample_gac} across all stores:")
    logger.info(f"\n{art_data.to_string()}")

    total_time = time.time() - t_start
    logger.info(f"\n{'='*60}")
    logger.info(f"TOTAL PIPELINE TIME: {total_time:.1f}s")
    logger.info(f"{'='*60}")


if __name__ == '__main__':
    main()
