"""
Report Builder — Multi-level allocation reports.

Generates 5 levels of summary from allocation results:
  Level 1: Company Summary  (1 row per MAJCAT)
  Level 2: Store Summary    (1 row per store)
  Level 3: Store x Article  (1 row per assignment)
  Level 4: Variant Detail   (1 row per size allocation)
  Level 5: MSA Article Summary (1 row per DC article)

Usage:
    reports = build_reports(assignments, variants, budget, msa_dc_stock, majcat)
"""
import logging
import time
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val, default: float = 0.0) -> float:
    try:
        v = float(val)
        return v if np.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return default


def _pct(numerator: float, denominator: float) -> float:
    """Return percentage rounded to 1 decimal, safe against zero-division."""
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator * 100, 1)


def _compute_mbq_per_opt(budget: pd.DataFrame) -> pd.Series:
    """Return a Series indexed by st_cd with MBQ-per-option for each store."""
    if budget.empty:
        return pd.Series(dtype=float)
    store_mbq = budget.groupby("st_cd")["mbq"].max()
    store_opts = budget.groupby("st_cd")["opt_count"].sum().clip(lower=1)
    return (store_mbq / store_opts).rename("mbq_per_opt")


# ---------------------------------------------------------------------------
# Short / Excess qty per assignment
# ---------------------------------------------------------------------------

def compute_short_excess(assignments: pd.DataFrame, budget: pd.DataFrame) -> pd.DataFrame:
    """
    Add short_qty and excess_qty columns to assignments.

    Short  = max(0, mbq_per_opt - store_stock - dispatch)
    Excess = max(0, store_stock - mbq_per_opt)
    """
    if assignments.empty:
        return assignments.copy()

    df = assignments.copy()
    mbq_map = _compute_mbq_per_opt(budget)

    df["_mbq_opt"] = df["st_cd"].map(mbq_map).fillna(0)
    df["short_qty"] = np.maximum(0, df["_mbq_opt"] - df["st_stock"] - df["disp_q"]).astype(int)
    df["excess_qty"] = np.maximum(0, df["st_stock"] - df["_mbq_opt"]).astype(int)
    df.drop(columns=["_mbq_opt"], inplace=True)
    return df


# ---------------------------------------------------------------------------
# Level 1: Company Summary
# ---------------------------------------------------------------------------

def _build_company_summary(
    assignments: pd.DataFrame,
    budget: pd.DataFrame,
    msa_dc_stock: pd.DataFrame,
    majcat: str,
    duration_s: float,
) -> Dict[str, Any]:
    """One row per MAJCAT — aggregate KPIs across all stores."""
    total_slots = _safe_int(budget["opt_count"].sum()) if not budget.empty else 0

    l_mask = assignments["art_status"] == "L"
    mix_mask = assignments["art_status"] == "MIX"
    newl_mask = assignments["art_status"] == "NEW_L"

    l_count = int(l_mask.sum())
    mix_articles = int(mix_mask.sum())
    mix_slots = int(
        assignments.loc[mix_mask].groupby(["st_cd", "opt_no"]).ngroups
    ) if mix_articles > 0 else 0
    newl_count = int(newl_mask.sum())

    slots_before_dc = l_count + mix_slots
    slots_after_dc = l_count + mix_slots + newl_count

    # DC metrics
    total_dispatch = _safe_float(assignments["disp_q"].sum())
    dispatched_gacs = set(
        assignments.loc[assignments["disp_q"] > 0, "gen_art_color"].unique()
    )

    dc_total_qty = 0.0
    dc_total_arts = 0
    dc_arts_used = 0
    if msa_dc_stock is not None and not msa_dc_stock.empty:
        dc_total_qty = _safe_float(msa_dc_stock["dc_stock"].sum())
        dc_total_arts = len(msa_dc_stock)
        dc_arts_used = len(dispatched_gacs & set(msa_dc_stock["gen_art_color"]))

    total_store_stock = _safe_float(assignments["st_stock"].sum())
    total_mbq = _safe_float(budget["mbq"].sum()) if not budget.empty else 0.0
    short_qty = int(assignments["short_qty"].sum()) if "short_qty" in assignments.columns else 0
    excess_qty = int(assignments["excess_qty"].sum()) if "excess_qty" in assignments.columns else 0
    total_value = round(float((assignments["disp_q"] * assignments["mrp"]).sum()), 2)

    return {
        "majcat": majcat,
        "total_slots": total_slots,
        "l_count": l_count,
        "mix_articles": mix_articles,
        "mix_slots": mix_slots,
        "new_l": newl_count,
        "fill_before_dc_pct": _pct(slots_before_dc, total_slots),
        "fill_after_dc_pct": _pct(slots_after_dc, total_slots),
        "dc_qty_dispatched": int(total_dispatch),
        "dc_qty_total": int(dc_total_qty),
        "dc_articles_used": dc_arts_used,
        "dc_articles_total": dc_total_arts,
        "total_store_stock": int(total_store_stock),
        "total_mbq": int(total_mbq),
        "short_qty": short_qty,
        "excess_qty": excess_qty,
        "total_dispatch": int(total_dispatch),
        "total_value": total_value,
        "duration_s": round(duration_s, 2),
    }


# ---------------------------------------------------------------------------
# Level 2: Store Summary
# ---------------------------------------------------------------------------

def _build_store_summary(
    assignments: pd.DataFrame,
    budget: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """One row per store — slot usage, fill rates, dispatch, short/excess."""
    if assignments.empty:
        return []

    store_budget = budget.groupby("st_cd").agg(
        total_slots=("opt_count", "sum"),
        mbq=("mbq", "max"),
    ).reset_index() if not budget.empty else pd.DataFrame(columns=["st_cd", "total_slots", "mbq"])

    rows = []
    for st_cd, grp in assignments.groupby("st_cd"):
        l_mask = grp["art_status"] == "L"
        mix_mask = grp["art_status"] == "MIX"
        newl_mask = grp["art_status"] == "NEW_L"

        l_count = int(l_mask.sum())
        mix_articles = int(mix_mask.sum())
        mix_slots = int(grp.loc[mix_mask].groupby("opt_no").ngroups) if mix_articles > 0 else 0
        newl_count = int(newl_mask.sum())

        bgt = store_budget[store_budget["st_cd"] == st_cd]
        total_slots = int(bgt["total_slots"].iloc[0]) if not bgt.empty else 0
        mbq = _safe_float(bgt["mbq"].iloc[0]) if not bgt.empty else 0.0

        slots_before = l_count + mix_slots
        slots_after = l_count + mix_slots + newl_count
        empty_slots = max(0, total_slots - slots_after)

        store_stock = _safe_float(grp["st_stock"].sum())
        dispatch_qty = _safe_float(grp["disp_q"].sum())
        short_qty = int(grp["short_qty"].sum()) if "short_qty" in grp.columns else 0
        excess_qty = int(grp["excess_qty"].sum()) if "excess_qty" in grp.columns else 0
        value = round(float((grp["disp_q"] * grp["mrp"]).sum()), 2)

        # Optimization percentages (how well L and MIX articles are utilized)
        l_opt_pct = _pct(l_count, total_slots) if total_slots > 0 else 0.0
        mix_opt_pct = _pct(mix_slots, total_slots) if total_slots > 0 else 0.0

        rows.append({
            "st_cd": st_cd,
            "total_slots": total_slots,
            "l_count": l_count,
            "mix_articles": mix_articles,
            "mix_slots": mix_slots,
            "new_l": newl_count,
            "fill_before_pct": _pct(slots_before, total_slots),
            "fill_after_pct": _pct(slots_after, total_slots),
            "store_stock": int(store_stock),
            "mbq": int(mbq),
            "dispatch_qty": int(dispatch_qty),
            "short_qty": short_qty,
            "excess_qty": excess_qty,
            "empty_slots": empty_slots,
            "l_opt_pct": l_opt_pct,
            "mix_opt_pct": mix_opt_pct,
            "value": value,
        })

    return sorted(rows, key=lambda r: r["st_cd"])


# ---------------------------------------------------------------------------
# Level 3: Store x Article Detail
# ---------------------------------------------------------------------------

def _build_article_detail(assignments: pd.DataFrame) -> List[Dict[str, Any]]:
    """One row per assignment — full article-level detail."""
    if assignments.empty:
        return []

    cols_map = {
        "st_cd": "st_cd",
        "opt_no": "opt_no",
        "gen_art_color": "gen_art_color",
        "gen_art": "gen_art",
        "color": "color",
        "art_status": "art_status",
        "total_score": "score",
        "st_stock": "store_stock",
        "disp_q": "dispatch_qty",
        "dc_stock_before": "dc_stock_before",
        "dc_stock_after": "dc_stock_after",
        "mrp": "mrp",
        "short_qty": "short_qty",
        "excess_qty": "excess_qty",
    }

    rows = []
    for _, r in assignments.iterrows():
        row = {}
        for src, dst in cols_map.items():
            if src in r.index:
                row[dst] = _safe_int(r[src]) if src in (
                    "opt_no", "st_stock", "disp_q", "short_qty", "excess_qty",
                    "dc_stock_before", "dc_stock_after",
                ) else r[src]
            else:
                row[dst] = 0 if dst in ("short_qty", "excess_qty") else ""

        # Computed value
        row["value"] = round(_safe_float(r.get("disp_q", 0)) * _safe_float(r.get("mrp", 0)), 2)

        # Optional enrichment columns (may not be in every run)
        row["vendor"] = r.get("vendor_code", r.get("vendor", ""))
        row["fabric"] = r.get("fabric", "")

        # MBQ per option (carried from assignments if present)
        row["mbq_per_opt"] = _safe_float(r.get("_mbq_opt", r.get("mbq_per_opt", 0)))

        rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Level 4: Variant Detail (size-level)
# ---------------------------------------------------------------------------

def _build_variant_detail(variants: pd.DataFrame) -> List[Dict[str, Any]]:
    """One row per size allocation."""
    if variants is None or variants.empty:
        return []

    rows = []
    for _, r in variants.iterrows():
        rows.append({
            "st_cd": r.get("st_cd", ""),
            "gen_art_color": r.get("gen_art_color", ""),
            "var_art": r.get("var_art", ""),
            "size": r.get("sz", ""),
            "alloc_qty": _safe_int(r.get("alloc_qty", 0)),
            "mrp": _safe_float(r.get("mrp", 0)),
            "value": round(
                _safe_float(r.get("alloc_qty", 0)) * _safe_float(r.get("mrp", 0)), 2
            ),
            "dc_size_stock_before": _safe_int(r.get("dc_sz_stock_before", 0)),
            "dc_size_stock_after": _safe_int(r.get("dc_sz_stock_after", 0)),
        })

    return rows


# ---------------------------------------------------------------------------
# Level 5: MSA Article Summary (DC utilization)
# ---------------------------------------------------------------------------

def _build_msa_summary(
    assignments: pd.DataFrame,
    msa_dc_stock: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """One row per DC article — stock utilization across all stores."""
    if msa_dc_stock is None or msa_dc_stock.empty:
        return []

    # Aggregate dispatch per DC article from assignments
    if not assignments.empty and "gen_art_color" in assignments.columns:
        dispatched = assignments[assignments["disp_q"] > 0].groupby("gen_art_color").agg(
            dc_dispatched=("disp_q", "sum"),
            stores_allocated=("st_cd", "nunique"),
            avg_score=("total_score", "mean"),
        ).reset_index()
    else:
        dispatched = pd.DataFrame(
            columns=["gen_art_color", "dc_dispatched", "stores_allocated", "avg_score"]
        )

    # Merge with DC stock
    msa = msa_dc_stock[["gen_art_color", "dc_stock"]].copy()
    msa = msa.rename(columns={"dc_stock": "dc_stock_total"})
    msa = msa.merge(dispatched, on="gen_art_color", how="left")
    msa["dc_dispatched"] = msa["dc_dispatched"].fillna(0).astype(int)
    msa["stores_allocated"] = msa["stores_allocated"].fillna(0).astype(int)
    msa["avg_score"] = msa["avg_score"].fillna(0).round(1)
    msa["dc_remaining"] = (msa["dc_stock_total"] - msa["dc_dispatched"]).clip(lower=0).astype(int)
    msa["pct_utilized"] = msa.apply(
        lambda row: _pct(row["dc_dispatched"], row["dc_stock_total"]), axis=1
    )

    rows = []
    for _, r in msa.iterrows():
        rows.append({
            "gen_art_color": r["gen_art_color"],
            "dc_stock_total": int(r["dc_stock_total"]),
            "dc_dispatched": int(r["dc_dispatched"]),
            "dc_remaining": int(r["dc_remaining"]),
            "stores_allocated_to": int(r["stores_allocated"]),
            "avg_score": float(r["avg_score"]),
            "pct_utilized": float(r["pct_utilized"]),
        })

    return sorted(rows, key=lambda r: -r["dc_dispatched"])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_reports(
    assignments: pd.DataFrame,
    variants: pd.DataFrame,
    budget: pd.DataFrame,
    msa_dc_stock: pd.DataFrame,
    majcat: str,
    duration_s: float = 0.0,
) -> Dict[str, Any]:
    """
    Build all 5 report levels from allocation results.

    Args:
        assignments: Option-level assignments from GlobalGreedyFiller.
            Required columns: st_cd, gen_art_color, gen_art, color, art_status,
            total_score, disp_q, mrp, st_stock, opt_no, dc_stock_before, dc_stock_after.
        variants: Size-level allocations from size_allocator.
            Columns: st_cd, gen_art_color, var_art, sz, alloc_qty, mrp,
            dc_sz_stock_before, dc_sz_stock_after.
        budget: Budget cascade from Snowflake ALLOC_BUDGET_CASCADE.
            Columns: st_cd, seg, opt_count, mbq.
        msa_dc_stock: DC article stock from Snowflake MSA_ARTICLES.
            Columns: gen_art_color, dc_stock.
        majcat: The MAJCAT being reported on.
        duration_s: Pipeline execution time in seconds.

    Returns:
        Dict with keys:
            company_summary  — single dict (Level 1)
            store_summary    — list of dicts (Level 2)
            article_detail   — list of dicts (Level 3)
            variant_detail   — list of dicts (Level 4)
            msa_summary      — list of dicts (Level 5)
    """
    t0 = time.time()

    # Ensure we have DataFrames (not None)
    if assignments is None:
        assignments = pd.DataFrame()
    if variants is None:
        variants = pd.DataFrame()
    if budget is None:
        budget = pd.DataFrame()

    # Compute short/excess before building reports
    if not assignments.empty and not budget.empty:
        assignments = compute_short_excess(assignments, budget)

    company = _build_company_summary(assignments, budget, msa_dc_stock, majcat, duration_s)
    stores = _build_store_summary(assignments, budget)
    articles = _build_article_detail(assignments)
    variant_rows = _build_variant_detail(variants)
    msa = _build_msa_summary(assignments, msa_dc_stock)

    build_time = round(time.time() - t0, 3)
    logger.info(
        f"[report_builder] {majcat}: built 5 reports in {build_time}s — "
        f"L1={1} row, L2={len(stores)} stores, L3={len(articles)} assignments, "
        f"L4={len(variant_rows)} variants, L5={len(msa)} DC articles"
    )

    return {
        "company_summary": company,
        "store_summary": stores,
        "article_detail": articles,
        "variant_detail": variant_rows,
        "msa_summary": msa,
    }
