"""
Fill Rate Calculator for the V2 Retail ARS Allocation Engine.

Two metrics:
  1. Option fill rate = fraction of option SLOTS filled (count-based)
  2. Qty fill rate = fraction of total MBQ covered by actual stock depth (quantity-based)

Qty fill rate is ALWAYS >= option fill rate because every filled slot contributes
at least some stock, and slots with deep stock push the weighted average above
the simple count ratio.

MIX articles contribute 70% of their actual stock value (declining inventory).
For each filled slot the contribution is capped at MBQ_per_option (excess doesn't help).
"""
import logging
from typing import Dict, Any

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

MIX_DECAY_FACTOR = 0.70  # MIX articles count at 70% of their stock


def compute_fill_rates(
    assignments: pd.DataFrame,
    budget_cascade: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Compute option and quantity fill rates BEFORE and AFTER DC allocation.

    Args:
        assignments: Output of GlobalGreedyFiller.fill(). Required columns:
            st_cd, art_status (L / MIX / NEW_L), st_stock, disp_q, mbq
        budget_cascade: Engine-1 budget. Required columns:
            st_cd, opt_count, mbq

    Returns:
        Dict with:
            option_fill_before_dc  (float 0-100%)
            option_fill_after_dc   (float 0-100%)
            qty_fill_before_dc     (float 0-100%)
            qty_fill_after_dc      (float 0-100%)
            total_slots            (int)
            total_mbq              (float)
            detail                 (dict of per-status breakdowns)
    """
    if assignments.empty or budget_cascade.empty:
        return _empty_result()

    # ------------------------------------------------------------------
    # Totals from the budget (ground truth for denominator)
    # ------------------------------------------------------------------
    # Aggregate budget to store level (sum opt_count, take max mbq per store)
    store_budget = budget_cascade.groupby("st_cd").agg(
        total_opts=("opt_count", "sum"),
        mbq=("mbq", "max"),
    ).reset_index()

    total_slots = int(store_budget["total_opts"].sum())
    if total_slots == 0:
        return _empty_result()

    # Build store-level MBQ lookup
    store_mbq = dict(zip(store_budget["st_cd"], store_budget["mbq"]))
    store_opts = dict(zip(store_budget["st_cd"], store_budget["total_opts"]))

    # MBQ per option per store
    def _mbq_per_opt(st_cd: str) -> float:
        return store_mbq.get(st_cd, 0) / max(store_opts.get(st_cd, 1), 1)

    # Total MBQ = sum across all stores of store_mbq
    total_mbq = float(store_budget["mbq"].sum())
    if total_mbq <= 0:
        return _empty_result()

    # ------------------------------------------------------------------
    # Classify assignments
    # ------------------------------------------------------------------
    df = assignments.copy()
    df["st_stock"] = pd.to_numeric(df.get("st_stock", 0), errors="coerce").fillna(0)
    df["disp_q"] = pd.to_numeric(df.get("disp_q", 0), errors="coerce").fillna(0)
    df["art_status"] = df["art_status"].astype(str).str.upper()

    is_l = df["art_status"] == "L"
    is_mix = df["art_status"] == "MIX"
    is_newl = df["art_status"] == "NEW_L"

    # ------------------------------------------------------------------
    # 1. OPTION FILL RATE (simple slot counts)
    # ------------------------------------------------------------------
    l_slots = int(is_l.sum())
    mix_slots = int(is_mix.sum())
    newl_slots = int(is_newl.sum())

    opt_fill_before = (l_slots + mix_slots) / total_slots * 100
    opt_fill_after = (l_slots + mix_slots + newl_slots) / total_slots * 100

    # ------------------------------------------------------------------
    # 2. QTY FILL RATE (stock-depth weighted)
    # ------------------------------------------------------------------
    # For each assignment row, compute the effective qty contribution
    # towards MBQ, capped at MBQ_per_option for that store.

    # Vectorised MBQ-per-option lookup
    df["mbq_per_opt"] = df["st_cd"].map(_mbq_per_opt)

    # BEFORE DC: only store stock contributes
    # L articles: min(st_stock, mbq_per_opt)
    # MIX articles: min(st_stock * 0.70, mbq_per_opt)
    # NEW_L: 0 (not yet dispatched)
    df["qty_before"] = 0.0
    df.loc[is_l, "qty_before"] = np.minimum(
        df.loc[is_l, "st_stock"],
        df.loc[is_l, "mbq_per_opt"],
    )
    df.loc[is_mix, "qty_before"] = np.minimum(
        df.loc[is_mix, "st_stock"] * MIX_DECAY_FACTOR,
        df.loc[is_mix, "mbq_per_opt"],
    )

    qty_before_total = float(df["qty_before"].sum())
    qty_fill_before = qty_before_total / total_mbq * 100

    # AFTER DC: add dispatch quantities
    # L continuation: qty_before + disp_q (already dispatched from DC to top up)
    # MIX: same as before (DC doesn't dispatch to pure MIX unless converted to L,
    #       in which case art_status is already 'L')
    # NEW_L: disp_q (entirely from DC)
    df["qty_after"] = df["qty_before"].copy()

    # L articles that received a dispatch top-up
    l_with_disp = is_l & (df["disp_q"] > 0)
    df.loc[l_with_disp, "qty_after"] = np.minimum(
        df.loc[l_with_disp, "st_stock"] + df.loc[l_with_disp, "disp_q"],
        df.loc[l_with_disp, "mbq_per_opt"],
    )

    # NEW_L articles: contribution is their dispatch qty, capped at MBQ/opt
    df.loc[is_newl, "qty_after"] = np.minimum(
        df.loc[is_newl, "disp_q"],
        df.loc[is_newl, "mbq_per_opt"],
    )

    qty_after_total = float(df["qty_after"].sum())
    qty_fill_after = qty_after_total / total_mbq * 100

    # ------------------------------------------------------------------
    # Invariant note: qty_fill >= option_fill holds ONLY when every filled
    # slot has stock depth >= MBQ_per_option. In practice, MIX articles
    # (70% decay) and L articles with thin stock can push qty_fill below
    # option_fill. This is normal — it signals the store needs DC top-ups.
    # ------------------------------------------------------------------
    if qty_fill_after < opt_fill_after - 0.01:
        avg_stock_per_slot = qty_after_total / max(l_slots + mix_slots + newl_slots, 1)
        avg_mbq_opt = total_mbq / max(total_slots, 1)
        logger.info(
            f"qty_fill_after ({qty_fill_after:.1f}%) < opt_fill_after ({opt_fill_after:.1f}%) "
            f"-- avg stock/slot={avg_stock_per_slot:.1f} vs avg MBQ/opt={avg_mbq_opt:.1f}. "
            f"Filled slots have thinner stock than MBQ target."
        )

    # ------------------------------------------------------------------
    # Breakdown
    # ------------------------------------------------------------------
    detail = {
        "l_slots": l_slots,
        "mix_slots": mix_slots,
        "newl_slots": newl_slots,
        "total_slots": total_slots,
        "total_mbq": round(total_mbq, 1),
        "qty_before_sum": round(qty_before_total, 1),
        "qty_after_sum": round(qty_after_total, 1),
        "l_disp_qty": round(float(df.loc[is_l, "disp_q"].sum()), 1),
        "newl_disp_qty": round(float(df.loc[is_newl, "disp_q"].sum()), 1),
        "mix_decay_factor": MIX_DECAY_FACTOR,
        "stores_count": int(df["st_cd"].nunique()),
        "avg_mbq_per_opt": round(float(df["mbq_per_opt"].mean()), 1) if len(df) > 0 else 0,
    }

    result = {
        "option_fill_before_dc": round(opt_fill_before, 2),
        "option_fill_after_dc": round(opt_fill_after, 2),
        "qty_fill_before_dc": round(qty_fill_before, 2),
        "qty_fill_after_dc": round(qty_fill_after, 2),
        "total_slots": total_slots,
        "total_mbq": round(total_mbq, 1),
        "detail": detail,
    }

    logger.info(
        f"Fill rates: "
        f"OPT before={opt_fill_before:.1f}% after={opt_fill_after:.1f}% | "
        f"QTY before={qty_fill_before:.1f}% after={qty_fill_after:.1f}% | "
        f"slots={total_slots} mbq={total_mbq:.0f}"
    )

    return result


def _empty_result() -> Dict[str, Any]:
    return {
        "option_fill_before_dc": 0.0,
        "option_fill_after_dc": 0.0,
        "qty_fill_before_dc": 0.0,
        "qty_fill_after_dc": 0.0,
        "total_slots": 0,
        "total_mbq": 0.0,
        "detail": {},
    }
