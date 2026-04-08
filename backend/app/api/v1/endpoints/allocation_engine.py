"""
Allocation Engine API Endpoints (v2 — Snowflake-direct, NO Azure SQL dependency)

GET  /allocation-engine/snowflake/test                — test Snowflake connection
GET  /allocation-engine/majcats                       — list available MAJCATs
POST /allocation-engine/run                           — run allocation for specific MAJCATs
POST /allocation-engine/run-all                       — run all 242 MAJCATs (sequential, progress tracked)
GET  /allocation-engine/runs                          — list past runs from memory
GET  /allocation-engine/results/{run_id}/summary      — run summary
GET  /allocation-engine/results/{run_id}/assignments  — option assignments (paginated)
GET  /allocation-engine/results/{run_id}/variants     — variant allocations (paginated)
"""
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import pandas as pd
import httpx
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/allocation-engine", tags=["Allocation Engine"])

# ── Supabase credentials for size contribution ──
SUPABASE_URL = "https://pymdqnnwwxrgeolvgvgv.supabase.co"
SUPABASE_SERVICE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB5bWRxbm53d3hyZ2VvbHZndmd2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIs"
    "ImlhdCI6MTc1MzMzNTQ3NiwiZXhwIjoyMDY4OTExNDc2fQ."
    "goCXYYC_gxKLm-1reF37I7Dpr-GA1fpMgDj4queofVY"
)

# ── Default engine settings (no DB dependency) ──
DEFAULT_SETTINGS: Dict[str, str] = {
    "min_score_threshold": "0",
    "multi_option_enabled": "true",
    "multi_option_min_score": "150",
    "multi_option_max_slots": "3",
    "max_colors_per_store": "5",
}

# ── In-memory run results store ──
# _run_results[run_id] = {"summary": {...}, "assignments": [...], "variants": [...]}
_run_results: Dict[str, Dict[str, Any]] = {}


# ── Request Models ──

class RunRequest(BaseModel):
    majcats: Optional[List[str]] = None  # null = ["M_JEANS"] default
    rdc_code: str = "DH24"
    current_month: int = 4


# ── Helpers ──

def _fetch_bgt_size(majcat: str) -> pd.DataFrame:
    """
    Fetch size contribution % for a single MAJCAT from Supabase.
    Returns DataFrame with columns [majcat, sz, bgt_cont_pct].
    """
    try:
        url = (
            f"{SUPABASE_URL}/rest/v1/co_budget_company_major_category_size"
            f"?major_category=eq.{majcat}&select=size,sale_q_feb_2026"
        )
        headers = {
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        }
        resp = httpx.get(url, headers=headers, timeout=30.0)
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            logger.warning(f"[bgt_size] No size data from Supabase for {majcat}")
            return pd.DataFrame(columns=["majcat", "sz", "bgt_cont_pct"])

        df = pd.DataFrame(rows)
        total = df["sale_q_feb_2026"].sum()
        if total <= 0:
            return pd.DataFrame(columns=["majcat", "sz", "bgt_cont_pct"])

        df["bgt_cont_pct"] = df["sale_q_feb_2026"] / total
        df["majcat"] = majcat
        df.rename(columns={"size": "sz"}, inplace=True)
        logger.info(f"[bgt_size] {majcat}: {len(df)} sizes from Supabase")
        return df[["majcat", "sz", "bgt_cont_pct"]]
    except Exception as e:
        logger.warning(f"[bgt_size] Supabase fetch failed for {majcat}: {e}")
        return pd.DataFrame(columns=["majcat", "sz", "bgt_cont_pct"])


def _build_majcat_summary(
    majcat: str,
    scored_pairs: pd.DataFrame,
    assignments: pd.DataFrame,
    budget_cascade: pd.DataFrame,
    variants: pd.DataFrame,
    duration_s: float,
    msa_dc_stock: pd.DataFrame = None,
) -> Dict[str, Any]:
    """Build comprehensive summary dict for a single MAJCAT run."""
    if assignments.empty:
        return {
            "majcat": majcat,
            "status": "completed",
            "scored_count": len(scored_pairs),
            "slots_filled": 0,
            "l_art_count": 0,
            "continuation_count": 0,
            "mix_count": 0,
            "fill_rate": 0.0,
            "avg_score": 0.0,
            "mbq_min": 0,
            "mbq_max": 0,
            "stores": 0,
            "variants_count": 0,
            "total_qty": 0,
            "total_value": 0.0,
            "duration_s": duration_s,
        }

    total_slots = int(budget_cascade["opt_count"].sum()) if not budget_cascade.empty else 0

    # Count by status
    l_count = int((assignments["art_status"] == "L").sum())
    mix_articles = int((assignments["art_status"] == "MIX").sum())
    mix_slots = int(assignments[assignments["art_status"] == "MIX"].groupby(["st_cd", "opt_no"]).ngroups) if mix_articles > 0 else 0
    newl_count = int((assignments["art_status"] == "NEW_L").sum())

    slots_used = l_count + mix_slots + newl_count
    fill_before = round((l_count + mix_slots) / max(total_slots, 1) * 100, 1)
    fill_after = round(min(slots_used / max(total_slots, 1) * 100, 100), 1)

    # DC metrics
    total_dispatch = float(assignments["disp_q"].sum())
    dc_total_qty = float(msa_dc_stock["dc_stock"].sum()) if msa_dc_stock is not None and not msa_dc_stock.empty else 0
    dispatched_gacs = set(assignments[assignments["disp_q"] > 0]["gen_art_color"].unique())
    dc_total_arts = len(msa_dc_stock) if msa_dc_stock is not None and not msa_dc_stock.empty else 0
    dc_arts_used = len(dispatched_gacs & set(msa_dc_stock["gen_art_color"])) if dc_total_arts > 0 else 0

    avg_score = round(float(assignments["total_score"].mean()), 1)
    mbq_vals = budget_cascade["mbq"] if not budget_cascade.empty else pd.Series([0])
    stores = int(assignments["st_cd"].nunique())
    total_value = round(float((assignments["disp_q"] * assignments["mrp"]).sum()), 2)

    return {
        "majcat": majcat,
        "status": "completed",
        "scored_count": len(scored_pairs),
        "total_slots": total_slots,
        "l_count": l_count,
        "mix_articles": mix_articles,
        "mix_slots": mix_slots,
        "newl_count": newl_count,
        "slots_used": slots_used,
        "fill_before_dc": fill_before,
        "fill_after_dc": fill_after,
        "dc_qty_dispatched": int(total_dispatch),
        "dc_qty_total": int(dc_total_qty),
        "dc_arts_used": dc_arts_used,
        "dc_arts_total": dc_total_arts,
        "avg_score": avg_score,
        "mbq_min": int(mbq_vals.min()),
        "mbq_max": int(mbq_vals.max()),
        "stores": stores,
        "total_dispatch": int(total_dispatch),
        "total_value": total_value,
        "duration_s": duration_s,
        # Legacy fields for dashboard compat
        "slots_filled": slots_used,
        "l_art_count": l_count,
        "mix_count": mix_articles,
        "fill_rate": fill_after,
        "continuation_count": l_count,
        "variants_count": 0,
        "total_qty": int(total_dispatch),
    }


def _run_single_majcat(majcat: str, rdc_code: str, current_month: int) -> Dict[str, Any]:
    """
    Run the full Snowflake-direct allocation pipeline for one MAJCAT.
    Returns dict with keys: summary, assignments (list of dicts), variants (list of dicts).
    """
    from app.services.allocation import snowflake_loader as sf
    from app.services.allocation.option_filler import GlobalGreedyFiller
    # Size allocator imported in step 6 below

    t0 = time.time()

    try:
        # ── Step 1: Scored pairs from Snowflake (top 500 per store for speed) ──
        scored_pairs = sf.get_scored_pairs(majcat, top_n=500)
        if scored_pairs.empty:
            return {
                "summary": {"majcat": majcat, "status": "skipped", "reason": "no scored pairs",
                            "duration_s": round(time.time() - t0, 1)},
                "assignments": [],
                "variants": [],
            }

        # ── Step 2: Budget cascade from Snowflake ──
        budget_cascade = sf.get_budget_cascade(majcat)
        if budget_cascade.empty:
            return {
                "summary": {"majcat": majcat, "status": "skipped", "reason": "no budget cascade",
                            "duration_s": round(time.time() - t0, 1)},
                "assignments": [],
                "variants": [],
            }

        # ── Step 3: Store stock (L-ART) from Snowflake ──
        scored_gacs = scored_pairs["gen_art_color"].unique().tolist()
        store_stock = sf.get_store_stock(scored_gacs, majcat)

        # ── Step 4: MSA DC stock (V01) ──
        msa_dc_stock = sf.get_msa_dc_stock(majcat, rdc_code=rdc_code)

        # ── Step 5: Engine 3 — Global Greedy Fill ──
        filler = GlobalGreedyFiller(DEFAULT_SETTINGS)
        assignments = filler.fill(
            scored_pairs=scored_pairs,
            budget_cascade=budget_cascade,
            majcat=majcat,
            store_stock_gencolor=store_stock,
            msa_dc_stock=msa_dc_stock,
        )

        # ── Step 6: Engine 4 — Size Allocation (new: uses CONT_SZ + MSA size stock) ──
        variants = pd.DataFrame()
        if not assignments.empty:
            from app.services.allocation.size_allocator import allocate_sizes
            variants = allocate_sizes(assignments, majcat)

        duration_s = round(time.time() - t0, 1)

        summary = _build_majcat_summary(
            majcat, scored_pairs, assignments, budget_cascade, variants, duration_s, msa_dc_stock
        )

        assignments_list = assignments.to_dict(orient="records") if not assignments.empty else []
        variants_list = variants.to_dict(orient="records") if not variants.empty else []

        logger.info(
            f"[{majcat}] Done: {summary['slots_filled']} slots, "
            f"{summary['fill_rate']}% fill, {summary['variants_count']} variants in {duration_s}s"
        )

        return {
            "summary": summary,
            "assignments": assignments_list,
            "variants": variants_list,
        }

    except Exception as e:
        logger.exception(f"[{majcat}] Allocation failed: {e}")
        return {
            "summary": {
                "majcat": majcat,
                "status": "error",
                "error": str(e),
                "duration_s": round(time.time() - t0, 1),
            },
            "assignments": [],
            "variants": [],
        }


def _run_all_background(rdc_code: str, current_month: int, run_id: str):
    """Background task: run all MAJCATs sequentially with progress tracking."""
    from app.services.allocation import snowflake_loader as sf

    t0 = time.time()
    _run_results[run_id] = {
        "summary": {
            "run_id": run_id,
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "processed": 0,
            "total": 0,
        },
        "assignments": [],
        "variants": [],
    }

    try:
        majcats = sf.get_available_majcats()
        _run_results[run_id]["summary"]["total"] = len(majcats)

        all_summaries = []
        all_assignments = []
        all_variants = []

        for i, mc in enumerate(majcats):
            res = _run_single_majcat(mc, rdc_code, current_month)
            all_summaries.append(res["summary"])
            all_assignments.extend(res["assignments"])
            all_variants.extend(res["variants"])

            # Update progress
            _run_results[run_id]["summary"]["processed"] = i + 1
            _run_results[run_id]["summary"]["last_majcat"] = mc

        # Final summary
        completed = [s for s in all_summaries if s.get("status") == "completed"]
        errored = [s for s in all_summaries if s.get("status") == "error"]
        skipped = [s for s in all_summaries if s.get("status") == "skipped"]

        total_slots = sum(s.get("slots_filled", 0) for s in completed)
        total_variants = sum(s.get("variants_count", 0) for s in completed)
        total_qty = sum(s.get("total_qty", 0) for s in completed)
        total_value = sum(s.get("total_value", 0) for s in completed)
        avg_fill = round(
            sum(s.get("fill_rate", 0) for s in completed) / max(len(completed), 1), 1
        )

        _run_results[run_id] = {
            "summary": {
                "run_id": run_id,
                "status": "completed",
                "started_at": _run_results[run_id]["summary"].get("started_at"),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "total_majcats": len(majcats),
                "completed_count": len(completed),
                "errored_count": len(errored),
                "skipped_count": len(skipped),
                "total_slots_filled": total_slots,
                "total_variants": total_variants,
                "total_qty": total_qty,
                "total_value": total_value,
                "avg_fill_rate": avg_fill,
                "duration_s": round(time.time() - t0, 1),
                "majcat_details": all_summaries,
            },
            "assignments": all_assignments,
            "variants": all_variants,
        }

        logger.info(
            f"[run-all] {run_id}: {len(completed)} completed, "
            f"{len(errored)} errors, {total_slots} slots in {time.time() - t0:.0f}s"
        )

    except Exception as e:
        logger.exception(f"[run-all] {run_id} failed: {e}")
        _run_results[run_id] = {
            "summary": {
                "run_id": run_id,
                "status": "error",
                "error": str(e),
                "duration_s": round(time.time() - t0, 1),
            },
            "assignments": [],
            "variants": [],
        }


# ═══════════════════════════════════════════════════════════════════
#  ENDPOINTS (no auth required)
# ═══════════════════════════════════════════════════════════════════


@router.get("/snowflake/test")
async def test_snowflake():
    """Test Snowflake connectivity and return row counts."""
    from app.services.allocation import snowflake_loader as sf
    try:
        result = sf.test_connection()
        return {"success": True, "data": result}
    except Exception as e:
        logger.exception("Snowflake connection test failed")
        raise HTTPException(500, detail=f"Snowflake connection failed: {e}")


@router.get("/majcats")
async def list_majcats():
    """Get all available MAJCATs from Snowflake ARTICLE_SCORES."""
    from app.services.allocation import snowflake_loader as sf
    try:
        majcats = sf.get_available_majcats()
        return {"success": True, "count": len(majcats), "data": majcats}
    except Exception as e:
        logger.exception("Failed to fetch MAJCATs")
        raise HTTPException(500, detail=f"Failed to fetch MAJCATs: {e}")


@router.post("/run")
async def run_allocation(request: RunRequest):
    """
    Run Snowflake-direct allocation for specified MAJCATs.
    Stores results in memory and returns comprehensive summary per MAJCAT.
    """
    majcats = request.majcats or ["M_JEANS"]
    run_id = str(uuid.uuid4())[:12]
    t0 = time.time()

    all_summaries = []
    all_assignments = []
    all_variants = []

    for mc in majcats:
        res = _run_single_majcat(mc, request.rdc_code, request.current_month)
        all_summaries.append(res["summary"])
        all_assignments.extend(res["assignments"])
        all_variants.extend(res["variants"])

    # Store in memory
    _run_results[run_id] = {
        "summary": {
            "run_id": run_id,
            "status": "completed",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "majcats_requested": majcats,
            "duration_s": round(time.time() - t0, 1),
            "majcat_details": all_summaries,
        },
        "assignments": all_assignments,
        "variants": all_variants,
    }

    completed = [s for s in all_summaries if s.get("status") == "completed"]

    return {
        "success": True,
        "run_id": run_id,
        "summary": {
            "majcats_requested": len(majcats),
            "completed": len(completed),
            "errored": len([s for s in all_summaries if s.get("status") == "error"]),
            "skipped": len([s for s in all_summaries if s.get("status") == "skipped"]),
            "total_slots_filled": sum(s.get("slots_filled", 0) for s in completed),
            "total_variants": sum(s.get("variants_count", 0) for s in completed),
            "total_qty": sum(s.get("total_qty", 0) for s in completed),
            "total_value": sum(s.get("total_value", 0) for s in completed),
            "avg_fill_rate": round(
                sum(s.get("fill_rate", 0) for s in completed) / max(len(completed), 1), 1
            ),
            "duration_s": round(time.time() - t0, 1),
        },
        "data": all_summaries,
    }


@router.post("/run-all")
async def run_all_allocation(
    background_tasks: BackgroundTasks,
    rdc_code: str = "DH24",
    current_month: int = 4,
):
    """
    Run allocation for ALL MAJCATs sequentially in background.
    Returns immediately with a run_id for status polling.
    """
    run_id = str(uuid.uuid4())[:12]
    background_tasks.add_task(_run_all_background, rdc_code, current_month, run_id)
    return {
        "success": True,
        "message": "Allocation run started for ALL MAJCATs (sequential with progress tracking)",
        "data": {
            "run_id": run_id,
            "status": "started",
            "poll_url": f"/api/v1/allocation-engine/runs",
            "summary_url": f"/api/v1/allocation-engine/results/{run_id}/summary",
        },
    }


@router.get("/runs")
async def list_runs():
    """List all past runs from in-memory store."""
    runs = []
    for run_id, data in _run_results.items():
        summary = data.get("summary", {})
        runs.append({
            "run_id": run_id,
            "status": summary.get("status", "unknown"),
            "created_at": summary.get("created_at") or summary.get("started_at"),
            "completed_at": summary.get("completed_at"),
            "duration_s": summary.get("duration_s"),
            "processed": summary.get("processed"),
            "total": summary.get("total") or summary.get("total_majcats"),
            "assignments_count": len(data.get("assignments", [])),
            "variants_count": len(data.get("variants", [])),
        })

    # Most recent first
    runs.sort(key=lambda r: r.get("created_at") or "", reverse=True)
    return {"success": True, "count": len(runs), "data": runs}


@router.get("/results/{run_id}/summary")
async def get_run_summary(run_id: str):
    """Get summary of an allocation run from in-memory store."""
    if run_id not in _run_results:
        raise HTTPException(404, detail=f"Run {run_id} not found")

    return {"success": True, "data": _run_results[run_id]["summary"]}


@router.get("/results/{run_id}/assignments")
async def get_assignments(
    run_id: str,
    limit: int = 500,
    offset: int = 0,
    majcat: Optional[str] = None,
    st_cd: Optional[str] = None,
):
    """
    Get option assignments for a run with pagination.
    Filter by majcat and/or st_cd optionally.
    """
    if run_id not in _run_results:
        raise HTTPException(404, detail=f"Run {run_id} not found")

    assignments = _run_results[run_id].get("assignments", [])

    # Apply filters
    if majcat:
        assignments = [a for a in assignments if a.get("majcat") == majcat]
    if st_cd:
        assignments = [a for a in assignments if a.get("st_cd") == st_cd]

    total = len(assignments)
    page = assignments[offset: offset + limit]

    return {
        "success": True,
        "total": total,
        "offset": offset,
        "limit": limit,
        "count": len(page),
        "data": page,
    }


@router.get("/results/{run_id}/variants")
async def get_variants(
    run_id: str,
    limit: int = 500,
    offset: int = 0,
    majcat: Optional[str] = None,
    st_cd: Optional[str] = None,
    gen_art_color: Optional[str] = None,
):
    """
    Get variant/size allocations for a run with pagination.
    Filter by majcat, st_cd, or gen_art_color optionally.
    """
    if run_id not in _run_results:
        raise HTTPException(404, detail=f"Run {run_id} not found")

    variants = _run_results[run_id].get("variants", [])

    # Apply filters
    if majcat:
        variants = [v for v in variants if v.get("majcat") == majcat]
    if st_cd:
        variants = [v for v in variants if v.get("st_cd") == st_cd]
    if gen_art_color:
        variants = [v for v in variants if v.get("gen_art_color") == gen_art_color]

    total = len(variants)
    page = variants[offset: offset + limit]

    return {
        "success": True,
        "total": total,
        "offset": offset,
        "limit": limit,
        "count": len(page),
        "data": page,
    }
