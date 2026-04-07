"""
Engine 3: Global Greedy Option Filler
The CORE algorithm that replaces the 8-level waterfall.

Takes scored (article, store) pairs sorted by score descending,
walks through them top-down, filling option slots and deducting DC stock.

Key insight: by processing globally across ALL stores (not per-store),
the highest-scoring articles naturally go to their best-match stores first.
Lower scores fill remaining slots as fallback — no waterfall needed.
"""
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class StoreSlots:
    """Tracks option slot state for a single store-segment."""
    st_cd: str
    majcat: str
    seg: str
    total_slots: int
    filled_slots: int = 0
    assignments: List[dict] = field(default_factory=list)

    @property
    def remaining(self) -> int:
        return max(0, self.total_slots - self.filled_slots)

    @property
    def is_full(self) -> bool:
        return self.filled_slots >= self.total_slots


class GlobalGreedyFiller:
    """
    Fill option slots across ALL stores simultaneously using global greedy approach.
    Process scored pairs in descending score order — highest score wins the slot.
    """

    def __init__(self, settings: Dict[str, str]):
        self.settings = settings
        self.multi_opt_enabled = settings.get('multi_option_enabled', 'true') == 'true'
        self.multi_opt_min_score = int(settings.get('multi_option_min_score', 150))
        self.multi_opt_max_slots = int(settings.get('multi_option_max_slots', 3))
        self.max_colors_per_store = int(settings.get('max_colors_per_store', 5))
        self.min_score_threshold = int(settings.get('min_score_threshold', 0))

    def fill(
        self,
        scored_pairs: pd.DataFrame,
        budget_cascade: pd.DataFrame,
        majcat: str,
        store_stock_gencolor: pd.DataFrame = None,
    ) -> pd.DataFrame:
        """
        Global greedy option filling with proper L-ART → MIX waterfall.

        Waterfall (matching Excel process):
          Phase 1: L-ART — articles already in store fill slots first
          Phase 2: Continuation — L-ART with DC stock get replenishment priority
          Phase 3: MIX — new articles from DC fill remaining empty slots

        Args:
            scored_pairs: From Engine 2, sorted by total_score DESC.
            budget_cascade: From Engine 1.
            majcat: The MAJCAT being processed.
            store_stock_gencolor: From Snowflake FACT_STOCK_GENCOLOR.
                                  Columns: [st_cd, gen_art_color, stock_qty]
        """
        if scored_pairs.empty or budget_cascade.empty:
            logger.warning(f"[{majcat}] Empty input — nothing to fill")
            return pd.DataFrame()

        if store_stock_gencolor is None:
            store_stock_gencolor = pd.DataFrame()

        logger.info(f"[{majcat}] Waterfall fill: {len(scored_pairs)} scored pairs")

        # ── Build store stock lookup ──
        # Key: (st_cd, gen_art_color) → stock_qty
        store_stock_map = {}
        store_l_art_set = {}  # st_cd → set of gen_art_colors in store
        if not store_stock_gencolor.empty:
            # Filter to articles that exist in scored pairs (same MAJCAT)
            scored_gacs = set(scored_pairs['gen_art_color'].unique())
            relevant_stock = store_stock_gencolor[
                store_stock_gencolor['gen_art_color'].isin(scored_gacs)
            ]
            for _, r in relevant_stock.iterrows():
                st = r['st_cd']
                gac = r['gen_art_color']
                qty = float(r.get('stock_qty', 0) or 0)
                if qty > 0:
                    store_stock_map[(st, gac)] = qty
                    if st not in store_l_art_set:
                        store_l_art_set[st] = set()
                    store_l_art_set[st].add(gac)
            logger.info(f"[{majcat}] Store stock: {len(store_stock_map):,} store×article pairs, "
                        f"{len(store_l_art_set)} stores with L-ART")

        # ── Initialize slot trackers ──
        slot_map: Dict[str, StoreSlots] = {}
        store_total_slots: Dict[str, int] = {}
        store_mbq: Dict[str, int] = {}
        store_bgt_disp: Dict[str, float] = {}  # total budget dispatch qty per store
        for _, row in budget_cascade.iterrows():
            st = row['st_cd']
            seg = str(row.get('seg', '')).strip()
            opt_count = int(row.get('opt_count', 0))
            mbq_val = int(row.get('mbq', 0) or 0)
            bgt_disp = float(row.get('bgt_disp_q', 0) or 0)
            if opt_count > 0:
                key = f"{st}|{seg}"
                slot_map[key] = StoreSlots(
                    st_cd=st, majcat=majcat, seg=seg, total_slots=opt_count
                )
                store_total_slots[st] = store_total_slots.get(st, 0) + opt_count
                store_mbq[st] = max(store_mbq.get(st, 0), mbq_val)
                store_bgt_disp[st] = store_bgt_disp.get(st, 0) + bgt_disp

        total_slots = sum(s.total_slots for s in slot_map.values())
        logger.info(f"[{majcat}] Total option slots across all stores: {total_slots}")

        # ── Initialize DC stock tracker ──
        dc_stock_tracker: Dict[str, int] = {}
        for _, row in scored_pairs.drop_duplicates('gen_art_color').iterrows():
            gac = row['gen_art_color']
            dc_stock_tracker[gac] = int(row.get('dc_stock_qty', 0))

        # ══════════════════════════════════════════════════════
        # PHASE 1: L-ART — Fill slots with existing store stock
        # Articles already IN the store fill option slots first.
        # This is the key difference from the old MIX-only approach.
        # ══════════════════════════════════════════════════════
        assignments = []
        l_art_filled = 0
        cont_filled = 0
        store_filled_arts: Dict[str, set] = {}  # st_cd → set of gen_art_colors already assigned

        if store_l_art_set:
            # Get scored article data for L-ART lookup
            scored_lookup = {}
            for _, row in scored_pairs.drop_duplicates('gen_art_color').iterrows():
                scored_lookup[row['gen_art_color']] = row

            for slot_key, slots in slot_map.items():
                st_cd = slots.st_cd
                seg = slots.seg
                l_arts = store_l_art_set.get(st_cd, set())
                if not l_arts:
                    continue

                # Sort L-ART by score (if scored) then by store stock qty
                l_art_scored = []
                for gac in l_arts:
                    score_row = scored_lookup.get(gac)
                    st_stock = store_stock_map.get((st_cd, gac), 0)
                    has_dc = dc_stock_tracker.get(gac, 0) > 0
                    l_art_scored.append({
                        'gac': gac,
                        'score': int(score_row['total_score']) if score_row is not None else 50,
                        'st_stock': st_stock,
                        'has_dc': has_dc,
                        'score_row': score_row,
                    })
                # Continuation (has DC stock) first, then by score
                l_art_scored.sort(key=lambda x: (-x['has_dc'], -x['score'], -x['st_stock']))

                if st_cd not in store_filled_arts:
                    store_filled_arts[st_cd] = set()

                for art_info in l_art_scored:
                    if slots.is_full:
                        break
                    gac = art_info['gac']
                    if gac in store_filled_arts[st_cd]:
                        continue

                    score_row = art_info['score_row']
                    gen_art = score_row['gen_art'] if score_row is not None else gac.split('_')[0]
                    color = score_row['color'] if score_row is not None else ''
                    score = art_info['score']
                    is_cont = art_info['has_dc']

                    opt_no = slots.filled_slots + 1
                    art_status = 'L' if is_cont else 'L_ONLY'  # L = continuation, L_ONLY = store only

                    dc_before = dc_stock_tracker.get(gac, 0)
                    # L-ART dispatch: need = MBQ per option - store stock for this article
                    # MBQ per option = total MBQ / total option slots
                    mbq_per_opt = store_mbq.get(st_cd, 0) / max(store_total_slots.get(st_cd, 1), 1)
                    st_stk = art_info['st_stock']
                    need = max(0, round(mbq_per_opt - st_stk))
                    if is_cont and dc_before > 0 and need > 0:
                        actual_disp = min(need, dc_before)
                        dc_stock_tracker[gac] = max(dc_before - actual_disp, 0)
                    else:
                        actual_disp = 0  # L_ONLY or fully stocked: no dispatch needed
                    assignment = {
                        'st_cd': st_cd, 'majcat': majcat, 'seg': seg,
                        'opt_no': opt_no, 'gen_art_color': gac,
                        'gen_art': gen_art, 'color': color,
                        'total_score': score, 'art_status': art_status,
                        'is_multi_opt': 0,
                        'disp_q': actual_disp,
                        'mbq': store_mbq.get(st_cd, 0),
                        'mrp': float(score_row.get('mrp', 0) or 0) if score_row is not None else 0,
                        'bgt_sales_per_day': 0,
                        'dc_stock_before': dc_before,
                        'dc_stock_after': dc_stock_tracker.get(gac, 0),
                        'st_stock': art_info['st_stock'],
                    }
                    assignments.append(assignment)
                    slots.filled_slots += 1
                    store_filled_arts[st_cd].add(gac)
                    l_art_filled += 1
                    if is_cont:
                        cont_filled += 1

        l_art_slots_remaining = sum(s.remaining for s in slot_map.values())
        logger.info(f"[{majcat}] Phase 1 L-ART: {l_art_filled} slots filled "
                    f"({cont_filled} continuation with DC stock), "
                    f"{l_art_slots_remaining} slots remaining for MIX")

        # ══════════════════════════════════════════════════════
        # PHASE 2: MIX — Fill remaining slots with NEW articles from DC
        # Only articles NOT already in the store (not L-ART)
        #
        # Equitable distribution (matches Excel algorithm):
        #   Sort by SEG + L-OPT% (ascending) + reverse_option_number
        #   i.e. stores with LOWEST fill rate get priority.
        #
        # Implementation: round-robin over store-segments sorted by
        # fill rate ascending.  For each under-served store-segment,
        # pick the highest-scored available article that fits.
        # ══════════════════════════════════════════════════════

        # ── Track per-store article assignments (for max color constraint) ──
        # key = "st_cd|gen_art" → count of colors allocated
        store_art_colors: Dict[str, int] = {}
        # key = "st_cd|gen_art_color" → number of slots this article has at this store
        store_art_slots: Dict[str, int] = {}

        # Pre-populate from L-ART assignments
        for a in assignments:
            art_key = f"{a['st_cd']}|{a['gen_art']}"
            store_art_colors[art_key] = store_art_colors.get(art_key, 0) + 1
            slot_key = f"{a['st_cd']}|{a['gen_art_color']}"
            store_art_slots[slot_key] = store_art_slots.get(slot_key, 0) + 1

        # ── Build per-store scored article lists (sorted by score DESC) ──
        # For each store, pre-filter and sort the candidate articles once.
        scored_pairs = scored_pairs.copy()
        scored_pairs = scored_pairs.sort_values('total_score', ascending=False).reset_index(drop=True)

        # Build lookup: st_cd → list of candidate article rows (sorted by score desc)
        store_candidates: Dict[str, List[dict]] = {}
        # Also track a cursor per store so we don't re-scan from the start
        store_cursor: Dict[str, int] = {}

        for _, row in scored_pairs.iterrows():
            st_cd = row['st_cd']
            if st_cd not in store_candidates:
                store_candidates[st_cd] = []
            store_candidates[st_cd].append(row.to_dict())

        for st_cd in store_candidates:
            store_cursor[st_cd] = 0

        # ── Walk: round-robin over store-segments by fill rate (ascending) ──
        filled_count = 0
        skipped_no_stock = 0
        skipped_max_color = 0
        skipped_min_score = 0
        skipped_l_art = 0

        # We loop until no store-segment made progress in a full pass
        made_progress = True
        while made_progress:
            made_progress = False

            # Sort store-segments by fill rate ascending (least-filled first),
            # then by seg for stable ordering (matches Excel SEG sort).
            # Only consider store-segments that still have empty slots.
            open_slots = [
                (key, ss) for key, ss in slot_map.items() if not ss.is_full
            ]
            if not open_slots:
                break

            # fill_rate = filled / total (lower = higher priority)
            open_slots.sort(key=lambda x: (
                x[1].filled_slots / max(x[1].total_slots, 1),  # fill rate ASC
                x[1].seg,                                        # seg ASC
                -x[1].remaining,                                 # more remaining first
            ))

            for slot_key, slots in open_slots:
                if slots.is_full:
                    continue

                st_cd = slots.st_cd
                seg = slots.seg
                candidates = store_candidates.get(st_cd, [])
                cursor = store_cursor.get(st_cd, 0)

                # Walk through this store's candidates from the cursor
                assigned_one = False
                while cursor < len(candidates):
                    row = candidates[cursor]
                    cursor += 1

                    gac = row['gen_art_color']
                    gen_art = row.get('gen_art', '')
                    color = row.get('color', '')
                    row_seg = str(row.get('seg', '')).strip()
                    score = int(row.get('total_score', 0))

                    # ── Skip if already assigned as L-ART ──
                    if st_cd in store_filled_arts and gac in store_filled_arts[st_cd]:
                        skipped_l_art += 1
                        continue

                    # ── Check minimum score threshold ──
                    if score < self.min_score_threshold and row.get('is_st_specific', 0) != 1:
                        skipped_min_score += 1
                        continue

                    # ── Check DC stock ──
                    dc_qty = dc_stock_tracker.get(gac, 0)
                    if dc_qty <= 0:
                        skipped_no_stock += 1
                        continue

                    # ── Segment matching: prefer same seg, but allow fallback ──
                    target_slots = slots
                    if row_seg and row_seg != seg:
                        # Check if there's a matching segment slot for this store
                        alt_key = f"{st_cd}|{row_seg}"
                        alt_slots = slot_map.get(alt_key)
                        if alt_slots and not alt_slots.is_full:
                            target_slots = alt_slots
                        # else: use the current under-filled segment as fallback

                    if target_slots.is_full:
                        continue

                    # ── Check max colors per store for same generic article ──
                    color_key = f"{st_cd}|{gen_art}"
                    current_colors = store_art_colors.get(color_key, 0)
                    if current_colors >= self.max_colors_per_store:
                        skipped_max_color += 1
                        continue

                    # ── Check if this exact article-color already assigned to this store ──
                    art_slot_key = f"{st_cd}|{gac}"
                    existing_at_store = store_art_slots.get(art_slot_key, 0)
                    if existing_at_store > 0 and not self.multi_opt_enabled:
                        continue
                    if existing_at_store >= self.multi_opt_max_slots:
                        continue

                    # ── ALLOCATE! ──
                    is_multi = existing_at_store > 0
                    opt_no = target_slots.filled_slots + 1 if not is_multi else target_slots.filled_slots

                    dc_before = dc_qty
                    dc_stock_tracker[gac] = max(0, dc_qty - 1)

                    # Determine art status
                    if row.get('is_st_specific', 0) == 1:
                        art_status = 'ST_SPEC'
                    elif row.get('priority_type') == 'NATIONAL_HERO':
                        art_status = 'HERO'
                    elif row.get('priority_type') == 'CORE_FOCUS':
                        art_status = 'FOCUS'
                    else:
                        art_status = 'MIX'

                    # MIX dispatch: new article, need = MBQ per option (no store stock)
                    mbq_per_opt = store_mbq.get(st_cd, 0) / max(store_total_slots.get(st_cd, 1), 1)
                    dc_remaining = dc_stock_tracker.get(gac, 0)
                    actual_disp = min(round(mbq_per_opt), max(dc_remaining, 0))

                    assignment = {
                        'st_cd': st_cd,
                        'majcat': majcat,
                        'seg': target_slots.seg,
                        'opt_no': opt_no,
                        'gen_art_color': gac,
                        'gen_art': gen_art,
                        'color': color,
                        'total_score': score,
                        'art_status': art_status,
                        'is_multi_opt': 1 if is_multi else 0,
                        'disp_q': actual_disp,
                        'mbq': store_mbq.get(st_cd, 0),
                        'mrp': float(row.get('mrp', 0) or 0),
                        'bgt_sales_per_day': 0,
                        'dc_stock_before': dc_before,
                        'dc_stock_after': max(dc_remaining - actual_disp, 0),
                    }
                    dc_stock_tracker[gac] = max(dc_remaining - actual_disp, 0)
                    assignments.append(assignment)

                    if not is_multi:
                        target_slots.filled_slots += 1
                    store_art_colors[color_key] = current_colors + 1
                    store_art_slots[art_slot_key] = existing_at_store + 1
                    if st_cd not in store_filled_arts:
                        store_filled_arts[st_cd] = set()
                    store_filled_arts[st_cd].add(gac)
                    filled_count += 1
                    assigned_one = True
                    break  # Move to next store-segment (equitable round-robin)

                store_cursor[st_cd] = cursor
                if assigned_one:
                    made_progress = True

        # ── Multi-option pass: after equitable fill, high-score articles
        #    can claim additional slots at already-full stores ──
        if self.multi_opt_enabled:
            for st_cd, candidates in store_candidates.items():
                for row in candidates:
                    gac = row['gen_art_color']
                    score = int(row.get('total_score', 0))
                    if score < self.multi_opt_min_score:
                        continue
                    art_slot_key = f"{st_cd}|{gac}"
                    existing_at_store = store_art_slots.get(art_slot_key, 0)
                    if existing_at_store == 0 or existing_at_store >= self.multi_opt_max_slots:
                        continue
                    dc_qty = dc_stock_tracker.get(gac, 0)
                    if dc_qty <= 0:
                        continue

                    gen_art = row.get('gen_art', '')
                    color = row.get('color', '')
                    seg = str(row.get('seg', '')).strip()

                    # Find a slot for this store
                    target_slots = None
                    for k, v in slot_map.items():
                        if k.startswith(f"{st_cd}|"):
                            target_slots = v
                            break
                    if target_slots is None:
                        continue

                    dc_before = dc_qty
                    dc_stock_tracker[gac] = max(0, dc_qty - 1)

                    if row.get('is_st_specific', 0) == 1:
                        art_status = 'ST_SPEC'
                    elif row.get('priority_type') == 'NATIONAL_HERO':
                        art_status = 'HERO'
                    elif row.get('priority_type') == 'CORE_FOCUS':
                        art_status = 'FOCUS'
                    else:
                        art_status = 'MIX'

                    mbq_per_opt = store_mbq.get(st_cd, 0) / max(store_total_slots.get(st_cd, 1), 1)
                    dc_remaining = dc_stock_tracker.get(gac, 0)
                    actual_disp = min(round(mbq_per_opt), max(dc_remaining, 0))

                    assignment = {
                        'st_cd': st_cd,
                        'majcat': majcat,
                        'seg': target_slots.seg,
                        'opt_no': target_slots.filled_slots,
                        'gen_art_color': gac,
                        'gen_art': gen_art,
                        'color': color,
                        'total_score': score,
                        'art_status': art_status,
                        'is_multi_opt': 1,
                        'disp_q': actual_disp,
                        'mbq': store_mbq.get(st_cd, 0),
                        'mrp': float(row.get('mrp', 0) or 0),
                        'bgt_sales_per_day': 0,
                        'dc_stock_before': dc_before,
                        'dc_stock_after': max(dc_remaining - actual_disp, 0),
                    }
                    dc_stock_tracker[gac] = max(dc_remaining - actual_disp, 0)
                    assignments.append(assignment)
                    store_art_slots[art_slot_key] = existing_at_store + 1
                    filled_count += 1

        # ── Report ──
        filled_slots = sum(s.filled_slots for s in slot_map.values())
        empty_slots = total_slots - filled_slots

        logger.info(
            f"[{majcat}] Allocation complete: "
            f"L-ART={l_art_filled} (cont={cont_filled}) + MIX={filled_count} = {l_art_filled+filled_count} total, "
            f"{filled_slots}/{total_slots} slots filled ({empty_slots} empty), "
            f"Skipped: {skipped_l_art} already-L-ART, {skipped_no_stock} no-stock, "
            f"{skipped_max_color} max-color"
        )

        if not assignments:
            return pd.DataFrame()

        result = pd.DataFrame(assignments)

        # ── Renumber opt_no sequentially per store-segment ──
        result = result.sort_values(['st_cd', 'seg', 'total_score'], ascending=[True, True, False])
        result['opt_no'] = result.groupby(['st_cd', 'seg']).cumcount() + 1

        return result
