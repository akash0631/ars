"""
Engine 3: Global Greedy Option Filler — L/MIX/Continuation/New-L Waterfall

Terminology:
  L (Listed)     = Article in store WITH stock, AND DC has stock too (can be continued)
  MIX            = Article in store WITH stock, but DC does NOT have it (dying article)
  Continuation   = L article where store_stock < MBQ/opt → top-up dispatched from DC
  New L          = New article from DC filling empty slots. Not currently in store.

The waterfall:
  Phase 1: Tag store articles to slots (L or MIX)
  Phase 2: Calculate continuation dispatch for L articles (store_stock < MBQ/opt)
  Phase 3: Fill empty slots with New L from DC (highest-scored first)
  Phase 4: Audit — warn if any MSA article has remaining DC stock but went unallocated
"""
import logging
from typing import Dict, List, Optional, Set, Tuple
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

    @property
    def fill_rate(self) -> float:
        return self.filled_slots / max(self.total_slots, 1)


class GlobalGreedyFiller:
    """
    Fill option slots across ALL stores using the L/MIX/Continuation/New-L waterfall.

    DC stock is shared globally — a single dc_stock_tracker dict is deducted
    as dispatches are assigned across all stores. Stores are processed in
    fill-rate-ascending order (equitable distribution).
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
        msa_dc_stock: pd.DataFrame = None,
    ) -> pd.DataFrame:
        """
        L/MIX/Continuation/New-L waterfall option filling.

        Args:
            scored_pairs: From Engine 2, with columns:
                [st_cd, gen_art_color, gen_art, color, seg, total_score,
                 dc_stock_qty, mrp, is_st_specific, priority_type, ...]
            budget_cascade: From Engine 1, with columns:
                [st_cd, seg, opt_count, mbq, bgt_disp_q, ...]
            majcat: The MAJCAT being processed.
            store_stock_gencolor: From Snowflake FACT_STOCK_GENCOLOR.
                Columns: [st_cd, gen_art_color, stock_qty]
            msa_dc_stock: MSA DC stock DataFrame.
                Columns: [gen_art_color, dc_stock]
                If None, falls back to dc_stock_qty from scored_pairs.

        Returns:
            DataFrame of assignments with columns:
                [st_cd, majcat, seg, opt_no, gen_art_color, gen_art, color,
                 total_score, art_status, is_multi_opt, disp_q, mbq, mrp,
                 bgt_sales_per_day, dc_stock_before, dc_stock_after, st_stock]
        """
        if scored_pairs.empty or budget_cascade.empty:
            logger.warning(f"[{majcat}] Empty input -- nothing to fill")
            return pd.DataFrame()

        if store_stock_gencolor is None:
            store_stock_gencolor = pd.DataFrame()

        logger.info(f"[{majcat}] Waterfall fill: {len(scored_pairs)} scored pairs")

        # ================================================================
        # SETUP: Build lookups, slot trackers, and DC stock tracker
        # ================================================================

        # -- DC stock tracker (global, shared across all stores) --
        dc_stock_tracker: Dict[str, float] = {}
        if msa_dc_stock is not None and not msa_dc_stock.empty:
            # MSA is the authoritative source for DC stock
            for _, row in msa_dc_stock.iterrows():
                gac = row['gen_art_color']
                dc_stock_tracker[gac] = float(row.get('dc_stock', 0) or 0)
            logger.info(f"[{majcat}] DC stock from MSA: {len(dc_stock_tracker)} articles")
        else:
            # Fallback: use dc_stock_qty from scored_pairs
            for _, row in scored_pairs.drop_duplicates('gen_art_color').iterrows():
                gac = row['gen_art_color']
                dc_stock_tracker[gac] = float(row.get('dc_stock_qty', 0) or 0)
            logger.info(f"[{majcat}] DC stock from scored_pairs (fallback): "
                        f"{len(dc_stock_tracker)} articles")

        # Set of all articles DC has stock for (MSA articles)
        msa_articles: Set[str] = {gac for gac, qty in dc_stock_tracker.items() if qty > 0}
        logger.info(f"[{majcat}] MSA articles with DC stock: {len(msa_articles)}")

        # -- Store stock lookup --
        # (st_cd, gen_art_color) -> stock_qty
        store_stock_map: Dict[Tuple[str, str], float] = {}
        # st_cd -> set of gen_art_colors in store
        store_articles: Dict[str, Set[str]] = {}
        if not store_stock_gencolor.empty:
            scored_gacs = set(scored_pairs['gen_art_color'].unique())
            relevant = store_stock_gencolor[
                store_stock_gencolor['gen_art_color'].isin(scored_gacs)
            ]
            for _, r in relevant.iterrows():
                st = r['st_cd']
                gac = r['gen_art_color']
                qty = float(r.get('stock_qty', 0) or 0)
                if qty > 0:
                    store_stock_map[(st, gac)] = qty
                    store_articles.setdefault(st, set()).add(gac)
            logger.info(f"[{majcat}] Store stock: {len(store_stock_map):,} (store x article) pairs "
                        f"across {len(store_articles)} stores")

        # -- Scored article lookup (for gen_art, color, mrp, score) --
        scored_lookup: Dict[str, dict] = {}
        for _, row in scored_pairs.drop_duplicates('gen_art_color').iterrows():
            scored_lookup[row['gen_art_color']] = row.to_dict()

        # -- Initialize slot trackers from budget cascade --
        slot_map: Dict[str, StoreSlots] = {}      # "st_cd|seg" -> StoreSlots
        store_total_opts: Dict[str, int] = {}      # st_cd -> total option count
        store_mbq: Dict[str, float] = {}           # st_cd -> MBQ (total)
        for _, row in budget_cascade.iterrows():
            st = row['st_cd']
            seg = str(row.get('seg', '')).strip()
            opt_count = int(row.get('opt_count', 0))
            mbq_val = float(row.get('mbq', 0) or 0)
            if opt_count > 0:
                key = f"{st}|{seg}"
                slot_map[key] = StoreSlots(
                    st_cd=st, majcat=majcat, seg=seg, total_slots=opt_count
                )
                store_total_opts[st] = store_total_opts.get(st, 0) + opt_count
                store_mbq[st] = max(store_mbq.get(st, 0), mbq_val)

        total_slots = sum(s.total_slots for s in slot_map.values())
        logger.info(f"[{majcat}] Total option slots: {total_slots} across "
                    f"{len(set(s.st_cd for s in slot_map.values()))} stores")

        # -- Per-store tracking --
        # Assignments list (will become the output DataFrame)
        assignments: List[dict] = []
        # st_cd -> set of gen_art_colors already tagged to a slot
        store_filled_arts: Dict[str, Set[str]] = {}
        # "st_cd|gen_art" -> count of colors allocated (for max_colors_per_store)
        store_art_colors: Dict[str, int] = {}

        # Helper: MBQ per option for a store
        def mbq_per_opt(st_cd: str) -> float:
            return store_mbq.get(st_cd, 0) / max(store_total_opts.get(st_cd, 1), 1)

        # ================================================================
        # PHASE 1: Tag store articles to slots (L or MIX)
        # ================================================================
        # For each store, look at articles from FACT_STOCK_GENCOLOR.
        # - If article also in MSA (DC has stock) -> status = "L"
        # - If article NOT in MSA (DC empty) -> status = "MIX" (dying)
        # Fill slots up to total_opt_count per store.
        # Process stores sorted by fill rate ascending (equitable).
        # ================================================================

        phase1_l = 0
        phase1_mix = 0

        # Get all stores that have stock and also have slots
        stores_with_slots = set(s.st_cd for s in slot_map.values())
        stores_to_process = sorted(
            stores_with_slots & set(store_articles.keys()),
            key=lambda st: 0  # All start at fill_rate 0, so just process them
        )

        for st_cd in stores_to_process:
            arts_in_store = store_articles.get(st_cd, set())
            if not arts_in_store:
                continue

            # Build sorted list: L articles first (has DC stock), then MIX, by score desc
            art_list = []
            for gac in arts_in_store:
                has_dc = gac in msa_articles
                info = scored_lookup.get(gac)
                score = int(info['total_score']) if info else 50
                st_stock = store_stock_map.get((st_cd, gac), 0)
                art_list.append((gac, has_dc, score, st_stock, info))

            # Sort: L articles first (has_dc=True), then by score descending
            art_list.sort(key=lambda x: (-x[1], -x[2], -x[3]))

            store_filled_arts.setdefault(st_cd, set())

            # Find slot(s) for this store
            store_slot_keys = [k for k, v in slot_map.items() if v.st_cd == st_cd]

            for gac, has_dc, score, st_stock, info in art_list:
                # Find a slot that isn't full
                target_slots = None
                for sk in store_slot_keys:
                    if not slot_map[sk].is_full:
                        target_slots = slot_map[sk]
                        break
                if target_slots is None:
                    break  # All slots for this store are full

                if gac in store_filled_arts[st_cd]:
                    continue

                # Check max colors per generic article
                gen_art = info['gen_art'] if info else gac.split('_')[0]
                color_key = f"{st_cd}|{gen_art}"
                if store_art_colors.get(color_key, 0) >= self.max_colors_per_store:
                    continue

                art_status = 'L' if has_dc else 'MIX'
                opt_no = target_slots.filled_slots + 1
                color = info['color'] if info else ''
                mrp = float(info.get('mrp', 0) or 0) if info else 0

                # Phase 1 does NOT dispatch -- just tags. Dispatch happens in Phase 2.
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
                    'is_multi_opt': 0,
                    'disp_q': 0,       # Will be set in Phase 2 for L articles
                    'mbq': store_mbq.get(st_cd, 0),
                    'mrp': mrp,
                    'bgt_sales_per_day': 0,
                    'dc_stock_before': 0,
                    'dc_stock_after': 0,
                    'st_stock': st_stock,
                }
                assignments.append(assignment)
                target_slots.filled_slots += 1
                store_filled_arts[st_cd].add(gac)
                store_art_colors[color_key] = store_art_colors.get(color_key, 0) + 1

                if has_dc:
                    phase1_l += 1
                else:
                    phase1_mix += 1

        phase1_total = phase1_l + phase1_mix
        slots_after_p1 = sum(s.remaining for s in slot_map.values())
        logger.info(f"[{majcat}] Phase 1 done: {phase1_total} slots tagged "
                    f"(L={phase1_l}, MIX={phase1_mix}), "
                    f"{slots_after_p1} empty slots remaining")

        # ================================================================
        # PHASE 2: Continuation dispatch for L articles
        # ================================================================
        # For each L article: if store_stock < MBQ/opt, top up from DC.
        # need = MBQ/opt - store_stock, capped by DC stock remaining.
        # DC stock tracker deducts globally.
        # Process stores by fill rate ascending for equitable DC deduction.
        # ================================================================

        phase2_dispatched = 0
        phase2_qty = 0

        # Sort L assignments by store fill rate ascending for equitable DC distribution
        l_assignments = [
            (i, a) for i, a in enumerate(assignments) if a['art_status'] == 'L'
        ]
        # Compute store fill rate at this point
        store_fill_rate = {}
        for s in slot_map.values():
            store_fill_rate[s.st_cd] = min(
                store_fill_rate.get(s.st_cd, 999),
                s.fill_rate
            )
        l_assignments.sort(key=lambda x: store_fill_rate.get(x[1]['st_cd'], 0))

        for idx, asgn in l_assignments:
            gac = asgn['gen_art_color']
            st_cd = asgn['st_cd']
            st_stock = asgn['st_stock']
            mbq_opt = mbq_per_opt(st_cd)

            dc_before = dc_stock_tracker.get(gac, 0)
            need = max(0, round(mbq_opt - st_stock))

            if need > 0 and dc_before > 0:
                actual_disp = min(need, dc_before)
                dc_stock_tracker[gac] = max(0, dc_before - actual_disp)
            else:
                actual_disp = 0

            # Update the assignment in-place
            assignments[idx]['disp_q'] = actual_disp
            assignments[idx]['dc_stock_before'] = dc_before
            assignments[idx]['dc_stock_after'] = dc_stock_tracker.get(gac, 0)

            if actual_disp > 0:
                phase2_dispatched += 1
                phase2_qty += actual_disp

        logger.info(f"[{majcat}] Phase 2 continuation: {phase2_dispatched} L articles topped up, "
                    f"{phase2_qty} total units dispatched from DC")

        # ================================================================
        # PHASE 3: Fill empty slots with New L from DC
        # ================================================================
        # Empty slots = total_opt_count - (L + MIX tagged in Phase 1)
        # For each empty slot, pick highest-scored DC article NOT already in store.
        # Status = "NEW_L", dispatch = MBQ/opt capped by DC stock.
        # Equitable: round-robin stores sorted by fill rate ascending.
        # ================================================================

        # Build per-store candidate lists (articles NOT already in store, from scored_pairs)
        scored_pairs_sorted = scored_pairs.sort_values(
            'total_score', ascending=False
        ).reset_index(drop=True)

        # st_cd -> list of candidate rows (pre-sorted by score desc)
        store_candidates: Dict[str, List[dict]] = {}
        for _, row in scored_pairs_sorted.iterrows():
            st_cd = row['st_cd']
            if st_cd not in stores_with_slots:
                continue
            store_candidates.setdefault(st_cd, []).append(row.to_dict())

        # Per-store cursor to avoid rescanning
        store_cursor: Dict[str, int] = {st: 0 for st in store_candidates}

        phase3_filled = 0
        skipped_no_stock = 0
        skipped_max_color = 0
        skipped_min_score = 0

        # Round-robin: loop until no store made progress in a full pass
        made_progress = True
        while made_progress:
            made_progress = False

            # Get open store-segments sorted by fill rate ascending
            open_slots = [
                (key, ss) for key, ss in slot_map.items() if not ss.is_full
            ]
            if not open_slots:
                break

            open_slots.sort(key=lambda x: (
                x[1].fill_rate,   # Lowest fill rate first (equitable)
                x[1].seg,         # Stable ordering by segment
                -x[1].remaining,  # More remaining slots first
            ))

            for slot_key, slots in open_slots:
                if slots.is_full:
                    continue

                st_cd = slots.st_cd
                candidates = store_candidates.get(st_cd, [])
                cursor = store_cursor.get(st_cd, 0)
                store_filled_arts.setdefault(st_cd, set())

                assigned_one = False
                while cursor < len(candidates):
                    row = candidates[cursor]
                    cursor += 1

                    gac = row['gen_art_color']
                    gen_art = row.get('gen_art', '')
                    color = row.get('color', '')
                    score = int(row.get('total_score', 0))

                    # Skip if already assigned to this store (L, MIX, or prior New L)
                    if gac in store_filled_arts[st_cd]:
                        continue

                    # Skip if article is already in store stock (it was tagged in Phase 1,
                    # or it has store stock but wasn't tagged because slots were full)
                    if (st_cd, gac) in store_stock_map:
                        continue

                    # Check minimum score threshold
                    if score < self.min_score_threshold and row.get('is_st_specific', 0) != 1:
                        skipped_min_score += 1
                        continue

                    # Check DC stock available
                    dc_qty = dc_stock_tracker.get(gac, 0)
                    if dc_qty <= 0:
                        skipped_no_stock += 1
                        continue

                    # Check max colors per generic article at this store
                    color_key = f"{st_cd}|{gen_art}"
                    if store_art_colors.get(color_key, 0) >= self.max_colors_per_store:
                        skipped_max_color += 1
                        continue

                    # -- ALLOCATE as NEW_L --
                    opt_no = slots.filled_slots + 1
                    dc_before = dc_qty
                    mbq_opt = mbq_per_opt(st_cd)
                    actual_disp = min(round(mbq_opt), dc_before)
                    dc_stock_tracker[gac] = max(0, dc_before - actual_disp)

                    assignment = {
                        'st_cd': st_cd,
                        'majcat': majcat,
                        'seg': slots.seg,
                        'opt_no': opt_no,
                        'gen_art_color': gac,
                        'gen_art': gen_art,
                        'color': color,
                        'total_score': score,
                        'art_status': 'NEW_L',
                        'is_multi_opt': 0,
                        'disp_q': actual_disp,
                        'mbq': store_mbq.get(st_cd, 0),
                        'mrp': float(row.get('mrp', 0) or 0),
                        'bgt_sales_per_day': 0,
                        'dc_stock_before': dc_before,
                        'dc_stock_after': dc_stock_tracker.get(gac, 0),
                        'st_stock': 0,
                    }
                    assignments.append(assignment)
                    slots.filled_slots += 1
                    store_filled_arts[st_cd].add(gac)
                    store_art_colors[color_key] = store_art_colors.get(color_key, 0) + 1
                    phase3_filled += 1
                    assigned_one = True
                    break  # Move to next store-segment (equitable round-robin)

                store_cursor[st_cd] = cursor
                if assigned_one:
                    made_progress = True

        slots_after_p3 = sum(s.remaining for s in slot_map.values())
        logger.info(f"[{majcat}] Phase 3 New L: {phase3_filled} new articles from DC, "
                    f"{slots_after_p3} empty slots remaining. "
                    f"Skipped: {skipped_no_stock} no-stock, {skipped_max_color} max-color, "
                    f"{skipped_min_score} min-score")

        # ================================================================
        # PHASE 4: Audit -- check no MSA article is left unallocated
        # ================================================================
        # Every article in MSA ideally goes to at least one store.
        # If DC stock remains after all stores filled, log a warning.
        # ================================================================

        all_allocated_gacs: Set[str] = set()
        for a in assignments:
            all_allocated_gacs.add(a['gen_art_color'])

        unallocated_msa = []
        remaining_dc_stock = 0
        for gac in msa_articles:
            remaining = dc_stock_tracker.get(gac, 0)
            if remaining > 0 and gac not in all_allocated_gacs:
                unallocated_msa.append((gac, remaining))
                remaining_dc_stock += remaining

        if unallocated_msa:
            logger.warning(
                f"[{majcat}] Phase 4 audit: {len(unallocated_msa)} MSA articles with "
                f"{remaining_dc_stock:.0f} total DC stock went UNALLOCATED to any store. "
                f"Top 5: {unallocated_msa[:5]}"
            )
        else:
            total_dc_remaining = sum(
                v for v in dc_stock_tracker.values() if v > 0
            )
            logger.info(f"[{majcat}] Phase 4 audit: All MSA articles allocated. "
                        f"Remaining DC stock (partially allocated): {total_dc_remaining:.0f}")

        # ================================================================
        # SUMMARY
        # ================================================================

        filled_slots = sum(s.filled_slots for s in slot_map.values())
        empty_slots = total_slots - filled_slots
        total_disp = sum(a['disp_q'] for a in assignments)

        logger.info(
            f"[{majcat}] ALLOCATION COMPLETE: "
            f"L={phase1_l} + MIX={phase1_mix} + Continuation={phase2_dispatched} + "
            f"New_L={phase3_filled} | "
            f"{filled_slots}/{total_slots} slots filled ({empty_slots} empty) | "
            f"Total dispatch: {total_disp:.0f} units"
        )

        if not assignments:
            return pd.DataFrame()

        result = pd.DataFrame(assignments)

        # Renumber opt_no sequentially per store-segment
        result = result.sort_values(
            ['st_cd', 'seg', 'art_status', 'total_score'],
            ascending=[True, True, True, False]
        )
        result['opt_no'] = result.groupby(['st_cd', 'seg']).cumcount() + 1

        return result
