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
        assignments: List[dict] = []
        store_filled_arts: Dict[str, Set[str]] = {}
        store_art_colors: Dict[str, int] = {}

        # ABSOLUTE slot counter per store (prevents overflow)
        store_slots_used: Dict[str, int] = {st: 0 for st in store_total_opts}

        def store_has_space(st_cd: str) -> bool:
            return store_slots_used.get(st_cd, 0) < store_total_opts.get(st_cd, 0)

        def use_store_slot(st_cd: str):
            store_slots_used[st_cd] = store_slots_used.get(st_cd, 0) + 1

        def mbq_per_opt(st_cd: str) -> float:
            return store_mbq.get(st_cd, 0) / max(store_total_opts.get(st_cd, 1), 1)

        # ================================================================
        # PHASE 1: Fill slots from STORE INVENTORY ONLY (no DC yet)
        # ================================================================
        # Step A: L articles with pipeline stock >= 90% of MBQ/opt → fill slots
        # Step B: Remaining MIX articles (< MBQ) merge/bundle into remaining slots
        # DC stock is NOT used in Phase 1 — that comes in Phase 2+3.
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

            # Classify store articles (NO DC involvement):
            #   L   = pipeline stock >= 90% of MBQ/opt (can display)
            #   MIX = pipeline stock < 90% of MBQ/opt (can't display alone)
            mbq_opt = mbq_per_opt(st_cd)
            l_threshold = mbq_opt * 0.9  # 90% of MBQ counts as displayable

            l_articles = []
            mix_articles = []
            for gac in arts_in_store:
                info = scored_lookup.get(gac)
                score = int(info['total_score']) if info else 50
                st_stock = store_stock_map.get((st_cd, gac), 0)

                if st_stock >= l_threshold and l_threshold > 0:
                    l_articles.append((gac, score, st_stock, info))
                else:
                    mix_articles.append((gac, score, st_stock, info))

            # Sort L by score desc, MIX by stock desc (biggest contribute first to bundling)
            l_articles.sort(key=lambda x: (-x[1], -x[2]))
            mix_articles.sort(key=lambda x: (-x[2], -x[1]))

            store_filled_arts.setdefault(st_cd, set())

            # Find slot(s) for this store
            store_slot_keys = [k for k, v in slot_map.items() if v.st_cd == st_cd]

            # Step A: Assign L articles (1 per slot, >= 90% MBQ)
            for gac, score, st_stock, info in l_articles:
                if not store_has_space(st_cd):
                    break
                target_slots = None
                for sk in store_slot_keys:
                    if not slot_map[sk].is_full:
                        target_slots = slot_map[sk]; break
                if target_slots is None:
                    break

                if gac in store_filled_arts[st_cd]:
                    continue
                gen_art = info['gen_art'] if info else str(gac).split('_')[0]
                color_key = f"{st_cd}|{gen_art}"
                if store_art_colors.get(color_key, 0) >= self.max_colors_per_store:
                    continue

                assignments.append({
                    'st_cd': st_cd, 'majcat': majcat, 'seg': target_slots.seg,
                    'opt_no': target_slots.filled_slots + 1,
                    'gen_art_color': gac, 'gen_art': gen_art,
                    'color': info['color'] if info else '',
                    'total_score': score, 'art_status': 'L', 'is_multi_opt': 0,
                    'disp_q': 0, 'mbq': store_mbq.get(st_cd, 0),
                    'mrp': float(info.get('mrp', 0) or 0) if info else 0,
                    'bgt_sales_per_day': 0, 'dc_stock_before': 0, 'dc_stock_after': 0,
                    'st_stock': st_stock,
                })
                target_slots.filled_slots += 1
                use_store_slot(st_cd)
                store_filled_arts[st_cd].add(gac)
                store_art_colors[color_key] = store_art_colors.get(color_key, 0) + 1
                phase1_l += 1

            # Step B: Bundle MIX articles into REMAINING slots
            remaining_slots_count = min(
                sum(1 for sk in store_slot_keys if not slot_map[sk].is_full),
                store_total_opts.get(st_cd, 0) - store_slots_used.get(st_cd, 0)
            )
            if remaining_slots_count > 0 and mbq_opt > 0:
                mix_cumulative_qty = 0
                mix_slots_used = 0
                current_mix_slot = None

                for gac, score, st_stock, info in mix_articles:
                    if gac in store_filled_arts.get(st_cd, set()):
                        continue

                    # Start new slot when cumulative reaches MBQ or first article
                    if current_mix_slot is None or mix_cumulative_qty >= mbq_opt:
                        if mix_slots_used >= remaining_slots_count or not store_has_space(st_cd):
                            break  # No more slots for MIX
                        current_mix_slot = None
                        for sk in store_slot_keys:
                            if not slot_map[sk].is_full:
                                current_mix_slot = slot_map[sk]; break
                        if current_mix_slot is None:
                            break
                        mix_cumulative_qty = 0
                        current_mix_slot.filled_slots += 1
                        use_store_slot(st_cd)
                        mix_slots_used += 1

                    gen_art = info['gen_art'] if info else str(gac).split('_')[0]
                    assignments.append({
                        'st_cd': st_cd, 'majcat': majcat, 'seg': current_mix_slot.seg,
                        'opt_no': current_mix_slot.filled_slots,
                        'gen_art_color': gac, 'gen_art': gen_art,
                        'color': info['color'] if info else '',
                        'total_score': score, 'art_status': 'MIX', 'is_multi_opt': 0,
                        'disp_q': 0, 'mbq': store_mbq.get(st_cd, 0),
                        'mrp': float(info.get('mrp', 0) or 0) if info else 0,
                        'bgt_sales_per_day': 0, 'dc_stock_before': 0, 'dc_stock_after': 0,
                        'st_stock': st_stock,
                    })
                    store_filled_arts[st_cd].add(gac)
                    mix_cumulative_qty += st_stock
                    phase1_mix += 1

        phase1_total = phase1_l + phase1_mix
        slots_after_p1 = sum(s.remaining for s in slot_map.values())
        logger.info(f"[{majcat}] Phase 1 done: {phase1_total} slots tagged "
                    f"(L={phase1_l}, MIX={phase1_mix}), "
                    f"{slots_after_p1} empty slots remaining")

        # ================================================================
        # PHASE 2: Try to convert MIX → L using DC, then top up L articles
        # ================================================================
        # Step A: For each MIX article, check if DC can fill gap to MBQ.
        #         If yes → convert to L, dispatch the gap.
        # Step B: For remaining L articles below MBQ, top up from DC.
        # DC stock deducts globally. Process equitably (lowest fill first).
        # ================================================================

        phase2_converted = 0  # MIX → L conversions
        phase2_topped = 0     # L top-ups
        phase2_qty = 0

        # Compute store fill rate for equitable ordering
        store_fill_rate = {}
        for s in slot_map.values():
            store_fill_rate[s.st_cd] = min(
                store_fill_rate.get(s.st_cd, 999), s.fill_rate
            )

        # Step A: Try converting MIX → L using DC stock
        # When MIX becomes L, it gets its OWN slot.
        # Remaining MIX articles repack into fewer slots.
        mix_assignments = [
            (i, a) for i, a in enumerate(assignments) if a['art_status'] == 'MIX'
        ]
        mix_assignments.sort(key=lambda x: (
            store_fill_rate.get(x[1]['st_cd'], 0),
            -x[1]['total_score']  # Higher score MIX articles convert first
        ))

        # Track which MIX assignments got converted (to remove from MIX pool)
        converted_indices = set()

        for idx, asgn in mix_assignments:
            gac = asgn['gen_art_color']
            st_cd = asgn['st_cd']
            st_stock = asgn['st_stock']
            mbq_opt_val = mbq_per_opt(st_cd)
            gap = max(0, round(mbq_opt_val - st_stock))
            dc_before = dc_stock_tracker.get(gac, 0)

            if gap > 0 and dc_before >= gap:
                dc_stock_tracker[gac] = max(0, dc_before - gap)
                assignments[idx]['art_status'] = 'L'
                assignments[idx]['disp_q'] = gap
                assignments[idx]['dc_stock_before'] = dc_before
                assignments[idx]['dc_stock_after'] = dc_stock_tracker.get(gac, 0)
                converted_indices.add(idx)
                phase2_converted += 1
                phase2_qty += gap

        # Repack remaining MIX: remove converted ones and re-bundle
        # After conversions, the slot count needs adjustment:
        # - Each converted MIX article now occupies its own L slot
        # - Remaining MIX articles need fewer slots
        if converted_indices:
            # Reset slot counts for affected stores and recount
            affected_stores = set(assignments[i]['st_cd'] for i in converted_indices)
            for st_cd in affected_stores:
                st_slot_keys = [k for k, v in slot_map.items() if v.st_cd == st_cd]
                # Recount: L articles (including conversions) each take 1 slot
                st_assignments = [(i, a) for i, a in enumerate(assignments) if a['st_cd'] == st_cd]
                l_count = sum(1 for _, a in st_assignments if a['art_status'] == 'L')
                mix_remaining = [(i, a) for i, a in st_assignments if a['art_status'] == 'MIX']

                # Bundle remaining MIX into slots
                mbq_opt_val = mbq_per_opt(st_cd)
                mix_cum = 0
                mix_slot_count = 0
                for _, a in mix_remaining:
                    if mix_cum == 0 or mix_cum >= mbq_opt_val:
                        mix_slot_count += 1
                        mix_cum = 0
                    mix_cum += a['st_stock']

                total_store_slots = sum(slot_map[k].total_slots for k in st_slot_keys)
                new_filled = min(l_count + mix_slot_count, total_store_slots)  # NEVER exceed budget

                # Reset slot_map AND store_slots_used for this store
                store_slots_used[st_cd] = new_filled
                remaining_to_fill = new_filled
                for sk in st_slot_keys:
                    cap = slot_map[sk].total_slots
                    slot_map[sk].filled_slots = min(remaining_to_fill, cap)
                    remaining_to_fill = max(0, remaining_to_fill - cap)

        # Step B: Top up L articles that are below MBQ
        l_assignments = [
            (i, a) for i, a in enumerate(assignments) if a['art_status'] == 'L' and a['disp_q'] == 0
        ]
        l_assignments.sort(key=lambda x: store_fill_rate.get(x[1]['st_cd'], 0))

        for idx, asgn in l_assignments:
            gac = asgn['gen_art_color']
            st_cd = asgn['st_cd']
            st_stock = asgn['st_stock']
            mbq_opt_val = mbq_per_opt(st_cd)
            need = max(0, round(mbq_opt_val - st_stock))
            dc_before = dc_stock_tracker.get(gac, 0)

            if need > 0 and dc_before > 0:
                actual_disp = min(need, dc_before)
                dc_stock_tracker[gac] = max(0, dc_before - actual_disp)
                assignments[idx]['disp_q'] = actual_disp
                assignments[idx]['dc_stock_before'] = dc_before
                assignments[idx]['dc_stock_after'] = dc_stock_tracker.get(gac, 0)
                phase2_topped += 1
                phase2_qty += actual_disp

        logger.info(f"[{majcat}] Phase 2: {phase2_converted} MIX→L conversions, "
                    f"{phase2_topped} L top-ups, {phase2_qty:.0f} units dispatched")

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
                # HARD GUARD: never exceed store's total budget
                if not store_has_space(st_cd):
                    continue

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
                    use_store_slot(st_cd)
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
        # PHASE 4: Push remaining MSA stock — ARTICLE-CENTRIC
        # ================================================================
        # For each MSA article with remaining DC stock:
        #   Find stores that need it (have empty slots), sorted by SCORE desc
        #   Allocate to highest-scoring store first, then next, until DC depleted
        #   Balance MSA decreases after each allocation
        # ================================================================

        phase4_pushed = 0
        phase4_qty = 0

        remaining_msa = [
            (gac, dc_stock_tracker.get(gac, 0))
            for gac in msa_articles
            if dc_stock_tracker.get(gac, 0) > 0
        ]
        remaining_msa.sort(key=lambda x: -x[1])  # Largest stock first

        if remaining_msa:
            # Build article→store score lookup from scored_pairs
            # For each article, which stores scored it and at what score?
            art_store_scores: Dict[str, List[Tuple[str, int]]] = {}
            for _, row in scored_pairs.iterrows():
                gac = row['gen_art_color']
                if gac in msa_articles and dc_stock_tracker.get(gac, 0) > 0:
                    st = row['st_cd']
                    score = int(row.get('total_score', 0))
                    art_store_scores.setdefault(gac, []).append((st, score))

            # Sort each article's stores by score DESC (highest need first)
            for gac in art_store_scores:
                art_store_scores[gac].sort(key=lambda x: -x[1])

            for gac, dc_qty in remaining_msa:
                if dc_stock_tracker.get(gac, 0) <= 0:
                    continue

                art_info = scored_lookup.get(gac, {})
                gac_str = str(gac)
                gen_art = art_info.get('gen_art', gac_str.split('_')[0] if '_' in gac_str else gac_str)
                color = art_info.get('color', '')
                mrp = float(art_info.get('mrp', 0) or 0)

                # For this article, go through stores by score (highest first)
                store_list = art_store_scores.get(gac, [])
                for st_cd, score in store_list:
                    if dc_stock_tracker.get(gac, 0) <= 0:
                        break

                    # HARD GUARD: never exceed store budget
                    if not store_has_space(st_cd):
                        continue

                    st_slot_keys = [k for k, v in slot_map.items() if v.st_cd == st_cd and not v.is_full]
                    if not st_slot_keys:
                        continue

                    store_filled_arts.setdefault(st_cd, set())
                    if gac in store_filled_arts[st_cd]:
                        continue
                    if (st_cd, gac) in store_stock_map:
                        continue

                    # Find an open slot
                    target = slot_map[st_slot_keys[0]]
                    dc_before = dc_stock_tracker.get(gac, 0)
                    mbq_opt_val = mbq_per_opt(st_cd)
                    actual_disp = min(round(mbq_opt_val), dc_before)

                    assignments.append({
                        'st_cd': st_cd, 'majcat': majcat, 'seg': target.seg,
                        'opt_no': target.filled_slots + 1, 'gen_art_color': gac,
                        'gen_art': gen_art, 'color': color,
                        'total_score': score, 'art_status': 'NEW_L',
                        'is_multi_opt': 0, 'disp_q': actual_disp,
                        'mbq': store_mbq.get(st_cd, 0),
                        'mrp': mrp, 'bgt_sales_per_day': 0,
                        'dc_stock_before': dc_before,
                        'dc_stock_after': max(0, dc_before - actual_disp),
                        'st_stock': 0,
                    })
                    dc_stock_tracker[gac] = max(0, dc_before - actual_disp)
                    target.filled_slots += 1
                    use_store_slot(st_cd)
                    store_filled_arts[st_cd].add(gac)
                    phase4_pushed += 1
                    phase4_qty += actual_disp

        # Final audit
        final_dc_remaining = sum(v for v in dc_stock_tracker.values() if v > 0)
        unallocated_count = sum(1 for gac in msa_articles if dc_stock_tracker.get(gac, 0) > 0
                                and gac not in {a['gen_art_color'] for a in assignments})
        logger.info(f"[{majcat}] Phase 4 push: {phase4_pushed} additional articles pushed, "
                    f"{phase4_qty:.0f} units. DC remaining: {final_dc_remaining:.0f}. "
                    f"Unallocated MSA articles: {unallocated_count}")

        # ================================================================
        # SUMMARY — correct fill rate excludes MIX (not displayable)
        # ================================================================

        total_disp = sum(a['disp_q'] for a in assignments)
        l_count = sum(1 for a in assignments if a['art_status'] == 'L')
        mix_count = sum(1 for a in assignments if a['art_status'] == 'MIX')
        newl_count = sum(1 for a in assignments if a['art_status'] == 'NEW_L')
        total_newl = newl_count + phase4_pushed

        # Fill rate: L + MIX + NEW_L all count (MIX articles bundled into display slots)
        filled = l_count + mix_count + total_newl
        empty_slots = max(0, total_slots - filled)
        fill_before = (l_count + mix_count) / max(total_slots, 1) * 100  # Before DC allocation
        fill_after = filled / max(total_slots, 1) * 100  # After DC allocation

        logger.info(
            f"[{majcat}] ALLOCATION COMPLETE: "
            f"L={l_count} MIX={mix_count} NEW_L={total_newl} | "
            f"Fill BEFORE (L+MIX): {fill_before:.1f}% | "
            f"Fill AFTER (L+MIX+NEW_L): {fill_after:.1f}% | "
            f"Empty: {empty_slots} | "
            f"Dispatch: {total_disp:.0f} units"
        )

        if not assignments:
            return pd.DataFrame()

        result = pd.DataFrame(assignments)

        # POST-PROCESS: Enforce per-store slot cap (drop excess assignments)
        # Priority: L > MIX > NEW_L. Within each, keep highest score.
        status_priority = {'L': 0, 'MIX': 1, 'NEW_L': 2}
        result['_priority'] = result['art_status'].map(status_priority).fillna(3)
        result = result.sort_values(['st_cd', '_priority', 'total_score'], ascending=[True, True, False])

        # For each store, count slots: L=1 each, MIX=grouped by opt_no, NEW_L=1 each
        # Simple approach: just keep first N assignments per store where N = budget
        keep = []
        for st_cd, grp in result.groupby('st_cd'):
            budget_cap = store_total_opts.get(st_cd, 999)
            slot_count = 0
            mix_opt_seen = set()
            for idx, row in grp.iterrows():
                if row['art_status'] == 'MIX':
                    # MIX shares slots — only count new opt_no
                    if row['opt_no'] not in mix_opt_seen:
                        if slot_count >= budget_cap:
                            continue
                        mix_opt_seen.add(row['opt_no'])
                        slot_count += 1
                    keep.append(idx)
                else:
                    if slot_count >= budget_cap:
                        continue
                    slot_count += 1
                    keep.append(idx)

        result = result.loc[keep].drop(columns=['_priority'])

        # Renumber opt_no: L and NEW_L get unique numbers, MIX articles share slot numbers
        result = result.sort_values(
            ['st_cd', 'seg', 'art_status', 'total_score'],
            ascending=[True, True, True, False]
        )
        new_opt_nos = []
        for (st_cd, seg), grp in result.groupby(['st_cd', 'seg'], sort=False):
            opt = 0
            mbq_opt_val = mbq_per_opt(st_cd)
            mix_cum = 0
            for idx, row in grp.iterrows():
                if row['art_status'] == 'MIX':
                    if mix_cum == 0 or mix_cum >= mbq_opt_val:
                        opt += 1
                        mix_cum = 0
                    mix_cum += row['st_stock']
                    new_opt_nos.append((idx, opt))
                else:
                    opt += 1
                    new_opt_nos.append((idx, opt))

        for idx, opt in new_opt_nos:
            result.at[idx, 'opt_no'] = opt

        return result
