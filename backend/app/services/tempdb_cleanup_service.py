"""
TempDB Cleanup Service
======================
Background daemon thread that periodically drops orphaned ARS global (##) temp
tables from SQL Server tempdb and shrinks tempdb data files.

Mirrors the AuditQueue threading pattern in audit_service.py.

Usage (called automatically from main.py lifespan):
    from app.services.tempdb_cleanup_service import tempdb_cleaner
    tempdb_cleaner.start()   # startup
    tempdb_cleaner.stop()    # shutdown
"""
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from loguru import logger

from app.core.config import get_settings
from app.database.session import get_data_engine

settings = get_settings()

# ── Queries ───────────────────────────────────────────────────────────────────

_FIND_ORPHANS_SQL = """
    SELECT name, create_date
    FROM   tempdb.sys.tables
    WHERE (
           name LIKE '##upsert_temp[_]%'
        OR name LIKE '##merge_output[_]%'
        OR name LIKE '##bulk_stage[_]%'
        OR name LIKE '##do_update[_]%'
        OR name LIKE '##do_qty_tmp[_]%'
        OR name LIKE '#do_update[_]%'
        OR name LIKE '#do_qty_tmp[_]%'
        OR name LIKE '#bulk_stage[_]%'
        OR name LIKE '#upsert_temp[_]%'
        OR name LIKE '#merge_output[_]%'
        OR name LIKE '#temp[_]%'
        OR name IN ('##do_update', '##do_qty_tmp')
    )
    AND DATEDIFF(MINUTE, create_date, GETDATE()) >= ?
    ORDER BY create_date;
"""

_TEMPDB_FILES_SQL = """
    SELECT name FROM tempdb.sys.database_files WHERE type_desc = 'ROWS';
"""

_TEMPDB_SIZE_SQL = """
    SELECT
        SUM(size * 8.0 / 1024)                                  AS allocated_mb,
        SUM(FILEPROPERTY(name, 'SpaceUsed') * 8.0 / 1024)       AS used_mb
    FROM tempdb.sys.database_files
    WHERE type_desc = 'ROWS';
"""


class TempDBCleanupService:
    """
    Daemon thread that wakes every `interval_minutes` and:
      1. Drops orphaned ARS ## global temp tables older than `orphan_age_minutes`.
      2. Runs DBCC SHRINKFILE TRUNCATEONLY on every tempdb data file.

    Thread-safe. Exposes run_now() for the API endpoint manual trigger.
    """

    def __init__(
        self,
        interval_minutes: int = 5,
        orphan_age_minutes: int = 10,
        shrink_after_cleanup: bool = True,
    ) -> None:
        self._interval = interval_minutes * 60   # stored as seconds
        self._orphan_age = orphan_age_minutes
        self._shrink = shrink_after_cleanup
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_run: Optional[datetime] = None
        self._last_stats: Dict[str, Any] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the background cleanup thread. Safe to call multiple times."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(
                target=self._loop, name="TempDBCleanup", daemon=True
            )
            self._thread.start()
        logger.info(
            f"TempDB cleanup service started — "
            f"interval={self._interval // 60} min, "
            f"orphan_age={self._orphan_age} min, "
            f"shrink={self._shrink}"
        )

    def stop(self) -> None:
        """Signal the thread to stop and wait up to 10 s for it to finish."""
        with self._lock:
            self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10.0)
        logger.info("TempDB cleanup service stopped")

    def run_now(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Blocking manual trigger — used by the maintenance API endpoint.
        Returns the stats dict from _do_cleanup().
        """
        return self._do_cleanup(dry_run=dry_run)

    @property
    def status(self) -> Dict[str, Any]:
        """Service state snapshot for the /maintenance/tempdb/status endpoint."""
        return {
            "running": self._running,
            "interval_minutes": self._interval // 60,
            "orphan_age_minutes": self._orphan_age,
            "shrink_enabled": self._shrink,
            "last_run": self._last_run.isoformat() if self._last_run else None,
            "last_stats": self._last_stats,
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    def _loop(self) -> None:
        """Thread body: wait one interval, then clean on each tick."""
        # Initial delay — let the app finish starting up before first run
        self._sleep_interruptible(self._interval)

        while self._running:
            try:
                self._do_cleanup()
            except Exception as exc:
                logger.warning(f"TempDB cleanup cycle error: {exc}")
            self._sleep_interruptible(self._interval)

    def _sleep_interruptible(self, seconds: int) -> None:
        """Sleep in 5-second slices so stop() is responsive."""
        elapsed = 0
        while self._running and elapsed < seconds:
            time.sleep(min(5, seconds - elapsed))
            elapsed += 5

    def _do_cleanup(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Core logic:
          1. Snapshot tempdb size.
          2. Find + drop orphaned ARS ## tables older than orphan_age_minutes.
          3. SHRINKFILE TRUNCATEONLY on every tempdb data file.
          4. Snapshot tempdb size again and return stats.
        """
        dropped: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []
        shrunk:  List[str] = []
        errors:  List[Dict[str, Any]] = []
        mb_before: Optional[float] = None
        mb_after:  Optional[float] = None

        engine = get_data_engine()
        if engine is None:
            logger.warning("TempDB cleanup skipped — Data DB engine not available")
            stats: Dict[str, Any] = {
                "run_at": datetime.utcnow().isoformat(),
                "dry_run": dry_run,
                "dropped_count": 0,
                "dropped": [],
                "skipped": [],
                "shrunk_files": [],
                "errors": [{"error": "Data DB engine not available"}],
                "tempdb_mb_before": None,
                "tempdb_mb_after": None,
                "mb_freed": 0,
            }
            self._last_run = datetime.utcnow()
            self._last_stats = stats
            return stats
        raw_conn = engine.raw_connection()
        try:
            cursor = raw_conn.cursor()

            # 1. Size before
            try:
                cursor.execute(_TEMPDB_SIZE_SQL)
                row = cursor.fetchone()
                if row and row[0] is not None:
                    mb_before = round(float(row[0]), 2)
            except Exception as exc:
                logger.debug(f"TempDB size (before) failed: {exc}")

            # 2. Find orphans
            cursor.execute(_FIND_ORPHANS_SQL, self._orphan_age)
            orphans = cursor.fetchall()   # list of (name, create_date)

            for tbl_name, create_date in orphans:
                age_min = 0
                if create_date:
                    age_min = int(
                        (datetime.utcnow() - create_date).total_seconds() / 60
                    )

                if dry_run:
                    skipped.append({"table": tbl_name, "age_minutes": age_min, "reason": "dry_run"})
                    continue

                drop_sql = (
                    f"IF OBJECT_ID('tempdb..[{tbl_name}]') IS NOT NULL "
                    f"DROP TABLE [{tbl_name}]"
                )
                try:
                    cursor.execute(drop_sql)
                    raw_conn.commit()
                    dropped.append({"table": tbl_name, "age_minutes": age_min})
                    logger.info(f"TempDB cleanup: dropped [{tbl_name}] (age {age_min} min)")
                except Exception as exc:
                    errors.append({"table": tbl_name, "error": str(exc)})
                    logger.warning(f"TempDB cleanup: failed to drop [{tbl_name}]: {exc}")

            # 3. Shrink (always attempt — TRUNCATEONLY is a no-op if nothing to free)
            if self._shrink and not dry_run:
                try:
                    cursor.execute(_TEMPDB_FILES_SQL)
                    file_names = [r[0] for r in cursor.fetchall()]
                    for fname in file_names:
                        try:
                            cursor.execute(
                                f"DBCC SHRINKFILE ([{fname}], TRUNCATEONLY) WITH NO_INFOMSGS"
                            )
                            raw_conn.commit()
                            shrunk.append(fname)
                        except Exception as exc:
                            errors.append({"file": fname, "error": str(exc)})
                            logger.warning(f"TempDB shrink [{fname}] failed: {exc}")
                except Exception as exc:
                    logger.warning(f"TempDB shrink file enumeration failed: {exc}")

            # 4. Size after
            try:
                cursor.execute(_TEMPDB_SIZE_SQL)
                row = cursor.fetchone()
                if row and row[0] is not None:
                    mb_after = round(float(row[0]), 2)
            except Exception as exc:
                logger.debug(f"TempDB size (after) failed: {exc}")

        finally:
            raw_conn.close()

        mb_freed = round((mb_before or 0.0) - (mb_after if mb_after is not None else (mb_before or 0.0)), 2)
        stats: Dict[str, Any] = {
            "run_at":            datetime.utcnow().isoformat(),
            "dry_run":           dry_run,
            "dropped_count":     len(dropped),
            "dropped":           dropped,
            "skipped":           skipped,
            "shrunk_files":      shrunk,
            "errors":            errors,
            "tempdb_mb_before":  mb_before,
            "tempdb_mb_after":   mb_after,
            "mb_freed":          mb_freed,
        }

        self._last_run = datetime.utcnow()
        self._last_stats = stats

        if dropped or errors:
            logger.info(
                f"TempDB cleanup done — dropped={len(dropped)}, "
                f"errors={len(errors)}, freed={mb_freed} MB"
            )

        return stats


# ── Module-level singleton ────────────────────────────────────────────────────
tempdb_cleaner = TempDBCleanupService(
    interval_minutes   = settings.DB_TEMPDB_CLEANUP_INTERVAL_MINUTES,
    orphan_age_minutes = settings.DB_TEMPDB_ORPHAN_AGE_MINUTES,
    shrink_after_cleanup = True,
)
