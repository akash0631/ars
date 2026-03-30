"""
High-Performance Upsert Engine
================================
Handles bulk INSERT/UPDATE (UPSERT) operations using:
- Pandas for chunk processing & differential detection
- SQL Server MERGE statement for atomic upsert
- Temp table staging for large datasets
- Audit logging of all changes

Supports 1M+ rows via chunked processing.
"""
import uuid
import time
import json
from typing import List, Dict, Any, Optional, Tuple, Callable

import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.orm import Session
from loguru import logger

from app.database.session import get_data_engine
from app.audit.service import AuditService


class UpsertEngine:
    """
    Enterprise upsert engine with differential update detection.

    Flow:
    1. Load incoming data into a SQL Server temp table
    2. Compare temp vs target using MERGE
    3. Track inserts/updates/unchanged
    4. Audit log all changes with column-level diffs
    """

    # SQL Server data type mapping for temp table creation
    DTYPE_MAP = {
        "object": "NVARCHAR(500)",
        "string": "NVARCHAR(500)",
        "int64": "BIGINT",
        "int32": "INT",
        "float64": "FLOAT",
        "float32": "FLOAT",
        "bool": "BIT",
        "datetime64[ns]": "DATETIME2",
        "datetime64": "DATETIME2",
    }

    def __init__(self, db: Session):
        self.db = db
        self.engine = get_data_engine()  # Use Data DB for business data
        self.audit = AuditService(db)

    def upsert(
        self,
        table_name: str,
        df: pd.DataFrame,
        primary_key_columns: List[str],
        changed_by: str,
        source: str = "API",
        ip_address: Optional[str] = None,
        chunk_size: int = 10000,
        cancel_check: Optional[Callable[[], bool]] = None,
        enable_row_audit: bool = True,  # Log individual row changes to audit_log
        progress_callback: Optional[Callable[[int, int], None]] = None,  # Callback(processed, total)
        collect_sample_changes: bool = False,  # Collect first 100 changes for validation
    ) -> Dict[str, Any]:
        """
        Perform high-performance upsert (INSERT or UPDATE) on a SQL Server table.

        Args:
            table_name: Target table name
            df: DataFrame with data to upsert
            primary_key_columns: Columns that form the unique key
            changed_by: Username performing the operation
            source: Source of data (API, UPLOAD, UI)
            ip_address: Client IP
            chunk_size: Rows per chunk for processing
            cancel_check: Optional callback returning True when processing should stop
            enable_row_audit: If True, log individual row changes (slower but detailed)
            progress_callback: Optional callback(processed, total) for progress updates
            collect_sample_changes: If True, collect first 100 row changes for batch report

        Returns:
            Dict with stats: inserted, updated, unchanged, errors, duration_ms, batch_id
        """
        start_time = time.time()
        batch_id = f"UST_{uuid.uuid4().hex[:10]}"
        total_inserted = 0
        total_updated = 0
        total_unchanged = 0
        total_errors = 0
        error_details = []
        changed_columns_summary: Dict[str, int] = {}
        all_row_changes = []  # Collect row-level changes for audit
        sample_changes = []  # First 100 changes for validation/report

        if df.empty:
            return self._build_result(
                table_name, batch_id, 0, 0, 0, 0, 0, start_time, {}
            )

        # Validate primary keys exist in DataFrame
        missing_pks = [pk for pk in primary_key_columns if pk not in df.columns]
        if missing_pks:
            raise ValueError(f"Primary key columns missing from data: {missing_pks}")

        # Drop duplicate PKs in incoming data (keep last)
        # RTRIM to match SQL Server's trailing-space-insensitive PK comparison
        dedup_key = df[primary_key_columns].astype(str).apply(
            lambda r: '|||'.join(v.rstrip() for v in r), axis=1
        )
        df = df[~dedup_key.duplicated(keep='last')].copy()

        # Get target table column info
        target_columns = self._get_table_columns(table_name)
        if not target_columns:
            raise ValueError(f"Table '{table_name}' not found or has no columns")

        # Align DataFrame columns to target table
        df = self._align_columns(df, target_columns)

        # Process in chunks
        total_chunks = (len(df) + chunk_size - 1) // chunk_size
        total_rows = len(df)
        logger.info(f"[{batch_id}] Upsert starting: {total_rows} rows in {total_chunks} chunks → {table_name}")

        for chunk_idx in range(total_chunks):
            if cancel_check and cancel_check():
                raise InterruptedError("Upsert cancelled by user")

            chunk_start = chunk_idx * chunk_size
            chunk_end = min(chunk_start + chunk_size, total_rows)
            chunk_df = df.iloc[chunk_start:chunk_end].copy()
            
            # Log progress every chunk
            logger.info(f"[{batch_id}] Processing rows {chunk_start + 1} to {chunk_end} of {total_rows} ({int((chunk_end / total_rows) * 100)}%)")

            try:
                # For sample collection, only enable row audit for first chunk if not already enabled
                should_collect_rows = enable_row_audit or (collect_sample_changes and len(sample_changes) < 100)
                
                inserted, updated, unchanged, chunk_changes, row_changes = self._process_chunk(
                    table_name=table_name,
                    chunk_df=chunk_df,
                    primary_key_columns=primary_key_columns,
                    target_columns=target_columns,
                    batch_id=batch_id,
                    changed_by=changed_by,
                    source=source,
                    ip_address=ip_address,
                    chunk_number=chunk_idx + 1,
                    enable_row_audit=should_collect_rows,
                )
                total_inserted += inserted
                total_updated += updated
                total_unchanged += unchanged

                # Aggregate column change counts
                for col, count in chunk_changes.items():
                    changed_columns_summary[col] = changed_columns_summary.get(col, 0) + count
                
                # Collect row-level changes for audit
                if enable_row_audit and row_changes:
                    all_row_changes.extend(row_changes)
                
                # Collect sample changes (first 100) for batch report
                if collect_sample_changes and row_changes and len(sample_changes) < 100:
                    for rc in row_changes:
                        if len(sample_changes) >= 100:
                            break
                        sample_changes.append({
                            "action_type": rc.get("action_type"),
                            "pk": rc.get("record_primary_key"),
                            "changed_columns": rc.get("changed_columns"),
                        })
                
                # Call progress callback
                if progress_callback:
                    progress_callback(chunk_end, total_rows)

            except Exception as e:
                logger.error(f"[{batch_id}] Chunk {chunk_idx + 1} failed: {e}", exc_info=True)
                total_errors += len(chunk_df)
                error_details.append({
                    "chunk": chunk_idx + 1,
                    "rows": f"{chunk_start}-{chunk_end}",
                    "error": str(e),
                })

        # Log row-level audit entries in bulk
        duration_ms = int((time.time() - start_time) * 1000)
        
        if enable_row_audit and all_row_changes:
            try:
                self._bulk_insert_audit_logs(
                    table_name=table_name,
                    changed_by=changed_by,
                    batch_id=batch_id,
                    source=source,
                    ip_address=ip_address,
                    row_changes=all_row_changes,
                )
                logger.info(f"[{batch_id}] Logged {len(all_row_changes)} row-level audit entries")
            except Exception as e:
                logger.warning(f"[{batch_id}] Failed to log row-level audit: {e}")
        
        # Log async to data_change_log (non-blocking)
        if all_row_changes:
            try:
                from app.services.audit_service import log_bulk_changes
                log_bulk_changes(
                    table_name=table_name,
                    batch_id=batch_id,
                    row_changes=[
                        {
                            "action_type": rc.get("action_type", "UPDATE"),
                            "record_key": rc.get("record_primary_key", ""),
                            "changes": self._build_changes_dict(rc.get("old_data"), rc.get("new_data"), rc.get("changed_columns")),
                            "row_index": i,
                        }
                        for i, rc in enumerate(all_row_changes)
                    ],
                    changed_by=changed_by,
                    source=source,
                )
                logger.info(f"[{batch_id}] Queued {len(all_row_changes)} changes for async audit")
            except Exception as e:
                logger.warning(f"[{batch_id}] Failed to queue async audit: {e}")
        
        # Build changed columns summary for audit log
        changed_cols_json = None
        if changed_columns_summary:
            changed_cols_json = json.dumps(changed_columns_summary)
        
        # Log bulk audit summary (always)
        self.audit.log_bulk_upsert(
            table_name=table_name,
            changed_by=changed_by,
            row_count=total_inserted + total_updated,
            batch_id=batch_id,
            duration_ms=duration_ms,
            notes=f"Inserted: {total_inserted}, Updated: {total_updated}, Unchanged: {total_unchanged}, Errors: {total_errors}",
            ip_address=ip_address,
            source=source,
            changed_columns=changed_cols_json,
        )
        self.db.commit()

        logger.info(
            f"[{batch_id}] Upsert complete: {total_inserted} inserted, "
            f"{total_updated} updated, {total_unchanged} unchanged, {total_errors} errors, "
            f"{duration_ms}ms"
        )

        return self._build_result(
            table_name, batch_id, len(df),
            total_inserted, total_updated, total_unchanged,
            total_errors, start_time, changed_columns_summary,
            error_details=error_details if error_details else None,
            sample_changes=sample_changes if sample_changes else None,
        )

    def _process_chunk(
        self,
        table_name: str,
        chunk_df: pd.DataFrame,
        primary_key_columns: List[str],
        target_columns: Dict[str, str],
        batch_id: str,
        changed_by: str,
        source: str,
        ip_address: Optional[str],
        chunk_number: int,
        enable_row_audit: bool = True,
    ) -> Tuple[int, int, int, Dict[str, int], List[Dict]]:
        """
        Process a single chunk using SQL Server MERGE.
        Returns: (inserted, updated, unchanged, changed_columns_count, row_changes)
        """
        # Deduplicate on PK columns (keep last occurrence) to prevent MERGE conflict
        # RTRIM to match SQL Server's trailing-space-insensitive PK comparison
        dedup_key = chunk_df[primary_key_columns].astype(str).apply(
            lambda r: '|||'.join(v.rstrip() for v in r), axis=1
        )
        chunk_df = chunk_df[~dedup_key.duplicated(keep='last')].copy()

        temp_table = f"##upsert_temp_{batch_id}_{chunk_number}"
        non_pk_columns = [c for c in chunk_df.columns if c not in primary_key_columns]
        changed_columns_count: Dict[str, int] = {}
        row_changes: List[Dict] = []

        conn = self.engine.raw_connection()
        try:
            cursor = conn.cursor()

            # 1. Create temp table
            create_temp_sql = self._build_create_temp_sql(
                temp_table, chunk_df, target_columns
            )
            cursor.execute(create_temp_sql)

            # 2. Bulk insert into temp table using fast_executemany
            insert_cols = list(chunk_df.columns)
            placeholders = ", ".join(["?" for _ in insert_cols])
            col_list = ", ".join([f"[{c}]" for c in insert_cols])
            insert_sql = f"INSERT INTO {temp_table} ({col_list}) VALUES ({placeholders})"

            # Convert DataFrame to list of tuples, handling NaN → None
            rows = []
            for _, row in chunk_df.iterrows():
                row_vals = []
                for col in insert_cols:
                    val = row[col]
                    if pd.isna(val):
                        row_vals.append(None)
                    else:
                        row_vals.append(val)
                rows.append(tuple(row_vals))

            cursor.fast_executemany = True
            cursor.executemany(insert_sql, rows)

            # 2.5 Before MERGE - capture old data for audit if enabled
            old_data_map = {}
            if enable_row_audit:
                try:
                    # Build PK list for query
                    pk_col_list = ", ".join([f"t.[{pk}]" for pk in primary_key_columns])
                    all_col_list = ", ".join([f"t.[{c}]" for c in chunk_df.columns])
                    pk_join = " AND ".join([f"t.[{pk}] = s.[{pk}]" for pk in primary_key_columns])
                    
                    # Get existing rows that match our temp table PKs
                    old_query = f"""
                        SELECT {pk_col_list}, {all_col_list}
                        FROM [{table_name}] t
                        INNER JOIN {temp_table} s ON {pk_join}
                    """
                    cursor.execute(old_query)
                    columns = [col[0] for col in cursor.description]
                    for row in cursor.fetchall():
                        row_dict = dict(zip(columns, row))
                        pk_key = "|".join([str(row_dict.get(pk, "")) for pk in primary_key_columns])
                        old_data_map[pk_key] = row_dict
                except Exception as e:
                    logger.warning(f"Failed to capture old data for audit: {e}")

            # 3. Execute MERGE
            merge_sql = self._build_merge_sql(
                target_table=table_name,
                temp_table=temp_table,
                primary_key_columns=primary_key_columns,
                non_pk_columns=non_pk_columns,
                target_columns=target_columns,
                enable_row_audit=enable_row_audit,
            )
            cursor.execute(merge_sql)

            # 4. Collect MERGE output from the output table
            output_table = f"##merge_output_{batch_id}_{chunk_number}"
            count_sql = f"""
                SELECT
                    SUM(CASE WHEN action_type = 'INSERT' THEN 1 ELSE 0 END) as inserted,
                    SUM(CASE WHEN action_type = 'UPDATE' THEN 1 ELSE 0 END) as updated
                FROM {output_table}
            """
            cursor.execute(count_sql)
            result = cursor.fetchone()
            inserted = result[0] or 0
            updated = result[1] or 0
            unchanged = len(chunk_df) - inserted - updated

            # 5. Collect row-level changes for audit
            if enable_row_audit and (inserted > 0 or updated > 0):
                try:
                    row_changes = self._collect_row_changes(
                        cursor=cursor,
                        output_table=output_table,
                        target_table=table_name,
                        primary_key_columns=primary_key_columns,
                        all_columns=list(chunk_df.columns),
                        old_data_map=old_data_map,
                    )
                except Exception as e:
                    logger.warning(f"Failed to collect row changes: {e}")

            # 6. Collect changed column details for updated rows
            if updated > 0 and non_pk_columns:
                try:
                    changed_columns_count = self._detect_changed_columns(
                        cursor, output_table, temp_table, table_name,
                        primary_key_columns, non_pk_columns
                    )
                except Exception as e:
                    logger.warning(f"Changed column detection failed: {e}")

            # 7. Cleanup temp tables
            cursor.execute(f"DROP TABLE IF EXISTS {output_table}")
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")

            conn.commit()

            return inserted, updated, unchanged, changed_columns_count, row_changes

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _build_create_temp_sql(
        self, temp_table: str, df: pd.DataFrame, target_columns: Dict[str, str]
    ) -> str:
        """
        Build CREATE TABLE SQL for temp table.
        
        All columns use NVARCHAR to support special markers like __SKIP__ and __NULL__.
        The MERGE statement handles casting back to target types.
        """
        col_defs = []
        for col in df.columns:
            # Always use NVARCHAR for temp table to handle special markers
            col_defs.append(f"[{col}] NVARCHAR(MAX) NULL")

        return f"CREATE TABLE {temp_table} ({', '.join(col_defs)})"

    def _build_merge_sql(
        self,
        target_table: str,
        temp_table: str,
        primary_key_columns: List[str],
        non_pk_columns: List[str],
        target_columns: Dict[str, str],
        enable_row_audit: bool = False,
    ) -> str:
        """
        Build SQL Server MERGE with OUTPUT clause.
        
        Special value handling:
        - '__SKIP__' : Keep existing value (don't update this column)
        - '__NULL__' : Set value to NULL
        - Normal values: Update with new value
        
        Since temp table uses NVARCHAR for all columns (to store special markers),
        we need to TRY_CAST values to the target column types.
        """
        def get_cast_expr(col: str, source_alias: str = "source") -> str:
            """Generate TRY_CAST expression for a column based on target type."""
            target_type = target_columns.get(col, "NVARCHAR(MAX)")
            # For string types, no cast needed
            if target_type.upper().startswith(("NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "NTEXT", "TEXT")):
                return f"{source_alias}.[{col}]"
            # For other types, use TRY_CAST
            return f"TRY_CAST({source_alias}.[{col}] AS {target_type})"

        # JOIN condition on PKs - need to cast source PK to target type
        join_cond_parts = []
        for pk in primary_key_columns:
            join_cond_parts.append(f"target.[{pk}] = {get_cast_expr(pk)}")
        join_cond = " AND ".join(join_cond_parts)

        # UPDATE SET clause with special value handling and type casting
        # __SKIP__ = keep existing, __NULL__ = set to NULL
        update_set_parts = []
        for c in non_pk_columns:
            cast_expr = get_cast_expr(c)
            update_set_parts.append(
                f"target.[{c}] = CASE "
                f"WHEN source.[{c}] = '__SKIP__' THEN target.[{c}] "
                f"WHEN source.[{c}] = '__NULL__' THEN NULL "
                f"ELSE {cast_expr} END"
            )
        update_set = ", ".join(update_set_parts)

        # Change detection: only update if at least one non-skip column differs
        when_matched_clause = ""
        if non_pk_columns:
            change_conditions = " OR ".join([
                f"(source.[{c}] <> '__SKIP__' AND "
                f"ISNULL(CAST(target.[{c}] AS NVARCHAR(MAX)), '') <> "
                f"CASE WHEN source.[{c}] = '__NULL__' THEN '' "
                f"ELSE ISNULL(CAST(source.[{c}] AS NVARCHAR(MAX)), '') END)"
                for c in non_pk_columns
            ])
            when_matched_clause = f"""WHEN MATCHED AND ({change_conditions})
            THEN UPDATE SET {update_set}
        """

        # INSERT columns/values with special value handling and type casting
        all_columns = primary_key_columns + non_pk_columns
        insert_cols = ", ".join([f"[{c}]" for c in all_columns])
        
        # For INSERT, __SKIP__ and __NULL__ both become NULL
        insert_vals_parts = []
        for c in all_columns:
            cast_expr = get_cast_expr(c)
            if c in primary_key_columns:
                insert_vals_parts.append(cast_expr)
            else:
                insert_vals_parts.append(
                    f"CASE WHEN source.[{c}] IN ('__SKIP__', '__NULL__') "
                    f"THEN NULL ELSE {cast_expr} END"
                )
        insert_vals = ", ".join(insert_vals_parts)

        # Output table for tracking
        output_table = temp_table.replace("upsert_temp", "merge_output")

        # Build output table columns and OUTPUT clause
        if enable_row_audit:
            # Include PK columns for row-level audit
            pk_output_cols = ", ".join([f"[pk_{pk}] NVARCHAR(MAX)" for pk in primary_key_columns])
            output_table_cols = f"action_type NVARCHAR(10), {pk_output_cols}"
            
            pk_inserted_refs = ", ".join([f"inserted.[{pk}]" for pk in primary_key_columns])
            output_into_cols = "action_type, " + ", ".join([f"[pk_{pk}]" for pk in primary_key_columns])
            output_clause = f"""
        OUTPUT
            CASE WHEN $action = 'INSERT' THEN 'INSERT'
                 WHEN $action = 'UPDATE' THEN 'UPDATE'
            END,
            {pk_inserted_refs}
        INTO {output_table} ({output_into_cols})"""
        else:
            output_table_cols = "action_type NVARCHAR(10)"
            output_clause = f"""
        OUTPUT
            CASE WHEN $action = 'INSERT' THEN 'INSERT'
                 WHEN $action = 'UPDATE' THEN 'UPDATE'
            END
        INTO {output_table} (action_type)"""

        merge_sql = f"""
        -- Create output tracking table
        CREATE TABLE {output_table} (
            {output_table_cols}
        );

        -- Execute MERGE
        MERGE [{target_table}] AS target
        USING {temp_table} AS source
        ON ({join_cond})
        {when_matched_clause}WHEN NOT MATCHED BY TARGET
            THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        {output_clause};
        """
        return merge_sql

    def _detect_changed_columns(
        self,
        cursor,
        output_table: str,
        temp_table: str,
        target_table: str,
        primary_key_columns: List[str],
        non_pk_columns: List[str],
    ) -> Dict[str, int]:
        """
        After MERGE, compare temp vs target for updated rows to identify
        which columns actually changed. Returns {col: change_count}.
        """
        # This is a post-MERGE analysis - the target already has new values,
        # so we compare using the merge output.
        # For simplicity and performance, we count columns that differ
        # between temp (source) and target (now updated) - but since target
        # is already updated, we need a different approach.
        # In practice, we log this at the MERGE output level.
        # For now, return empty - detailed column tracking is done via
        # audit log entries in the individual upsert method.
        return {}

    def _collect_row_changes(
        self,
        cursor,
        output_table: str,
        target_table: str,
        primary_key_columns: List[str],
        all_columns: List[str],
        old_data_map: Dict[str, Dict],
    ) -> List[Dict]:
        """
        Collect row-level changes from MERGE output for audit logging.
        Returns list of dicts with action_type, pk, old_data, new_data, changed_columns.
        """
        row_changes = []
        
        try:
            # Query output table with PK columns
            pk_cols_select = ", ".join([f"[pk_{pk}]" for pk in primary_key_columns])
            query = f"SELECT action_type, {pk_cols_select} FROM {output_table}"
            cursor.execute(query)
            output_rows = cursor.fetchall()
            
            if not output_rows:
                return row_changes
            
            # For each row in output, get new data from target table
            for row in output_rows:
                action_type = row[0]
                pk_values = {}
                for i, pk in enumerate(primary_key_columns):
                    pk_values[pk] = row[i + 1]
                
                pk_key = "|".join([str(pk_values.get(pk, "")) for pk in primary_key_columns])
                
                # Get new data from target table
                pk_conditions = " AND ".join([f"[{pk}] = ?" for pk in primary_key_columns])
                new_data_query = f"SELECT * FROM [{target_table}] WHERE {pk_conditions}"
                cursor.execute(new_data_query, list(pk_values.values()))
                new_row = cursor.fetchone()
                
                if new_row:
                    col_names = [desc[0] for desc in cursor.description]
                    new_data = dict(zip(col_names, new_row))
                    
                    # Get old data from map
                    old_data = old_data_map.get(pk_key, {})
                    
                    # Determine changed columns (only for UPDATE)
                    changed_columns = []
                    if action_type == "UPDATE" and old_data:
                        for col in all_columns:
                            old_val = old_data.get(col)
                            new_val = new_data.get(col)
                            if str(old_val) != str(new_val):
                                changed_columns.append(col)
                    
                    # Build primary key string
                    pk_str = "|".join([f"{pk}={pk_values.get(pk)}" for pk in primary_key_columns])
                    
                    # Convert values to JSON-serializable
                    def make_serializable(d):
                        result = {}
                        for k, v in d.items():
                            if v is None:
                                result[k] = None
                            elif isinstance(v, (int, float, bool, str)):
                                result[k] = v
                            else:
                                result[k] = str(v)
                        return result
                    
                    row_changes.append({
                        "action_type": action_type,
                        "record_primary_key": pk_str,
                        "old_data": make_serializable(old_data) if old_data else None,
                        "new_data": make_serializable(new_data),
                        "changed_columns": changed_columns if changed_columns else None,
                    })
                    
        except Exception as e:
            logger.warning(f"Error collecting row changes: {e}")
        
        return row_changes

    def _bulk_insert_audit_logs(
        self,
        table_name: str,
        row_changes: List[Dict],
        changed_by: str,
        batch_id: str,
        source: str = "BULK_UPLOAD",
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        """
        Bulk insert audit log entries for row-level changes.
        Uses system database connection for audit logging.
        """
        if not row_changes:
            return
        
        try:
            # Get system database connection string
            from ..core.config import settings
            import pyodbc
            
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={settings.SYSTEM_DB_SERVER};"
                f"DATABASE={settings.SYSTEM_DB_NAME};"
                f"Trusted_Connection=yes;"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.fast_executemany = True
            
            insert_sql = """
                INSERT INTO audit_log (
                    table_name, action_type, record_primary_key,
                    old_data, new_data, changed_columns,
                    changed_by, batch_id, source,
                    ip_address, user_agent, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            """
            
            rows_to_insert = []
            for change in row_changes:
                rows_to_insert.append((
                    table_name,
                    change["action_type"],
                    change["record_primary_key"],
                    json.dumps(change["old_data"]) if change["old_data"] else None,
                    json.dumps(change["new_data"]) if change["new_data"] else None,
                    json.dumps(change["changed_columns"]) if change["changed_columns"] else None,
                    changed_by,
                    batch_id,
                    source,
                    ip_address,
                    user_agent,
                ))
            
            cursor.executemany(insert_sql, rows_to_insert)
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Bulk inserted {len(rows_to_insert)} audit log entries for batch {batch_id}")
            
        except Exception as e:
            logger.error(f"Failed to bulk insert audit logs: {e}", exc_info=True)

    def _build_changes_dict(
        self,
        old_data: Optional[Dict],
        new_data: Optional[Dict],
        changed_columns: Optional[List[str]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Build changes dict for audit_service format.
        Returns: {column_name: {"old": old_val, "new": new_val}}
        """
        if not changed_columns:
            return {}
        
        changes = {}
        for col in changed_columns:
            old_val = old_data.get(col) if old_data else None
            new_val = new_data.get(col) if new_data else None
            changes[col] = {"old": old_val, "new": new_val}
        
        return changes

    def _get_table_columns(self, table_name: str) -> Dict[str, str]:
        """Get column names and SQL types from the target table."""
        sql = text("""
            SELECT COLUMN_NAME, DATA_TYPE,
                   CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
        """)
        with self.engine.connect() as conn:
            result = conn.execute(sql, {"table_name": table_name})
            columns = {}
            for row in result:
                col_name = row[0]
                data_type = row[1].upper()
                char_len = row[2]
                num_prec = row[3]
                num_scale = row[4]

                if data_type in ("NVARCHAR", "VARCHAR", "NCHAR", "CHAR"):
                    length = char_len if char_len and char_len > 0 else "MAX"
                    sql_type = f"{data_type}({length})"
                elif data_type == "DECIMAL" or data_type == "NUMERIC":
                    p = num_prec or 18
                    s = num_scale or 2
                    sql_type = f"{data_type}({p},{s})"
                else:
                    sql_type = data_type

                columns[col_name] = sql_type
            return columns

    def _align_columns(self, df: pd.DataFrame, target_columns: Dict[str, str]) -> pd.DataFrame:
        """
        Align DataFrame columns to match target table.
        - Normalize column names (uppercase, replace special chars)
        - Case-insensitive matching
        - Drop columns not in target
        - Keep order of target columns
        """
        import re
        
        # Create mapping from normalized names to target column names
        target_upper = {col.upper(): col for col in target_columns.keys()}
        
        # Normalize and rename DataFrame columns to match target
        new_columns = {}
        for col in df.columns:
            # Normalize: uppercase, replace special chars with underscore
            normalized = re.sub(r'[^A-Z0-9_]', '_', str(col).upper().strip())
            normalized = re.sub(r'_+', '_', normalized)  # Collapse multiple underscores
            normalized = normalized.strip('_')  # Remove leading/trailing underscores
            
            # Try to find match in target columns (case-insensitive)
            if normalized in target_upper:
                new_columns[col] = target_upper[normalized]
            elif col.upper() in target_upper:
                new_columns[col] = target_upper[col.upper()]
        
        # Rename columns in DataFrame
        if new_columns:
            df = df.rename(columns=new_columns)
        
        # Filter to only columns that exist in target
        valid_cols = [c for c in df.columns if c in target_columns]
        return df[valid_cols].copy()

    def _build_result(
        self,
        table_name: str,
        batch_id: str,
        total: int,
        inserted: int,
        updated: int,
        unchanged: int,
        errors: int,
        start_time: float,
        changed_columns_summary: Dict[str, int],
        error_details: Optional[List] = None,
        sample_changes: Optional[List] = None,
    ) -> Dict[str, Any]:
        duration_ms = int((time.time() - start_time) * 1000)
        return {
            "table_name": table_name,
            "batch_id": batch_id,
            "total_records": total,
            "inserted": inserted,
            "updated": updated,
            "unchanged": unchanged,
            "errors": errors,
            "duration_ms": duration_ms,
            "changed_columns_summary": changed_columns_summary or None,
            "error_details": error_details,
            "sample_changes": sample_changes,
        }


# ============================================================================
# Direct Single/Small-Batch Update (for inline grid edits)
# ============================================================================

class DirectUpdateEngine:
    """
    Handles small direct updates (1-100 rows) for inline cell edits.
    Uses parameterized UPDATE statements with audit logging.
    Uses the DATA database (Rep_data) for all operations.
    """

    def __init__(self, db: Session):
        self.db = db  # System DB for audit logging
        self.data_engine = get_data_engine()  # Data DB for actual updates
        self.audit = AuditService(db)

    def update_record(
        self,
        table_name: str,
        primary_key_columns: List[str],
        primary_key_values: Dict[str, Any],
        updates: Dict[str, Any],
        changed_by: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update a single record with audit logging.
        Compares old vs new and only updates changed columns.
        """
        # 1. Fetch current record from DATA database
        pk_conditions = " AND ".join([f"[{k}] = :{k}" for k in primary_key_columns])
        select_sql = text(f"SELECT * FROM [{table_name}] WHERE {pk_conditions}")

        with self.data_engine.connect() as conn:
            result = conn.execute(select_sql, primary_key_values)
            row = result.mappings().first()

            if not row:
                raise ValueError(f"Record not found in {table_name}")

            old_data = dict(row)

            # 2. Detect actual changes
            actual_changes = {}
            for col, new_val in updates.items():
                old_val = old_data.get(col)
                if str(old_val) != str(new_val):
                    actual_changes[col] = new_val

            if not actual_changes:
                return {"changed": False, "message": "No changes detected"}

            # 3. Build UPDATE
            set_clauses = ", ".join([f"[{c}] = :upd_{c}" for c in actual_changes])
            update_sql = text(f"UPDATE [{table_name}] SET {set_clauses} WHERE {pk_conditions}")

            params = {**primary_key_values}
            for c, v in actual_changes.items():
                params[f"upd_{c}"] = v

            conn.execute(update_sql, params)
            conn.commit()

        # 4. Audit (in system database)
        pk_str = "|".join([f"{k}={v}" for k, v in primary_key_values.items()])
        self.audit.log_update(
            table_name=table_name,
            changed_by=changed_by,
            record_pk=pk_str,
            old_data={c: old_data.get(c) for c in actual_changes},
            new_data=actual_changes,
            changed_columns=list(actual_changes.keys()),
            ip_address=ip_address,
            user_agent=user_agent,
            source="UI",
        )

        return {
            "changed": True,
            "changed_columns": list(actual_changes.keys()),
            "message": f"Updated {len(actual_changes)} column(s)",
        }

    def delete_records(
        self,
        table_name: str,
        primary_key_columns: List[str],
        primary_key_values_list: List[Dict[str, Any]],
        changed_by: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Delete multiple records with audit logging."""
        deleted = 0
        batch_id = f"DEL_{uuid.uuid4().hex[:10]}"

        with self.data_engine.connect() as conn:
            for pk_values in primary_key_values_list:
                pk_conditions = " AND ".join([f"[{k}] = :{k}" for k in primary_key_columns])

                # Fetch for audit
                select_sql = text(f"SELECT * FROM [{table_name}] WHERE {pk_conditions}")
                result = conn.execute(select_sql, pk_values)
                row = result.mappings().first()

                if row:
                    old_data = dict(row)

                    # Delete
                    delete_sql = text(f"DELETE FROM [{table_name}] WHERE {pk_conditions}")
                    conn.execute(delete_sql, pk_values)

                    pk_str = "|".join([f"{k}={v}" for k, v in pk_values.items()])
                    self.audit.log_delete(
                        table_name=table_name,
                        changed_by=changed_by,
                        record_pk=pk_str,
                        old_data=old_data,
                        ip_address=ip_address,
                        user_agent=user_agent,
                        batch_id=batch_id,
                    )
                    deleted += 1
            conn.commit()

        return {"deleted": deleted, "batch_id": batch_id}
