"""
Contribution Percentage Processing Service
Handles KPI calculations, data aggregation, and contribution percentage analysis
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import io
import zipfile
import re
import gc
from sqlalchemy import text, create_engine, inspect


class ContributionProcessor:
    """Processes stock data and calculates contribution percentages and KPIs"""
    
    # Numeric scaling factors
    Q_SCALE = 1000  # Quantity scale factor
    V_SCALE = 100000  # Value scale factor
    
    # Safety guards for production
    MAX_COLUMNS = 200_000  # Column explosion prevention
    MAX_ROWS_PER_FILE = 800_000  # File size limit for exports
    
    def __init__(self, data_df: pd.DataFrame, engine=None):
        """
        Initialize processor with raw data
        
        Args:
            data_df: DataFrame with columns from COUNT_STOCK_DATA_18M
            engine: SQLAlchemy engine for database operations (optional)
        """
        self.raw_data = data_df.copy()
        self.processed_data = None
        self.engine = engine
        
    def compute_kpis(self, avg_days: int = 30, grouping_column: str = 'MACRO_MVGR', gr: int = 1) -> pd.DataFrame:
        """
        Compute all KPIs from raw stock data - OPTIMIZED VERSION
        
        KPIs Calculated:
        1. STOCK KPIs: 0001_STK_Q, 0001_STK_V
        2. DISPLAY KPIs: FIX, DISP_AREA, GM_%
        3. PER-DAY AVERAGES: per_day_sale_q, per_day_sale_v
        4. ADVANCED KPIs: STR, SALES_PSF, GM_PSF
        5. CATEGORY BENCHMARKS: SALE_PSF_MJ, GM_PSF_MJ
        6. ACHIEVEMENT %: SALES_PSF_ACH%, GM_PSF_ACH%
        7. CONTRIBUTION %: STOCK_CONT%, SALE_CONT%
        8. ALGORITHM: ALGO, INITIAL AUTO CONT%
        
        Args:
            avg_days: Average days for per-day calculations (default: 30)
            grouping_column: Column for grouping (default: 'MACRO_MVGR')
            gr: Grouping rate for ALGO calculation (default: 1, use 2 for M_VND_CD)
            
        Returns:
            DataFrame with computed KPIs
        """
        df = self.raw_data.copy()
        
        # Ensure numeric dtype early (memory + speed)
        num_cols = df.select_dtypes(include="number").columns
        df[num_cols] = df[num_cols].astype("float32")
        
        # --- BASIC KPIs ---
        op_q = df['OP_STK_Q']
        cl_q = df['CL_STK_Q']
        op_v = df['OP_STK_V']
        cl_v = df['CL_STK_V']
        
        df['0001_STK_Q'] = np.where(
            (op_q == 0) & (cl_q == 0),
            0,
            (op_q + cl_q) / np.where((op_q != 0) & (cl_q != 0), 2, 1)
        )
        
        df['0001_STK_V'] = np.where(
            (op_v == 0) & (cl_v == 0),
            0,
            (op_v + cl_v) / np.where((op_v != 0) & (cl_v != 0), 2, 1)
        )
        
        df['FIX'] = df['0001_STK_Q'] * self.Q_SCALE / np.where(df['AVG_DNSTY'] != 0, df['AVG_DNSTY'], 1)
        df['DISP_AREA'] = np.maximum(df['APF'] * df['FIX'], np.where(df['SALE_V'] > 0, 1, 0))
        df['GM_%'] = df['GM_V'] / np.where(df['SALE_V'] != 0, df['SALE_V'], 1)
        
        # --- PER-DAY AVERAGES ---
        per_day_sale_q = np.where(df['SALE_Q'] > 0, df['SALE_Q'] / avg_days, 0) * self.Q_SCALE
        per_day_sale_v = np.where(df['SALE_V'] > 0, df['SALE_V'] / avg_days, 0) * self.V_SCALE
        
        df['STR'] = np.where(per_day_sale_q == 0, 0, df['0001_STK_Q'] / per_day_sale_q * self.Q_SCALE)
        df['SALES_PSF'] = np.where(df['DISP_AREA'] == 0, 0, per_day_sale_v / df['DISP_AREA'])
        
        # --- GROUPING COLUMNS ---
        group_cols = ['MAJ_CAT']
        if 'ST_CD' in df.columns:
            group_cols.insert(0, 'ST_CD')
        
        # --- CACHE GROUPBY SUMS (CRITICAL OPTIMIZATION) ---
        grp = df.groupby(group_cols, dropna=False)
        
        sale_v_sum = grp['SALE_V'].transform('sum')
        disp_area_sum = grp['DISP_AREA'].transform('sum')
        gm_v_sum = grp['GM_V'].transform('sum')
        
        df['SALE_PSF_MJ'] = np.where(
            disp_area_sum == 0, 0,
            (sale_v_sum * self.V_SCALE / disp_area_sum) / avg_days
        )
        
        df['SALES_PSF_ACH%'] = np.where(
            df['SALE_PSF_MJ'] == 0, 0,
            df['SALES_PSF'] / df['SALE_PSF_MJ']
        )
        
        df['GM_PSF'] = np.where(
            df['DISP_AREA'] == 0, 0,
            (df['GM_V'] * self.V_SCALE / df['DISP_AREA']) / avg_days
        )
        
        df['GM_PSF_MJ'] = np.where(
            disp_area_sum == 0, 0,
            (gm_v_sum * self.V_SCALE / disp_area_sum) / avg_days
        )
        
        df['GM_PSF_ACH%'] = np.where(
            df['GM_PSF_MJ'] == 0, 0,
            df['GM_PSF'] / df['GM_PSF_MJ']
        )
        
        # --- CONTRIBUTION % ---
        stk_q_pos_sum = df.loc[df['0001_STK_Q'] > 0].groupby(group_cols)['0001_STK_Q'].transform('sum')
        sale_v_pos_sum = df.loc[df['SALE_V'] > 0].groupby(group_cols)['SALE_V'].transform('sum')
        
        df['STOCK_CONT%'] = np.where(
            df['0001_STK_Q'] <= 0, 0,
            df['0001_STK_Q'] / stk_q_pos_sum
        )
        
        df['SALE_CONT%'] = np.where(
            df['SALE_V'] <= 0, 0,
            df['SALE_V'] / sale_v_pos_sum
        )
        
        # --- ALGO LOGIC ---
        algo_raw = df['SALE_CONT%'] * np.where(df['SALE_CONT%'] < 0.05, 5.0, 3.0)
        algo_adj = df['SALE_CONT%'] * (1 + (df['GM_PSF_ACH%'] - 1) * gr)
        df['ALGO'] = np.minimum(algo_raw, np.maximum(algo_adj, 0))
        
        algo_sum = grp['ALGO'].transform('sum')
        df['INITIAL AUTO CONT%'] = np.where(algo_sum == 0, 0, df['ALGO'] / algo_sum)
        
        # --- FINAL NORMALIZATION ---
        self.processed_data = self._normalize_dataframe(df)
        return self.processed_data
    
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize numeric columns: replace infinities, coerce to numeric, round and fill NaN
        
        Args:
            df: DataFrame to normalize
            
        Returns:
            Normalized DataFrame
        """
        # Replace infinities with NaN
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # List of KPI columns to normalize
        kpi_columns = [
            '0001_STK_Q', '0001_STK_V', 'FIX', 'DISP_AREA', 'GM_%', 'STR',
            'SALES_PSF', 'SALE_PSF_MJ', 'SALES_PSF_ACH%', 'GM_PSF', 'GM_PSF_MJ',
            'GM_PSF_ACH%', 'STOCK_CONT%', 'SALE_CONT%', 'ALGO', 'INITIAL AUTO CONT%'
        ]
        
        # Process each KPI column
        for col in kpi_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float).round(4)
        
        return df
    
    def compute_contribution_percentages(self, groupby_col: str = 'ST_CD') -> pd.DataFrame:
        """
        Calculate contribution percentages at specified group level
        
        Args:
            groupby_col: Column to group by (default: 'ST_CD' for store level)
            
        Returns:
            DataFrame with contribution percentages
        """
        if self.processed_data is None:
            raise ValueError("Must call compute_kpis() first")
        
        df = self.processed_data.copy()
        
        # Filter positive values for contribution calculations
        stk_q_pos_mask = df['0001_STK_Q'] > 0
        sale_v_pos_mask = df['SALE_V'] > 0
        
        # Calculate totals
        total_stock_q = df.loc[stk_q_pos_mask, '0001_STK_Q'].sum()
        total_sale_v = df.loc[sale_v_pos_mask, 'SALE_V'].sum()
        
        # Global STOCK CONTRIBUTION %
        df['STOCK_CONT%'] = np.where(
            df['0001_STK_Q'] > 0, 
            (df['0001_STK_Q'] / total_stock_q * 100) if total_stock_q > 0 else 0,
            0
        )
        
        # Global SALES CONTRIBUTION %
        df['SALE_CONT%'] = np.where(
            df['SALE_V'] > 0,
            (df['SALE_V'] / total_sale_v * 100) if total_sale_v > 0 else 0,
            0
        )
        
        return df
    
    def aggregate_data(self, groupby_columns: List[str], aggregations: Optional[Dict] = None) -> pd.DataFrame:
        """
        Aggregate processed data by specified columns
        
        Args:
            groupby_columns: Columns to group by
            aggregations: Dict of column -> aggregation function mappings
            
        Returns:
            Aggregated DataFrame
        """
        if self.processed_data is None:
            raise ValueError("Must call compute_kpis() first")
        
        df = self.processed_data.copy()
        
        # Default aggregations
        if aggregations is None:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            aggregations = {col: 'sum' for col in numeric_cols if col not in groupby_columns}
        
        # Ensure groupby columns exist
        missing_cols = [col for col in groupby_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found: {missing_cols}")
        
        aggregated = df.groupby(groupby_columns).agg(aggregations).reset_index()
        return aggregated
    
    def apply_suffix_mapping(self, mappings: Dict[str, Dict], col_name: str) -> pd.DataFrame:
        """
        Apply suffix mapping transformations to a column
        
        Args:
            mappings: Dict with 'suffix_mapping' and 'fallback_suffixes'
            col_name: Column name to apply mapping to
            
        Returns:
            DataFrame with mapped column values
        """
        if col_name not in self.raw_data.columns:
            return self.raw_data.copy()
        
        df = self.raw_data.copy()
        suffix_map = mappings.get('suffix_mapping', {})
        fallback = mappings.get('fallback_suffixes', {}).get('default', None)
        
        df[col_name] = df[col_name].map(suffix_map).fillna(fallback if fallback else df[col_name])
        return df
    
    def normalize_columns(self) -> pd.DataFrame:
        """
        Ensure all numeric columns are float type
        
        Returns:
            DataFrame with normalized numeric columns
        """
        if self.processed_data is None:
            df = self.raw_data.copy()
        else:
            df = self.processed_data.copy()
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].astype(float)
        
        return df
    
    def get_summary_stats(self, groupby_col: str = 'ST_CD') -> Dict:
        """
        Get summary statistics from processed data
        
        Args:
            groupby_col: Column to group by for summary
            
        Returns:
            Dictionary with summary statistics
        """
        if self.processed_data is None:
            raise ValueError("Must call compute_kpis() first")
        
        df = self.processed_data.copy()
        
        summary = {
            'total_records': len(df),
            'total_stock_value': df['0001_STK_V'].sum(),
            'total_sales_value': df['SALE_V'].sum(),
            'avg_sales_psf': df['SALES_PSF'].mean() if 'SALES_PSF' in df.columns else 0,
            'avg_gm_psf': df['GM_PSF'].mean() if 'GM_PSF' in df.columns else 0,
        }
        
        if groupby_col in df.columns:
            summary['unique_groups'] = df[groupby_col].nunique()
            summary['group_breakdown'] = df.groupby(groupby_col).agg({
                '0001_STK_V': 'sum',
                'SALE_V': 'sum'
            }).to_dict('index')
        
        return summary
    
    # ==================== NEW PRODUCTION-GRADE METHODS ====================
    
    def build_dynamic_query(self, grouping_column: str, where_clause: str = "1=1") -> str:
        """
        Build dynamic SQL query based on grouping column and filters
        
        Args:
            grouping_column: Column to group by (MACRO_MVGR, M_VND_CD, etc.)
            where_clause: WHERE clause filter (default: "1=1")
            
        Returns:
            SQL query string
        """
        if not self.engine:
            raise ValueError("Engine required for dynamic query building")
        
        # Validate and set table name
        table_name = f"Master_HIER_{grouping_column}"
        
        # Check if table exists
        try:
            inspector = inspect(self.engine)
            table_exists = table_name in inspector.get_table_names()
            
            if not table_exists:
                # Fallback to default table
                table_name = "Master_HIER_MACRO_MVGR"
                grouping_column = 'MACRO_MVGR'
        except Exception:
            table_name = "Master_HIER_MACRO_MVGR"
            grouping_column = 'MACRO_MVGR'
        
        # Get all columns from table
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)
            col_names = [col['name'] for col in columns]
        except Exception:
            raise ValueError(f"Could not retrieve columns from {table_name}")
        
        # Exclude unwanted columns
        exclude_columns = {"UPLOAD_DATETIME"}
        exclude_lower = {col.lower() for col in exclude_columns}
        
        # Build SELECT clause
        select_cols = [
            f"A.{col}"
            for col in col_names
            if col.lower() not in exclude_lower and col.lower() != grouping_column.lower()
        ]
        
        # Add grouping column at the end
        select_cols.append(f"A.{grouping_column} AS {grouping_column}")
        
        # Build final query
        select_columns_text = ",\n        ".join(select_cols)
        
        query = f"""
        SELECT
            B.ST_CD,
            B.ST_NM,
            {select_columns_text}            
        FROM {table_name} A WITH (NOLOCK)
        CROSS JOIN dbo.Master_STORE_PLAN B WITH (NOLOCK)
        WHERE {where_clause};
        """
        
        return query
    
    def create_aggregated_data(self, grouping_column: str) -> pd.DataFrame:
        """
        Create aggregated (company-level) data by grouping master columns
        
        Args:
            grouping_column: Column for grouping aggregation
            
        Returns:
            Aggregated DataFrame
        """
        if self.processed_data is None:
            raise ValueError("Must call compute_kpis() first")
        
        df = self.processed_data.copy()
        
        # Determine group columns
        group_cols = ['MAJ_CAT']
        if 'ST_CD' in df.columns:
            group_cols.insert(0, 'ST_CD')
        
        # Exclude columns from aggregation
        exclude_cols = {'ST_NM', 'AVG_DNSTY'}
        
        agg_map = {}
        for col in df.columns:
            if col in group_cols or col in exclude_cols:
                continue
            else:
                agg_map[col] = 'sum'
        
        df_aggregated = df.groupby(group_cols, dropna=False).agg(agg_map).reset_index()
        
        # Post-aggregation: reset APF and recalculate DISP_AREA for company level
        if 'APF' in df_aggregated.columns:
            df_aggregated['APF'] = 25  # Default APF for company-level
        
        return df_aggregated
    
    def combine_dataframes(
        self,
        dataframes: Dict[str, pd.DataFrame],
        is_aggregated: bool = False,
        grouping_column: str = None,
        allowed_kpi_prefixes: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Production-safe dataframe combiner.
        Prevents column explosion and memory crashes.
        
        Args:
            dataframes: Dict of preset_name -> DataFrame
            is_aggregated: Whether data is aggregated (company-level)
            grouping_column: Column for grouping
            allowed_kpi_prefixes: List of KPI prefixes to keep (filters columns)
            
        Returns:
            Combined DataFrame with all presets merged horizontally
        """
        if not dataframes:
            return pd.DataFrame()
        
        # Determine merge keys based on data type
        if is_aggregated:
            merge_keys = ['MAJ_CAT']
            if grouping_column:
                merge_keys.append(grouping_column)
            merge_keys.extend(['AVG_DNSTY'])
        else:
            merge_keys = ['ST_CD', 'ST_NM', 'MAJ_CAT']
            if grouping_column:
                merge_keys.append(grouping_column)
            merge_keys.extend(['AVG_DNSTY'])
        
        # Dedupe merge keys
        merge_keys = list(dict.fromkeys(merge_keys))
        
        if not merge_keys:
            raise ValueError("No merge keys available")
        
        # --- Prepare dataframes ---
        dfs = []
        
        for preset_name, df in dataframes.items():
            if df is None or df.empty:
                continue
            
            df = df.copy()
            
            # Remove duplicate column labels
            df = df.loc[:, ~df.columns.duplicated()]
            
            # Drop duplicates on merge keys
            df = df.drop_duplicates(subset=[k for k in merge_keys if k in df.columns])
            
            # KPI filtering (CRITICAL)
            if allowed_kpi_prefixes:
                allowed_cols = set(merge_keys)
                for c in df.columns:
                    if any(c.startswith(p) for p in allowed_kpi_prefixes):
                        allowed_cols.add(c)
                df = df[[col for col in allowed_cols if col in df.columns]]
            
            # Rename non-key columns with preset suffix
            rename_map = {
                col: f"{col}|{preset_name}"
                for col in df.columns
                if col not in merge_keys
            }
            df = df.rename(columns=rename_map)
            
            # Keep only required columns
            cols_to_keep = [k for k in merge_keys if k in df.columns] + list(rename_map.values())
            df = df[cols_to_keep]
            
            # Downcast numerics to save RAM
            num_cols = df.select_dtypes(include="number").columns
            df[num_cols] = df[num_cols].astype("float32")
            
            dfs.append(df)
        
        if not dfs:
            return pd.DataFrame()
        
        # --- Merge safely (iterative) ---
        combined_df = dfs[0]
        
        for i, df in enumerate(dfs[1:], start=1):
            actual_merge_keys = [k for k in merge_keys if k in combined_df.columns and k in df.columns]
            
            combined_df = combined_df.merge(
                df,
                on=actual_merge_keys,
                how="outer",
                copy=False,
                sort=False,
            )
            
            # COLUMN LIMIT GUARD
            if combined_df.shape[1] > self.MAX_COLUMNS:
                raise MemoryError(
                    f"Column limit exceeded ({combined_df.shape[1]} > {self.MAX_COLUMNS}). "
                    "Reduce KPIs or switch to long format."
                )
            
            if combined_df.empty:
                break
            
            gc.collect()  # Free intermediate memory
        
        combined_df["Generated_Date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return combined_df
    
    def compute_column_after_combine(
        self,
        df: pd.DataFrame,
        suffix_mapping: Dict,
        fallback_suffixes: List[str],
        prefix: str = "INITIAL AUTO CONT%|"
    ) -> np.ndarray:
        """
        Compute result column based on SSN-based suffix mapping and fallback
        
        Args:
            df: Combined DataFrame with multiple preset columns
            suffix_mapping: Dict mapping SSN -> list of suffixes
            fallback_suffixes: List of fallback suffixes
            prefix: Column prefix (default: "INITIAL AUTO CONT%|")
            
        Returns:
            NumPy array with computed result column
        """
        if df is None or df.empty:
            return np.full(0, np.nan)
        
        nrows = len(df)
        ssn_series = df.get("SSN", pd.Series([None] * nrows))
        mapped_max = np.full(nrows, np.nan, dtype=float)
        
        # Apply SSN mapping
        for key, suffixes in suffix_mapping.items():
            suffix_list = ([s for s in suffixes if s] if isinstance(suffixes, (list, tuple))
                          else ([suffixes] if suffixes else []))
            
            full_cols = [prefix + suf for suf in suffix_list]
            existing_cols = [c for c in full_cols if c in df.columns]
            
            if not existing_cols:
                continue
            
            try:
                vals = df[existing_cols].to_numpy(dtype=float)
                row_max = np.nanmax(vals, axis=1)
            except Exception:
                row_max = (df[existing_cols]
                          .apply(pd.to_numeric, errors="coerce")
                          .max(axis=1)
                          .fillna(0)
                          .to_numpy())
            
            mask = (ssn_series == key).to_numpy()
            mapped_max[mask] = row_max[mask]
        
        # Fallback calculation
        fallback_cols = [prefix + suf for suf in fallback_suffixes 
                        if (prefix + suf) in df.columns]
        
        if fallback_cols:
            fallback_max = (df[fallback_cols]
                           .apply(pd.to_numeric, errors="coerce")
                           .max(axis=1)
                           .fillna(0)
                           .to_numpy())
        else:
            fallback_max = np.zeros(nrows)
        
        # Final result
        has_mapping = ssn_series.isin(suffix_mapping.keys()).to_numpy()
        return np.where(has_mapping, mapped_max, fallback_max)
    
    @staticmethod
    def sanitize_filename(s: str) -> str:
        """
        Sanitize a string to be used in filenames
        
        Args:
            s: String to sanitize
            
        Returns:
            Sanitized filename-safe string
        """
        if s is None:
            return 'NA'
        s = str(s)
        s = re.sub(r"[^0-9A-Za-z._-]", "_", s)
        return s[:80]
    
    def split_dataframe(self, df: pd.DataFrame, table_name: str, max_rows: int = None) -> Dict[str, bytes]:
        """
        Split DataFrame into chunks based on hierarchy and row limits
        
        Args:
            df: DataFrame to split
            table_name: Base name for output files
            max_rows: Maximum rows per file (default: MAX_ROWS_PER_FILE)
            
        Returns:
            Dict of filename -> CSV bytes
        """
        if max_rows is None:
            max_rows = self.MAX_ROWS_PER_FILE
        
        files = {}
        total_rows = len(df)
        
        if total_rows == 0:
            return files
        
        if total_rows <= max_rows:
            # Single file
            filename = f"{table_name}.csv"
            files[filename] = df.to_csv(index=False).encode('utf-8')
            return files
        
        # Try hierarchical splitting
        hierarchy = ['DIV', 'SUB_DIV', 'MAJ_CAT']
        
        for level, col in enumerate(hierarchy):
            if col in df.columns:
                for value, group in df.groupby(col):
                    group_name = self.sanitize_filename(str(value))
                    
                    if len(group) <= max_rows:
                        filename = f"{table_name}_{col}_{group_name}.csv"
                        files[filename] = group.to_csv(index=False).encode('utf-8')
                    else:
                        # Chunk further
                        chunks = np.array_split(group, int(np.ceil(len(group) / max_rows)))
                        for i, chunk in enumerate(chunks, 1):
                            filename = f"{table_name}_{col}_{group_name}_part{i}.csv"
                            files[filename] = chunk.to_csv(index=False).encode('utf-8')
                break  # Processed at this level
        
        # If no hierarchy columns or still too large, chunk entire dataframe
        if not files:
            chunks = np.array_split(df, int(np.ceil(total_rows / max_rows)))
            for i, chunk in enumerate(chunks, 1):
                filename = f"{table_name}_part{i}.csv"
                files[filename] = chunk.to_csv(index=False).encode('utf-8')
        
        return files
    
    @staticmethod
    def create_zip(files_dict: Dict[str, bytes]) -> bytes:
        """
        Create ZIP file from dictionary of files
        
        Args:
            files_dict: Dict of filename -> file bytes
            
        Returns:
            ZIP file as bytes
        """
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            for filename, data in files_dict.items():
                zip_file.writestr(filename, data)
        buffer.seek(0)
        return buffer.read()
    
    # ==================== EXPORT TO EXCEL ====================
    
    def export_to_excel(
        self,
        filename: str = "contribution_analysis.xlsx",
        include_sheets: List[str] = None
    ) -> bytes:
        """
        Export processed data to Excel workbook with multiple sheets
        
        Args:
            filename: Output filename
            include_sheets: List of sheet types to include. Options:
                'kpis', 'contributions', 'summary', 'combined'
                If None, includes all available sheets
                
        Returns:
            Excel file as bytes
        """
        if include_sheets is None:
            include_sheets = ['kpis', 'contributions', 'summary']
        
        if self.processed_data is None:
            raise ValueError("Must call compute_kpis() first")
        
        # Create Excel writer
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            # KPIs sheet
            if 'kpis' in include_sheets and self.processed_data is not None:
                self.processed_data.to_excel(writer, sheet_name='KPIs', index=False)
            
            # Contributions sheet
            if 'contributions' in include_sheets and self.processed_data is not None:
                contribution_cols = [
                    col for col in self.processed_data.columns
                    if 'CONT%' in col or 'CONT' in col
                ]
                if contribution_cols:
                    contribution_df = self.processed_data[
                        [c for c in self.processed_data.columns if c in contribution_cols or c in ['ST_CD', 'ST_NM', 'MAJ_CAT']]
                    ]
                    contribution_df.to_excel(writer, sheet_name='Contributions', index=False)
            
            # Summary statistics sheet
            if 'summary' in include_sheets:
                summary = self.get_summary_stats()
                summary_df = pd.DataFrame([summary])
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        buffer.seek(0)
        return buffer.getvalue()
    
    def export_to_csv(self, table_name: str = "contribution_analysis") -> Dict[str, bytes]:
        """
        Export data to CSV format with optional splitting
        
        Args:
            table_name: Base name for output files
            
        Returns:
            Dict of filename -> CSV bytes
        """
        if self.processed_data is None:
            raise ValueError("Must call compute_kpis() first")
        
        files = self.split_dataframe(self.processed_data, table_name)
        return files
    
    def export_combined_to_excel(
        self,
        combined_df: pd.DataFrame,
        preset_names: List[str] = None,
        filename: str = "combined_analysis.xlsx"
    ) -> bytes:
        """
        Export combined multi-preset results to Excel
        
        Args:
            combined_df: Combined DataFrame from combine_dataframes()
            preset_names: List of preset names in results
            filename: Output filename
            
        Returns:
            Excel file as bytes
        """
        if combined_df.empty:
            raise ValueError("Combined DataFrame is empty")
        
        buffer = io.BytesIO()
        
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            # Full combined data
            combined_df.to_excel(writer, sheet_name='Combined_Data', index=False)
            
            # Summary statistics per preset
            if preset_names:
                summary_data = []
                for preset in preset_names:
                    preset_cols = [c for c in combined_df.columns if f'|{preset}' in c]
                    summary_data.append({
                        'Preset': preset,
                        'Rows': len(combined_df),
                        'Columns': len(preset_cols),
                        'Has_Data': len(preset_cols) > 0
                    })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Preset_Summary', index=False)
            
            # Contribution columns only
            contribution_cols = [c for c in combined_df.columns if 'CONT%' in c]
            if contribution_cols:
                fixed_cols = [c for c in combined_df.columns if '|' not in c]
                cont_df = combined_df[fixed_cols + contribution_cols]
                cont_df.to_excel(writer, sheet_name='Contributions', index=False)
        
        buffer.seek(0)
        return buffer.getvalue()
    
    def export_combined_to_csv(
        self,
        combined_df: pd.DataFrame,
        table_name: str = "combined_analysis"
    ) -> Dict[str, bytes]:
        """
        Export combined multi-preset results to CSV(s) with splitting
        
        Args:
            combined_df: Combined DataFrame from combine_dataframes()
            table_name: Base name for output files
            
        Returns:
            Dict of filename -> CSV bytes
        """
        return self.split_dataframe(combined_df, table_name)
