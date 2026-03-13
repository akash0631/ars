"""
Integration Guide: PresetManager + ContributionProcessor

This module shows how to use PresetManager with ContributionProcessor for
complete preset lifecycle management and contribution analysis workflows.
"""

from sqlalchemy import create_engine, text
from app.services import PresetManager, ContributionProcessor
import pandas as pd
from typing import Dict, List, Tuple, Optional
import time


class ContributionWorkflow:
    """
    High-level workflow combining preset management and contribution analysis
    """
    
    def __init__(self, engine, preset_table: str = 'Cont_presets'):
        """
        Initialize workflow
        
        Args:
            engine: SQLAlchemy engine
            preset_table: Preset table name
        """
        self.engine = engine
        self.preset_manager = PresetManager(engine, preset_table)
    
    # ==================== PRESET LIFECYCLE ====================
    
    def setup_presets(self) -> Dict[str, Dict]:
        """
        Set up presets for the first time, ensuring defaults exist
        
        Returns:
            Dict of all presets
        """
        presets, error = self.preset_manager.ensure_default_preset()
        if error:
            raise RuntimeError(f"Failed to setup presets: {error}")
        return presets
    
    def create_custom_preset(
        self,
        name: str,
        description: str,
        months: List[str],
        avg_days: int = 30
    ) -> bool:
        """
        Create a custom preset for contribution analysis
        
        Args:
            name: Unique preset name
            description: Human-readable description
            months: List of months to include
            avg_days: Number of days for averaging
            
        Returns:
            True if successful
        """
        config = {
            'type': 'custom',
            'description': description,
            'months': months,
            'avg_days': avg_days,
            'kpi': f'L{avg_days}D'
        }
        
        # Validate before creating
        is_valid, errors = self.preset_manager.validate_preset_config(config)
        if not is_valid:
            raise ValueError(f"Invalid preset config: {errors}")
        
        success, error = self.preset_manager.create_preset(name, config)
        if not success:
            raise RuntimeError(f"Failed to create preset: {error}")
        
        return True
    
    def update_preset_sequence(self, sequence: List[str]) -> bool:
        """
        Update the execution sequence of presets
        
        Args:
            sequence: List of preset names in order
            
        Returns:
            True if successful
        """
        success, error = self.preset_manager.update_sequence(sequence)
        if not success:
            raise RuntimeError(f"Failed to update sequence: {error}")
        return True
    
    def get_execution_sequence(self) -> List[str]:
        """
        Get current execution sequence (non-formula presets)
        
        Returns:
            List of preset names in execution order
        """
        sequence, error = self.preset_manager.get_sequence(include_formula=False)
        if error:
            raise RuntimeError(f"Failed to get sequence: {error}")
        return sequence
    
    # ==================== CONTRIBUTION ANALYSIS ====================
    
    def process_preset(
        self,
        preset_name: str,
        data: pd.DataFrame,
        grouping_column: str = 'MACRO_MVGR'
    ) -> Dict:
        """
        Process contribution analysis for a single preset
        
        Args:
            preset_name: Name of preset to use
            data: Input DataFrame with raw stock data
            grouping_column: Column for grouping analysis
            
        Returns:
            Dict with results: {
                'kpis': DataFrame,
                'contributions': DataFrame,
                'summary': Dict
            }
        """
        # Load preset config
        preset_config, error = self.preset_manager.get_preset(preset_name)
        if error:
            raise RuntimeError(f"Failed to load preset: {error}")
        
        # Initialize processor
        processor = ContributionProcessor(data, engine=self.engine)
        
        # Compute KPIs with preset parameters
        avg_days = preset_config.get('avg_days', 30)
        gr = 2 if grouping_column == 'M_VND_CD' else 1
        
        kpi_data = processor.compute_kpis(
            avg_days=avg_days,
            grouping_column=grouping_column,
            gr=gr
        )
        
        # Compute contributions
        contribution_data = processor.compute_contribution_percentages()
        
        # Get summary
        summary = processor.get_summary_stats()
        
        return {
            'preset_name': preset_name,
            'kpis': kpi_data,
            'contributions': contribution_data,
            'summary': summary
        }
    
    def process_multiple_presets(
        self,
        data: pd.DataFrame,
        preset_names: List[str] = None,
        grouping_column: str = 'MACRO_MVGR'
    ) -> Dict[str, Dict]:
        """
        Process contribution analysis for multiple presets sequentially
        
        Args:
            data: Input DataFrame
            preset_names: List of presets to process. If None, uses sequence.
            grouping_column: Column for grouping
            
        Returns:
            Dict of preset_name -> results
        """
        if preset_names is None:
            preset_names = self.get_execution_sequence()
        
        results = {}
        for preset_name in preset_names:
            try:
                results[preset_name] = self.process_preset(data, preset_name, grouping_column)
            except Exception as e:
                print(f"Error processing preset '{preset_name}': {e}")
                results[preset_name] = {'error': str(e)}
        
        return results
    
    def combine_preset_results(
        self,
        results: Dict[str, Dict],
        allowed_kpi_prefixes: List[str] = None
    ) -> pd.DataFrame:
        """
        Combine results from multiple presets into single DataFrame
        
        Args:
            results: Output from process_multiple_presets
            allowed_kpi_prefixes: KPI columns to include
            
        Returns:
            Combined DataFrame
        """
        # Extract KPI dataframes
        dataframes = {}
        for preset_name, result in results.items():
            if 'kpis' in result and not isinstance(result.get('error'), str):
                dataframes[preset_name] = result['kpis']
        
        if not dataframes:
            return pd.DataFrame()
        
        # Use processor to combine
        processor = ContributionProcessor(pd.DataFrame(), engine=self.engine)
        
        combined = processor.combine_dataframes(
            dataframes,
            is_aggregated=False,
            allowed_kpi_prefixes=allowed_kpi_prefixes or [
                '0001_STK_', 'FIX', 'DISP_AREA', 'GM_',
                'STR', 'SALES_PSF', 'SALE_PSF_ACH%',
                'STOCK_CONT%', 'SALE_CONT%'
            ]
        )
        
        return combined
    
    # ==================== EXPORT OPERATIONS ====================
    
    def export_to_excel(
        self,
        data: pd.DataFrame,
        preset_names: List[str] = None,
        filename: str = "contribution_analysis.xlsx",
        is_combined: bool = False
    ) -> bytes:
        """
        Export results to Excel format
        
        Args:
            data: DataFrame to export (from process_preset or combined results)
            preset_names: List of preset names (for title purposes)
            filename: Output filename
            is_combined: Whether this is combined multi-preset data
            
        Returns:
            Excel file as bytes
        """
        processor = ContributionProcessor(data, engine=self.engine)
        
        if is_combined and preset_names:
            return processor.export_combined_to_excel(data, preset_names, filename)
        else:
            return processor.export_to_excel(filename)
    
    def export_to_csv(
        self,
        data: pd.DataFrame,
        table_name: str = "contribution_analysis",
        is_combined: bool = False
    ) -> Dict[str, bytes]:
        """
        Export results to CSV format (with optional splitting)
        
        Args:
            data: DataFrame to export
            table_name: Base name for output files
            is_combined: Whether this is combined data
            
        Returns:
            Dict of filename -> CSV bytes
        """
        processor = ContributionProcessor(data, engine=self.engine)
        
        if is_combined:
            return processor.export_combined_to_csv(data, table_name)
        else:
            return processor.export_to_csv(table_name)
    
    def export_with_zip(
        self,
        data: pd.DataFrame,
        include_csv: bool = True,
        include_excel: bool = True,
        preset_names: List[str] = None,
        is_combined: bool = False
    ) -> bytes:
        """
        Export results as ZIP containing both CSV and Excel (for convenience)
        
        Args:
            data: DataFrame to export
            include_csv: Include CSV files in ZIP
            include_excel: Include Excel file in ZIP
            preset_names: Preset names for combined export
            is_combined: Whether this is combined data
            
        Returns:
            ZIP file as bytes
        """
        processor = ContributionProcessor(data, engine=self.engine)
        files = {}
        
        # Add Excel
        if include_excel:
            excel_data = self.export_to_excel(data, preset_names, is_combined=is_combined)
            files['contribution_analysis.xlsx'] = excel_data
        
        # Add CSVs
        if include_csv:
            csv_files = self.export_to_csv(data, is_combined=is_combined)
            files.update(csv_files)
        
        # Create ZIP
        return processor.create_zip(files)
    
    # ==================== EXPORT/IMPORT ====================
    
    def export_preset_configs(self) -> Dict:
        """
        Export all preset CONFIGURATIONS for backup or transfer
        (NOT result data - results are exported as Excel/CSV only)
        
        Returns:
            Export data dictionary with all preset configs
        """
        export_data, error = self.preset_manager.export_presets()
        if error:
            raise RuntimeError(f"Export failed: {error}")
        return export_data
    
    def import_preset_configs(self, import_data: Dict, overwrite: bool = False) -> int:
        """
        Import preset CONFIGURATIONS from export data
        (NOT result data - results are imported as Excel/CSV files separately)
        
        Args:
            import_data: Data from export_preset_configs()
            overwrite: Whether to overwrite existing configs
            
        Returns:
            Number of presets imported
        """
        count, error = self.preset_manager.import_presets(import_data, overwrite)
        if error:
            raise RuntimeError(f"Import failed: {error}")
        return count
    
    # ==================== ANALYTICS ====================
    
    def get_preset_statistics(self) -> Dict:
        """
        Get statistics about presets
        
        Returns:
            Statistics dictionary
        """
        return self.preset_manager.get_statistics()
    
    def list_all_presets(self) -> List[Dict]:
        """
        List all presets with full information
        
        Returns:
            List of preset dictionaries
        """
        presets, error = self.preset_manager.list_presets()
        if error:
            raise RuntimeError(f"Failed to list presets: {error}")
        return presets
    
    # ==================== ADVANCED EXECUTION ====================
    
    def execute_sequential_analysis(
        self,
        preset_names: List[str],
        majcats: List[str] = None,
        grouping_column: str = 'MACRO_MVGR',
        apply_mappings: bool = True,
        save_to_db: bool = False,
        table_prefix: str = 'CONTRIB'
    ) -> Dict:
        """
        Execute sequential multi-preset contribution analysis with mapping assignments
        Similar to Streamlit workflow
        
        Args:
            preset_names: List of presets to execute (in order)
            majcats: Major categories to filter (None = all)
            grouping_column: Grouping column name
            apply_mappings: Whether to apply mapping assignments
            save_to_db: Whether to save results to database
            table_prefix: Prefix for saved table names
            
        Returns:
            Dict with store_level and company_level DataFrames
        """
        overall_start = time.time()
        results_by_preset = {}
        timing_log = []
        
        # Execute each preset sequentially
        for idx, preset_name in enumerate(preset_names, 1):
            try:
                preset_start = time.time()
                
                # Load preset config
                preset_config, error = self.preset_manager.get_preset(preset_name)
                if error:
                    timing_log.append({
                        'preset': preset_name,
                        'status': 'error',
                        'error': error,
                        'duration': 0
                    })
                    continue
                
                # Execute preset processing
                detailed_df, extra_dfs = self._execute_preset(
                    preset_name,
                    preset_config,
                    majcats,
                    grouping_column
                )
                
                if not detailed_df.empty:
                    results_by_preset[preset_name] = {
                        'detail': detailed_df,
                        'aggregated': extra_dfs.get('aggregated', pd.DataFrame())
                    }
                    status = 'success'
                else:
                    status = 'no_data'
                
                duration = time.time() - preset_start
                timing_log.append({
                    'preset': preset_name,
                    'status': status,
                    'duration': round(duration, 2),
                    'records': len(detailed_df) if not detailed_df.empty else 0
                })
                
            except Exception as e:
                timing_log.append({
                    'preset': preset_name,
                    'status': 'error',
                    'error': str(e),
                    'duration': round(time.time() - preset_start, 2)
                })
        
        # Combine and apply mappings
        if results_by_preset:
            store_df, company_df = self._combine_and_apply_mappings(
                results_by_preset,
                grouping_column,
                apply_mappings
            )
            
            # Save if requested
            if save_to_db:
                self._save_results_to_db(store_df, company_df, table_prefix, grouping_column)
        else:
            store_df = pd.DataFrame()
            company_df = pd.DataFrame()
        
        total_duration = time.time() - overall_start
        
        return {
            'store_level': store_df,
            'company_level': company_df,
            'timing': timing_log,
            'total_duration': round(total_duration, 2),
            'presets_executed': len([t for t in timing_log if t['status'] == 'success'])
        }
    
    def _execute_preset(
        self,
        preset_name: str,
        preset_config: Dict,
        majcats: List[str],
        grouping_column: str
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        Execute single preset - to be implemented with your data query logic
        This is a template that should be extended with actual SQL queries
        
        Args:
            preset_name: Name of preset
            preset_config: Preset configuration
            majcats: Major categories filter
            grouping_column: Grouping column
            
        Returns:
            Tuple of (detailed_df, extra_dfs)
        """
        processor = ContributionProcessor(pd.DataFrame(), engine=self.engine)
        
        # TODO: Implement actual data loading and processing
        # This should:
        # 1. Build WHERE clause for majcats
        # 2. Query raw stock data
        # 3. Query master hierarchy data
        # 4. Merge and compute KPIs
        # 5. Create aggregated company-level data
        # 6. Return (detailed, aggregated)
        
        return pd.DataFrame(), {}
    
    def _combine_and_apply_mappings(
        self,
        results_by_preset: Dict[str, Dict],
        grouping_column: str,
        apply_mappings: bool = True
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Combine results from multiple presets and apply mapping assignments
        
        Args:
            results_by_preset: Results from execute_sequential_analysis
            grouping_column: Grouping column
            apply_mappings: Whether to apply mappings
            
        Returns:
            Tuple of (store_level_df, company_level_df)
        """
        # Extract detailed and aggregated dataframes
        detailed_dfs = {}
        aggregated_dfs = {}
        
        for preset_name, data in results_by_preset.items():
            detailed_dfs[preset_name] = data['detail']
            if not data.get('aggregated', pd.DataFrame()).empty:
                aggregated_dfs[preset_name] = data['aggregated']
        
        # Combine store-level (detailed) data
        processor = ContributionProcessor(pd.DataFrame(), engine=self.engine)
        
        df_combined_store = processor.combine_dataframes(
            detailed_dfs,
            is_aggregated=False,
            grouping_column=grouping_column
        ) if detailed_dfs else pd.DataFrame()
        
        # Combine company-level (aggregated) data
        df_combined_company = processor.combine_dataframes(
            aggregated_dfs,
            is_aggregated=True,
            grouping_column=grouping_column
        ) if aggregated_dfs else pd.DataFrame()
        
        # Apply mapping assignments if requested
        if apply_mappings and (not df_combined_store.empty or not df_combined_company.empty):
            df_combined_store, df_combined_company = self._apply_mapping_assignments(
                df_combined_store,
                df_combined_company
            )
        
        return df_combined_store, df_combined_company
    
    def _apply_mapping_assignments(
        self,
        df_store: pd.DataFrame,
        df_company: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Apply mapping assignments to store and company level data
        
        Args:
            df_store: Store-level DataFrame
            df_company: Company-level DataFrame
            
        Returns:
            Tuple of (updated_store_df, updated_company_df)
        """
        # TODO: Load and apply mapping assignments
        # This should:
        # 1. Load all mapping assignments from database
        # 2. Load corresponding mapping configurations
        # 3. Apply suffix transformations to both DataFrames
        # 4. Return updated DataFrames
        
        return df_store, df_company
    
    def _save_results_to_db(
        self,
        df_store: pd.DataFrame,
        df_company: pd.DataFrame,
        table_prefix: str,
        grouping_column: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Save results to database with dynamic table names
        
        Args:
            df_store: Store-level results
            df_company: Company-level results
            table_prefix: Prefix for table names
            grouping_column: Grouping column name
            
        Returns:
            Tuple of (success: bool, error: Optional[str])
        """
        try:
            from datetime import datetime
            current_month = datetime.now().strftime('%Y_%m')
            safe_grouping_col = grouping_column.replace(' ', '_').replace('-', '_').upper()
            
            # Save store-level data
            if not df_store.empty:
                store_table = f'{table_prefix}_{safe_grouping_col}_{current_month}'
                df_store.to_sql(store_table, self.engine, if_exists='replace', index=False)
            
            # Save company-level data
            if not df_company.empty:
                company_table = f'{table_prefix}_{safe_grouping_col}_CO_{current_month}'
                df_company.to_sql(company_table, self.engine, if_exists='replace', index=False)
            
            return True, None
        except Exception as e:
            return False, str(e)


# ==================== USAGE EXAMPLES ====================

def example_basic_workflow():
    """Example: Basic preset and contribution analysis with Excel/CSV export"""
    
    # Initialize
    engine = create_engine('mssql+pyodbc://...')
    workflow = ContributionWorkflow(engine)
    
    # Setup presets
    presets = workflow.setup_presets()
    print(f"Initial presets: {list(presets.keys())}")
    
    # Create custom preset
    workflow.create_custom_preset(
        name='Q1_ANALYSIS',
        description='Q1 contribution analysis',
        months=['Jan', 'Feb', 'Mar'],
        avg_days=30
    )
    
    # Update sequence
    workflow.update_preset_sequence(['L7D', 'Q1_ANALYSIS'])
    
    # Load data
    data = pd.read_csv('stock_data.csv')
    
    # Process single preset
    results = workflow.process_preset(data, 'Q1_ANALYSIS')
    kpi_data = results['kpis']
    
    # Export to Excel
    excel_bytes = workflow.export_to_excel(kpi_data)
    with open('Q1_Analysis.xlsx', 'wb') as f:
        f.write(excel_bytes)
    
    # Export to CSV
    csv_files = workflow.export_to_csv(kpi_data, 'Q1_Analysis')
    for filename, file_bytes in csv_files.items():
        with open(filename, 'wb') as f:
            f.write(file_bytes)


def example_multi_preset_workflow():
    """Example: Multi-preset analysis with combined export"""
    
    engine = create_engine('mssql+pyodbc://...')
    workflow = ContributionWorkflow(engine)
    
    # Create multiple presets
    presets_to_create = [
        ('L7D_CUSTOM', 'Last 7 days', ['Jan'], 7),
        ('L30D_CUSTOM', 'Last 30 days', ['Jan', 'Feb', 'Mar', 'Apr'], 30),
        ('L90D_CUSTOM', 'Quarterly', ['Jan', 'Apr', 'Jul', 'Oct'], 90)
    ]
    
    for name, desc, months, days in presets_to_create:
        workflow.create_custom_preset(name, desc, months, days)
    
    # Load data
    data = pd.read_csv('master_data.csv')
    
    # Process ALL presets sequentially
    results = workflow.process_multiple_presets(data)
    
    # Combine results
    combined = workflow.combine_preset_results(results)
    
    # Export combined as EXCEL (with multiple sheets)
    excel_bytes = workflow.export_to_excel(
        combined,
        preset_names=['L7D_CUSTOM', 'L30D_CUSTOM', 'L90D_CUSTOM'],
        is_combined=True
    )
    with open('combined_analysis.xlsx', 'wb') as f:
        f.write(excel_bytes)
    
    # Export combined as CSV(s) - may split into multiple files
    csv_files = workflow.export_to_csv(
        combined,
        table_name='combined_analysis',
        is_combined=True
    )
    for filename, file_bytes in csv_files.items():
        with open(filename, 'wb') as f:
            f.write(file_bytes)


def example_zip_export():
    """Example: Export as ZIP containing both Excel and CSV"""
    
    engine = create_engine('mssql+pyodbc://...')
    workflow = ContributionWorkflow(engine)
    
    # Setup and process
    workflow.setup_presets()
    workflow.create_custom_preset('Q2', 'Q2 analysis', ['Apr', 'May', 'Jun'], 30)
    
    data = pd.read_csv('stock_data.csv')
    results = workflow.process_preset(data, 'Q2')
    
    # Export as ZIP (single file, easy for download)
    zip_bytes = workflow.export_with_zip(
        results['kpis'],
        include_csv=True,
        include_excel=True
    )
    
    with open('Q2_Analysis.zip', 'wb') as f:
        f.write(zip_bytes)
    
    print("✅ Exported: Q2_Analysis.zip (contains Excel + CSV files)")


def example_preset_config_backup():
    """Example: Backup and restore preset CONFIGURATIONS"""
    
    engine = create_engine('mssql+pyodbc://...')
    workflow = ContributionWorkflow(engine)
    
    # Setup presets
    workflow.setup_presets()
    workflow.create_custom_preset('BACKUP_TEST', 'Test preset', ['Jan'], 7)
    
    # Export preset CONFIGURATIONS (not result data)
    backup = workflow.export_preset_configs()
    
    # Save backup
    import json
    with open('presets_backup.json', 'w') as f:
        json.dump(backup, f, indent=2, default=str)
    
    print(f"✅ Backed up {len(backup['presets'])} preset configurations")
    
    # Later: Restore presets
    with open('presets_backup.json', 'r') as f:
        backup_data = json.load(f)
    
    imported_count = workflow.import_preset_configs(backup_data, overwrite=True)
    print(f"✅ Restored {imported_count} presets from backup")


if __name__ == '__main__':
    # Run example (modify connection string as needed)
    print("ContributionWorkflow Examples")
    print("See function definitions for usage patterns")
