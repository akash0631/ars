"""
Contribution Analysis API - Processing & Export Endpoints
Handles data processing and export to Excel/CSV (no database storage of results)
"""
from fastapi import APIRouter, HTTPException, Depends, File, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import pandas as pd
import io
from typing import List, Optional

from app.database.session import get_db, get_data_db
from app.services import ContributionProcessor
from app.services.contribution_workflow import ContributionWorkflow
from app.services.preset_manager import PresetManager

router = APIRouter(prefix="/contribution", tags=["Contribution Analysis"])


# ==================== HELPER FUNCTIONS ====================

def get_master_columns_from_db(engine):
    """
    Get master columns from VW_MASTER_PRODUCT.
    Returns the columns: CLR, SZ, RNG_SEG, M_VND_CD, MACRO_MVGR, MICRO_MVGR, FAB
    """
    try:
        sql = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'VW_MASTER_PRODUCT' 
        AND TABLE_SCHEMA = 'dbo'
        AND COLUMN_NAME IN ('CLR','SZ','RNG_SEG','M_VND_CD','MACRO_MVGR','MICRO_MVGR','FAB')    
        ORDER BY ORDINAL_POSITION
        """
        df = pd.read_sql(sql, engine)
        return df['COLUMN_NAME'].tolist() if not df.empty else []
    except Exception as e:
        print(f"⚠️ Failed to get master columns from DB: {e}")
        # Return defaults if query fails
        return ['CLR', 'SZ', 'RNG_SEG', 'M_VND_CD', 'MACRO_MVGR', 'MICRO_MVGR', 'FAB']


# ==================== INITIALIZATION ====================

@router.post("/init")
def initialize_database(db: Session = Depends(get_data_db)):
    """Initialize contribution analysis database tables"""
    try:
        engine = db.get_bind()
        manager = PresetManager(engine)
        
        # Ensure tables exist
        success, error = manager._ensure_table_exists()
        if not success:
            raise HTTPException(status_code=500, detail=error)
        
        # Ensure default preset
        presets_dict, error = manager.ensure_default_preset()
        if error:
            raise HTTPException(status_code=500, detail=error)
        
        return {
            'status': 'success',
            'message': 'Contribution analysis tables initialized',
            'presets_initialized': len(presets_dict)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== PRESET MANAGEMENT ====================

@router.get("/presets")
def list_presets(db: Session = Depends(get_data_db)):
    """List all available presets"""
    try:
        engine = db.get_bind()
        manager = PresetManager(engine)
        
        # Ensure table exists
        success, error = manager._ensure_table_exists()
        if not success:
            raise HTTPException(status_code=500, detail=f"Database init failed: {error}")
        
        presets_list, error = manager.list_presets()
        if error:
            raise HTTPException(status_code=500, detail=error)
        
        return {
            'status': 'success',
            'count': len(presets_list),
            'presets': presets_list
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/presets/sequence")
def get_sequence(db: Session = Depends(get_data_db)):
    """Get preset execution sequence"""
    try:
        engine = db.get_bind()
        manager = PresetManager(engine)
        
        # Ensure table exists
        success, error = manager._ensure_table_exists()
        if not success:
            raise HTTPException(status_code=500, detail=f"Database init failed: {error}")
        
        sequence, error = manager.get_sequence(include_formula=False)
        if error:
            raise HTTPException(status_code=500, detail=error)
        return {
            'status': 'success',
            'sequence': sequence,
            'count': len(sequence)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/execution/majcats")
def get_major_categories(grouping_column: str = "MACRO_MVGR", db: Session = Depends(get_data_db)):
    """
    Get major categories - optionally filtered by grouping column context
    
    Args:
        grouping_column: Current grouping column (for context)
    """
    try:
        engine = db.get_bind()
        majcats = []
        
        # Try multiple sources for major categories
        queries_to_try = [
            # Primary: Master product view
            ("""
            SELECT DISTINCT MAJ_CAT 
            FROM dbo.VW_MASTER_PRODUCT WITH (NOLOCK)
            WHERE MAJ_CAT IS NOT NULL
            ORDER BY MAJ_CAT
            """, "VW_MASTER_PRODUCT"),
            
            # Fallback 1: Direct table if it exists
            ("""
            SELECT DISTINCT MAJ_CAT 
            FROM dbo.Master_PRODUCT WITH (NOLOCK)
            WHERE MAJ_CAT IS NOT NULL
            ORDER BY MAJ_CAT
            """, "Master_PRODUCT"),
            
            # Fallback 2: From hierarchy tables
            ("""
            SELECT DISTINCT MAJ_CAT 
            FROM dbo.Master_HIER_MACRO_MVGR WITH (NOLOCK)
            WHERE MAJ_CAT IS NOT NULL
            ORDER BY MAJ_CAT
            """, "Master_HIER_MACRO_MVGR"),
        ]
        
        for query_text, source_name in queries_to_try:
            try:
                df = pd.read_sql(query_text, engine)
                if not df.empty:
                    majcats = df['MAJ_CAT'].astype(str).str.strip().unique().tolist()
                    print(f"✅ Loaded {len(majcats)} major categories from {source_name}")
                    break
            except Exception as e:
                print(f"⚠️ {source_name} query failed: {str(e)}")
                continue
        
        return {
            'status': 'success',
            'majcats': sorted(majcats),
            'count': len(majcats),
            'grouping_column': grouping_column
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sidebar/months")
def get_available_months(db: Session = Depends(get_data_db)):
    """Get available months from stock data"""
    try:
        engine = db.get_bind()
        
        query = """
        SELECT DISTINCT CONVERT(VARCHAR(7), STOCK_DATE, 120) AS month
        FROM dbo.COUNT_STOCK_DATA_18M WITH (NOLOCK)
        WHERE STOCK_DATE IS NOT NULL
        ORDER BY month DESC
        """
        
        df = pd.read_sql(query, engine)
        months = df['month'].astype(str).str.strip().unique().tolist()
        
        return {
            'status': 'success',
            'months': months,
            'count': len(months)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sidebar/grouping-columns")
def get_grouping_columns_sidebar(db: Session = Depends(get_data_db)):
    """Get available grouping columns for sidebar selector"""
    try:
        available_columns = [
            {
                'name': 'MACRO_MVGR',
                'label': 'Macro Category',
                'description': 'Group by macro vendor category'
            },
            {
                'name': 'M_VND_CD',
                'label': 'Vendor Code',
                'description': 'Group by vendor code'
            },
            {
                'name': 'CATEGORY',
                'label': 'Category',
                'description': 'Group by product category'
            },
            {
                'name': 'SEGMENT',
                'label': 'Segment',
                'description': 'Group by market segment'
            }
        ]
        
        return {
            'status': 'success',
            'columns': available_columns,
            'count': len(available_columns),
            'default': 'MACRO_MVGR'
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/execution/columns")
def get_available_columns(db: Session = Depends(get_data_db)):
    """Get available grouping columns and aggregation columns"""
    try:
        engine = db.get_bind()
        
        # Get master columns dynamically from database
        master_columns = get_master_columns_from_db(engine)
        
        # Standard aggregation/result columns
        result_columns = [
            'OP_STK_Q',
            'OP_STK_V',
            'CL_STK_Q',
            'CL_STK_V',
            'SALE_Q',
            'SALE_V',
            'GM_V',
            'AVG_DNSTY',
            'APF',
            'SALES_PSF',
            'SALE_PSF_ACH%',
            'STOCK_CONT%',
            'SALE_CONT%'
        ]
        
        return {
            'status': 'success',
            'master_columns': master_columns,
            'grouping_columns': ['MACRO_MVGR', 'M_VND_CD', 'CATEGORY', 'SEGMENT'],
            'result_columns': result_columns,
            'metrics': {
                'stock': ['OP_STK_Q', 'CL_STK_Q', 'OP_STK_V', 'CL_STK_V'],
                'sales': ['SALE_Q', 'SALE_V'],
                'profitability': ['GM_V', 'SALES_PSF'],
                'contribution': ['STOCK_CONT%', 'SALE_CONT%'],
                'efficiency': ['AVG_DNSTY', 'APF', 'SALE_PSF_ACH%']
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/execution/options")
def get_execution_options(grouping_column: str = "MACRO_MVGR", db: Session = Depends(get_data_db)):
    """
    Get all execution options including presets, major categories, columns, and mappings
    
    Args:
        grouping_column: Current grouping column
    """
    try:
        engine = db.get_bind()
        manager = PresetManager(engine)
        
        # Ensure tables exist
        success, error = manager._ensure_table_exists()
        if not success:
            raise HTTPException(status_code=500, detail=f"Database init failed: {error}")
        
        # Get presets and sequence
        presets_list, error = manager.list_presets()
        if error:
            presets_list = []
        
        sequence, error = manager.get_sequence(include_formula=False)
        if error:
            sequence = []
        
        # Get major categories for this grouping column
        majcats = []
        majcats_errors = []
        
        # Try multiple sources for major categories
        queries_to_try = [
            # Primary: Master product view
            ("""
            SELECT DISTINCT MAJ_CAT 
            FROM dbo.VW_MASTER_PRODUCT WITH (NOLOCK)
            WHERE MAJ_CAT IS NOT NULL
            ORDER BY MAJ_CAT
            """, "VW_MASTER_PRODUCT"),
            
            # Fallback 1: Direct table if it exists
            ("""
            SELECT DISTINCT MAJ_CAT 
            FROM dbo.Master_PRODUCT WITH (NOLOCK)
            WHERE MAJ_CAT IS NOT NULL
            ORDER BY MAJ_CAT
            """, "Master_PRODUCT"),
            
            # Fallback 2: From hierarchy tables
            ("""
            SELECT DISTINCT MAJ_CAT 
            FROM dbo.Master_HIER_MACRO_MVGR WITH (NOLOCK)
            WHERE MAJ_CAT IS NOT NULL
            ORDER BY MAJ_CAT
            """, "Master_HIER_MACRO_MVGR"),
        ]
        
        for query_text, source_name in queries_to_try:
            try:
                majcats_df = pd.read_sql(query_text, engine)
                if not majcats_df.empty:
                    majcats = majcats_df['MAJ_CAT'].astype(str).str.strip().unique().tolist()
                    print(f"✅ Loaded {len(majcats)} major categories from {source_name}")
                    break
            except Exception as e:
                majcats_errors.append(f"{source_name}: {str(e)}")
                continue
        
        # If all queries failed, log and continue with empty list
        if not majcats:
            print(f"⚠️ Warning: Could not load major categories. Tried sources: {majcats_errors}")
            majcats = []
        
        # Get master columns dynamically from database
        master_columns = get_master_columns_from_db(engine)
        
        # Get available grouping columns
        grouping_columns = [
            'MACRO_MVGR',
            'M_VND_CD',
            'CATEGORY',
            'SEGMENT'
        ]
        
        # Get mappings
        mappings_query = """
        SELECT mapping_name, mapping_json, fallback_json, description
        FROM Cont_mappings
        ORDER BY modified_date DESC
        """
        mappings_list = []
        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                result = conn.execute(text(mappings_query))
                import json
                for row in result:
                    try:
                        mapping_data = {
                            'name': row[0],
                            'suffix_mapping': json.loads(row[1]) if row[1] else {},
                            'fallback_suffixes': json.loads(row[2]) if row[2] else [],
                            'description': row[3] or ''
                        }
                        mappings_list.append(mapping_data)
                    except:
                        pass
        except:
            mappings_list = []
        
        # Get mapping assignments
        assignments_query = """
        SELECT id, col_name, mapping_name, prefix, target
        FROM Cont_mapping_assignments
        ORDER BY id ASC
        """
        assignments_list = []
        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                result = conn.execute(text(assignments_query))
                for row in result:
                    assignment = {
                        'id': row[0],
                        'col_name': row[1],
                        'mapping_name': row[2],
                        'prefix': row[3] or '',
                        'target': row[4] or 'Both'
                    }
                    assignments_list.append(assignment)
        except:
            assignments_list = []
        
        # Result sections organized by metric type
        result_sections = {
            'stock': ['OP_STK_Q', 'CL_STK_Q', 'OP_STK_V', 'CL_STK_V'],
            'sales': ['SALE_Q', 'SALE_V'],
            'profitability': ['GM_V', 'SALES_PSF'],
            'contribution': ['STOCK_CONT%', 'SALE_CONT%'],
            'efficiency': ['AVG_DNSTY', 'APF', 'SALE_PSF_ACH%']
        }
        
        return {
            'status': 'success',
            'grouping_column': grouping_column,
            'master_columns': master_columns,
            'presets': {
                'list': presets_list,
                'sequence': sequence,
                'count': len(presets_list)
            },
            'majcats': sorted(majcats),
            'grouping_columns': grouping_columns,
            'mappings': {
                'list': mappings_list,
                'count': len(mappings_list)
            },
            'assignments': {
                'list': assignments_list,
                'count': len(assignments_list)
            },
            'result_sections': result_sections
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== MAPPING MANAGEMENT ====================

@router.get("/mappings")
def list_mappings(db: Session = Depends(get_data_db)):
    """List all suffix mappings"""
    try:
        from sqlalchemy import text
        engine = db.get_bind()
        
        query = """
        SELECT mapping_name, mapping_json, fallback_json, description, created_date, modified_date
        FROM Cont_mappings
        ORDER BY modified_date DESC
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            mappings = []
            for row in result:
                try:
                    import json
                    mapping_data = {
                        'name': row[0],
                        'suffix_mapping': json.loads(row[1]) if row[1] else {},
                        'fallback_suffixes': json.loads(row[2]) if row[2] else [],
                        'description': row[3] or '',
                        'created_date': str(row[4]) if row[4] else None,
                        'modified_date': str(row[5]) if row[5] else None
                    }
                    mappings.append(mapping_data)
                except:
                    pass
            
            return {
                'status': 'success',
                'count': len(mappings),
                'mappings': mappings
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mappings")
def create_or_update_mapping(
    mapping_name: str,
    suffix_mapping: dict,
    fallback_suffixes: List[str] = None,
    description: str = "",
    db: Session = Depends(get_data_db)
):
    """Create or update a suffix mapping"""
    try:
        import json
        from sqlalchemy import text
        engine = db.get_bind()
        
        if not mapping_name:
            raise HTTPException(status_code=400, detail="mapping_name is required")
        if not suffix_mapping or not isinstance(suffix_mapping, dict):
            raise HTTPException(status_code=400, detail="suffix_mapping must be a non-empty dict")
        
        fallback_suffixes = fallback_suffixes or []
        
        with engine.connect() as conn:
            # Check if mapping exists
            exists = conn.execute(
                text("SELECT COUNT(*) FROM Cont_mappings WHERE mapping_name = :name"),
                {"name": mapping_name}
            ).scalar() > 0
            
            if exists:
                # Update
                conn.execute(
                    text("""
                    UPDATE Cont_mappings 
                    SET mapping_json = :mapping, fallback_json = :fallback, 
                        description = :desc, modified_date = GETDATE()
                    WHERE mapping_name = :name
                    """),
                    {
                        'name': mapping_name,
                        'mapping': json.dumps(suffix_mapping),
                        'fallback': json.dumps(fallback_suffixes),
                        'desc': description
                    }
                )
            else:
                # Create
                conn.execute(
                    text("""
                    INSERT INTO Cont_mappings (mapping_name, mapping_json, fallback_json, description)
                    VALUES (:name, :mapping, :fallback, :desc)
                    """),
                    {
                        'name': mapping_name,
                        'mapping': json.dumps(suffix_mapping),
                        'fallback': json.dumps(fallback_suffixes),
                        'desc': description
                    }
                )
            
            conn.commit()
        
        return {
            'status': 'success',
            'message': f"Mapping '{mapping_name}' {'updated' if exists else 'created'}",
            'mapping_name': mapping_name
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/mappings/{mapping_name}")
def delete_mapping(mapping_name: str, db: Session = Depends(get_data_db)):
    """Delete a suffix mapping"""
    try:
        from sqlalchemy import text
        engine = db.get_bind()
        
        with engine.connect() as conn:
            conn.execute(
                text("DELETE FROM Cont_mappings WHERE mapping_name = :name"),
                {"name": mapping_name}
            )
            conn.commit()
        
        return {
            'status': 'success',
            'message': f"Mapping '{mapping_name}' deleted"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== MAPPING ASSIGNMENTS ====================

@router.get("/mapping-assignments")
def list_mapping_assignments(db: Session = Depends(get_data_db)):
    """List all mapping assignments"""
    try:
        from sqlalchemy import text
        engine = db.get_bind()
        
        query = """
        SELECT id, col_name, mapping_name, prefix, target, created_date, modified_date
        FROM Cont_mapping_assignments
        ORDER BY id ASC
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            assignments = []
            for row in result:
                assignment = {
                    'id': row[0],
                    'col_name': row[1],
                    'mapping_name': row[2],
                    'prefix': row[3] or '',
                    'target': row[4] or 'Both',
                    'created_date': str(row[5]) if row[5] else None,
                    'modified_date': str(row[6]) if row[6] else None
                }
                assignments.append(assignment)
            
            return {
                'status': 'success',
                'count': len(assignments),
                'assignments': assignments
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mapping-assignments")
def create_mapping_assignment(
    col_name: str,
    mapping_name: str,
    prefix: str = "",
    target: str = "Both",
    db: Session = Depends(get_data_db)
):
    """Create a new mapping assignment"""
    try:
        from sqlalchemy import text
        engine = db.get_bind()
        
        if not col_name or not mapping_name:
            raise HTTPException(status_code=400, detail="col_name and mapping_name are required")
        
        # Validate that mapping exists
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT COUNT(*) FROM Cont_mappings WHERE mapping_name = :name"),
                {"name": mapping_name}
            ).scalar() > 0
            
            if not exists:
                raise HTTPException(status_code=400, detail=f"Mapping '{mapping_name}' not found")
            
            conn.execute(
                text("""
                INSERT INTO Cont_mapping_assignments (col_name, mapping_name, prefix, target)
                VALUES (:col_name, :mapping_name, :prefix, :target)
                """),
                {
                    'col_name': col_name,
                    'mapping_name': mapping_name,
                    'prefix': prefix,
                    'target': target
                }
            )
            conn.commit()
        
        return {
            'status': 'success',
            'message': f"Assignment for column '{col_name}' created",
            'col_name': col_name
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/mapping-assignments/{assignment_id}")
def update_mapping_assignment(
    assignment_id: int,
    col_name: str = None,
    mapping_name: str = None,
    prefix: str = None,
    target: str = None,
    db: Session = Depends(get_data_db)
):
    """Update a mapping assignment"""
    try:
        from sqlalchemy import text
        engine = db.get_bind()
        
        with engine.connect() as conn:
            # Build update query dynamically
            updates = []
            params = {'id': assignment_id}
            
            if col_name is not None:
                updates.append("col_name = :col_name")
                params['col_name'] = col_name
            if mapping_name is not None:
                updates.append("mapping_name = :mapping_name")
                params['mapping_name'] = mapping_name
            if prefix is not None:
                updates.append("prefix = :prefix")
                params['prefix'] = prefix
            if target is not None:
                updates.append("target = :target")
                params['target'] = target
            
            if not updates:
                raise HTTPException(status_code=400, detail="No fields to update")
            
            updates.append("modified_date = GETDATE()")
            
            query = f"UPDATE Cont_mapping_assignments SET {', '.join(updates)} WHERE id = :id"
            conn.execute(text(query), params)
            conn.commit()
        
        return {
            'status': 'success',
            'message': f"Assignment {assignment_id} updated",
            'assignment_id': assignment_id
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/mapping-assignments/{assignment_id}")
def delete_mapping_assignment(assignment_id: int, db: Session = Depends(get_data_db)):
    """Delete a mapping assignment"""
    try:
        from sqlalchemy import text
        engine = db.get_bind()
        
        with engine.connect() as conn:
            conn.execute(
                text("DELETE FROM Cont_mapping_assignments WHERE id = :id"),
                {"id": assignment_id}
            )
            conn.commit()
        
        return {
            'status': 'success',
            'message': f"Assignment {assignment_id} deleted"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/mapping-assignments")
def delete_all_mapping_assignments(db: Session = Depends(get_data_db)):
    """Delete all mapping assignments"""
    try:
        from sqlalchemy import text
        engine = db.get_bind()
        
        with engine.connect() as conn:
            result = conn.execute(text("DELETE FROM Cont_mapping_assignments"))
            conn.commit()
        
        return {
            'status': 'success',
            'message': 'All mapping assignments deleted'
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
def create_preset(
    preset_name: str,
    description: str,
    avg_days: int = 30,
    months: List[str] = [],
    db: Session = Depends(get_data_db)
):
    """Create a new preset"""
    try:
        engine = db.get_bind()
        manager = PresetManager(engine)
        
        # Ensure table exists
        success, error = manager._ensure_table_exists()
        if not success:
            raise HTTPException(status_code=500, detail=f"Database init failed: {error}")
        
        config = {
            'type': 'custom',
            'description': description,
            'months': months,
            'avg_days': avg_days,
            'kpi': f'L{avg_days}D'
        }
        
        # Validate
        is_valid, errors = manager.validate_preset_config(config)
        if not is_valid:
            raise HTTPException(status_code=400, detail={'errors': errors})
        
        success, error = manager.create_preset(preset_name, config)
        if not success:
            raise HTTPException(status_code=400, detail=error)
        
        return {'status': 'success', 'message': f"Preset '{preset_name}' created"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== DATA PROCESSING ====================

@router.post("/analyze/upload")
async def analyze_uploaded_file(
    file: UploadFile = File(...),
    preset_name: str = "L7D",
    grouping_column: str = "MACRO_MVGR",
    export_format: str = "excel",
    db: Session = Depends(get_data_db)
):
    """
    Upload CSV file and process with preset
    
    Args:
        file: CSV file with stock data
        preset_name: Preset to use for analysis
        grouping_column: Column for grouping (MACRO_MVGR, M_VND_CD)
        export_format: 'excel', 'csv', or 'zip'
    """
    try:
        engine = db.get_bind()
        
        # Ensure presets are initialized
        manager = PresetManager(engine)
        success, error = manager._ensure_table_exists()
        if not success:
            raise HTTPException(status_code=500, detail=f"Database init failed: {error}")
        
        # Read uploaded file
        contents = await file.read()
        data = pd.read_csv(io.BytesIO(contents))
        
        # Initialize workflow
        workflow = ContributionWorkflow(engine)
        
        # Process with preset
        results = workflow.process_preset(data, preset_name, grouping_column)
        
        if 'error' in results:
            raise HTTPException(status_code=400, detail=results['error'])
        
        kpi_data = results['kpis']
        
        # Export in requested format
        if export_format.lower() == 'zip':
            content = workflow.export_with_zip(kpi_data, include_csv=True, include_excel=True)
            filename = f"{preset_name}_analysis.zip"
            media_type = "application/zip"
        elif export_format.lower() == 'csv':
            csv_dict = workflow.export_to_csv(kpi_data, preset_name)
            # Return first CSV file if multiple
            content = list(csv_dict.values())[0]
            filename = f"{preset_name}_analysis.csv"
            media_type = "text/csv"
        else:  # excel
            content = workflow.export_to_excel(kpi_data)
            filename = f"{preset_name}_analysis.xlsx"
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        return StreamingResponse(
            iter([content]),
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except pd.errors.ParserError as e:
        raise HTTPException(status_code=400, detail="Invalid CSV file format")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/multi-preset")
async def analyze_multi_preset(
    file: UploadFile = File(...),
    preset_names: List[str] = None,
    grouping_column: str = "MACRO_MVGR",
    export_format: str = "excel",
    db: Session = Depends(get_data_db)
):
    """
    Upload file and process with multiple presets
    
    Args:
        file: CSV file with stock data
        preset_names: List of presets to use. If none, uses sequence.
        grouping_column: Column for grouping
        export_format: 'excel', 'csv', or 'zip'
    """
    try:
        engine = db.get_bind()
        
        # Ensure presets are initialized
        manager = PresetManager(engine)
        success, error = manager._ensure_table_exists()
        if not success:
            raise HTTPException(status_code=500, detail=f"Database init failed: {error}")
        
        # Read uploaded file
        contents = await file.read()
        data = pd.read_csv(io.BytesIO(contents))
        
        # Initialize workflow
        workflow = ContributionWorkflow(engine)
        
        # Get preset sequence if not specified
        if not preset_names:
            preset_names = workflow.get_execution_sequence()
        
        # Process multiple presets
        results = workflow.process_multiple_presets(data, preset_names, grouping_column)
        
        # Check for errors
        errors = {k: v.get('error') for k, v in results.items() if 'error' in v}
        if errors:
            raise HTTPException(status_code=400, detail={'failed_presets': errors})
        
        # Combine results
        combined = workflow.combine_preset_results(results)
        
        if combined.empty:
            raise HTTPException(status_code=400, detail="No data to combine")
        
        # Export in requested format
        if export_format.lower() == 'zip':
            content = workflow.export_with_zip(
                combined,
                include_csv=True,
                include_excel=True,
                preset_names=preset_names,
                is_combined=True
            )
            filename = "combined_analysis.zip"
            media_type = "application/zip"
        elif export_format.lower() == 'csv':
            csv_dict = workflow.export_to_csv(combined, "combined_analysis", is_combined=True)
            # Return first CSV if multiple
            content = list(csv_dict.values())[0]
            filename = "combined_analysis.csv"
            media_type = "text/csv"
        else:  # excel
            content = workflow.export_to_excel(
                combined,
                preset_names=preset_names,
                is_combined=True
            )
            filename = "combined_analysis.xlsx"
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        return StreamingResponse(
            iter([content]),
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except pd.errors.ParserError:
        raise HTTPException(status_code=400, detail="Invalid CSV file format")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== SIDEBAR / CONNECTION ====================

@router.get("/sidebar/status")
def get_sidebar_status(db: Session = Depends(get_data_db)):
    """Get database connection status and configuration for sidebar"""
    try:
        from sqlalchemy import text
        engine = db.get_bind()
        
        # Get connection info from engine URL
        url = engine.url
        connection_info = {
            'server': url.host or 'localhost',
            'database': url.database or 'unknown',
            'username': url.username or 'unknown',
            'driver': url.drivername or 'mssql'
        }
        
        # Test connection
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            connected = True
        except:
            connected = False
        
        # Get presets count
        manager = PresetManager(engine)
        presets_list, _ = manager.list_presets()
        preset_count = len([p for p in presets_list if p.get('type') != 'formula'])
        
        # Get majcats count (for default grouping column)
        try:
            query = """
            SELECT COUNT(DISTINCT MAJ_CAT) 
            FROM dbo.VW_MASTER_PRODUCT WITH (NOLOCK)
            WHERE MAJ_CAT IS NOT NULL
            """
            majcat_count = pd.read_sql(query, engine).iloc[0, 0]
        except:
            majcat_count = 0
        
        return {
            'status': 'success',
            'connected': connected,
            'connection_info': connection_info,
            'presets_count': preset_count,
            'majcat_count': majcat_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== EXECUTION MANAGEMENT ====================
def get_execution_options(
    grouping_column: str = "MACRO_MVGR",
    db: Session = Depends(get_data_db)
):
    """
    Get all options needed for execution view:
    - Available presets and sequences
    - Available major categories (for current grouping column)
    - Available grouping columns
    - Result column options
    - Mapping configurations
    - Current context
    """
    try:
        engine = db.get_bind()
        manager = PresetManager(engine)
        
        # Ensure tables exist
        success, error = manager._ensure_table_exists()
        if not success:
            raise HTTPException(status_code=500, detail=f"Database init failed: {error}")
        
        # Get presets
        presets_list, error = manager.list_presets()
        if error:
            presets_list = []
        
        # Get sequence
        sequence, seq_error = manager.get_sequence(include_formula=False)
        if seq_error:
            sequence = []
        
        # Get majcats
        try:
            query = """
            SELECT DISTINCT MAJ_CAT 
            FROM dbo.VW_MASTER_PRODUCT WITH (NOLOCK)
            WHERE MAJ_CAT IS NOT NULL
            ORDER BY MAJ_CAT
            """
            df = pd.read_sql(query, engine)
            majcats = df['MAJ_CAT'].astype(str).str.strip().unique().tolist()
        except:
            majcats = []
        
        # Get mappings
        try:
            from sqlalchemy import text
            query = "SELECT mapping_name FROM Cont_mappings ORDER BY mapping_name"
            with engine.connect() as conn:
                result = conn.execute(text(query))
                mapping_names = [row[0] for row in result]
        except:
            mapping_names = []
        
        # Get mapping assignments
        try:
            from sqlalchemy import text
            query = "SELECT col_name, mapping_name, prefix, target FROM Cont_mapping_assignments ORDER BY id"
            with engine.connect() as conn:
                result = conn.execute(text(query))
                assignments = []
                for row in result:
                    assignments.append({
                        'col_name': row[0],
                        'mapping_name': row[1],
                        'prefix': row[2] or '',
                        'target': row[3] or 'Both'
                    })
        except:
            assignments = []
        
        return {
            'status': 'success',
            'current_context': {
                'grouping_column': grouping_column,
                'majcat_count': len(majcats)
            },
            'execution_options': {
                'presets': {
                    'available': presets_list,
                    'sequence': sequence,
                    'count': len(presets_list)
                },
                'major_categories': {
                    'available': sorted(majcats),
                    'count': len(majcats),
                    'grouping_column': grouping_column
                },
                'grouping_columns': {
                    'available': [
                        'MACRO_MVGR',
                        'M_VND_CD',
                        'CATEGORY',
                        'SEGMENT'
                    ],
                    'current': grouping_column
                },
                'result_sections': {
                    'stock': ['OP_STK_Q', 'CL_STK_Q', 'OP_STK_V', 'CL_STK_V'],
                    'sales': ['SALE_Q', 'SALE_V'],
                    'profitability': ['GM_V', 'SALES_PSF'],
                    'contribution': ['STOCK_CONT%', 'SALE_CONT%'],
                    'efficiency': ['AVG_DNSTY', 'APF', 'SALE_PSF_ACH%']
                },
                'mappings': {
                    'available': mapping_names,
                    'count': len(mapping_names),
                    'assignments': assignments,
                    'assignments_count': len(assignments)
                },
                'export_formats': ['excel', 'csv', 'zip']
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/execute-sequence")
async def execute_sequential_analysis(
    file: UploadFile = File(...),
    preset_names: List[str] = None,
    majcats: List[str] = None,
    grouping_column: str = "MACRO_MVGR",
    apply_mappings: bool = True,
    save_to_db: bool = False,
    export_format: str = "excel",
    db: Session = Depends(get_data_db)
):
    """
    Execute sequential multi-preset analysis with mapping assignments
    Advanced workflow matching Streamlit execution
    
    This endpoint handles the complete execution flow:
    1. Load presets and sequence (if not specified)
    2. Filter by major categories (majcats)
    3. Group data by selected grouping_column
    4. Execute each preset sequentially with timing
    5. Apply mapping assignments to create output columns
    6. Export results in requested format
    
    Args:
        file: CSV file with stock data
        preset_names: List of presets to execute in order
                      (None = use preset sequence order)
        majcats: List of major categories to filter
                (None = include all major categories)
        grouping_column: Column for grouping analysis
                        Options: MACRO_MVGR, M_VND_CD, CATEGORY, SEGMENT
                        Default: MACRO_MVGR
                        Note: Affects majcat availability and result granularity
        apply_mappings: Apply mapping assignments to output columns
                       (applies suffix transformations defined in mappings)
        save_to_db: Save results to database with dynamic table naming
                   Tables: PREFIX_GROUPING_YYYYMM (store-level)
                          PREFIX_GROUPING_CO_YYYYMM (company-level)
        export_format: Output format
                      Options: excel, csv, zip (combines both)
    
    Returns:
        File download response with requested format
    """
    try:
        engine = db.get_bind()
        
        # Initialize database
        manager = PresetManager(engine)
        success, error = manager._ensure_table_exists()
        if not success:
            raise HTTPException(status_code=500, detail=f"Database init failed: {error}")
        
        # Validate grouping_column
        valid_columns = ['MACRO_MVGR', 'M_VND_CD', 'CATEGORY', 'SEGMENT']
        if grouping_column not in valid_columns:
            raise HTTPException(status_code=400, detail=f"Invalid grouping_column. Must be one of: {valid_columns}")
        
        # Get or load preset sequence
        if not preset_names:
            sequence, error = manager.get_sequence(include_formula=False)
            if error:
                raise HTTPException(status_code=500, detail=f"Failed to get preset sequence: {error}")
            preset_names = sequence
        
        if not preset_names:
            raise HTTPException(status_code=400, detail="No presets available")
        
        # Initialize workflow
        workflow = ContributionWorkflow(engine)
        
        # Execute advanced sequential analysis
        results = workflow.execute_sequential_analysis(
            preset_names=preset_names,
            majcats=majcats,
            grouping_column=grouping_column,
            apply_mappings=apply_mappings,
            save_to_db=save_to_db
        )
        
        # Extract results
        store_df = results.get('store_level', pd.DataFrame())
        company_df = results.get('company_level', pd.DataFrame())
        
        if store_df.empty and company_df.empty:
            raise HTTPException(status_code=400, detail="No data generated from analysis")
        
        # Use store-level for export if available
        export_df = store_df if not store_df.empty else company_df
        
        # Export in requested format
        if export_format.lower() == 'zip':
            # Create ZIP with both store and company level
            processor = ContributionProcessor(export_df, engine=engine)
            files = {}
            
            if not store_df.empty:
                store_bytes = processor.export_to_excel("store_level.xlsx")
                files['store_level.xlsx'] = store_bytes
            
            if not company_df.empty:
                company_bytes = processor.export_to_excel("company_level.xlsx")
                files['company_level.xlsx'] = company_bytes
            
            content = processor.create_zip(files)
            filename = "sequential_analysis.zip"
            media_type = "application/zip"
        elif export_format.lower() == 'csv':
            processor = ContributionProcessor(export_df, engine=engine)
            csv_dict = processor.export_to_csv("sequential_analysis")
            content = list(csv_dict.values())[0] if csv_dict else b''
            filename = "sequential_analysis.csv"
            media_type = "text/csv"
        else:  # excel
            processor = ContributionProcessor(export_df, engine=engine)
            content = processor.export_to_excel("sequential_analysis.xlsx")
            filename = "sequential_analysis.xlsx"
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        return StreamingResponse(
            iter([content]),
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== EXPORT TEMPLATES ====================

@router.get("/templates/sample-input")
def get_sample_input():
    """Download sample input CSV template"""
    try:
        # Create sample data with required columns
        sample_data = pd.DataFrame({
            'ST_CD': ['S001', 'S002', 'S003'],
            'ST_NM': ['Store 1', 'Store 2', 'Store 3'],
            'MAJ_CAT': ['CAT_A', 'CAT_B', 'CAT_A'],
            'OP_STK_Q': [100, 200, 150],
            'CL_STK_Q': [95, 210, 140],
            'OP_STK_V': [10000, 20000, 15000],
            'CL_STK_V': [9500, 21000, 14000],
            'SALE_Q': [50, 100, 75],
            'SALE_V': [5000, 10000, 7500],
            'GM_V': [1000, 2000, 1500],
            'AVG_DNSTY': [50.0, 45.0, 55.0],
            'APF': [25.0, 28.0, 26.0]
        })
        
        # Convert to Excel
        from app.services import ContributionProcessor
        processor = ContributionProcessor(sample_data)
        excel_bytes = processor.export_to_excel("sample_input.xlsx")
        
        return StreamingResponse(
            iter([excel_bytes]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=sample_input.xlsx"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates/instructions")
def get_instructions():
    """Get API usage instructions"""
    return {
        'title': 'Contribution Analysis API',
        'description': 'Process stock data and export to Excel/CSV',
        'endpoints': {
            'POST /analyze/upload': {
                'description': 'Upload CSV and process with single preset',
                'parameters': {
                    'file': 'CSV file with stock data',
                    'preset_name': 'Preset name (default: L7D)',
                    'grouping_column': 'MACRO_MVGR or M_VND_CD (default: MACRO_MVGR)',
                    'export_format': 'excel, csv, or zip (default: excel)'
                }
            },
            'POST /analyze/multi-preset': {
                'description': 'Upload CSV and process with multiple presets',
                'parameters': {
                    'file': 'CSV file with stock data',
                    'preset_names': 'List of preset names (optional, uses sequence if not provided)',
                    'grouping_column': 'MACRO_MVGR or M_VND_CD (default: MACRO_MVGR)',
                    'export_format': 'excel, csv, or zip (default: excel)'
                }
            }
        },
        'required_columns': [
            'ST_CD', 'ST_NM', 'MAJ_CAT',
            'OP_STK_Q', 'CL_STK_Q', 'OP_STK_V', 'CL_STK_V',
            'SALE_Q', 'SALE_V', 'GM_V', 'AVG_DNSTY', 'APF'
        ]
    }
