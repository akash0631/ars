"""
Contribution Percentage API Routes
FastAPI endpoints for preset, mapping, and calculation management
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import List
import json
from datetime import datetime
import pandas as pd

from app.models.contribution import ContPreset, ContMapping, ContMappingAssignment
from app.schemas.contribution import (
    PresetCreate, PresetUpdate, PresetResponse,
    MappingCreate, MappingUpdate, MappingResponse,
    AssignmentCreate, AssignmentUpdate, AssignmentResponse,
    ContributionCalculationRequest, ContributionCalculationResponse,
    DynamicQueryRequest, QueryResponse
)
from app.database.session import get_db, get_data_db
from app.services.preset_manager import PresetManager

router = APIRouter(prefix="/contribution", tags=["Contribution Percentage"])


# ============ HELPER FUNCTIONS ============
def get_available_columns(engine):
    """Get available grouping columns from database"""
    try:
        valid_columns = ['MACRO_MVGR', 'M_VND_CD', 'CATEGORY', 'SEGMENT']
        return valid_columns
    except Exception:
        return ['MACRO_MVGR', 'M_VND_CD', 'CATEGORY', 'SEGMENT']


def get_majcats(engine, grouping_column, master_column=None):
    """
    Load major categories from database using dynamic table name.
    
    Source: Master_HIER_{grouping_column} table
    Filter: WHERE SEG IN ('APP', 'GM')
    
    Args:
        engine: Database engine
        grouping_column: MACRO_MVGR, M_VND_CD, CATEGORY, or SEGMENT
        master_column: Ignored - majcats are always loaded from hierarchy, filtering handled on frontend
    
    Returns:
        tuple: (majcats_list, error_message)
    """
    try:
        # Validate grouping_column to prevent SQL injection
        valid_columns = get_available_columns(engine)
        if grouping_column not in valid_columns:
            grouping_column = 'MACRO_MVGR'  # Default fallback
        
        # Load all majcats from hierarchy table (exact approach per documentation)
        sql = f"""
        SELECT DISTINCT MAJ_CAT 
        FROM dbo.Master_HIER_{grouping_column} A 
        WHERE SEG IN ('APP', 'GM')
        ORDER BY MAJ_CAT
        """
        df = pd.read_sql(sql, engine)
        majcats = df['MAJ_CAT'].tolist() if not df.empty else []
        
        print(f"✅ Loaded {len(majcats)} majcats from Master_HIER_{grouping_column} (APP/GM segments)")
        return majcats, None
        
    except Exception as e:
        # Fallback to default table if specified table doesn't exist
        try:
            sql = """
            SELECT DISTINCT MAJ_CAT 
            FROM dbo.Master_HIER_MACRO_MVGR A 
            WHERE SEG IN ('APP', 'GM')
            ORDER BY MAJ_CAT
            """
            df = pd.read_sql(sql, engine)
            majcats = df['MAJ_CAT'].tolist() if not df.empty else []
            
            print(f"⚠️ Using fallback table Master_HIER_MACRO_MVGR - {len(majcats)} majcats")
            return majcats, f"Using default table due to: {str(e)}"
        except Exception as e2:
            print(f"❌ Failed to load majcats: {str(e2)}")
            return [], f"Failed to load majcats: {str(e)}"


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


# ============ EXECUTION OPTIONS ENDPOINT ============
@router.get("/execution/options")
def get_execution_options(
    grouping_column: str = "MACRO_MVGR", 
    master_column: str = None,
    db: Session = Depends(get_data_db)
):
    """
    Get all execution options including presets, major categories, columns, and mappings
    
    Args:
        grouping_column: Current grouping column
        master_column: Selected master column to filter majcats (optional)
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
        
        # Get major categories using the function (loads from Master_HIER_{grouping_column} with SEG filter)
        majcats, majcats_error = get_majcats(engine, grouping_column)
        if majcats_error:
            print(f"⚠️ {majcats_error}")
        
        print(f"✅ Execution options loaded - {len(majcats)} majcats available")
        
        # Get master columns dynamically from database
        master_columns = get_master_columns_from_db(engine)
        
        # Get available grouping columns
        grouping_columns = [
            'MACRO_MVGR',
            'M_VND_CD',
            'CATEGORY',
            'SEGMENT'
        ]
        
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
            'result_sections': result_sections
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ PRESET ENDPOINTS ============
@router.get("/presets", response_model=List[PresetResponse])
def list_presets(db: Session = Depends(get_data_db)):
    """List all presets ordered by sequence"""
    presets = db.query(ContPreset).order_by(ContPreset.sequence_order).all()
    return presets


@router.post("/presets", response_model=PresetResponse)
def create_preset(preset: PresetCreate, db: Session = Depends(get_data_db)):
    """Create a new preset"""
    try:
        # Validate JSON
        json.loads(preset.config_json)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in config_json")
    
    # Check if preset already exists
    existing = db.query(ContPreset).filter(ContPreset.preset_name == preset.preset_name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Preset with this name already exists")
    
    db_preset = ContPreset(**preset.dict())
    db.add(db_preset)
    db.commit()
    db.refresh(db_preset)
    return db_preset


@router.get("/presets/{preset_name}", response_model=PresetResponse)
def get_preset(preset_name: str, db: Session = Depends(get_data_db)):
    """Get preset by name"""
    preset = db.query(ContPreset).filter(ContPreset.preset_name == preset_name).first()
    if not preset:
        raise HTTPException(status_code=404, detail="Preset not found")
    return preset


@router.put("/presets/{preset_name}", response_model=PresetResponse)
def update_preset(preset_name: str, preset: PresetUpdate, db: Session = Depends(get_data_db)):
    """Update a preset"""
    db_preset = db.query(ContPreset).filter(ContPreset.preset_name == preset_name).first()
    if not db_preset:
        raise HTTPException(status_code=404, detail="Preset not found")
    
    if preset.config_json:
        try:
            json.loads(preset.config_json)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in config_json")
    
    update_data = preset.dict(exclude_unset=True)
    update_data['modified_date'] = datetime.utcnow()
    
    for key, value in update_data.items():
        setattr(db_preset, key, value)
    
    db.commit()
    db.refresh(db_preset)
    return db_preset


@router.delete("/presets/{preset_name}")
def delete_preset(preset_name: str, db: Session = Depends(get_data_db)):
    """Delete a preset"""
    db_preset = db.query(ContPreset).filter(ContPreset.preset_name == preset_name).first()
    if not db_preset:
        raise HTTPException(status_code=404, detail="Preset not found")
    
    db.delete(db_preset)
    db.commit()
    return {"status": "success", "message": f"Preset '{preset_name}' deleted"}


# ============ MAPPING ENDPOINTS ============
@router.get("/mappings", response_model=List[MappingResponse])
def list_mappings(db: Session = Depends(get_data_db)):
    """List all mappings"""
    mappings = db.query(ContMapping).all()
    return mappings


@router.post("/mappings", response_model=MappingResponse)
def create_mapping(mapping: MappingCreate, db: Session = Depends(get_data_db)):
    """Create a new mapping"""
    try:
        json.loads(mapping.mapping_json)
        if mapping.fallback_json:
            json.loads(mapping.fallback_json)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in mapping data")
    
    existing = db.query(ContMapping).filter(ContMapping.mapping_name == mapping.mapping_name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Mapping with this name already exists")
    
    db_mapping = ContMapping(**mapping.dict())
    db.add(db_mapping)
    db.commit()
    db.refresh(db_mapping)
    return db_mapping


@router.get("/mappings/{mapping_name}", response_model=MappingResponse)
def get_mapping(mapping_name: str, db: Session = Depends(get_data_db)):
    """Get mapping by name"""
    mapping = db.query(ContMapping).filter(ContMapping.mapping_name == mapping_name).first()
    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return mapping


@router.put("/mappings/{mapping_name}", response_model=MappingResponse)
def update_mapping(mapping_name: str, mapping: MappingUpdate, db: Session = Depends(get_data_db)):
    """Update a mapping"""
    db_mapping = db.query(ContMapping).filter(ContMapping.mapping_name == mapping_name).first()
    if not db_mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    
    update_data = mapping.dict(exclude_unset=True)
    update_data['modified_date'] = datetime.utcnow()
    
    for key, value in update_data.items():
        setattr(db_mapping, key, value)
    
    db.commit()
    db.refresh(db_mapping)
    return db_mapping


@router.delete("/mappings/{mapping_name}")
def delete_mapping(mapping_name: str, db: Session = Depends(get_data_db)):
    """Delete a mapping"""
    db_mapping = db.query(ContMapping).filter(ContMapping.mapping_name == mapping_name).first()
    if not db_mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    
    db.delete(db_mapping)
    db.commit()
    return {"status": "success", "message": f"Mapping '{mapping_name}' deleted"}


# ============ ASSIGNMENT ENDPOINTS ============
@router.get("/assignments", response_model=List[AssignmentResponse])
def list_assignments(db: Session = Depends(get_data_db)):
    """List all assignments"""
    assignments = db.query(ContMappingAssignment).all()
    return assignments


@router.post("/assignments", response_model=AssignmentResponse)
def create_assignment(assignment: AssignmentCreate, db: Session = Depends(get_data_db)):
    """Create a new assignment"""
    # Verify mapping exists
    mapping = db.query(ContMapping).filter(ContMapping.mapping_name == assignment.mapping_name).first()
    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    
    db_assignment = ContMappingAssignment(**assignment.dict())
    db.add(db_assignment)
    db.commit()
    db.refresh(db_assignment)
    return db_assignment


@router.get("/assignments/{assignment_id}", response_model=AssignmentResponse)
def get_assignment(assignment_id: int, db: Session = Depends(get_data_db)):
    """Get assignment by ID"""
    assignment = db.query(ContMappingAssignment).filter(ContMappingAssignment.id == assignment_id).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Assignment not found")
    return assignment


@router.put("/assignments/{assignment_id}", response_model=AssignmentResponse)
def update_assignment(assignment_id: int, assignment: AssignmentUpdate, db: Session = Depends(get_data_db)):
    """Update an assignment"""
    db_assignment = db.query(ContMappingAssignment).filter(ContMappingAssignment.id == assignment_id).first()
    if not db_assignment:
        raise HTTPException(status_code=404, detail="Assignment not found")
    
    if assignment.mapping_name:
        mapping = db.query(ContMapping).filter(ContMapping.mapping_name == assignment.mapping_name).first()
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
    
    update_data = assignment.dict(exclude_unset=True)
    update_data['modified_date'] = datetime.utcnow()
    
    for key, value in update_data.items():
        setattr(db_assignment, key, value)
    
    db.commit()
    db.refresh(db_assignment)
    return db_assignment


@router.delete("/assignments/{assignment_id}")
def delete_assignment(assignment_id: int, db: Session = Depends(get_data_db)):
    """Delete an assignment"""
    db_assignment = db.query(ContMappingAssignment).filter(ContMappingAssignment.id == assignment_id).first()
    if not db_assignment:
        raise HTTPException(status_code=404, detail="Assignment not found")
    
    db.delete(db_assignment)
    db.commit()
    return {"status": "success", "message": f"Assignment {assignment_id} deleted"}


# ============ EXECUTION ENDPOINTS ============
@router.post("/calculate", response_model=ContributionCalculationResponse)
def calculate_contribution(request: ContributionCalculationRequest, db: Session = Depends(get_data_db)):
    """
    Execute contribution percentage calculation with actual data processing
    
    Args:
        request: Contains presets, major_categories, group_by, etc.
        db: Database session
    
    Returns:
        Calculation results with store and company level aggregations
    """
    import time
    from app.services.preset_manager import PresetManager
    from app.services.contribution_processor import ContributionProcessor
    
    start_time = time.time()
    
    try:
        engine = db.get_bind()
        manager = PresetManager(engine)
        
        # ============ STEP 1: LOAD PRESETS ============
        presets_list, error = manager.list_presets()
        if error or not presets_list:
            raise HTTPException(status_code=400, detail="Failed to load presets")
        
        # Filter to requested presets only
        selected_presets = [p for p in presets_list if p['preset_name'] in request.presets]
        if not selected_presets:
            raise HTTPException(status_code=400, detail="No matching presets found")
        
        # ============ STEP 2: BUILD MAJCAT FILTER ============
        majcat_filter = ""
        if request.major_categories:
            majcat_str = "','".join([m.replace("'", "''") for m in request.major_categories])
            majcat_filter = f"AND prod.MAJ_CAT IN ('{majcat_str}')"
        
        # ============ STEP 3: LOAD MASTER DATA ============
        # Load store plan and avg density
        apf = pd.read_sql("SELECT ST_CD, APF, STATUS FROM Master_STORE_PLAN", engine)
        avg_density = pd.read_sql("SELECT MAJ_CAT, AVG_DNSTY FROM master_avg_density", engine)
        
        # ============ STEP 4: PROCESS EACH PRESET ============
        all_results = []
        grouping_column = request.group_by or "ST_CD"
        
        for preset in selected_presets:
            try:
                preset_name = preset['preset_name']
                preset_config = preset.get('config', {})
                months = preset_config.get('months', [])
                avg_days = preset_config.get('avg_days', 30)
                
                # Build date filter
                if preset_name.upper() == 'L7D':
                    date_filter = "sal_stk.KPI = 'L7D'"
                else:
                    if months:
                        months_str = "','".join(months)
                        date_filter = f"sal_stk.STOCK_DATE IN ('{months_str}') AND sal_stk.KPI = 'L18M'"
                    else:
                        date_filter = "1=1"
                
                # ============ FETCH DATA FOR THIS PRESET ============
                data_query = f"""
                SELECT
                    sal_stk.STOCK_DATE,
                    sal_stk.WERKS AS ST_CD,
                    prod.MAJ_CAT,
                    prod.{grouping_column},
                    COALESCE(SUM(sal_stk.OP_STK_QTY), 0) AS OP_STK_QTY,
                    COALESCE(SUM(sal_stk.OP_STK_VAL), 0) AS OP_STK_VAL,
                    COALESCE(SUM(sal_stk.CL_STK_QTY), 0) AS CL_STK_QTY,
                    COALESCE(SUM(sal_stk.CL_STK_VAL), 0) AS CL_STK_VAL,
                    COALESCE(SUM(sal_stk.SALE_QTY), 0) AS SALE_QTY,
                    COALESCE(SUM(sal_stk.SALE_VAL), 0) AS SALE_VAL,
                    COALESCE(SUM(sal_stk.GM_VAL), 0) AS GM_VAL
                FROM dbo.COUNT_STOCK_DATA_18M sal_stk WITH (NOLOCK)
                LEFT JOIN (
                    SELECT ARTICLE_NUMBER AS MATNR, MAJ_CAT, {grouping_column}, SEG
                    FROM dbo.VW_MASTER_PRODUCT WITH (NOLOCK)
                ) prod ON sal_stk.MATNR = prod.MATNR
                WHERE {date_filter}
                  AND prod.SEG IN ('APP', 'GM')
                  {majcat_filter}
                GROUP BY sal_stk.WERKS, sal_stk.STOCK_DATE, prod.MAJ_CAT, prod.{grouping_column}
                """
                
                df_data = pd.read_sql(data_query, engine)
                
                if df_data.empty:
                    print(f"⚠️ No data found for preset: {preset_name}")
                    continue
                
                print(f"✅ Loaded {len(df_data)} records for preset: {preset_name}")
                
                # ============ NORMALIZE AND AGGREGATE DATA ============
                # Normalize quantities (divide by 1000) and values (divide by 100000)
                df_data['OP_STK_Q'] = df_data['OP_STK_QTY'] / 1000.0
                df_data['CL_STK_Q'] = df_data['CL_STK_QTY'] / 1000.0
                df_data['SALE_Q'] = df_data['SALE_QTY'] / 1000.0
                df_data['OP_STK_V'] = df_data['OP_STK_VAL'] / 100000.0
                df_data['CL_STK_V'] = df_data['CL_STK_VAL'] / 100000.0
                df_data['SALE_V'] = df_data['SALE_VAL'] / 100000.0
                df_data['GM_V'] = df_data['GM_VAL'] / 100000.0
                
                # Aggregate by store and majcat
                df_agg = df_data.groupby(['ST_CD', 'MAJ_CAT', grouping_column]).agg({
                    'OP_STK_Q': 'mean',
                    'CL_STK_Q': 'mean',
                    'SALE_Q': 'mean',
                    'OP_STK_V': 'mean',
                    'CL_STK_V': 'mean',
                    'SALE_V': 'mean',
                    'GM_V': 'mean'
                }).reset_index()
                
                # ============ CALCULATE KPIs ============
                # Stock Contribution % = Stock Value / (Opening Stock Value + Closing Stock Value)
                total_stock_v = df_agg['OP_STK_V'] + df_agg['CL_STK_V']
                df_agg['STOCK_CONT%'] = (df_agg['CL_STK_V'] / total_stock_v * 100).fillna(0)
                
                # Sale Contribution % = Sale Value / Total Value
                df_agg['SALE_CONT%'] = (df_agg['SALE_V'] / df_agg['SALE_V'].sum() * 100).fillna(0)
                
                # Sales PSF Achievement % = (Actual Sales / Expected Sales) * 100
                # For now, use a simple metric
                df_agg['SALES_PSF_ACH%'] = (df_agg['SALE_V'] / (df_agg['CL_STK_V'] + 1) * 100).fillna(0)
                
                # GM PSF Achievement % similar
                df_agg['GM_PSF_ACH%'] = (df_agg['GM_V'] / (df_agg['CL_STK_V'] + 1) * 100).fillna(0)
                
                # Add preset name and grouping info
                df_agg['PRESET'] = preset_name
                df_agg['GROUPING_COL'] = grouping_column
                
                all_results.append(df_agg)
                
            except Exception as e:
                print(f"❌ Error processing preset {preset_name}: {str(e)}")
                continue
        
        # ============ STEP 5: COMBINE RESULTS ============
        if not all_results:
            raise HTTPException(status_code=400, detail="No data found for selected criteria")
        
        df_combined = pd.concat(all_results, ignore_index=True)
        
        # Aggregate by Store (ST_CD)
        store_results = df_combined.groupby(['PRESET', 'ST_CD', 'MAJ_CAT']).agg({
            'STOCK_CONT%': 'mean',
            'SALE_CONT%': 'mean',
            'SALES_PSF_ACH%': 'mean',
            'GM_PSF_ACH%': 'mean'
        }).reset_index().to_dict('records')
        
        # Aggregate by Company (All stores)
        company_results = df_combined.groupby(['PRESET', 'MAJ_CAT']).agg({
            'STOCK_CONT%': 'mean',
            'SALE_CONT%': 'mean',
            'SALES_PSF_ACH%': 'mean',
            'GM_PSF_ACH%': 'mean'
        }).reset_index().to_dict('records')
        
        # Return full detailed data for export
        store_detailed = df_combined.to_dict('records')
        
        execution_time = time.time() - start_time
        
        return ContributionCalculationResponse(
            status="success",
            message=f"Processed {len(all_results)} presets with {len(df_combined)} records",
            presets_executed=request.presets,
            major_categories=request.major_categories or [],
            store_results={
                'count': len(store_detailed),
                'columns': list(df_combined.columns),
                'sample': store_detailed[:5],
                'full': store_detailed
            },
            company_results={
                'count': len(company_results),
                'sample': company_results
            },
            saved_tables=[],
            execution_time=execution_time
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Calculation error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Calculation failed: {str(e)}")


@router.get("/dynamic-query")
def get_dynamic_query(
    grouping_column: str = Query(..., description="Hierarchy column"),
    db: Session = Depends(get_data_db)
):
    """
    Build and return dynamic SQL query based on grouping column
    """
    try:
        # TODO: Implement dynamic query building
        # 1. Validate grouping column
        # 2. Select appropriate Master_HIER_{grouping_column} table
        # 3. Build SELECT statement with cross-join
        
        query = f"""
        SELECT 
            h.*, 
            m.ST_CD, 
            m.ST_NM,
            d.STOCK_DATE, d.KPI, d.OP_STK_Q, d.CL_STK_Q, 
            d.OP_STK_V, d.CL_STK_V, d.SALE_Q, d.SALE_V, 
            d.GM_V, d.AVG_DNSTY, d.APF, d.MAJ_CAT
        FROM Master_HIER_{grouping_column} h
        CROSS JOIN Master_STORE_PLAN m
        LEFT JOIN COUNT_STOCK_DATA_18M d ON h.{grouping_column} = d.{grouping_column}
        WHERE d.KPI != 'L7D'
        """
        
        return QueryResponse(
            query=query,
            table_name="COUNT_STOCK_DATA_18M",
            columns=[]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
