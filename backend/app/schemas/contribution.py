"""
Contribution Percentage Schemas
Pydantic models for API requests and responses
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any
from datetime import datetime


# ============ PRESET SCHEMAS ============
class PresetCreate(BaseModel):
    preset_name: str = Field(..., description="Unique preset name")
    preset_type: str = Field(..., description="Type: 'formula' or 'standard'")
    description: Optional[str] = None
    config_json: str = Field(..., description="JSON configuration")
    sequence_order: Optional[int] = 9999


class PresetUpdate(BaseModel):
    preset_type: Optional[str] = None
    description: Optional[str] = None
    config_json: Optional[str] = None
    sequence_order: Optional[int] = None


class PresetResponse(BaseModel):
    preset_name: str
    preset_type: str
    description: Optional[str]
    config_json: str
    sequence_order: int
    created_date: datetime
    modified_date: datetime

    class Config:
        from_attributes = True


# ============ MAPPING SCHEMAS ============
class MappingCreate(BaseModel):
    mapping_name: str = Field(..., description="Unique mapping name")
    mapping_json: str = Field(..., description="JSON with suffix mappings")
    fallback_json: Optional[str] = None
    description: Optional[str] = None


class MappingUpdate(BaseModel):
    mapping_json: Optional[str] = None
    fallback_json: Optional[str] = None
    description: Optional[str] = None


class MappingResponse(BaseModel):
    mapping_name: str
    mapping_json: str
    fallback_json: Optional[str]
    description: Optional[str]
    created_date: datetime
    modified_date: datetime

    class Config:
        from_attributes = True


# ============ ASSIGNMENT SCHEMAS ============
class AssignmentCreate(BaseModel):
    col_name: str = Field(..., description="Column name")
    mapping_name: str = Field(..., description="Reference to mapping")
    prefix: Optional[str] = None
    target: str = Field(default="Both", description="'Both', 'Store', or 'Company'")


class AssignmentUpdate(BaseModel):
    mapping_name: Optional[str] = None
    prefix: Optional[str] = None
    target: Optional[str] = None


class AssignmentResponse(BaseModel):
    id: int
    col_name: str
    mapping_name: str
    prefix: Optional[str]
    target: str
    created_date: datetime
    modified_date: datetime

    class Config:
        from_attributes = True


# ============ EXECUTION SCHEMAS ============
class ContributionCalculationRequest(BaseModel):
    """Request for contribution percentage calculation"""
    presets: List[str] = Field(..., description="List of preset names")
    major_categories: Optional[List[str]] = None
    date_range: Optional[Dict[str, str]] = None  # {'start': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'}
    group_by: str = Field(default="ST_CD", description="Column to group by")
    sequence_execution: bool = Field(default=False, description="Execute in sequence order")
    save_to_db: bool = Field(default=True, description="Save results to database")


class KPICalculationResponse(BaseModel):
    """Response with calculated KPIs"""
    status: str
    record_count: int
    columns: List[str]
    sample_data: Optional[List[Dict[str, Any]]] = None
    summary_stats: Optional[Dict[str, Any]] = None


class ContributionCalculationResponse(BaseModel):
    """Response from full execution"""
    status: str = "success"
    message: str
    presets_executed: List[str]
    major_categories: List[str]
    store_results: Optional[Dict[str, Any]] = None
    company_results: Optional[Dict[str, Any]] = None
    saved_tables: Optional[List[str]] = None
    execution_time: Optional[float] = None


# ============ QUERY SCHEMAS ============
class DynamicQueryRequest(BaseModel):
    """Request for dynamic query building"""
    grouping_column: str = Field(..., description="Hierarchy column for cross-join")
    filters: Optional[Dict[str, Any]] = None
    date_range: Optional[Dict[str, str]] = None
    major_categories: Optional[List[str]] = None


class QueryResponse(BaseModel):
    """Response with generated SQL query"""
    query: str
    table_name: str
    columns: List[str]


# ============ EXPORT SCHEMAS ============
class ExportRequest(BaseModel):
    """Request to export results"""
    format: str = Field(default="csv", description="'csv' or 'zip'")
    include_store_level: bool = True
    include_company_level: bool = True
    split_by_size: Optional[int] = None  # in KB, ~800 for 800KB chunks


class ExportResponse(BaseModel):
    """Response with export information"""
    status: str
    format: str
    files: List[str]
    total_size: int
    download_url: Optional[str] = None
