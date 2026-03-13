from sqlalchemy import Column, Integer, String, DateTime, Text, NVARCHAR, ForeignKey
from datetime import datetime

from app.database.session import DataBase


class ContPreset(DataBase):
    """Preset configuration table for contribution percentage analysis"""
    __tablename__ = 'Cont_presets'
    __table_args__ = {'extend_existing': True}
    
    preset_name = Column(NVARCHAR(255), primary_key=True)
    preset_type = Column(NVARCHAR(50))  # 'formula' or 'standard'
    description = Column(Text)
    config_json = Column(Text)  # JSON format configuration
    sequence_order = Column(Integer, default=9999)
    created_date = Column(DateTime, default=datetime.utcnow)
    modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ContMapping(DataBase):
    """Suffix mapping table for data transformations"""
    __tablename__ = 'Cont_mappings'
    __table_args__ = {'extend_existing': True}
    
    mapping_name = Column(NVARCHAR(255), primary_key=True)
    mapping_json = Column(Text)  # JSON object with suffix mappings
    fallback_json = Column(Text)  # JSON object with fallback values
    description = Column(Text)
    created_date = Column(DateTime, default=datetime.utcnow)
    modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ContMappingAssignment(DataBase):
    """Links columns to mapping rules"""
    __tablename__ = 'Cont_mapping_assignments'
    __table_args__ = {'extend_existing': True}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    col_name = Column(NVARCHAR(255))
    mapping_name = Column(NVARCHAR(255), ForeignKey('Cont_mappings.mapping_name'))
    prefix = Column(NVARCHAR(255))
    target = Column(NVARCHAR(20))  # 'Both', 'Store', 'Company'
    created_date = Column(DateTime, default=datetime.utcnow)
    modified_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
