"""
Preset Management Service
Handles CRUD operations, sequencing, and persistence of contribution presets
"""
import json
import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from sqlalchemy import text, inspect


class PresetManager:
    """Manages presets: creation, sequencing, persistence, and lifecycle"""
    
    # Default preset constants
    DEFAULT_PRESET_NAME = 'L7D'
    PROTECTED_PRESETS = {'L7D'}
    FORMULA_PRESET_TYPE = 'formula'
    CUSTOM_PRESET_TYPE = 'custom'
    
    def __init__(self, engine, table_name: str = 'Cont_presets'):
        """
        Initialize preset manager
        
        Args:
            engine: SQLAlchemy engine
            table_name: Database table for presets (default: Cont_presets)
        """
        self.engine = engine
        self.table_name = table_name
        self._ensure_table_exists()
    
    def _ensure_table_exists(self) -> Tuple[bool, Optional[str]]:
        """
        Ensure all contribution tables exist with required columns
        Creates: Cont_presets, Cont_mappings, Cont_mapping_assignments
        
        Returns:
            Tuple of (success: bool, error: Optional[str])
        """
        try:
            with self.engine.connect() as conn:
                inspector = inspect(self.engine)
                
                # Create Cont_presets if not exists
                if self.table_name not in inspector.get_table_names():
                    conn.execute(text(f"""
                    CREATE TABLE {self.table_name} (
                        preset_name NVARCHAR(255) PRIMARY KEY,
                        preset_type NVARCHAR(50),
                        description NVARCHAR(MAX),
                        config_json NVARCHAR(MAX),
                        sequence_order INT DEFAULT 9999,
                        created_date DATETIME DEFAULT GETDATE(),
                        modified_date DATETIME DEFAULT GETDATE()
                    )
                    """))
                    conn.commit()
                else:
                    # Check and add missing columns
                    columns = [col['name'] for col in inspector.get_columns(self.table_name)]
                    if 'sequence_order' not in columns:
                        conn.execute(text(f"""
                        ALTER TABLE {self.table_name} 
                        ADD sequence_order INT DEFAULT 9999
                        """))
                        conn.commit()
                
                # Create Cont_mappings if not exists
                if 'Cont_mappings' not in inspector.get_table_names():
                    conn.execute(text("""
                    CREATE TABLE Cont_mappings (
                        mapping_name NVARCHAR(255) PRIMARY KEY,
                        mapping_json NVARCHAR(MAX) NOT NULL,
                        fallback_json NVARCHAR(MAX),
                        description NVARCHAR(MAX),
                        created_date DATETIME DEFAULT GETDATE(),
                        modified_date DATETIME DEFAULT GETDATE()
                    )
                    """))
                    conn.commit()
                
                # Create Cont_mapping_assignments if not exists
                if 'Cont_mapping_assignments' not in inspector.get_table_names():
                    conn.execute(text("""
                    CREATE TABLE Cont_mapping_assignments (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        col_name NVARCHAR(255) NOT NULL,
                        mapping_name NVARCHAR(255) NOT NULL,
                        prefix NVARCHAR(255),
                        target NVARCHAR(20),
                        created_date DATETIME DEFAULT GETDATE(),
                        modified_date DATETIME DEFAULT GETDATE(),
                        FOREIGN KEY (mapping_name) REFERENCES Cont_mappings(mapping_name)
                    )
                    """))
                    conn.commit()
                
                return True, None
        except Exception as e:
            return False, str(e)
    
    # ==================== PRESET CRUD OPERATIONS ====================
    
    def create_preset(
        self,
        preset_name: str,
        config: Dict,
        preset_type: str = CUSTOM_PRESET_TYPE,
        sequence_order: Optional[int] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Create a new preset
        
        Args:
            preset_name: Unique preset identifier
            config: Configuration dictionary (must include description, months, avg_days, etc.)
            preset_type: Type of preset (custom, formula, default)
            sequence_order: Position in execution sequence (auto-calculated if None)
            
        Returns:
            Tuple of (success: bool, error: Optional[str])
        """
        try:
            if preset_name in self.PROTECTED_PRESETS:
                return False, f"Preset '{preset_name}' is protected and cannot be created"
            
            with self.engine.connect() as conn:
                # Check if preset exists
                exists = conn.execute(
                    text(f"SELECT COUNT(*) FROM {self.table_name} WHERE preset_name = :name"),
                    {"name": preset_name}
                ).scalar() > 0
                
                if exists:
                    return False, f"Preset '{preset_name}' already exists. Use update_preset() instead."
                
                # Calculate sequence_order if not provided
                if sequence_order is None:
                    max_seq_result = conn.execute(
                        text(f"SELECT COALESCE(MAX(sequence_order), 0) FROM {self.table_name}")
                    )
                    sequence_order = (max_seq_result.scalar() or 0) + 1
                
                # Insert new preset
                conn.execute(
                    text(f"""
                    INSERT INTO {self.table_name} 
                    (preset_name, preset_type, description, config_json, sequence_order)
                    VALUES (:name, :ptype, :desc, :config, :seq)
                    """),
                    {
                        'name': preset_name,
                        'ptype': preset_type,
                        'desc': config.get('description', ''),
                        'config': json.dumps(config),
                        'seq': sequence_order
                    }
                )
                conn.commit()
            
            return True, None
        except Exception as e:
            return False, str(e)
    
    def get_preset(self, preset_name: str) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Retrieve a single preset configuration
        
        Args:
            preset_name: Name of preset to retrieve
            
        Returns:
            Tuple of (config: Optional[Dict], error: Optional[str])
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT config_json FROM {self.table_name} WHERE preset_name = :name"),
                    {"name": preset_name}
                )
                row = result.fetchone()
                
                if row:
                    return json.loads(row[0]), None
                else:
                    return None, f"Preset '{preset_name}' not found"
        except Exception as e:
            return None, str(e)
    
    def update_preset(
        self,
        preset_name: str,
        config: Dict
    ) -> Tuple[bool, Optional[str]]:
        """
        Update an existing preset configuration
        
        Args:
            preset_name: Name of preset to update
            config: New configuration dictionary
            
        Returns:
            Tuple of (success: bool, error: Optional[str])
        """
        try:
            if preset_name in self.PROTECTED_PRESETS:
                return False, f"Preset '{preset_name}' is protected and cannot be updated"
            
            with self.engine.connect() as conn:
                conn.execute(
                    text(f"""
                    UPDATE {self.table_name}
                    SET config_json = :config,
                        description = :desc,
                        modified_date = GETDATE()
                    WHERE preset_name = :name
                    """),
                    {
                        "config": json.dumps(config),
                        "desc": config.get('description', ''),
                        "name": preset_name
                    }
                )
                conn.commit()
            
            return True, None
        except Exception as e:
            return False, str(e)
    
    def delete_preset(self, preset_name: str) -> Tuple[bool, Optional[str]]:
        """
        Delete a preset (protected presets cannot be deleted)
        
        Args:
            preset_name: Name of preset to delete
            
        Returns:
            Tuple of (success: bool, error: Optional[str])
        """
        try:
            if preset_name in self.PROTECTED_PRESETS:
                return False, f"Preset '{preset_name}' is protected and cannot be deleted"
            
            with self.engine.connect() as conn:
                conn.execute(
                    text(f"DELETE FROM {self.table_name} WHERE preset_name = :name"),
                    {"name": preset_name}
                )
                conn.commit()
            
            return True, None
        except Exception as e:
            return False, str(e)
    
    def list_presets(self, preset_type: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        List all presets, optionally filtered by type
        
        Args:
            preset_type: Filter by preset type (custom, formula, default). None means all.
            
        Returns:
            Tuple of (presets: List[Dict], error: Optional[str])
        """
        try:
            query = f"SELECT * FROM {self.table_name}"
            params = {}
            
            if preset_type:
                query += " WHERE preset_type = :ptype"
                params['ptype'] = preset_type
            
            query += " ORDER BY sequence_order, preset_name"
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            
            presets = []
            for _, row in df.iterrows():
                presets.append({
                    'preset_name': row['preset_name'],
                    'preset_type': row['preset_type'],
                    'description': row['description'],
                    'config': json.loads(row['config_json']),
                    'sequence_order': row['sequence_order'],
                    'created_date': row['created_date'],
                    'modified_date': row['modified_date']
                })
            
            return presets, None
        except Exception as e:
            return [], str(e)
    
    # ==================== SEQUENCING OPERATIONS ====================
    
    def update_sequence(self, sequence_list: List[str]) -> Tuple[bool, Optional[str]]:
        """
        Update the execution sequence of presets
        
        Args:
            sequence_list: List of preset names in desired order
            
        Returns:
            Tuple of (success: bool, error: Optional[str])
        """
        try:
            with self.engine.connect() as conn:
                # Reset all to high number
                conn.execute(
                    text(f"UPDATE {self.table_name} SET sequence_order = 9999")
                )
                
                # Update based on sequence list
                for idx, preset_name in enumerate(sequence_list, 1):
                    conn.execute(
                        text(f"""
                        UPDATE {self.table_name} 
                        SET sequence_order = :order 
                        WHERE preset_name = :name
                        """),
                        {"order": idx, "name": preset_name}
                    )
                
                conn.commit()
            
            return True, None
        except Exception as e:
            return False, str(e)
    
    def get_sequence(self, include_formula: bool = False) -> Tuple[List[str], Optional[str]]:
        """
        Get current execution sequence of presets
        
        Args:
            include_formula: Whether to include formula presets in sequence
            
        Returns:
            Tuple of (sequence: List[str], error: Optional[str])
        """
        try:
            query = f"SELECT preset_name FROM {self.table_name}"
            
            if not include_formula:
                query += f" WHERE preset_type != '{self.FORMULA_PRESET_TYPE}'"
            
            query += " ORDER BY sequence_order, preset_name"
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            sequence = df['preset_name'].tolist()
            return sequence, None
        except Exception as e:
            return [], str(e)
    
    def move_preset(
        self,
        preset_name: str,
        direction: str = 'up',
        steps: int = 1
    ) -> Tuple[bool, Optional[str]]:
        """
        Move a preset up or down in sequence
        
        Args:
            preset_name: Name of preset to move
            direction: 'up' or 'down'
            steps: Number of positions to move
            
        Returns:
            Tuple of (success: bool, error: Optional[str])
        """
        try:
            # Get current sequence
            sequence, error = self.get_sequence(include_formula=False)
            if error:
                return False, error
            
            if preset_name not in sequence:
                return False, f"Preset '{preset_name}' not in sequence"
            
            current_idx = sequence.index(preset_name)
            
            # Calculate new index
            if direction.lower() == 'up':
                new_idx = max(0, current_idx - steps)
            elif direction.lower() == 'down':
                new_idx = min(len(sequence) - 1, current_idx + steps)
            else:
                return False, "Direction must be 'up' or 'down'"
            
            # Swap in sequence
            if new_idx != current_idx:
                sequence[current_idx], sequence[new_idx] = sequence[new_idx], sequence[current_idx]
                return self.update_sequence(sequence)
            
            return True, None
        except Exception as e:
            return False, str(e)
    
    # ==================== DEFAULT PRESET OPERATIONS ====================
    
    def ensure_default_preset(self) -> Tuple[Dict, Optional[str]]:
        """
        Ensure the default L7D preset exists in database
        
        Returns:
            Tuple of (all_presets: Dict[str, Dict], error: Optional[str])
        """
        try:
            # Check if L7D exists
            config, error = self.get_preset(self.DEFAULT_PRESET_NAME)
            
            if config is None:
                # Create default preset
                default_config = {
                    'type': 'default',
                    'description': 'System default L7D preset (7-day average)',
                    'months': [],
                    'avg_days': 7,
                    'kpi': 'L7D',
                    'protected': True
                }
                
                success, error = self.create_preset(
                    self.DEFAULT_PRESET_NAME,
                    default_config,
                    preset_type='default',
                    sequence_order=0
                )
                
                if not success:
                    return {}, f"Failed to create default preset: {error}"
            
            # Load and return all presets
            presets_list, error = self.list_presets()
            if error:
                return {}, error
            
            presets_dict = {p['preset_name']: p['config'] for p in presets_list}
            return presets_dict, None
        except Exception as e:
            return {}, str(e)
    
    # ==================== BULK OPERATIONS ====================
    
    def get_presets_dict(self, preset_type: Optional[str] = None) -> Dict[str, Dict]:
        """
        Get all presets as a dictionary (convenient format)
        
        Args:
            preset_type: Filter by preset type (optional)
            
        Returns:
            Dictionary of preset_name -> config
        """
        presets_list, error = self.list_presets(preset_type)
        if error:
            return {}
        
        return {p['preset_name']: p['config'] for p in presets_list}
    
    def delete_all_except(self, preserve_names: List[str]) -> Tuple[int, Optional[str]]:
        """
        Delete all presets except those in preserve_names
        
        Args:
            preserve_names: Names of presets to preserve
            
        Returns:
            Tuple of (deleted_count: int, error: Optional[str])
        """
        try:
            presets_list, error = self.list_presets()
            if error:
                return 0, error
            
            deleted_count = 0
            for preset in presets_list:
                if preset['preset_name'] not in preserve_names:
                    success, _ = self.delete_preset(preset['preset_name'])
                    if success:
                        deleted_count += 1
            
            return deleted_count, None
        except Exception as e:
            return 0, str(e)
    
    def validate_preset_config(self, config: Dict) -> Tuple[bool, List[str]]:
        """
        Validate preset configuration for required fields
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            Tuple of (is_valid: bool, errors: List[str])
        """
        errors = []
        
        # Check required fields
        required_fields = {'type', 'description', 'avg_days'}
        for field in required_fields:
            if field not in config:
                errors.append(f"Missing required field: {field}")
        
        # Validate avg_days
        if 'avg_days' in config:
            try:
                avg_days = config['avg_days']
                if not isinstance(avg_days, int) or avg_days < 1:
                    errors.append("avg_days must be a positive integer")
            except Exception:
                errors.append("avg_days must be numeric")
        
        # Validate months if present
        if 'months' in config:
            if not isinstance(config['months'], list):
                errors.append("months must be a list")
        
        return len(errors) == 0, errors
    
    # ==================== EXPORT/IMPORT OPERATIONS ====================
    
    def export_presets(self) -> Tuple[Dict, Optional[str]]:
        """
        Export all presets to dictionary format (useful for backup/transfer)
        
        Returns:
            Tuple of (presets: Dict, error: Optional[str])
        """
        try:
            presets_list, error = self.list_presets()
            if error:
                return {}, error
            
            export_data = {
                'export_date': datetime.now().isoformat(),
                'presets': {}
            }
            
            for preset in presets_list:
                export_data['presets'][preset['preset_name']] = {
                    'config': preset['config'],
                    'preset_type': preset['preset_type'],
                    'sequence_order': preset['sequence_order']
                }
            
            return export_data, None
        except Exception as e:
            return {}, str(e)
    
    def import_presets(self, import_data: Dict, overwrite: bool = False) -> Tuple[int, Optional[str]]:
        """
        Import presets from exported data
        
        Args:
            import_data: Dictionary with presets to import
            overwrite: Whether to overwrite existing presets
            
        Returns:
            Tuple of (imported_count: int, error: Optional[str])
        """
        try:
            imported_count = 0
            
            for preset_name, preset_info in import_data.get('presets', {}).items():
                # Skip protected presets unless overwriting
                if preset_name in self.PROTECTED_PRESETS and not overwrite:
                    continue
                
                config = preset_info.get('config', {})
                preset_type = preset_info.get('preset_type', 'custom')
                
                # Validate config
                is_valid, errors = self.validate_preset_config(config)
                if not is_valid:
                    continue
                
                # Try to create or update
                if overwrite:
                    # Delete if exists
                    self.delete_preset(preset_name)
                
                success, _ = self.create_preset(preset_name, config, preset_type)
                if success:
                    imported_count += 1
            
            return imported_count, None
        except Exception as e:
            return 0, str(e)
    
    def get_statistics(self) -> Dict:
        """
        Get preset management statistics
        
        Returns:
            Dictionary with statistics
        """
        try:
            presets_list, _ = self.list_presets()
            
            # Count by type
            type_count = {}
            for preset in presets_list:
                ptype = preset['preset_type']
                type_count[ptype] = type_count.get(ptype, 0) + 1
            
            stats = {
                'total_presets': len(presets_list),
                'by_type': type_count,
                'protected_presets': list(self.PROTECTED_PRESETS),
                'oldest_preset': min([p['created_date'] for p in presets_list]) if presets_list else None,
                'newest_preset': max([p['modified_date'] for p in presets_list]) if presets_list else None,
            }
            
            return stats
        except Exception as e:
            return {'error': str(e)}
