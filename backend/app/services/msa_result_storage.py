"""
MSA Result Storage Service
Handles storing MSA calculation results into database tables with sequence tracking
and automatic column management for new calculated fields
"""
import pandas as pd
import json
from sqlalchemy import text, inspect
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger


class MSAResultStorageService:
    """Service for storing MSA calculation results with sequence tracking"""

    def __init__(self, db):
        """
        Initialize MSAResultStorageService
        
        Args:
            db: SQLAlchemy session (Main DB session, not Data DB)
        """
        self.db = db
        self.result_tables = {
            'msa': 'dbo.cl_msa',
            'msa_gen_clr': 'dbo.cl_generated_color',
            'msa_gen_clr_var': 'dbo.cl_color_variant'
        }
        self.tracking_table = 'dbo.MSA_Calculation_Sequence'
        self.column_definitions_table = 'dbo.MSA_Column_Definitions'

    # ========================================================================
    # Sequence Management
    # ========================================================================

    def get_last_sequence_id(self) -> int:
        """
        Get the last sequence ID from the tracking table
        
        Returns:
            Last sequence_id (0 if no sequences yet)
        """
        try:
            sql = f"SELECT MAX(sequence_id) as max_seq FROM {self.tracking_table}"
            result = pd.read_sql(text(sql), self.db.bind)
            last_seq = int(result['max_seq'].iloc[0]) if result['max_seq'].iloc[0] is not None else 0
            logger.info(f"Last sequence ID: {last_seq}")
            return last_seq
        except Exception as e:
            logger.warning(f"Error getting last sequence ID: {e}, returning 0")
            return 0

    def create_sequence_record(
        self,
        date_filter: str,
        filter_columns: List[str],
        filters: Dict[str, List[str]],
        threshold: int,
        slocs: List[str],
        msa_row_count: int,
        gen_color_row_count: int,
        color_variant_row_count: int,
        created_by: str = "system"
    ) -> int:
        """
        Create a sequence record for this calculation
        
        Args:
            date_filter: Date filter applied
            filter_columns: List of filter columns
            filters: Dict of filter values
            threshold: Threshold percentage
            slocs: List of SLOC codes
            msa_row_count: Row count for MSA results
            gen_color_row_count: Row count for generated colors
            color_variant_row_count: Row count for color variants
            created_by: User who triggered calculation
        
        Returns:
            New sequence_id
        """
        try:
            # Get the raw pyodbc connection from SQLAlchemy
            connection = self.db.connection().connection
            cursor = connection.cursor()
            
            try:
                # Simple approach: INSERT, then query max sequence_id
                insert_sql = f"""
                INSERT INTO {self.tracking_table}
                (date_filter, filter_columns, filters, threshold, slocs, 
                 msa_row_count, gen_color_row_count, color_variant_row_count,
                 created_by, status)
                VALUES
                (?, ?, ?, ?, ?,
                 ?, ?, ?,
                 ?, 'COMPLETED')
                """
                
                # Execute the INSERT
                cursor.execute(
                    insert_sql,
                    (
                        date_filter,
                        json.dumps(filter_columns),
                        json.dumps(filters),
                        int(threshold),
                        json.dumps(slocs),
                        int(msa_row_count),
                        int(gen_color_row_count),
                        int(color_variant_row_count),
                        created_by
                    )
                )
                
                # Commit the insert
                connection.commit()
                
                # Now get the max sequence_id (which should be the one we just inserted)
                cursor.execute(f"SELECT MAX(sequence_id) FROM {self.tracking_table}")
                result = cursor.fetchone()
                sequence_id = int(result[0]) if result and result[0] is not None else None
                
                if sequence_id:
                    logger.info(f"✅ Created sequence record: {sequence_id}")
                    return sequence_id
                else:
                    raise ValueError("Could not retrieve sequence_id after insert")
                    
            except Exception as e:
                try:
                    connection.rollback()
                except:
                    pass
                raise
            finally:
                cursor.close()
                # Don't close connection - SQLAlchemy manages it
                
        except Exception as e:
            logger.error(f"❌ Error creating sequence record: {e}")
            raise

    # ========================================================================
    # Column Management
    # ========================================================================

    def get_existing_columns(self, table_name: str) -> List[str]:
        """
        Get all existing column names for a table
        
        Args:
            table_name: Table name (msa, msa_gen_clr, msa_gen_clr_var)
        
        Returns:
            List of column names
        """
        try:
            db_table = self.result_tables.get(table_name)
            if not db_table:
                logger.warning(f"Unknown table name: {table_name}")
                return []
            
            # Get columns from information schema
            sql = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{db_table.split('.')[-1]}'
            AND TABLE_SCHEMA = 'dbo'
            ORDER BY ORDINAL_POSITION
            """
            
            df = pd.read_sql(text(sql), self.db.bind)
            columns = df['COLUMN_NAME'].tolist()
            logger.debug(f"Existing columns in {db_table}: {columns}")
            return columns
        except Exception as e:
            logger.warning(f"Error getting existing columns for {table_name}: {e}")
            return ['id', 'sequence_id', 'calculation_date', 'created_by', 'created_at', 'updated_at']

    def get_new_columns(self, table_name: str, data: List[Dict]) -> List[str]:
        """
        Identify new columns in the data that don't exist in the table
        
        Args:
            table_name: Table name (msa, msa_gen_clr, msa_gen_clr_var)
            data: List of dictionaries with result data
        
        Returns:
            List of new column names
        """
        if not data:
            return []
        
        existing = set(self.get_existing_columns(table_name))
        data_columns = set(data[0].keys()) if data else set()
        reserved_columns = {'id', 'sequence_id', 'calculation_date', 'created_by', 'created_at', 'updated_at'}
        
        new_columns = list(data_columns - existing - reserved_columns)
        
        if new_columns:
            logger.info(f"New columns detected in {table_name}: {new_columns}")
        
        return new_columns

    def create_columns(self, table_name: str, new_columns: List[str], sequence_id: int) -> None:
        """
        Create new columns in the result table and record them in definitions
        
        Args:
            table_name: Table name (msa, msa_gen_clr, msa_gen_clr_var)
            new_columns: List of new column names to create
            sequence_id: Sequence ID for this calculation
        """
        if not new_columns:
            return
        
        try:
            db_table = self.result_tables.get(table_name)
            if not db_table:
                logger.error(f"Unknown table name: {table_name}")
                return
            
            # Use raw connection to avoid transaction conflicts
            connection = self.db.connection().connection
            cursor = connection.cursor()
            
            try:
                # Create new columns
                for col_name in new_columns:
                    try:
                        # Check if column already exists
                        check_sql = f"""
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = '{db_table.split('.')[-1]}' 
                        AND COLUMN_NAME = '{col_name}'
                        """
                        cursor.execute(check_sql)
                        exists = cursor.fetchone() is not None
                        
                        if not exists:
                            alter_sql = f"""
                            ALTER TABLE {db_table}
                            ADD [{col_name}] VARCHAR(MAX) NULL
                            """
                            cursor.execute(alter_sql)
                            logger.info(f"✅ Created column {db_table}.{col_name}")
                        
                        # Record in column definitions
                        insert_col_def_sql = f"""
                        INSERT INTO {self.column_definitions_table}
                        (table_name, column_name, column_type, first_sequence_id)
                        VALUES ('{table_name}', '{col_name}', 'VARCHAR(MAX)', {sequence_id})
                        """
                        try:
                            cursor.execute(insert_col_def_sql)
                        except:
                            # Might already exist, ignore
                            pass
                            
                    except Exception as col_err:
                        logger.warning(f"Could not create column {col_name}: {col_err}")
                
                # Commit all column changes
                connection.commit()
                logger.info(f"✅ All columns created for {table_name}")
                
            except Exception as e:
                try:
                    connection.rollback()
                except:
                    pass
                raise
            finally:
                cursor.close()
                # Don't close connection - SQLAlchemy manages it
                
        except Exception as e:
            logger.error(f"❌ Error in create_columns: {e}")
            raise
                
        except Exception as e:
            logger.error(f"❌ Error in create_columns: {e}")
            raise

    # ========================================================================
    # Data Storage
    # ========================================================================

    def store_results(
        self,
        calculation_results: Dict[str, Any],
        date_filter: str,
        filter_columns: List[str],
        filters: Dict[str, List[str]],
        threshold: int,
        slocs: List[str],
        created_by: str = "system"
    ) -> Dict[str, Any]:
        """
        Store MSA calculation results to database with sequence tracking
        
        Args:
            calculation_results: Dict with keys: msa, msa_gen_clr, msa_gen_clr_var, row_counts
            date_filter: Date filter applied
            filter_columns: List of filter columns
            filters: Dict of filter values
            threshold: Threshold percentage
            slocs: List of SLOC codes
            created_by: User who triggered calculation
        
        Returns:
            Dict with sequence_id and storage info
        """
        try:
            logger.info(f"📦 Starting MSA result storage...")
            
            # Extract results
            msa_data = calculation_results.get('msa', [])
            msa_gen_clr_data = calculation_results.get('msa_gen_clr', [])
            msa_gen_clr_var_data = calculation_results.get('msa_gen_clr_var', [])
            row_counts = calculation_results.get('row_counts', {})
            
            logger.info(f"   MSA: {len(msa_data)} rows")
            logger.info(f"   Generated Colors: {len(msa_gen_clr_data)} rows")
            logger.info(f"   Color Variants: {len(msa_gen_clr_var_data)} rows")
            
            # Create sequence record first
            sequence_id = self.create_sequence_record(
                date_filter=date_filter,
                filter_columns=filter_columns,
                filters=filters,
                threshold=threshold,
                slocs=slocs,
                msa_row_count=len(msa_data),
                gen_color_row_count=len(msa_gen_clr_data),
                color_variant_row_count=len(msa_gen_clr_var_data),
                created_by=created_by
            )
            
            storage_info = {
                'sequence_id': sequence_id,
                'msa_stored': False,
                'gen_color_stored': False,
                'color_variant_stored': False,
                'errors': []
            }
            
            # Store each result set
            if msa_data:
                try:
                    self._store_table_data('msa', msa_data, sequence_id)
                    storage_info['msa_stored'] = True
                except Exception as e:
                    storage_info['errors'].append(f"MSA storage error: {str(e)}")
                    logger.error(f"❌ Error storing MSA data: {e}")
            
            if msa_gen_clr_data:
                try:
                    self._store_table_data('msa_gen_clr', msa_gen_clr_data, sequence_id)
                    storage_info['gen_color_stored'] = True
                except Exception as e:
                    storage_info['errors'].append(f"Generated color storage error: {str(e)}")
                    logger.error(f"❌ Error storing generated color data: {e}")
            
            if msa_gen_clr_var_data:
                try:
                    self._store_table_data('msa_gen_clr_var', msa_gen_clr_var_data, sequence_id)
                    storage_info['color_variant_stored'] = True
                except Exception as e:
                    storage_info['errors'].append(f"Color variant storage error: {str(e)}")
                    logger.error(f"❌ Error storing color variant data: {e}")
            
            logger.info(f"✅ Result storage complete: sequence {sequence_id}")
            return storage_info
        except Exception as e:
            logger.error(f"❌ Error in store_results: {e}")
            raise

    def _store_table_data(self, table_name: str, data: List[Dict], sequence_id: int) -> int:
        try:
            if not data:
                logger.info(f"No data to store for {table_name}")
                return 0

            db_table = self.result_tables.get(table_name)
            if not db_table:
                raise ValueError(f"Unknown table name: {table_name}")

            # Detect new columns
            new_columns = self.get_new_columns(table_name, data)
            if new_columns:
                logger.info(f"Creating {len(new_columns)} new columns in {table_name}")
                self.create_columns(table_name, new_columns, sequence_id)

            existing_columns = self.get_existing_columns(table_name)

            # Prepare rows
            rows_to_insert = []
            for row in data:
                insert_row = {'sequence_id': sequence_id}

                for col in existing_columns:
                    if col in ['id', 'sequence_id', 'calculation_date', 'created_by', 'created_at', 'updated_at']:
                        continue

                    insert_row[col] = row.get(col)

                rows_to_insert.append(insert_row)

            # Raw connection
            connection = self.db.connection().connection
            cursor = connection.cursor()

            try:
                # IMPORTANT: enable fast executemany
                cursor.fast_executemany = True

                column_list = [
                    col for col in existing_columns
                    if col not in ['id', 'calculation_date', 'created_at', 'updated_at']
                ]

                if 'sequence_id' not in column_list:
                    column_list.insert(0, 'sequence_id')

                column_names = ', '.join([f'[{c}]' for c in column_list])
                placeholders = ', '.join(['?' for _ in column_list])

                insert_sql = f"""
                INSERT INTO {db_table}
                ({column_names})
                VALUES ({placeholders})
                """

                batch_size = 20000
                total_inserted = 0

                for i in range(0, len(rows_to_insert), batch_size):
                    batch = rows_to_insert[i:i + batch_size]

                    values_list = [
                        tuple(row_data.get(col) for col in column_list)
                        for row_data in batch
                    ]

                    cursor.executemany(insert_sql, values_list)
                    total_inserted += len(batch)

                    logger.info(
                        f"📊 Inserted {total_inserted}/{len(rows_to_insert)} rows into {table_name}"
                    )

                # SINGLE COMMIT
                connection.commit()

                logger.info(f"✅ Stored {total_inserted} rows in {table_name}")
                return total_inserted

            except Exception as e:
                connection.rollback()
                raise

            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"❌ Error storing table data for {table_name}: {e}")
            raise
    # ========================================================================
    # Retrieval Methods
    # ========================================================================

    def get_sequence_data(
        self,
        sequence_id: int,
        table_name: str
    ) -> Tuple[List[Dict], Dict[str, Any]]:
        """
        Retrieve stored data for a specific sequence and table
        
        Args:
            sequence_id: Sequence ID
            table_name: Table name (msa, msa_gen_clr, msa_gen_clr_var)
        
        Returns:
            Tuple of (data list, metadata dict)
        """
        try:
            # Get sequence metadata
            meta_sql = f"""
            SELECT sequence_id, calculation_date, date_filter, filter_columns,
                   filters, threshold, slocs, msa_row_count, gen_color_row_count,
                   color_variant_row_count, created_by, created_at, status
            FROM {self.tracking_table}
            WHERE sequence_id = :seq_id
            """
            
            meta_df = pd.read_sql(text(meta_sql), self.db.bind, params={'seq_id': sequence_id})
            
            if meta_df.empty:
                logger.warning(f"No sequence found for ID: {sequence_id}")
                return [], {}
            
            meta = meta_df.iloc[0].to_dict()
            
            # Parse JSON fields
            meta['filter_columns'] = json.loads(meta['filter_columns']) if meta['filter_columns'] else []
            meta['filters'] = json.loads(meta['filters']) if meta['filters'] else {}
            meta['slocs'] = json.loads(meta['slocs']) if meta['slocs'] else []
            
            # Get result data
            db_table = self.result_tables.get(table_name)
            if not db_table:
                raise ValueError(f"Unknown table name: {table_name}")
            
            data_sql = f"""
            SELECT *
            FROM {db_table}
            WHERE sequence_id = :seq_id
            ORDER BY id
            """
            
            data_df = pd.read_sql(text(data_sql), self.db.bind, params={'seq_id': sequence_id})
            data = data_df.where(pd.notna(data_df), None).to_dict('records')
            
            logger.info(f"✅ Retrieved {len(data)} rows from {table_name} (sequence {sequence_id})")
            
            return data, meta
        except Exception as e:
            logger.error(f"❌ Error retrieving sequence data: {e}")
            return [], {}

    def get_latest_sequences(self, limit: int = 10) -> List[Dict]:
        """
        Get the latest calculation sequences
        
        Args:
            limit: Number of sequences to return
        
        Returns:
            List of sequence metadata dicts
        """
        try:
            # Use raw connection to ensure consistent access
            connection = self.db.connection().connection
            cursor = connection.cursor()
            
            try:
                sql = f"""
                SELECT TOP {limit}
                       sequence_id, calculation_date, date_filter, msa_row_count,
                       gen_color_row_count, color_variant_row_count, created_by,
                       created_at, status
                FROM {self.tracking_table}
                ORDER BY sequence_id DESC
                """
                
                cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                # Convert to list of dicts
                sequences = []
                for row in rows:
                    sequences.append(dict(zip(columns, row)))
                
                logger.info(f"✅ Retrieved {len(sequences)} latest sequences")
                return sequences
                
            finally:
                cursor.close()
                # Don't close connection - SQLAlchemy manages it
                
        except Exception as e:
            logger.error(f"Error retrieving latest sequences: {e}")
            return []
