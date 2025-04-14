"""
Load module for inserting data into PostgreSQL.
"""
import logging
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from typing import Dict, List, Tuple, Optional, Any
import numpy as np

from src.config import get_target_connection_params
from src.transform import convert_sql_server_to_postgres_type

logger = logging.getLogger(__name__)

def _is_numpy_type(value):
    """
    Safely determine if a value is a NumPy type without relying on deprecated type references.
    
    Args:
        value: Any value to check
    
    Returns:
        bool: True if value is a NumPy type, False otherwise
    """
    # Handle None values
    if value is None:
        return False
        
    # Check if it's from the numpy module
    module_name = type(value).__module__
    if module_name.startswith('numpy'):
        return True
    
    # Check if it has numpy-specific attributes
    if hasattr(value, 'dtype'):
        return True
        
    return False

def _convert_numpy_value_to_python(value):
    """
    Convert a NumPy value to its Python equivalent.
    
    Args:
        value: NumPy value to convert
    
    Returns:
        The equivalent Python value
    """
    if value is None:
        return None
        
    # If it has an item method, use it (works for all numpy scalar types)
    if hasattr(value, 'item') and callable(getattr(value, 'item')):
        try:
            return value.item()
        except:
            # In case item() fails
            pass
    
    # Handle numpy arrays
    if hasattr(value, 'tolist') and callable(getattr(value, 'tolist')):
        return value.tolist()
    
    # Handle specific types based on the string representation of the type
    type_str = str(type(value)).lower()
    
    if 'int' in type_str:
        return int(value)
    elif 'float' in type_str:
        return float(value)
    elif 'bool' in type_str:
        return bool(value)
    elif 'datetime' in type_str or 'timestamp' in type_str:
        # Convert to pandas Timestamp then to Python datetime
        return pd.Timestamp(value).to_pydatetime()
    
    # Try converting using dtype.name if available
    if hasattr(value, 'dtype') and hasattr(value.dtype, 'name'):
        dtype_name = value.dtype.name.lower()
        
        if 'int' in dtype_name:
            return int(value)
        elif 'float' in dtype_name:
            return float(value)
        elif 'bool' in dtype_name:
            return bool(value)
        elif 'datetime' in dtype_name:
            return pd.Timestamp(value).to_pydatetime()
            
    # Last resort fallback
    try:
        # Try direct conversion
        if hasattr(value, '__int__'):
            return int(value)
        elif hasattr(value, '__float__'):
            return float(value)
        elif hasattr(value, '__bool__'):
            return bool(value)
    except:
        pass
            
    # Final string fallback
    return str(value)

def get_target_connection():
    """
    Create a connection to the target PostgreSQL database.
    
    Returns:
        psycopg2.connection: Connection to the target database
    
    Raises:
        Exception: If connection cannot be established
    """
    try:
        connection_params = get_target_connection_params()
        logger.info("Connecting to target database")
        connection = psycopg2.connect(**connection_params)
        connection.autocommit = False
        logger.info("Successfully connected to target database")
        return connection
    except psycopg2.OperationalError as e:
        error_msg = f"Failed to connect to target database: {str(e)}"
        logger.error(error_msg)
        
        # More descriptive error messages based on common PostgreSQL errors
        if "timeout" in str(e).lower():
            logger.error("Connection timeout - check if the PostgreSQL server is running and accessible")
        elif "authentication" in str(e).lower() or "password" in str(e).lower():
            logger.error("Authentication failed - check your username and password in the .env file")
        elif "does not exist" in str(e).lower():
            logger.error("Database does not exist - make sure the database has been created")
            
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to target database: {str(e)}")
        raise

def table_exists(connection: psycopg2.connect, schema: str, table: str) -> bool:
    """
    Check if a table exists in the target database.
    
    Args:
        connection: psycopg2.connect - Connection to the target database
        schema: str - Schema name
        table: str - Table name
        
    Returns:
        bool: True if the table exists, False otherwise
    """
    if not schema or not table:
        logger.error("Schema and table names are required")
        return False
    
    try:
        cursor = connection.cursor()
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """
        cursor.execute(query, (schema, table))
        return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error checking if table '{schema}.{table}' exists: {str(e)}")
        return False

def get_table_row_count(connection: psycopg2.connect, schema: str, table: str) -> int:
    """
    Get the number of rows in a specific table in PostgreSQL.
    
    Args:
        connection: psycopg2.connect - Connection to the target database
        schema: str - Schema name
        table: str - Table name
        
    Returns:
        int: Number of rows in the table
    """
    if not schema or not table:
        logger.error("Schema and table names are required")
        return 0
    
    try:
        cursor = connection.cursor()
        query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        )
        cursor.execute(query)
        row_count = cursor.fetchone()[0]
        logger.info(f"Table '{schema}.{table}' has {row_count} rows in target database")
        return row_count
    except Exception as e:
        logger.error(f"Failed to retrieve row count for table '{schema}.{table}' in target: {str(e)}")
        return 0

def create_schema_if_not_exists(connection: psycopg2.connect, schema: str) -> bool:
    """
    Create a schema in the target database if it doesn't already exist.
    
    Args:
        connection: psycopg2.connect - Connection to the target database
        schema: str - Schema name
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not schema:
        logger.error("Schema name is required")
        return False
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema,))
        if not cursor.fetchone():
            logger.info(f"Creating schema '{schema}'")
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            connection.commit()
            logger.info(f"Schema '{schema}' created successfully")
        else:
            logger.info(f"Schema '{schema}' already exists")
        return True
    except Exception as e:
        logger.error(f"Failed to create schema '{schema}': {str(e)}")
        connection.rollback()
        return False

def create_table(
    connection: psycopg2.connect, 
    schema: str, 
    table: str, 
    columns: List[Dict[str, Any]],
    primary_keys: Optional[List[str]] = None,
    drop_if_exists: bool = False
) -> bool:
    """
    Create a table in the target database.
    
    Args:
        connection: psycopg2.connect - Connection to the target database
        schema: str - Schema name
        table: str - Table name
        columns: List[Dict[str, Any]] - Column definitions
        primary_keys: Optional[List[str]] - List of primary key columns
        drop_if_exists: bool - Whether to drop the table if it exists
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not schema or not table or not columns:
        logger.error("Schema, table names, and column definitions are required")
        return False
    
    try:
        # Create schema if it doesn't exist
        if not create_schema_if_not_exists(connection, schema):
            return False
        
        cursor = connection.cursor()
        
        # Check if table exists
        table_exists_flag = table_exists(connection, schema, table)
        
        # Drop table if it exists and drop_if_exists is True
        if table_exists_flag and drop_if_exists:
            logger.info(f"Dropping existing table '{schema}.{table}'")
            drop_query = sql.SQL("DROP TABLE {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
            cursor.execute(drop_query)
            table_exists_flag = False
        
        # Create table if it doesn't exist
        if not table_exists_flag:
            # Build column definitions
            column_defs = []
            for column in columns:
                column_name = column["name"]
                
                # Convert SQL Server type to PostgreSQL type
                pg_type = convert_sql_server_to_postgres_type(column["type"], column["max_length"])
                
                # Build column definition
                if column["is_nullable"] == "YES":
                    column_def = sql.SQL("{} {} NULL").format(
                        sql.Identifier(column_name),
                        sql.SQL(pg_type)
                    )
                else:
                    column_def = sql.SQL("{} {} NOT NULL").format(
                        sql.Identifier(column_name),
                        sql.SQL(pg_type)
                    )
                
                column_defs.append(column_def)
            
            # Add primary key constraint if specified
            if primary_keys and len(primary_keys) > 0:
                pk_columns = [sql.Identifier(pk) for pk in primary_keys]
                pk_constraint = sql.SQL("PRIMARY KEY ({})").format(
                    sql.SQL(", ").join(pk_columns)
                )
                column_defs.append(pk_constraint)
            
            # Create table query
            create_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(column_defs)
            )
            
            logger.info(f"Creating table '{schema}.{table}'")
            cursor.execute(create_query)
            connection.commit()
            logger.info(f"Table '{schema}.{table}' created successfully")
            return True
        else:
            logger.info(f"Table '{schema}.{table}' already exists, skipping creation")
            return True
    
    except Exception as e:
        logger.error(f"Failed to create table '{schema}.{table}': {str(e)}")
        connection.rollback()
        return False

def truncate_table(connection: psycopg2.connect, schema: str, table: str) -> bool:
    """
    Truncate a table in the target database.
    Temporarily disables foreign key constraints during truncation.
    
    Args:
        connection: psycopg2.connect - Connection to the target database
        schema: str - Schema name
        table: str - Table name
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not schema or not table:
        logger.error("Schema and table names are required")
        return False
    
    try:
        cursor = connection.cursor()
        
        # Check if table exists
        if not table_exists(connection, schema, table):
            logger.warning(f"Table '{schema}.{table}' does not exist, cannot truncate")
            return False
        
        success = False
        errors = []
        
        # Method 1: Try with session_replication_role = 'replica'
        try:
            # Save the current isolation level
            original_isolation_level = connection.isolation_level
            # Set to autocommit mode for altering session_replication_role
            connection.set_isolation_level(0)
            
            logger.info(f"Temporarily disabling foreign key constraints for table '{schema}.{table}'")
            cursor.execute("SET session_replication_role = 'replica';")
            
            # Truncate the table
            truncate_query = sql.SQL("TRUNCATE TABLE {}.{};").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
            cursor.execute(truncate_query)
            
            # Reset the session_replication_role
            logger.info("Re-enabling foreign key constraints")
            cursor.execute("SET session_replication_role = 'origin';")
            
            # Restore original isolation level
            connection.set_isolation_level(original_isolation_level)
            
            logger.info(f"Successfully truncated table '{schema}.{table}' with disabled constraints")
            success = True
        except Exception as e1:
            errors.append(f"Method 1 (session_replication_role) failed: {str(e1)}")
            logger.warning(f"Failed to truncate with disabled constraints: {str(e1)}")
            # Make sure to reset session_replication_role and isolation level in case of error
            try:
                cursor.execute("SET session_replication_role = 'origin';")
                connection.set_isolation_level(original_isolation_level)
            except:
                pass
        
        # Method 2: Standard TRUNCATE if Method 1 failed
        if not success:
            try:
                # Create a new cursor
                cursor = connection.cursor()
                truncate_query = sql.SQL("TRUNCATE TABLE {}.{};").format(
                    sql.Identifier(schema),
                    sql.Identifier(table)
                )
                cursor.execute(truncate_query)
                connection.commit()
                logger.info(f"Successfully truncated table '{schema}.{table}' using standard TRUNCATE")
                success = True
            except Exception as e2:
                errors.append(f"Method 2 (standard TRUNCATE) failed: {str(e2)}")
                logger.warning(f"Standard TRUNCATE failed: {str(e2)}")
                connection.rollback()
        
        # Method 3: Try DELETE if previous methods failed (slower but may work with constraints)
        if not success:
            try:
                # Create a new cursor
                cursor = connection.cursor()
                delete_query = sql.SQL("DELETE FROM {}.{};").format(
                    sql.Identifier(schema),
                    sql.Identifier(table)
                )
                cursor.execute(delete_query)
                connection.commit()
                logger.info(f"Successfully deleted all rows from '{schema}.{table}' using DELETE")
                success = True
            except Exception as e3:
                errors.append(f"Method 3 (DELETE) failed: {str(e3)}")
                logger.warning(f"DELETE operation failed: {str(e3)}")
                connection.rollback()
        
        # Method 4: Try with direct constraint disabling if all other methods failed
        if not success:
            try:
                # Create a new cursor and disable all constraints for this table temporarily
                cursor = connection.cursor()
                
                # Get all foreign key constraints affecting this table
                fk_query = """
                SELECT conname FROM pg_constraint
                WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = %s)
                AND contype = 'f';
                """
                cursor.execute(fk_query, (table,))
                constraints = [row[0] for row in cursor.fetchall()]
                
                # Save original isolation level
                original_isolation_level = connection.isolation_level
                connection.set_isolation_level(0)  # autocommit
                
                # Disable constraints
                for constraint in constraints:
                    alter_query = sql.SQL("ALTER TABLE {}.{} DISABLE TRIGGER {}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(constraint)
                    )
                    cursor.execute(alter_query)
                
                # Truncate the table
                truncate_query = sql.SQL("TRUNCATE TABLE {}.{};").format(
                    sql.Identifier(schema),
                    sql.Identifier(table)
                )
                cursor.execute(truncate_query)
                
                # Re-enable constraints
                for constraint in constraints:
                    alter_query = sql.SQL("ALTER TABLE {}.{} ENABLE TRIGGER {}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(constraint)
                    )
                    cursor.execute(alter_query)
                
                # Restore isolation level
                connection.set_isolation_level(original_isolation_level)
                logger.info(f"Successfully truncated table '{schema}.{table}' with manually disabled constraints")
                success = True
            except Exception as e4:
                errors.append(f"Method 4 (manual constraint disabling) failed: {str(e4)}")
                logger.warning(f"Manual constraint disabling failed: {str(e4)}")
                # Make sure to restore isolation level
                try:
                    connection.set_isolation_level(original_isolation_level)
                except:
                    pass
        
        if success:
            return True
        else:
            error_details = "; ".join(errors)
            logger.error(f"Failed to truncate table '{schema}.{table}': {error_details}")
            return False
    
    except Exception as e:
        logger.error(f"Failed to truncate table '{schema}.{table}': {str(e)}")
        try:
            connection.rollback()
        except:
            pass
        return False

def insert_dataframe(
    connection: psycopg2.connect, 
    df: pd.DataFrame, 
    schema: str, 
    table: str, 
    batch_size: int = 1000
) -> Tuple[int, bool]:
    """
    Insert a DataFrame into a table in the target database.
    
    Args:
        connection: psycopg2.connect - Connection to the target database
        df: pd.DataFrame - DataFrame to insert
        schema: str - Schema name
        table: str - Table name
        batch_size: int - Number of rows to insert at once
        
    Returns:
        Tuple[int, bool]: Number of rows inserted and success flag
    """
    if df.empty:
        logger.warning(f"Empty DataFrame, no data to insert into '{schema}.{table}'")
        return 0, True
    
    if not schema or not table:
        logger.error("Schema and table names are required")
        return 0, False
    
    try:
        cursor = connection.cursor()
        
        # Check if table exists
        if not table_exists(connection, schema, table):
            logger.error(f"Table '{schema}.{table}' does not exist, cannot insert data")
            return 0, False
        
        # Create a deep copy to avoid modifying the original DataFrame
        df_clean = df.copy(deep=True)
        
        # Handle numpy types and other data conversions
        for col in df_clean.columns:
            # First check if the column contains any non-None values
            non_null_mask = df_clean[col].notnull()
            
            # Only process columns with non-null values
            if non_null_mask.any():
                # Get the first non-null value to determine type
                sample_val = df_clean.loc[non_null_mask, col].iloc[0]
                
                # Use our type detection function to safely handle NumPy types
                if _is_numpy_type(sample_val):
                    # Apply our conversion function to all non-null values
                    df_clean.loc[non_null_mask, col] = df_clean.loc[non_null_mask, col].apply(
                        lambda x: _convert_numpy_value_to_python(x)
                    )
                elif isinstance(sample_val, pd.Timestamp):
                    # Convert pandas Timestamp to Python datetime
                    df_clean.loc[non_null_mask, col] = df_clean.loc[non_null_mask, col].apply(
                        lambda x: x.to_pydatetime() if isinstance(x, pd.Timestamp) else x
                    )
                
                # Additional safety measure: Convert any remaining NumPy types
                if pd.api.types.is_numeric_dtype(df_clean[col]):
                    # Apply safe conversion to Python types
                    df_clean.loc[non_null_mask, col] = df_clean.loc[non_null_mask, col].apply(
                        lambda x: _convert_numpy_value_to_python(x) if _is_numpy_type(x) else x
                    )
        
        # Replace NaN with None (which converts to SQL NULL)
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        # Get column names
        columns = df_clean.columns.tolist()
        column_identifiers = [sql.Identifier(col) for col in columns]
        
        # Build insert query
        insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(column_identifiers)
        )
        
        # Insert data in batches
        total_rows = len(df_clean)
        inserted_rows = 0
        
        logger.info(f"Inserting {total_rows} rows into '{schema}.{table}' in batches of {batch_size}")
        
        try:
            # Convert DataFrame to list of tuples
            tuples = [tuple(x) for x in df_clean.to_numpy()]
            
            # Ensure all values are Python native types by additional processing of NumPy values
            processed_tuples = []
            for row_tuple in tuples:
                processed_row = []
                for val in row_tuple:
                    # Use the new helper functions for more robust NumPy type handling
                    if _is_numpy_type(val):
                        processed_row.append(_convert_numpy_value_to_python(val))
                    else:
                        processed_row.append(val)
                processed_tuples.append(tuple(processed_row))
            
            # Use the processed tuples instead of the original
            tuples = processed_tuples
            
            # Insert in batches
            for i in range(0, total_rows, batch_size):
                batch = tuples[i:i + batch_size]
                try:
                    execute_values(cursor, insert_query, batch)
                    connection.commit()
                    inserted_rows += len(batch)
                    
                    # Log progress
                    progress = (inserted_rows / total_rows) * 100
                    logger.info(f"Progress: {inserted_rows}/{total_rows} rows ({progress:.2f}%)")
                
                except Exception as batch_error:
                    connection.rollback()
                    # If batch fails, try row by row for better error reporting
                    logger.warning(f"Batch insert failed, trying row by row: {str(batch_error)}")
                    
                    row_insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.SQL(", ").join(column_identifiers),
                        sql.SQL(", ").join([sql.Placeholder() for _ in columns])
                    )
                    
                    for row in batch:
                        try:
                            # Ensure every value in the row is properly converted
                            processed_row = []
                            for val in row:
                                # Use the new helper functions for more robust NumPy type handling
                                if _is_numpy_type(val):
                                    processed_row.append(_convert_numpy_value_to_python(val))
                                else:
                                    processed_row.append(val)
                            
                            # Use the processed row for the insertion
                            cursor.execute(row_insert_query, tuple(processed_row))
                            connection.commit()
                            inserted_rows += 1
                        except Exception as row_error:
                            connection.rollback()
                            logger.error(f"Error inserting row: {str(row_error)}")
                            # Continue with next row despite errors
            
            logger.info(f"Successfully inserted {inserted_rows} rows into '{schema}.{table}'")
            
            return inserted_rows, inserted_rows > 0
            
        except psycopg2.Error as e:
            # Handle errors related to foreign key constraints
            error_msg = str(e)
            if "foreign key" in error_msg.lower() or "violates foreign key constraint" in error_msg.lower() or "narusza klucz obcy" in error_msg.lower():
                logger.error(f"Foreign key constraint violation in '{schema}.{table}': {error_msg}")
            else:
                logger.error(f"Failed to insert data into '{schema}.{table}': {error_msg}")
            connection.rollback()
            return 0, False
            
    except Exception as e:
        logger.error(f"Failed to insert data into '{schema}.{table}': {str(e)}")
        try:
            connection.rollback()
        except:
            pass
        return 0, False

def verify_row_count(
    source_count: int, 
    target_count: int,
    schema: str,
    table: str
) -> Tuple[bool, float]:
    """
    Verify that the row count in the target matches the source.
    
    Args:
        source_count: int - Number of rows in the source table
        target_count: int - Number of rows in the target table
        schema: str - Schema name
        table: str - Table name
        
    Returns:
        Tuple[bool, float]: Success flag and percentage of transferred rows
    """
    if source_count == 0:
        logger.warning(f"Source table '{schema}.{table}' has 0 rows, verification skipped")
        return True, 100.0
    
    percentage = (target_count / source_count) * 100
    
    if target_count == source_count:
        logger.info(f"Row count verification successful for '{schema}.{table}': {target_count}/{source_count} rows (100%)")
        return True, 100.0
    else:
        logger.warning(f"Row count mismatch for '{schema}.{table}': {target_count}/{source_count} rows ({percentage:.2f}%)")
        return False, percentage 