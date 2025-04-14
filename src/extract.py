"""
Extract module for retrieving data from SQL Server.
"""
import logging
import pyodbc
import pandas as pd
from typing import Dict, List, Tuple, Optional, Iterator

from src.config import get_source_connection_string

logger = logging.getLogger(__name__)

def get_source_connection():
    """
    Create a connection to the source SQL Server database.
    
    Returns:
        pyodbc.Connection: Connection to the source database
    
    Raises:
        Exception: If connection cannot be established
    """
    try:
        connection_string = get_source_connection_string()
        logger.info("Connecting to source database")
        logger.info(connection_string)
        connection = pyodbc.connect(connection_string)

        # connection = pyodbc.connect('DSN=localdb;Trusted_Connection=yes;')
        logger.info("Successfully connected to source database")
        return connection
    except pyodbc.OperationalError as e:
        error_msg = f"Failed to connect to source database: {str(e)}"
        logger.error(error_msg)
        
        # More descriptive error messages for common connection issues
        if "timeout" in str(e).lower():
            logger.error("Connection timeout - check if the server is accessible and firewall rules allow the connection")
        elif "login" in str(e).lower():
            logger.error("Login failed - check your username and password in the .env file")
        elif "server" in str(e).lower():
            logger.error("Server not found - check the server address in the .env file")
            
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to source database: {str(e)}")
        raise

def get_schema_tables(connection: pyodbc.Connection, schema: str) -> List[str]:
    """
    Get all tables in a specific schema.
    
    Args:
        connection: pyodbc.Connection - Connection to the source database
        schema: str - Schema name
        
    Returns:
        List[str]: List of table names in the schema
    """
    if not schema:
        logger.error("Schema name is required")
        return []
    
    try:
        cursor = connection.cursor()
        query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ?
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        cursor.execute(query, schema)
        tables = [row.TABLE_NAME for row in cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables in schema '{schema}'")
        return tables
    except Exception as e:
        logger.error(f"Failed to retrieve tables from schema '{schema}': {str(e)}")
        return []

def get_table_columns(connection: pyodbc.Connection, schema: str, table: str) -> List[Dict[str, str]]:
    """
    Get all columns and their data types for a specific table.
    
    Args:
        connection: pyodbc.Connection - Connection to the source database
        schema: str - Schema name
        table: str - Table name
        
    Returns:
        List[Dict[str, str]]: List of column dictionaries with name, type, nullable, etc.
    """
    if not schema or not table:
        logger.error("Schema and table names are required")
        return []
    
    try:
        cursor = connection.cursor()
        query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
            AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query, schema, table)
        
        columns = []
        for row in cursor.fetchall():
            column = {
                "name": row.COLUMN_NAME,
                "type": row.DATA_TYPE,
                "max_length": row.CHARACTER_MAXIMUM_LENGTH,
                "is_nullable": row.IS_NULLABLE,
                "default": row.COLUMN_DEFAULT
            }
            columns.append(column)
        
        logger.info(f"Found {len(columns)} columns in table '{schema}.{table}'")
        return columns
    except Exception as e:
        logger.error(f"Failed to retrieve columns for table '{schema}.{table}': {str(e)}")
        return []

def get_table_primary_keys(connection: pyodbc.Connection, schema: str, table: str) -> List[str]:
    """
    Get primary key columns for a specific table.
    
    Args:
        connection: pyodbc.Connection - Connection to the source database
        schema: str - Schema name
        table: str - Table name
        
    Returns:
        List[str]: List of primary key column names
    """
    if not schema or not table:
        logger.error("Schema and table names are required")
        return []
    
    try:
        cursor = connection.cursor()
        query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
            AND TABLE_SCHEMA = ?
            AND TABLE_NAME = ?
        """
        cursor.execute(query, schema, table)
        primary_keys = [row.COLUMN_NAME for row in cursor.fetchall()]
        
        if primary_keys:
            logger.info(f"Found primary keys for table '{schema}.{table}': {', '.join(primary_keys)}")
        else:
            logger.warning(f"No primary keys found for table '{schema}.{table}'")
            
        return primary_keys
    except Exception as e:
        logger.error(f"Failed to retrieve primary keys for table '{schema}.{table}': {str(e)}")
        return []

def get_table_row_count(connection: pyodbc.Connection, schema: str, table: str) -> int:
    """
    Get the number of rows in a specific table.
    
    Args:
        connection: pyodbc.Connection - Connection to the source database
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
        query = f"SELECT COUNT(*) AS row_count FROM [{schema}].[{table}]"
        cursor.execute(query)
        row_count = cursor.fetchone().row_count
        logger.info(f"Table '{schema}.{table}' has {row_count} rows")
        return row_count
    except Exception as e:
        logger.error(f"Failed to retrieve row count for table '{schema}.{table}': {str(e)}")
        return 0

def get_filtered_table_row_count(connection: pyodbc.Connection, schema: str, table: str, where_clause: Optional[str] = None) -> int:
    """
    Get the number of rows in a specific table that match the given WHERE clause.
    
    Args:
        connection: pyodbc.Connection - Connection to the source database
        schema: str - Schema name
        table: str - Table name
        where_clause: Optional[str] - WHERE clause to filter rows
        
    Returns:
        int: Number of rows in the table that match the WHERE clause
    """
    if not schema or not table:
        logger.error("Schema and table names are required")
        return 0
    
    try:
        cursor = connection.cursor()
        query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        cursor.execute(query)
        row_count = cursor.fetchone()[0]
        logger.info(f"Table '{schema}.{table}' has {row_count} rows matching WHERE condition")
        return row_count
    except Exception as e:
        logger.error(f"Failed to retrieve filtered row count for table '{schema}.{table}': {str(e)}")
        return 0

def extract_table_data(
    connection: pyodbc.Connection, 
    schema: str, 
    table: str, 
    batch_size: int = 10000,
    where_clause: Optional[str] = None
) -> Tuple[pd.DataFrame, int]:
    """
    Extract data from a specific table, optionally with batching.
    
    Args:
        connection: pyodbc.Connection - Connection to the source database
        schema: str - Schema name
        table: str - Table name
        batch_size: int - Number of rows to fetch at once
        where_clause: Optional[str] - Optional WHERE clause for filtering data
        
    Returns:
        Tuple[pd.DataFrame, int]: DataFrame with table data and total row count
    """
    if not schema or not table:
        logger.error("Schema and table names are required")
        return pd.DataFrame(), 0
    
    try:
        # Get total row count for reporting
        total_rows = get_table_row_count(connection, schema, table)
        
        # For extremely large tables, we use batch processing
        if total_rows > batch_size:
            logger.info(f"Table '{schema}.{table}' has {total_rows} rows. Using batch processing with batch size {batch_size}")
            return pd.DataFrame(), total_rows  # Return empty DataFrame but correct count to signal batch processing needed
        
        # Build query
        query = f"SELECT * FROM [{schema}].[{table}]"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        logger.info(f"Extracting data from table '{schema}.{table}'")
        
        # Load data from SQL Server to Pandas DataFrame
        df = pd.read_sql(query, connection)
        
        logger.info(f"Successfully extracted {len(df)} rows from table '{schema}.{table}'")
        return df, total_rows
    except Exception as e:
        logger.error(f"Failed to extract data from table '{schema}.{table}': {str(e)}")
        return pd.DataFrame(), 0

def extract_table_data_in_batches(
    connection: pyodbc.Connection,
    schema: str,
    table: str,
    batch_size: int = 10000,
    where_clause: Optional[str] = None
) -> Iterator[pd.DataFrame]:
    """
    Extract data from a table in batches using server-side pagination.
    
    This is suitable for extremely large tables where loading all data at once
    would cause memory issues.
    
    Args:
        connection: pyodbc.Connection - Connection to the source database
        schema: str - Schema name
        table: str - Table name
        batch_size: int - Number of rows to fetch at once
        where_clause: Optional[str] - Optional WHERE clause for filtering data
        
    Yields:
        pd.DataFrame: Batches of data from the source table
    """
    try:
        # Get primary keys for efficient ordering
        primary_keys = get_table_primary_keys(connection, schema, table)
        
        # If no primary keys, try to find a unique identifier column
        order_columns = primary_keys if primary_keys else get_unique_columns(connection, schema, table)
        
        # If still no suitable columns, use the first column (not ideal but fallback)
        if not order_columns:
            columns = get_table_columns(connection, schema, table)
            if columns and len(columns) > 0:
                # Access the column name safely with proper error handling
                try:
                    if 'name' in columns[0]:
                        order_columns = [columns[0]['name']]
                        logger.info(f"Using column '{columns[0]['name']}' for batch ordering in '{schema}.{table}'")
                    else:
                        for key in columns[0]:
                            if key.lower() in ['name', 'column_name']:
                                order_columns = [columns[0][key]]
                                logger.info(f"Using column '{columns[0][key]}' for batch ordering in '{schema}.{table}'")
                                break
                except Exception as e:
                    logger.error(f"Error accessing column information: {str(e)}")
                    
                # If we still couldn't find a column name, try a different approach
                if not order_columns and isinstance(columns[0], dict):
                    # Just get the first key-value pair from the dictionary
                    for key, value in columns[0].items():
                        if isinstance(value, str):
                            order_columns = [value]
                            logger.info(f"Falling back to using value from key '{key}' for ordering: {value}")
                            break
            
            # If we still have no ordering columns, we need an alternative approach
            if not order_columns:
                logger.warning(f"No suitable columns found for ordering in '{schema}.{table}'. Using a row number approach.")
                # Use SQL Server's ROW_NUMBER() function as a last resort
                base_query = f"""
                    SELECT t.* 
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as __row_num 
                        FROM [{schema}].[{table}]
                        {f"WHERE {where_clause}" if where_clause else ""}
                    ) t
                """
                
                for offset in range(0, 1_000_000_000, batch_size):  # Arbitrary large number
                    pagination_query = f"{base_query} WHERE t.__row_num > {offset} AND t.__row_num <= {offset + batch_size}"
                    logger.debug(f"Executing row number based query: {pagination_query}")
                    
                    # Fetch this batch
                    batch_df = pd.read_sql(pagination_query, connection)
                    
                    # If we got no rows, we're done
                    if batch_df.empty:
                        break
                    
                    # Drop the row number column we added
                    if '__row_num' in batch_df.columns:
                        batch_df.drop(columns=['__row_num'], inplace=True)
                    
                    batch_row_count = len(batch_df)
                    logger.info(f"Extracted batch of {batch_row_count} rows from '{schema}.{table}' (offset: {offset})")
                    
                    yield batch_df
                    
                    # If we got fewer rows than the batch size, we're done
                    if batch_row_count < batch_size:
                        break
                
                # Exit the function since we've handled it with an alternative approach
                return
        
        # Regular approach with ORDER BY when we have suitable columns
        # Build base query
        base_query = f"SELECT * FROM [{schema}].[{table}]"
        if where_clause:
            base_query += f" WHERE {where_clause}"
        
        # We'll use SQL Server's OFFSET-FETCH for pagination
        order_by_clause = f"ORDER BY {', '.join([f'[{col}]' for col in order_columns])}"
        
        offset = 0
        while True:
            pagination_query = f"{base_query} {order_by_clause} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
            logger.debug(f"Executing batch query: {pagination_query}")
            
            # Fetch this batch
            batch_df = pd.read_sql(pagination_query, connection)
            
            # If we got no rows, we're done
            if batch_df.empty:
                break
                
            batch_row_count = len(batch_df)
            logger.info(f"Extracted batch of {batch_row_count} rows from '{schema}.{table}' (offset: {offset})")
            
            yield batch_df
            
            # If we got fewer rows than the batch size, we're done
            if batch_row_count < batch_size:
                break
                
            # Move to the next batch
            offset += batch_size
            
    except Exception as e:
        logger.error(f"Error in batch extraction for '{schema}.{table}': {str(e)}", exc_info=True)
        # Re-raise to ensure the error is properly handled by the caller
        raise

def get_unique_columns(connection: pyodbc.Connection, schema: str, table: str) -> List[str]:
    """
    Find columns with unique constraints that can be used for pagination ordering.
    
    Args:
        connection: pyodbc.Connection - Connection to the source database
        schema: str - Schema name
        table: str - Table name
        
    Returns:
        List[str]: Names of columns with unique constraints
    """
    try:
        cursor = connection.cursor()
        query = """
        SELECT kcu.column_name
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        WHERE tc.TABLE_SCHEMA = ?
            AND tc.TABLE_NAME = ?
            AND tc.CONSTRAINT_TYPE = 'UNIQUE'
        """
        cursor.execute(query, (schema, table))
        
        # Handle column names with proper attribute access
        columns = []
        for row in cursor.fetchall():
            # Different ODBC drivers might return columns in different ways
            if hasattr(row, 'column_name'):
                columns.append(row.column_name)
            elif isinstance(row, tuple) and len(row) > 0:
                columns.append(row[0])
            else:
                # Try to access as dictionary - some ODBC drivers return dict-like objects
                try:
                    columns.append(row['column_name'])
                except (TypeError, KeyError):
                    logger.warning(f"Cannot extract column name from result row: {row}")
        
        if columns:
            logger.info(f"Found unique columns for '{schema}.{table}': {', '.join(columns)}")
        
        return columns
    except Exception as e:
        logger.warning(f"Error getting unique columns for '{schema}.{table}': {str(e)}")
        return [] 