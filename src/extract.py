"""
Extract module for retrieving data from SQL Server.
"""
import logging
import pyodbc
import pandas as pd
from typing import Dict, List, Tuple, Optional

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