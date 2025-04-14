"""
Transform module for converting data types and cleaning data.
"""
import logging
import pandas as pd
import numpy as np
import hashlib
import re
import json
from typing import Dict, List, Any, Optional

from src.config import TYPE_MAPPING

logger = logging.getLogger(__name__)

def transform_dataframe(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """
    Transform a DataFrame by applying appropriate data type conversions.
    
    Args:
        df: pd.DataFrame - DataFrame to transform
        schema: list - Column definitions with types
        
    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    if df.empty:
        logger.warning("Empty DataFrame, no transformation needed")
        return df
    
    try:
        df_transformed = df.copy()
        
        # Convert all numpy int64 to Python int to avoid "can't adapt type numpy.int64" errors
        for col in df_transformed.columns:
            # Make the first pass to convert numpy types to native Python types
            if pd.api.types.is_integer_dtype(df_transformed[col]):
                # Check for extremely large integers that exceed PostgreSQL bigint range
                # PostgreSQL bigint range: -9223372036854775808 to 9223372036854775807
                if df_transformed[col].max() > 9223372036854775807 or df_transformed[col].min() < -9223372036854775808:
                    logger.warning(f"Column '{col}' contains values outside PostgreSQL bigint range, converting to strings")
                    # Convert integers that exceed bigint range to strings
                    df_transformed[col] = df_transformed[col].astype(str)
                else:
                    # Convert to Python int, but preserve NULL values
                    df_transformed[col] = df_transformed[col].astype(object).where(df_transformed[col].notnull(), None)
            elif pd.api.types.is_float_dtype(df_transformed[col]):
                # Convert numpy.float64 to Python float
                df_transformed[col] = df_transformed[col].astype(float).where(df_transformed[col].notnull(), None)
            elif pd.api.types.is_bool_dtype(df_transformed[col]):
                # Convert numpy.bool_ to Python bool
                df_transformed[col] = df_transformed[col].astype(bool).where(df_transformed[col].notnull(), None)
        
        # Now apply schema-specific transformations
        for col_def in schema:
            col_name = col_def.get('name')
            col_type = col_def.get('type', '').lower()
            is_nullable = col_def.get('is_nullable', 'NO') == 'YES'
            
            # Skip columns that don't exist in the DataFrame
            if col_name not in df_transformed.columns:
                logger.warning(f"Column '{col_name}' not found in DataFrame, skipping")
                continue
            
            # Handle NULL values
            if is_nullable:
                # Replace NaN with None for proper SQL NULL handling
                df_transformed[col_name] = df_transformed[col_name].apply(
                    lambda x: None if pd.isna(x) else x
                )
                logger.info(f"Column '{col_name}' is nullable, preserving NULL values")
            else:
                # For non-nullable columns, fill with appropriate defaults
                if 'int' in col_type:
                    df_transformed[col_name] = df_transformed[col_name].fillna(0)
                elif 'float' in col_type or 'numeric' in col_type or 'decimal' in col_type or 'real' in col_type or 'double' in col_type:
                    df_transformed[col_name] = df_transformed[col_name].fillna(0.0)
                elif 'bool' in col_type:
                    df_transformed[col_name] = df_transformed[col_name].fillna(False)
                elif 'date' in col_type or 'timestamp' in col_type or 'time' in col_type:
                    # Use current date for date fields
                    df_transformed[col_name] = df_transformed[col_name].fillna(pd.Timestamp.now())
                elif 'char' in col_type or 'text' in col_type:
                    df_transformed[col_name] = df_transformed[col_name].fillna('')
                else:
                    logger.warning(f"Unknown data type '{col_type}' for column '{col_name}', unable to set default value")
            
            # Apply type conversions based on the source data type
            try:
                # Integer types with overflow protection for different PostgreSQL integer types
                if any(int_type in col_type for int_type in ['smallint', 'integer', 'int', 'bigint', 'tinyint']):
                    # Check ranges based on PostgreSQL integer types
                    if 'smallint' in col_type:
                        # smallint: -32768 to 32767
                        min_val, max_val = -32768, 32767
                    elif 'integer' in col_type or ('int' in col_type and 'bigint' not in col_type and 'smallint' not in col_type):
                        # integer: -2147483648 to 2147483647
                        min_val, max_val = -2147483648, 2147483647
                    elif 'bigint' in col_type:
                        # bigint: -9223372036854775808 to 9223372036854775807
                        min_val, max_val = -9223372036854775808, 9223372036854775807
                    else:
                        # Default/fallback case
                        min_val, max_val = -2147483648, 2147483647
                    
                    # Convert with pd.to_numeric and handle errors by coercing
                    df_transformed[col_name] = pd.to_numeric(df_transformed[col_name], errors='coerce')
                    
                    # Identify values outside valid range
                    mask_overflow = ~pd.isna(df_transformed[col_name]) & (
                        (df_transformed[col_name] > max_val) | (df_transformed[col_name] < min_val)
                    )
                    
                    if mask_overflow.any():
                        logger.warning(
                            f"Column {col_name} has {mask_overflow.sum()} values outside "
                            f"valid range ({min_val} to {max_val}). Converting to float."
                        )
                        df_transformed[col_name] = df_transformed[col_name].astype(float)
                    else:
                        # Convert to Python int if within range
                        non_null_mask = ~pd.isna(df_transformed[col_name])
                        df_transformed.loc[non_null_mask, col_name] = df_transformed.loc[non_null_mask, col_name].apply(
                            lambda x: int(x) if pd.notnull(x) else None
                        )
                
                # Floating point and numeric types
                elif any(float_type in col_type for float_type in ['real', 'double', 'numeric', 'decimal', 'float', 'money']):
                    # Convert to numeric values
                    df_transformed[col_name] = pd.to_numeric(df_transformed[col_name], errors='coerce')
                    # Convert to Python float
                    non_null_mask = ~pd.isna(df_transformed[col_name])
                    df_transformed.loc[non_null_mask, col_name] = df_transformed.loc[non_null_mask, col_name].apply(
                        lambda x: float(x) if pd.notnull(x) else None
                    )
                
                # Date and timestamp types
                elif any(date_type in col_type for date_type in ['date', 'timestamp', 'time', 'datetime', 'datetime2', 'smalldatetime']):
                    try:
                        # Keep NaT values as they are for proper NULL handling
                        df_transformed[col_name] = pd.to_datetime(
                            df_transformed[col_name], errors='coerce'
                        )
                    except Exception as e:
                        logger.warning(f"Failed to convert column {col_name} to datetime: {str(e)}")
                
                # Boolean type
                elif 'bool' in col_type or col_type == 'bit':
                    # Convert various truthiness values to actual booleans
                    non_null_mask = ~pd.isna(df_transformed[col_name])
                    df_transformed.loc[non_null_mask, col_name] = df_transformed.loc[non_null_mask, col_name].apply(
                        lambda x: bool(x) if pd.notnull(x) else None
                    )
                
                # Character and text types with length handling
                elif any(char_type in col_type for char_type in ['char', 'text']):
                    # Extract the maximum length for varchar/char columns
                    max_length = None
                    
                    if 'varchar' in col_type:
                        match = re.search(r'varchar\((\d+)\)', col_type)
                        if match:
                            max_length = int(match.group(1))
                    elif 'character varying' in col_type:
                        match = re.search(r'character varying\((\d+)\)', col_type)
                        if match:
                            max_length = int(match.group(1))
                    elif 'char' in col_type and 'varchar' not in col_type:
                        match = re.search(r'char\((\d+)\)', col_type)
                        if match:
                            max_length = int(match.group(1))
                    
                    # Convert to string if not already
                    non_null_mask = ~pd.isna(df_transformed[col_name])
                    df_transformed.loc[non_null_mask, col_name] = df_transformed.loc[non_null_mask, col_name].apply(
                        lambda x: str(x) if pd.notnull(x) else None
                    )
                    
                    # Truncate strings that exceed maximum length
                    if max_length:
                        truncate_mask = (
                            ~pd.isna(df_transformed[col_name]) & 
                            (df_transformed[col_name].astype(str).str.len() > max_length)
                        )
                        
                        if truncate_mask.any():
                            logger.warning(
                                f"Truncating {truncate_mask.sum()} values in column {col_name} "
                                f"that exceed maximum length {max_length}"
                            )
                            df_transformed.loc[truncate_mask, col_name] = df_transformed.loc[truncate_mask, col_name].str[:max_length]
                
                # JSON and JSONB handling
                elif 'json' in col_type:
                    # Ensure JSON columns contain valid JSON
                    non_null_mask = ~pd.isna(df_transformed[col_name])
                    df_transformed.loc[non_null_mask, col_name] = df_transformed.loc[non_null_mask, col_name].apply(
                        lambda x: json.dumps(x) if not isinstance(x, str) else x
                    )
                
                # For other data types, log and keep as is
                else:
                    logger.info(f"No specific transformation for data type '{col_type}' in column '{col_name}'")
            
            except Exception as e:
                logger.error(f"Error transforming column '{col_name}' to type '{col_type}': {str(e)}")
                logger.info(f"Keeping original data for column '{col_name}'")
        
        # Final check for any remaining NumPy data types and convert to Python native types
        for col in df_transformed.columns:
            if hasattr(df_transformed[col].dtype, 'name'):
                dtype_name = df_transformed[col].dtype.name
                
                if 'int' in dtype_name:
                    # Check if the column contains values that would overflow a 32-bit integer
                    if df_transformed[col].max() > 2147483647 or df_transformed[col].min() < -2147483648:
                        df_transformed[col] = df_transformed[col].astype(float)
                    else:
                        # Convert NumPy int dtype to Python int objects
                        df_transformed[col] = df_transformed[col].apply(lambda x: int(x) if pd.notnull(x) else None)
                elif 'float' in dtype_name:
                    # Convert NumPy float dtype to Python float objects
                    df_transformed[col] = df_transformed[col].apply(lambda x: float(x) if pd.notnull(x) else None)
                elif 'bool' in dtype_name:
                    # Convert NumPy bool dtype to Python bool objects
                    df_transformed[col] = df_transformed[col].apply(lambda x: bool(x) if pd.notnull(x) else None)
        
        logger.info(f"Successfully transformed DataFrame with {len(df_transformed)} rows")
        return df_transformed
    
    except Exception as e:
        logger.error(f"Error during DataFrame transformation: {str(e)}")
        return df

def validate_data_types(df: pd.DataFrame, schema: list) -> dict:
    """
    Validate data types in a DataFrame against expected types.
    
    Args:
        df: pd.DataFrame - DataFrame to validate
        schema: list - Column definitions with types
    
    Returns:
        dict: Dictionary of columns with type validation issues
    """
    if df.empty:
        logger.warning("Empty DataFrame, no validation needed")
        return {}
    
    issues = {}
    
    for col_def in schema:
        col_name = col_def.get('name')
        expected_type = col_def.get('type', '').lower()
        
        # Skip columns that don't exist in the DataFrame
        if col_name not in df.columns:
            issues[col_name] = [f"Column {col_name} is missing."]
            continue
        
        # For non-nullable columns, report an error if null values are found
        if col_def.get('is_nullable', 'NO') != 'YES' and df[col_name].isnull().any():
            issues.setdefault(col_name, []).append("Non-nullable column contains null values.")
        
        for idx, value in df[col_name].items():
            if pd.isnull(value):
                continue
            if expected_type == 'int':
                converted = pd.to_numeric(value, errors='coerce')
                if pd.isnull(converted):
                    issues.setdefault(col_name, []).append(f"Value {value} at index {idx} is not an int.")
            elif expected_type == 'datetime':
                try:
                    pd.to_datetime(value)
                except Exception:
                    issues.setdefault(col_name, []).append(f"Value {value} at index {idx} is not a valid datetime.")
            # Additional type validations can be added if needed
    
    if issues:
        logger.warning(f"Found data type validation issues in {len(issues)} columns")
        for column, errs in issues.items():
            logger.warning(f"Column '{column}': {', '.join(errs)}")
    else:
        logger.info("No data type validation issues found")
    
    return issues

def convert_sql_server_to_postgres_type(sql_type: str, max_length=None) -> str:
    """
    Convert SQL Server data type to PostgreSQL data type.
    
    Args:
        sql_type: str - SQL Server data type
        max_length: Optional[int] - Maximum length for character types
    
    Returns:
        str: Corresponding PostgreSQL data type
    """
    if not sql_type:
        return "text"  # Default to text as a safe fallback
    
    # Convert to lowercase for case-insensitive matching
    sql_type = sql_type.lower()
    
    # Handle decimal types with precision/scale directly
    if 'decimal' in sql_type:
        return 'numeric'
    if 'money' in sql_type or 'smallmoney' in sql_type:
        return 'numeric'
    if 'bigint' in sql_type:
        return 'numeric'  # Use numeric instead of bigint to avoid overflow
    if 'smallint' in sql_type:
        return 'smallint'
    if 'tinyint' in sql_type:
        return 'smallint'  # PostgreSQL doesn't have tinyint
    if sql_type == 'int':
        # Use numeric instead of bigint to avoid overflow issues
        return 'numeric'
    elif sql_type in ('varchar', 'nvarchar', 'char', 'nchar'):
        # Always use text for all character types to avoid length issues
        # This prevents "value too long for type character varying" errors
        return 'text'
    elif 'text' in sql_type or 'ntext' in sql_type:
        return 'text'
    elif sql_type == 'datetime' or sql_type == 'datetime2' or sql_type == 'smalldatetime':
        return 'timestamp'
    elif sql_type == 'date':
        return 'date'
    elif sql_type == 'time':
        return 'time'
    elif sql_type == 'bit':
        return 'boolean'
    elif sql_type == 'uniqueidentifier':
        return 'uuid'
    elif sql_type == 'xml':
        return 'xml'
    elif 'binary' in sql_type or 'varbinary' in sql_type or 'image' in sql_type:
        return 'bytea'
    else:
        return 'text'  # Use text as a fallback for unknown types

def calculate_checksum(df: pd.DataFrame) -> str:
    """
    Calculate a checksum for a DataFrame to verify data integrity.
    
    Args:
        df: pd.DataFrame - DataFrame to calculate checksum for
    
    Returns:
        str: Checksum value
    """
    if df.empty:
        return "empty_dataframe"
    
    try:
        # Convert DataFrame to CSV string and calculate its MD5 hash
        csv_data = df.to_csv(index=False).encode('utf-8')
        checksum = hashlib.md5(csv_data).hexdigest()
        
        return checksum
    except Exception as e:
        logger.error(f"Error calculating checksum: {str(e)}")
        return "checksum_error" 