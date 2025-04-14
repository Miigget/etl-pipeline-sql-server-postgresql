"""
Main ETL orchestration script for migrating data from SQL Server to PostgreSQL.
"""
import os
import sys
import logging
import json
import time
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd

from src.config import get_source_connection_string, get_target_connection_params
from src.extract import (
    get_source_connection,
    get_schema_tables,
    get_table_columns,
    get_table_primary_keys,
    extract_table_data,
    extract_table_data_in_batches,
    get_filtered_table_row_count,
)
from src.transform import (
    transform_dataframe,
    validate_data_types,
    calculate_checksum,
)
from src.load import (
    get_target_connection,
    create_table,
    truncate_table,
    insert_dataframe,
    get_table_row_count as get_target_row_count,
    verify_row_count,
)

# Configure logger
logger = logging.getLogger(__name__)

class ETLPipeline:
    """ETL Pipeline for migrating data from SQL Server to PostgreSQL."""
    
    def __init__(self, source_schemas: List[str], target_schemas: Optional[List[str]] = None, 
                 tables: Optional[List[str]] = None, batch_size: int = 10000,
                 drop_tables: bool = False, truncate_tables: bool = True,
                 disable_foreign_keys: bool = True, max_retries: int = 3,
                 where_clauses: Optional[Dict[str, str]] = None):
        """
        Initialize the ETL Pipeline.
        
        Args:
            source_schemas: List[str] - List of source schemas to migrate
            target_schemas: Optional[List[str]] - List of target schemas (defaults to same as source)
            tables: Optional[List[str]] - List of specific tables to migrate (defaults to all)
            batch_size: int - Number of rows to process at once
            drop_tables: bool - Whether to drop tables before creating
            truncate_tables: bool - Whether to truncate tables before inserting
            disable_foreign_keys: bool - Whether to disable foreign key constraints during migration (default True)
            max_retries: int - Maximum number of retries for tables with foreign key constraint failures
            where_clauses: Optional[Dict[str, str]] - Dictionary mapping table names (schema.table) to WHERE clauses
        """
        self.source_schemas = source_schemas
        self.target_schemas = target_schemas if target_schemas else source_schemas
        self.tables = tables
        self.batch_size = batch_size
        self.drop_tables = drop_tables
        self.truncate_tables = truncate_tables
        self.disable_foreign_keys = disable_foreign_keys
        self.max_retries = max_retries
        self.where_clauses = where_clauses or {}
        
        # Tracking processed tables
        self.processed_tables = set()
        self.failed_tables = set()
        
        # Validation schemas
        if len(self.source_schemas) != len(self.target_schemas):
            raise ValueError("Source and target schemas must have the same length")
        
        # Migration statistics
        self.migration_stats = {
            "start_time": None,
            "end_time": None,
            "total_duration": None,
            "total_tables": 0,
            "successful_tables": 0,
            "failed_tables": 0,
            "total_rows_source": 0,
            "total_rows_target": 0,
            "success_percentage": 0,
            "tables_details": []
        }
    
    def get_tables_to_process(self, source_conn):
        """
        Get a list of tables to process.
        
        Args:
            source_conn: Database connection to source
            
        Returns:
            List of tuples containing (source_schema, target_schema, table)
        """
        all_tables = []
        
        # Process tables from all specified schemas
        for i, source_schema in enumerate(self.source_schemas):
            target_schema = self.target_schemas[i]
            
            # Get tables for the schema
            tables_list = self.tables
            if not tables_list:
                try:
                    tables_list = get_schema_tables(source_conn, source_schema)
                except Exception as e:
                    logger.warning(f"Could not get tables for schema '{source_schema}': {str(e)}")
                    tables_list = []
            
            # Add tables to process list
            for table in tables_list:
                all_tables.append((source_schema, target_schema, table))
        
        logger.info(f"Found {len(all_tables)} tables to process")
        return all_tables
    
    def process_table(self, source_conn, target_conn, source_schema: str, target_schema: str, table: str, retry_count: int = 0) -> dict:
        """
        Process a single table for migration.
        This method allows for retries in case of errors.
        
        Args:
            source_conn: Database connection to source
            target_conn: Database connection to target
            source_schema: str - Source schema name
            target_schema: str - Target schema name
            table: str - Table name
            retry_count: int - Number of times this table has been retried
            
        Returns:
            dict: Table statistics
        """
        table_key = f"{source_schema}.{table}"
        
        # Skip already processed or failed tables
        if table_key in self.processed_tables:
            logger.info(f"Table '{table_key}' already processed, skipping")
            return None
        
        if table_key in self.failed_tables and retry_count >= self.max_retries:
            logger.warning(f"Table '{table_key}' already failed {retry_count} times, skipping")
            return None
        
        table_stats = {
            "source_schema": source_schema,
            "target_schema": target_schema,
            "table_name": table,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "duration": None,
            "source_rows": 0,
            "target_rows": 0,
            "success": False,
            "errors": [],
            "warnings": [],
            "retry_count": retry_count
        }
        
        try:
            logger.info(f"Processing table: {source_schema}.{table} -> {target_schema}.{table}")
            
            # Get table columns and primary keys from source
            columns = get_table_columns(source_conn, source_schema, table)
            primary_keys = get_table_primary_keys(source_conn, source_schema, table)
            
            if not columns:
                error_msg = f"No columns found for table '{source_schema}.{table}'"
                logger.error(error_msg)
                table_stats["errors"].append(error_msg)
                self.failed_tables.add(table_key)
                return table_stats
            
            # Create table in target database
            create_success = create_table(
                target_conn,
                target_schema,
                table,
                columns,
                primary_keys,
                self.drop_tables
            )
            
            if not create_success:
                error_msg = f"Failed to create table '{target_schema}.{table}'"
                logger.error(error_msg)
                table_stats["errors"].append(error_msg)
                self.failed_tables.add(table_key)
                return table_stats
            
            # Truncate or delete all rows if needed
            if self.truncate_tables:
                try:
                    # Try DELETE instead of TRUNCATE to avoid foreign key issues
                    cursor = target_conn.cursor()
                    cursor.execute(f'DELETE FROM "{target_schema}"."{table}"')
                    target_conn.commit()
                    logger.info(f"Successfully deleted all rows from '{target_schema}.{table}' using DELETE")
                except Exception as e:
                    warning_msg = f"Failed to delete rows from table '{target_schema}.{table}': {str(e)}"
                    logger.warning(warning_msg)
                    table_stats["warnings"].append(warning_msg)
            
            # Get WHERE clause for this table if specified
            where_clause = self.where_clauses.get(table_key)
            if where_clause:
                logger.info(f"Using WHERE clause for '{table_key}': {where_clause}")
                table_stats["where_clause"] = where_clause
                
                # Get filtered row count (only rows matching WHERE clause)
                filtered_row_count = get_filtered_table_row_count(
                    source_conn, 
                    source_schema, 
                    table, 
                    where_clause
                )
                logger.info(f"Filtered row count for '{table_key}': {filtered_row_count} rows")
                # Store the filtered count for verification
                table_stats["source_rows_filtered"] = filtered_row_count
            
            # Extract data from source
            df, source_row_count = extract_table_data(
                source_conn, 
                source_schema, 
                table, 
                self.batch_size,
                where_clause
            )
            
            # Update source row count - use filtered count if available
            if where_clause and "source_rows_filtered" in table_stats:
                # For WHERE clause filtered tables, we use the filtered count as source
                table_stats["source_rows"] = table_stats["source_rows_filtered"]
                logger.info(f"Using filtered row count ({table_stats['source_rows_filtered']}) for verification instead of total table size ({source_row_count})")
            else:
                table_stats["source_rows"] = source_row_count
            
            if df.empty and source_row_count > 0:
                # This indicates a large table that needs batch processing
                logger.info(f"Table '{source_schema}.{table}' has {source_row_count} rows. Using batch processing.")
                
                # Process table in batches
                batch_insert_success = self.process_table_in_batches(
                    source_conn,
                    target_conn,
                    source_schema,
                    target_schema,
                    table,
                    table_stats
                )
                
                if not batch_insert_success:
                    error_msg = f"Failed to process large table '{source_schema}.{table}' in batches"
                    logger.error(error_msg)
                    table_stats["errors"].append(error_msg)
                    
                    # Mark table as failed
                    table_stats["success"] = False
                    
                    # Check if we should retry
                    if retry_count < self.max_retries:
                        logger.warning(f"Will retry table '{table_key}' later (attempt {retry_count + 1})")
                        return table_stats
                    
                    self.failed_tables.add(table_key)
                    return table_stats
                else:
                    # Batch processing was successful
                    table_stats["success"] = True
                    logger.info(f"Successfully processed '{source_schema}.{table}' with {table_stats['target_rows']} rows transferred ({table_stats.get('completion_percentage', 0):.2f}%)")
            elif not df.empty:
                # Transform data
                df_transformed = transform_dataframe(df, columns)
                
                # Force python native types to avoid np.float64 issues
                for col in df_transformed.columns:
                    if df_transformed[col].dtype.name.startswith('float'):
                        # Explicitly convert numpy float types to Python float
                        non_null = ~df_transformed[col].isna()
                        df_transformed.loc[non_null, col] = df_transformed.loc[non_null, col].apply(
                            lambda x: float(x) if pd.notnull(x) else None
                        )
                    elif df_transformed[col].dtype.name.startswith('int'):
                        # Convert integers to float if they might be too large
                        # This prevents integer overflow errors
                        if df_transformed[col].max() > 2147483647 or df_transformed[col].min() < -2147483648:
                            df_transformed[col] = df_transformed[col].astype(float)
                        else:
                            # Explicitly convert to Python int
                            non_null = ~df_transformed[col].isna()
                            df_transformed.loc[non_null, col] = df_transformed.loc[non_null, col].apply(
                                lambda x: int(x) if pd.notnull(x) else None
                            )
                    elif 'datetime' in df_transformed[col].dtype.name or df_transformed[col].dtype.name.startswith('datetime'):
                        # Ensure datetime types are properly converted
                        non_null = ~df_transformed[col].isna()
                        df_transformed.loc[non_null, col] = df_transformed.loc[non_null, col].apply(
                            lambda x: x.to_pydatetime() if hasattr(x, 'to_pydatetime') else x
                        )
                    elif df_transformed[col].dtype.name.startswith('bool'):
                        # Explicitly convert to Python bool
                        non_null = ~df_transformed[col].isna()
                        df_transformed.loc[non_null, col] = df_transformed.loc[non_null, col].apply(
                            lambda x: bool(x) if pd.notnull(x) else None
                        )
                    elif hasattr(df_transformed[col].dtype, 'type'):
                        # Handle other numpy types by converting them to Python native types
                        non_null = ~df_transformed[col].isna()
                        df_transformed.loc[non_null, col] = df_transformed.loc[non_null, col].apply(
                            lambda x: x.item() if hasattr(x, 'item') else x
                        )
                
                # Validate data types
                validation_issues = validate_data_types(df_transformed, columns)
                if validation_issues:
                    for column, issues in validation_issues.items():
                        warning_msg = f"Data validation issues in column '{column}': {', '.join(issues)}"
                        logger.warning(warning_msg)
                        table_stats["warnings"].append(warning_msg)
                
                # Calculate data checksum
                checksum = calculate_checksum(df_transformed)
                table_stats["source_checksum"] = checksum
                
                # Insert data into target
                rows_inserted, insert_success = insert_dataframe(
                    target_conn,
                    df_transformed,
                    target_schema,
                    table,
                    self.batch_size
                )
                
                if not insert_success:
                    error_msg = f"Failed to insert data into table '{target_schema}.{table}'"
                    logger.error(error_msg)
                    table_stats["errors"].append(error_msg)
                    
                    # Check if we should retry
                    if retry_count < self.max_retries:
                        logger.warning(f"Will retry table '{table_key}' later (attempt {retry_count + 1})")
                        return table_stats
                    
                    self.failed_tables.add(table_key)
                    return table_stats
                
                # Get row count from target
                target_row_count = get_target_row_count(target_conn, target_schema, table)
                table_stats["target_rows"] = target_row_count
                
                # Verify row count
                verify_success, percentage = verify_row_count(
                    source_row_count,
                    target_row_count,
                    target_schema,
                    table
                )
                
                table_stats["row_verification"] = verify_success
                table_stats["completion_percentage"] = percentage
                
                # Calculate whether the transfer was successful based on filtered counts when needed
                if where_clause:
                    # When filtering is used, we care about whether the number of transferred rows matches the filtered count
                    success = target_row_count == table_stats["source_rows"]  # Require exact match (100%)
                    logger.info(f"Row verification with WHERE filter: {target_row_count}/{table_stats['source_rows']} rows ({percentage:.2f}%). Success: {success}")
                    
                    # Update row verification result based on filtered comparison
                    table_stats["row_verification"] = success
                    
                    if not success:
                        # Not an exact match - add a warning
                        warning_msg = f"Row count mismatch: {target_row_count}/{table_stats['source_rows']} rows ({percentage:.2f}%)"
                        logger.warning(warning_msg)
                        table_stats["warnings"].append(warning_msg)
            else:
                # Empty source table
                logger.info(f"Table '{source_schema}.{table}' is empty, no data to transfer")
                table_stats["target_rows"] = 0
                table_stats["row_verification"] = True
                table_stats["completion_percentage"] = 100.0
            
            # Mark table as processed and successful - base success on row_verification field
            table_stats["success"] = True if table_stats["row_verification"] and not table_stats["errors"] else False
            self.processed_tables.add(table_key)
            
            # Update end time and duration
            table_stats["end_time"] = datetime.now().isoformat()
            start_time = datetime.fromisoformat(table_stats["start_time"])
            end_time = datetime.fromisoformat(table_stats["end_time"])
            table_stats["duration"] = (end_time - start_time).total_seconds()
            
            return table_stats
            
        except Exception as e:
            error_msg = f"Error processing table '{source_schema}.{table}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            table_stats["errors"].append(error_msg)
            
            # Only add to failed tables if we've tried enough times
            if retry_count >= self.max_retries - 1:
                self.failed_tables.add(table_key)
            
            # Update end time and duration
            table_stats["end_time"] = datetime.now().isoformat()
            start_time = datetime.fromisoformat(table_stats["start_time"])
            end_time = datetime.fromisoformat(table_stats["end_time"])
            table_stats["duration"] = (end_time - start_time).total_seconds()
            
            return table_stats
    
    def process_table_in_batches(self, source_conn, target_conn, source_schema, target_schema, table, table_stats):
        """
        Process a large table in batches to avoid memory issues.
        
        Args:
            source_conn: Database connection to source
            target_conn: Database connection to target
            source_schema: str - Source schema name
            target_schema: str - Target schema name
            table: str - Table name
            table_stats: dict - Current table statistics to update
            
        Returns:
            bool: Success status
        """
        try:
            logger.info(f"Processing large table in batches: {source_schema}.{table}")
            
            # Get table columns from source (needed for transformation)
            columns = get_table_columns(source_conn, source_schema, table)
            
            if not columns:
                error_msg = f"No columns found for table '{source_schema}.{table}'"
                logger.error(error_msg)
                table_stats["errors"].append(error_msg)
                return False
            
            # Get WHERE clause for this table if specified
            table_key = f"{source_schema}.{table}"
            where_clause = self.where_clauses.get(table_key)
            if where_clause:
                logger.info(f"Using WHERE clause for batch processing of '{table_key}': {where_clause}")
                # Add WHERE clause to table_stats for reporting
                table_stats["where_clause"] = where_clause
                
                # Get filtered row count (only rows matching WHERE clause)
                if "source_rows_filtered" not in table_stats:
                    filtered_row_count = get_filtered_table_row_count(
                        source_conn, 
                        source_schema, 
                        table, 
                        where_clause
                    )
                    logger.info(f"Filtered row count for '{table_key}': {filtered_row_count} rows")
                    # Store the filtered count for verification
                    table_stats["source_rows_filtered"] = filtered_row_count
                    # Update the source row count to use filtered count
                    table_stats["source_rows"] = filtered_row_count
                    logger.info(f"Using filtered row count ({filtered_row_count}) for verification instead of total table size ({table_stats['source_rows']})")
            
            # Get batches iterator
            try:
                batches = extract_table_data_in_batches(
                    source_conn,
                    source_schema,
                    table,
                    self.batch_size,
                    where_clause
                )
            except Exception as e:
                error_msg = f"Failed to initialize batch extraction for '{source_schema}.{table}': {str(e)}"
                logger.error(error_msg, exc_info=True)
                table_stats["errors"].append(error_msg)
                return False
            
            total_inserted = 0
            batch_count = 0
            
            # Track if we processed any batches
            processed_any_batches = False
            
            # Process each batch
            for batch_df in batches:
                processed_any_batches = True
                batch_count += 1
                
                if batch_df.empty:
                    logger.warning(f"Empty batch received for '{source_schema}.{table}', skipping")
                    continue
                
                # Transform this batch
                batch_transformed = transform_dataframe(batch_df, columns)
                
                # Force python native types to avoid np.float64 issues
                for col in batch_transformed.columns:
                    if batch_transformed[col].dtype.name.startswith('float'):
                        # Explicitly convert numpy float types to Python float
                        non_null = ~batch_transformed[col].isna()
                        batch_transformed.loc[non_null, col] = batch_transformed.loc[non_null, col].apply(
                            lambda x: float(x) if pd.notnull(x) else None
                        )
                    elif batch_transformed[col].dtype.name.startswith('int'):
                        # Convert integers to float if they might be too large
                        if batch_transformed[col].max() > 2147483647 or batch_transformed[col].min() < -2147483648:
                            batch_transformed[col] = batch_transformed[col].astype(float)
                        else:
                            # Explicitly convert to Python int
                            non_null = ~batch_transformed[col].isna()
                            batch_transformed.loc[non_null, col] = batch_transformed.loc[non_null, col].apply(
                                lambda x: int(x) if pd.notnull(x) else None
                            )
                    elif 'datetime' in batch_transformed[col].dtype.name or batch_transformed[col].dtype.name.startswith('datetime'):
                        # Ensure datetime types are properly converted
                        non_null = ~batch_transformed[col].isna()
                        batch_transformed.loc[non_null, col] = batch_transformed.loc[non_null, col].apply(
                            lambda x: x.to_pydatetime() if hasattr(x, 'to_pydatetime') else x
                        )
                    elif batch_transformed[col].dtype.name.startswith('bool'):
                        # Explicitly convert to Python bool
                        non_null = ~batch_transformed[col].isna()
                        batch_transformed.loc[non_null, col] = batch_transformed.loc[non_null, col].apply(
                            lambda x: bool(x) if pd.notnull(x) else None
                        )
                    elif hasattr(batch_transformed[col].dtype, 'type'):
                        # Handle other numpy types by converting them to Python native types
                        non_null = ~batch_transformed[col].isna()
                        batch_transformed.loc[non_null, col] = batch_transformed.loc[non_null, col].apply(
                            lambda x: x.item() if hasattr(x, 'item') else x
                        )
                
                # Validate data types
                validation_issues = validate_data_types(batch_transformed, columns)
                if validation_issues:
                    for column, issues in validation_issues.items():
                        warning_msg = f"Data validation issues in batch {batch_count}, column '{column}': {', '.join(issues)}"
                        logger.warning(warning_msg)
                        table_stats["warnings"].append(warning_msg)
                
                # Insert this batch
                rows_inserted, insert_success = insert_dataframe(
                    target_conn,
                    batch_transformed,
                    target_schema,
                    table,
                    self.batch_size
                )
                
                if not insert_success:
                    error_msg = f"Failed to insert batch {batch_count} into '{target_schema}.{table}'. Reason: {rows_inserted} rows inserted."
                    logger.error(error_msg)
                    table_stats["errors"].append(error_msg)
                    # Continue trying other batches rather than failing the whole table immediately
                    if rows_inserted == 0:
                        warning_msg = f"Batch {batch_count} had 0 rows inserted, will continue with next batch but table might be incomplete"
                        logger.warning(warning_msg)
                        table_stats["warnings"].append(warning_msg)
                    else:
                        # Some rows were inserted, so continue
                        total_inserted += rows_inserted
                        logger.info(f"Batch {batch_count} partially inserted {rows_inserted} rows. Total so far: {total_inserted}")
                else:
                    total_inserted += rows_inserted
                    logger.info(f"Batch {batch_count} inserted {rows_inserted} rows. Total so far: {total_inserted}")
            
            # Check if we processed any batches at all
            if not processed_any_batches:
                error_msg = f"No batches were processed for '{source_schema}.{table}'. Extraction likely failed."
                logger.error(error_msg)
                table_stats["errors"].append(error_msg)
                return False
            
            # Check if we managed to insert any rows
            if total_inserted == 0:
                error_msg = f"Failed to insert any rows into '{target_schema}.{table}' after processing {batch_count} batches."
                logger.error(error_msg)
                table_stats["errors"].append(error_msg)
                return False
            
            # Get final row count from target
            target_row_count = get_target_row_count(target_conn, target_schema, table)
            table_stats["target_rows"] = target_row_count
            
            # Verify row count
            verify_success, percentage = verify_row_count(
                table_stats["source_rows"],
                target_row_count,
                target_schema,
                table
            )
            
            table_stats["row_verification"] = verify_success
            table_stats["completion_percentage"] = percentage
            
            # Only report success if we actually transferred some data
            if target_row_count == 0 and table_stats["source_rows"] > 0:
                error_msg = f"Failed to transfer any data for '{source_schema}.{table}'. Source has {table_stats['source_rows']} rows but target has 0."
                logger.error(error_msg)
                table_stats["errors"].append(error_msg)
                return False
            
            # Calculate whether the transfer was successful based on filtered counts when needed
            if where_clause:
                # When filtering is used, we care about whether the number of transferred rows matches the filtered count
                success = target_row_count == table_stats["source_rows"]  # Require exact match (100%)
                logger.info(f"Row verification with WHERE filter: {target_row_count}/{table_stats['source_rows']} rows ({percentage:.2f}%). Success: {success}")
                
                if not success:
                    # Not an exact match - add a warning
                    warning_msg = f"Row count mismatch: {target_row_count}/{table_stats['source_rows']} rows ({percentage:.2f}%)"
                    logger.warning(warning_msg)
                    table_stats["warnings"].append(warning_msg)
                    
                logger.info(f"Successfully processed large table '{source_schema}.{table}' in {batch_count} batches. Transferred {target_row_count}/{table_stats['source_rows']} rows ({percentage:.2f}%).")
                return success
            else:
                # Standard behavior for non-filtered tables
                logger.info(f"Successfully processed large table '{source_schema}.{table}' in {batch_count} batches. Transferred {target_row_count}/{table_stats['source_rows']} rows ({percentage:.2f}%).")
                return verify_success  # Return based on row count verification
            
        except Exception as e:
            error_msg = f"Error processing large table '{source_schema}.{table}' in batches: {str(e)}"
            logger.error(error_msg, exc_info=True)
            table_stats["errors"].append(error_msg)
            return False
    
    def run(self):
        """
        Run the ETL Pipeline with simplified processing (assuming foreign keys are disabled).
        """
        # Record start time
        self.migration_stats["start_time"] = datetime.now().isoformat()
        start_time = time.time()
        
        logger.info("Starting ETL pipeline")
        logger.info(f"Source schemas: {', '.join(self.source_schemas)}")
        logger.info(f"Target schemas: {', '.join(self.target_schemas)}")
        
        try:
            # Get source and target connections
            source_conn = get_source_connection()
            target_conn = get_target_connection()
            
            # Always disable foreign key constraints 
            logger.info("Disabling foreign key constraints globally for the entire migration process")
            cursor = target_conn.cursor()
            original_isolation_level = target_conn.isolation_level
            
            try:
                # Set to autocommit mode for altering session_replication_role
                target_conn.set_isolation_level(0)
                cursor.execute("SET session_replication_role = 'replica';")
                logger.info("Foreign key constraints disabled successfully")
            except Exception as e:
                logger.error(f"Failed to disable foreign key constraints: {str(e)}")
                logger.warning("Attempting to continue, but might encounter foreign key errors")
            
            # Get all tables to process (no dependency analysis needed when foreign keys are disabled)
            tables_to_process = self.get_tables_to_process(source_conn)
            
            # Count total tables
            self.migration_stats["total_tables"] = len(tables_to_process)
            
            # Process all tables
            tables_to_retry = []
            
            for source_schema, target_schema, table in tables_to_process:
                # Process the table
                table_stats = self.process_table(source_conn, target_conn, source_schema, target_schema, table)
                
                if table_stats:
                    # If processing failed, add to retry queue
                    if not table_stats["success"]:
                        tables_to_retry.append((source_schema, target_schema, table, 1))
                    else:
                        self.migration_stats["tables_details"].append(table_stats)
                        self.migration_stats["successful_tables"] += 1
                        self.migration_stats["total_rows_source"] += table_stats["source_rows"]
                        self.migration_stats["total_rows_target"] += table_stats["target_rows"]
            
            # Process retry queue with multiple passes
            for retry_pass in range(self.max_retries):
                if not tables_to_retry:
                    break
                    
                logger.info(f"Starting retry pass {retry_pass + 1} with {len(tables_to_retry)} tables")
                next_retry_queue = []
                
                for source_schema, target_schema, table, retry_count in tables_to_retry:
                    # Process the table with retry count
                    table_stats = self.process_table(
                        source_conn, target_conn, source_schema, target_schema, table, retry_count
                    )
                    
                    if table_stats:
                        if not table_stats["success"] and retry_count < self.max_retries - 1:
                            next_retry_queue.append((source_schema, target_schema, table, retry_count + 1))
                        else:
                            self.migration_stats["tables_details"].append(table_stats)
                            if table_stats["success"]:
                                self.migration_stats["successful_tables"] += 1
                                self.migration_stats["total_rows_source"] += table_stats["source_rows"]
                                self.migration_stats["total_rows_target"] += table_stats["target_rows"]
                            else:
                                self.migration_stats["failed_tables"] += 1
                
                tables_to_retry = next_retry_queue
            
            # Handle any remaining failed tables
            for source_schema, target_schema, table, retry_count in tables_to_retry:
                logger.warning(f"Table '{source_schema}.{table}' could not be processed after {retry_count} retries")
                table_stats = {
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "table_name": table,
                    "success": False,
                    "errors": ["Failed after maximum retries"],
                    "retry_count": retry_count
                }
                self.migration_stats["tables_details"].append(table_stats)
                self.migration_stats["failed_tables"] += 1
            
            # Update migration statistics
            end_time = time.time()
            self.migration_stats["end_time"] = datetime.now().isoformat()
            self.migration_stats["total_duration"] = end_time - start_time
            
            if self.migration_stats["total_tables"] > 0:
                self.migration_stats["success_percentage"] = (
                    self.migration_stats["successful_tables"] / self.migration_stats["total_tables"]
                ) * 100
            
            # Generate reports
            self.generate_report()
            
            # Print summary
            self.print_summary(self.migration_stats)
            
            logger.info("ETL pipeline completed")
            
        except Exception as e:
            logger.error(f"Error in ETL pipeline: {str(e)}", exc_info=True)
            self.migration_stats["end_time"] = datetime.now().isoformat()
            end_time = time.time()
            self.migration_stats["total_duration"] = end_time - start_time
        
        finally:
            # Re-enable foreign key constraints
            try:
                logger.info("Re-enabling foreign key constraints")
                cursor = target_conn.cursor()
                cursor.execute("SET session_replication_role = 'origin';")
                target_conn.set_isolation_level(original_isolation_level)
                logger.info("Foreign key constraints re-enabled successfully")
            except Exception as e:
                logger.error(f"Failed to re-enable foreign key constraints: {str(e)}")
                logger.critical("Foreign key constraints might still be disabled! Please check your database.")
            
            # Close connections
            try:
                source_conn.close()
                target_conn.close()
            except:
                pass
    
    def generate_report(self, report_file: str = "migration_report.json"):
        """
        Generate a migration report.
        
        Args:
            report_file: str - Path to the report file
        """
        try:
            # Create a summary
            summary = {
                "timestamp": datetime.now().isoformat(),
                "duration": self.migration_stats["total_duration"],
                "duration_formatted": self.format_duration(self.migration_stats["total_duration"]),
                "total_tables": self.migration_stats["total_tables"],
                "successful_tables": self.migration_stats["successful_tables"],
                "failed_tables": self.migration_stats["failed_tables"],
                "success_percentage": self.migration_stats["success_percentage"],
                "total_rows_source": self.migration_stats["total_rows_source"],
                "total_rows_target": self.migration_stats["total_rows_target"],
                "transfer_percentage": (
                    (self.migration_stats["total_rows_target"] / self.migration_stats["total_rows_source"]) * 100
                    if self.migration_stats["total_rows_source"] > 0 else 100
                ),
            }
            
            # Add summary to migration stats
            self.migration_stats["summary"] = summary
            
            # Write report to file
            with open(report_file, "w") as f:
                json.dump(self.migration_stats, f, indent=2)
            
            logger.info(f"Migration report generated: {report_file}")
            
            # Print summary to console
            self.print_summary(summary)
        
        except Exception as e:
            logger.error(f"Error generating migration report: {str(e)}", exc_info=True)
    
    def print_summary(self, summary: Dict[str, Any]):
        """
        Print a summary of the migration.
        
        Args:
            summary: Dict[str, Any] - Migration summary
        """
        # Ensure duration_formatted is available
        if 'duration_formatted' not in summary and 'total_duration' in summary:
            summary['duration_formatted'] = self.format_duration(summary["total_duration"])
        elif 'duration_formatted' not in summary and 'total_duration' in self.migration_stats:
            summary['duration_formatted'] = self.format_duration(self.migration_stats["total_duration"])
        elif 'duration_formatted' not in summary:
            summary['duration_formatted'] = "00:00:00"
            
        print("\n" + "=" * 80)
        print(f"ETL MIGRATION SUMMARY - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        print(f"Duration: {summary.get('duration_formatted', 'N/A')}")
        print(f"Total tables: {summary.get('total_tables', 0)}")
        print(f"Successfully migrated: {summary.get('successful_tables', 0)} tables")
        print(f"Failed: {summary.get('failed_tables', 0)} tables")
        print(f"Success rate: {summary.get('success_percentage', 0):.2f}%")
        print(f"Rows in source: {summary.get('total_rows_source', 0):,}")
        print(f"Rows in target: {summary.get('total_rows_target', 0):,}")
        
        # Categorize errors
        if summary.get('tables_details'):
            error_categories = {}
            for table in summary['tables_details']:
                if not table.get('success', False) and table.get('errors'):
                    for error in table.get('errors', []):
                        # Extract error type
                        error_type = "Unknown error"
                        
                        if "foreign key" in str(error).lower():
                            error_type = "Foreign Key Constraint Violation"
                        elif "integer out of range" in str(error).lower():
                            error_type = "Integer Overflow"
                        elif "value too long" in str(error).lower():
                            error_type = "Text Value Too Long"
                        elif "cannot truncate" in str(error).lower():
                            error_type = "Truncate Error (Foreign Key Constraint)"
                        elif "np" in str(error).lower() and "schema" in str(error).lower():
                            error_type = "NumPy Data Type Error"
                        elif "Failed after maximum retries" in str(error):
                            error_type = "Maximum Retries Exceeded"
                        
                        if error_type not in error_categories:
                            error_categories[error_type] = []
                        
                        # Add table to the error category
                        table_key = f"{table.get('source_schema', 'unknown')}.{table.get('table_name', 'unknown')}"
                        if table_key not in error_categories[error_type]:
                            error_categories[error_type].append(table_key)
            
            # Print error summary
            if error_categories:
                print("\nERROR SUMMARY:")
                print("-" * 80)
                for error_type, tables in error_categories.items():
                    print(f"{error_type}: {len(tables)} tables")
                    # Print up to 5 example tables for each error type
                    for i, table in enumerate(tables[:5]):
                        print(f"  - {table}")
                    if len(tables) > 5:
                        print(f"  - ... and {len(tables) - 5} more tables")
                
                # Suggest solutions for common error types
                print("\nSUGGESTED SOLUTIONS:")
                print("-" * 80)
                
                if "Integer Overflow" in error_categories:
                    print("Integer Overflow: Consider modifying the schema to use numeric data type instead of int/bigint")
                
                if "Text Value Too Long" in error_categories:
                    print("Text Value Too Long: Consider modifying the schema to use text instead of varchar with length limit")
                
                if "Foreign Key Constraint Violation" in error_categories or "Truncate Error (Foreign Key Constraint)" in error_categories:
                    print("Foreign Key Constraints: Review table dependencies and ensure correct migration order")
                    print("  - Consider using --disable-foreign-keys option which is now the default")
                
                if "NumPy Data Type Error" in error_categories:
                    print("NumPy Data Type Error: Issues with NumPy data types in SQL")
                    print("  - The code has been modified to convert NumPy types to Python native types")
        
        print("=" * 80)
        
        # Save detailed error report
        try:
            error_report_file = "error_report.txt"
            with open(error_report_file, 'w') as f:
                f.write(f"ETL MIGRATION ERROR REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")
                
                failed_tables = [t for t in summary.get('tables_details', []) if not t.get('success', False)]
                f.write(f"FAILED TABLES ({len(failed_tables)}):\n\n")
                
                for table in failed_tables:
                    f.write(f"Table: {table.get('source_schema', 'unknown')}.{table.get('table_name', 'unknown')}\n")
                    f.write(f"Retry count: {table.get('retry_count', 0)}\n")
                    f.write("Errors:\n")
                    
                    for error in table.get('errors', ["Unknown error"]):
                        f.write(f"  - {error}\n")
                    
                    f.write("\n" + "-" * 40 + "\n\n")
            
            print(f"\nDetailed error report saved to {error_report_file}")
        except Exception as e:
            logger.error(f"Failed to write error report: {str(e)}")
    
    @staticmethod
    def format_duration(seconds: float) -> str:
        """
        Format duration in seconds to a human-readable format.
        
        Args:
            seconds: float - Duration in seconds
            
        Returns:
            str: Formatted duration
        """
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"


def parse_args():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="ETL Pipeline for migrating data from SQL Server to PostgreSQL")
    
    parser.add_argument(
        "--source-schemas",
        type=str,
        nargs="+",
        required=True,
        help="Source schemas to migrate (space-separated)"
    )
    
    parser.add_argument(
        "--target-schemas",
        type=str,
        nargs="+",
        help="Target schemas (defaults to same as source)"
    )
    
    parser.add_argument(
        "--tables",
        type=str,
        nargs="+",
        help="Specific tables to migrate (defaults to all)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Number of rows to process at once (default: 10000)"
    )
    
    parser.add_argument(
        "--drop-tables",
        action="store_true",
        help="Drop tables before creating"
    )
    
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Don't truncate tables before inserting"
    )
    
    parser.add_argument(
        "--disable-foreign-keys",
        action="store_true",
        help="Disable foreign key constraints during migration (use with caution)"
    )
    
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retries for tables with foreign key constraint failures (default: 3)"
    )
    
    parser.add_argument(
        "--report-file",
        type=str,
        default="migration_report.json",
        help="Path to the report file (default: migration_report.json)"
    )
    
    parser.add_argument(
        "--where-clause",
        type=str,
        nargs="+",
        help="Specify WHERE clauses for specific tables. Format: 'schema.table:condition'"
    )
    
    return parser.parse_args()


def main():
    """
    Main entry point.
    """
    # Parse command line arguments
    args = parse_args()
    
    # Process where clauses if provided
    where_clauses = {}
    if args.where_clause:
        for clause in args.where_clause:
            # Split the argument by first colon to get table and condition
            parts = clause.split(':', 1)
            if len(parts) != 2:
                logger.error(f"Invalid where clause format: {clause}. Format should be 'schema.table:condition'")
                continue
                
            table_key, condition = parts
            where_clauses[table_key] = condition
            logger.info(f"Added WHERE clause for table '{table_key}': {condition}")
    
    # Create and run ETL pipeline
    etl = ETLPipeline(
        source_schemas=args.source_schemas,
        target_schemas=args.target_schemas,
        tables=args.tables,
        batch_size=args.batch_size,
        drop_tables=args.drop_tables,
        truncate_tables=not args.no_truncate,
        disable_foreign_keys=args.disable_foreign_keys,
        max_retries=args.max_retries,
        where_clauses=where_clauses
    )
    
    etl.run()


if __name__ == "__main__":
    main() 