"""
Test module for ETL pipeline components.
"""
import os
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from src.config import TYPE_MAPPING
from src.transform import (
    transform_dataframe,
    validate_data_types,
    convert_sql_server_to_postgres_type,
    calculate_checksum
)

# Sample test data
SAMPLE_COLUMNS = [
    {"name": "id", "type": "int", "max_length": None, "is_nullable": "NO", "default": None},
    {"name": "name", "type": "varchar", "max_length": 50, "is_nullable": "YES", "default": None},
    {"name": "created_date", "type": "datetime", "max_length": None, "is_nullable": "YES", "default": None},
    {"name": "is_active", "type": "bit", "max_length": None, "is_nullable": "NO", "default": None},
    {"name": "amount", "type": "decimal", "max_length": None, "is_nullable": "YES", "default": None},
]

@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Test 1", "Test 2", None, "Test 4", "Test 5"],
        "created_date": ["2023-01-01", "2023-01-02", None, "2023-01-04", "2023-01-05"],
        "is_active": [1, 0, 1, 1, 0],
        "amount": [100.50, 200.75, None, 400.25, 500.00],
    }
    return pd.DataFrame(data)

class TestTransformation:
    """Test cases for transformation module."""
    
    def test_transform_dataframe(self, sample_dataframe):
        """Test transforming a DataFrame with various data types."""
        df_transformed = transform_dataframe(sample_dataframe, SAMPLE_COLUMNS)
        
        # Check data types
        assert pd.api.types.is_numeric_dtype(df_transformed["id"])
        assert pd.api.types.is_object_dtype(df_transformed["name"])
        assert pd.api.types.is_datetime64_dtype(df_transformed["created_date"])
        
        # Check NULL values are preserved
        assert df_transformed["name"].isna().sum() == 1
        assert df_transformed["created_date"].isna().sum() == 1
        assert df_transformed["amount"].isna().sum() == 1
        
        # Check boolean conversion
        assert set(df_transformed["is_active"].unique()) == {True, False}
    
    def test_validate_data_types(self, sample_dataframe):
        """Test validation of data types."""
        # Normal case - no validation issues
        issues = validate_data_types(sample_dataframe, SAMPLE_COLUMNS)
        assert not issues
        
        # Create a DataFrame with validation issues
        df_with_issues = sample_dataframe.copy()
        df_with_issues.loc[0, "id"] = "not_a_number"
        df_with_issues.loc[1, "created_date"] = "invalid_date"
        
        issues = validate_data_types(df_with_issues, SAMPLE_COLUMNS)
        assert "id" in issues
        assert "created_date" in issues
    
    def test_convert_sql_server_to_postgres_type(self):
        """Test conversion of SQL Server types to PostgreSQL types."""
        # Test basic type conversions
        assert convert_sql_server_to_postgres_type("int") == "integer"
        assert convert_sql_server_to_postgres_type("varchar") == "text"
        assert convert_sql_server_to_postgres_type("datetime") == "timestamp"
        assert convert_sql_server_to_postgres_type("bit") == "boolean"
        
        # Test with max_length
        assert convert_sql_server_to_postgres_type("varchar", 50) == "varchar(50)"
        assert convert_sql_server_to_postgres_type("nvarchar", 5000) == "varchar(5000)"
        assert convert_sql_server_to_postgres_type("varchar", 20000) == "text"
        
        # Test with types that have precision and scale
        assert convert_sql_server_to_postgres_type("decimal(18,2)") == "numeric"
    
    def test_calculate_checksum(self, sample_dataframe):
        """Test calculation of DataFrame checksum."""
        # Same data should give same checksum
        df1 = sample_dataframe.copy()
        df2 = sample_dataframe.copy()
        
        checksum1 = calculate_checksum(df1)
        checksum2 = calculate_checksum(df2)
        
        assert checksum1 == checksum2
        
        # Different data should give different checksum
        df3 = sample_dataframe.copy()
        df3.loc[0, "name"] = "Modified"
        
        checksum3 = calculate_checksum(df3)
        
        assert checksum1 != checksum3
        
        # Empty DataFrame should return special value
        empty_df = pd.DataFrame()
        empty_checksum = calculate_checksum(empty_df)
        
        assert empty_checksum == "empty_dataframe"

if __name__ == "__main__":
    pytest.main(["-xvs", __file__]) 