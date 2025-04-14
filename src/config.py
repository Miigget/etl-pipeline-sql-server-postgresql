"""
Configuration module for database connections and settings.
"""
import os
import logging
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/etl.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# SQL Server (source) configuration
SOURCE_CONFIG = {
    "driver": os.getenv("SOURCE_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    "server": os.getenv("SOURCE_SERVER", ""),
    "database": os.getenv("SOURCE_DATABASE", ""),
    "uid": os.getenv("SOURCE_UID", ""),
    "pwd": os.getenv("SOURCE_PWD", ""),
    "port": os.getenv("SOURCE_PORT", "1433"),
    "trusted_connection": os.getenv("SOURCE_TRUSTED_CONNECTION", "no"),
}

# PostgreSQL (target) configuration
TARGET_CONFIG = {
    "host": os.getenv("TARGET_HOST", ""),
    "database": os.getenv("TARGET_DATABASE", ""),
    "user": os.getenv("TARGET_USER", ""),
    "password": os.getenv("TARGET_PASSWORD", ""),
    "port": os.getenv("TARGET_PORT", "5432"),
}

# Mapping of SQL Server data types to PostgreSQL data types
TYPE_MAPPING = {
    "nvarchar": "text",
    "varchar": "text",
    "char": "text",
    "nchar": "text",
    "datetime": "timestamp",
    "datetime2": "timestamp",
    "date": "date",
    "time": "time",
    "int": "integer",
    "bigint": "bigint",
    "smallint": "smallint",
    "tinyint": "smallint",
    "bit": "boolean",
    "decimal": "numeric",
    "numeric": "numeric",
    "float": "double precision",
    "real": "real",
    "money": "numeric",
    "smallmoney": "numeric",
    "binary": "bytea",
    "varbinary": "bytea",
    "image": "bytea",
    "uniqueidentifier": "uuid",
    "xml": "xml",
    "text": "text",
    "ntext": "text",
}

def get_source_connection_string() -> str:
    """
    Generate the connection string for SQL Server.
    
    Returns:
        str: Connection string for SQL Server
    """
    if not SOURCE_CONFIG["server"]:
        logger.error("Source database server not configured")
        raise ValueError("Source database server not configured")
    
    conn_str = (
        f"DRIVER={SOURCE_CONFIG['driver']};"
        f"SERVER={SOURCE_CONFIG['server']};"
        f"DATABASE={SOURCE_CONFIG['database']};"
        f"UID={SOURCE_CONFIG['uid']};"
        f"PWD={SOURCE_CONFIG['pwd']};"
        f"PORT={SOURCE_CONFIG['port']};"
        f"Trusted_Connection={SOURCE_CONFIG['trusted_connection']};"
    )
    
    return conn_str

def get_target_connection_params() -> Dict[str, Any]:
    """
    Get the connection parameters for PostgreSQL.
    
    Returns:
        Dict[str, Any]: Connection parameters for PostgreSQL
    """
    if not TARGET_CONFIG["host"]:
        logger.error("Target database host not configured")
        raise ValueError("Target database host not configured")
    
    return {
        "host": TARGET_CONFIG["host"],
        "database": TARGET_CONFIG["database"],
        "user": TARGET_CONFIG["user"],
        "password": TARGET_CONFIG["password"],
        "port": TARGET_CONFIG["port"],
    } 