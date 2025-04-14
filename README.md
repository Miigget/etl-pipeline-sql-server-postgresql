# ETL Pipeline: SQL Server to PostgreSQL

A robust ETL (Extract, Transform, Load) pipeline for migrating data from Azure SQL Server to PostgreSQL, ensuring complete data transfer with proper validation and reporting.

## Features

- Complete extraction of data from SQL Server source tables
- Data type conversion from SQL Server to PostgreSQL
- Data validation and transformation
- Detailed logging and reporting
- Row count verification and data integrity checks
- Configurable batch processing
- Support for schema mapping

## Requirements

- Python 3.8+
- SQL Server with ODBC Driver (for source database)
- PostgreSQL (for target database)
- Required Python packages listed in `requirements.txt`

## Installation

1. Clone this repository:
   ```
   git clone <repository-url>
   cd etl-pipeline-azure-postgres
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Copy the template environment file and configure it:
   ```
   cp .env.template .env
   ```
   
   Edit the `.env` file with your database credentials and configuration settings.

## Usage

### Basic Usage

Run the ETL pipeline for all tables in a schema:

```bash
python -m src.main --source-schemas conf
```

### Advanced Options

Specify target schemas (if different from source):

```bash
python -m src.main --source-schemas conf data live --target-schemas conf_pg data_pg live_pg
```

Migrate specific tables only:

```bash
python -m src.main --source-schemas conf --tables users roles user_permissions
```

Configure batch size for large datasets:

```bash
python -m src.main --source-schemas data --batch-size 5000
```

Drop and recreate tables (caution: destroys existing data):

```bash
python -m src.main --source-schemas conf --drop-tables
```

Skip truncating tables before inserting:

```bash
python -m src.main --source-schemas conf --no-truncate
```

Disable foreign key constraints during migration (use with caution):

```bash
python -m src.main --source-schemas conf --disable-foreign-keys
```

Set maximum retries for tables with foreign key constraint failures:

```bash
python -m src.main --source-schemas conf --max-retries 5
```

Specify a custom report file location:

```bash
python -m src.main --source-schemas conf --report-file /path/to/migration_report.json
```

## Testing

Run the tests with pytest:

```bash
pytest
```

## Structure

- `src/` - Main source code
  - `__init__.py` - Package initialization
  - `config.py` - Configuration and settings
  - `extract.py` - Data extraction from SQL Server
  - `transform.py` - Data transformation and validation
  - `load.py` - Data loading into PostgreSQL
  - `main.py` - Main ETL orchestration
- `tests/` - Test suite
- `logs/` - Log files
- `.env.template` - Template for environment variables

## Migration Report

After the ETL process completes, a detailed migration report is generated in JSON format. This report includes:

- Summary statistics (duration, success rate, etc.)
- Detailed information for each migrated table
- Row counts and verification results
- Errors and warnings encountered during migration

## Error Handling

The ETL pipeline is designed to continue processing even if individual tables fail. All errors are logged and included in the final report. This approach ensures that a single table failure doesn't stop the entire migration process.

## Troubleshooting

### Common Connection Issues

#### SQL Server Connection Problems

- **Login timeout expired**: Check if the SQL Server is accessible from your network. Verify firewall rules allow the connection.
- **Login failed**: Verify the username and password in your `.env` file.
- **Server not found**: Double-check the server address in the `.env` file.

#### PostgreSQL Connection Problems

- **Connection refused**: Ensure PostgreSQL is running and accessible on the specified host and port.
- **Authentication failed**: Verify the username and password in your `.env` file.
- **Database does not exist**: Create the database before running the ETL pipeline.

### Data Type Issues

If you encounter data type conversion issues:
- Check the mapping in `src/config.py` and adjust if necessary
- Look for unsupported data types in your source database
- Consider adding custom transformations for complex data types

### Performance Tips

- Adjust the batch size in the `.env` file based on your available memory
- For very large tables, consider using table partitioning
- Make sure your target PostgreSQL instance has adequate resources

### Foreign Key Constraints Issues

If you encounter problems with foreign key constraints during the migration process:

- Use the `--disable-foreign-keys` option to temporarily disable all foreign key constraints during migration (fastest solution, but use with caution)
- Increase the number of retries with `--max-retries` option (helps with circular dependencies)
- Migrate tables in smaller groups, starting with tables that have no dependencies
- Consider temporarily dropping foreign key constraints in the target database and re-adding them after migration

## License

[MIT License](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 