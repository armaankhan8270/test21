import csv
import json
import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any

import gzip
import re

# Configure logging with timestamps and more detailed logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

class OracleSnowflakeDataExtractor:
    """
    Advanced data extraction utility for Oracle databases with Snowflake compatibility.
    
    Key Features:
    - Robust handling of special characters and complex data types
    - Consistent date/timestamp formatting
    - CSV output with proper quoting
    - Batch processing for large datasets
    - Comprehensive error handling and logging
    """

    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        service_name: str,
        schema: str,
        table: str,
        base_directory: str = "OutputData",
        batch_size: int = 50000,
        file_format: str = "csv",
        where_clause: Optional[str] = None,
        order_by: Optional[str] = None,
        timeout: int = 3600,
        compress: bool = True,
        max_retries: int = 3,
        nls_date_format: str = "YYYY-MM-DD HH24:MI:SS.FF3",  # Microsecond precision
        additional_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the data extractor with comprehensive configuration options.
        
        :param username: Oracle database username
        :param password: Oracle database password
        :param host: Database host
        :param port: Database port
        :param service_name: Oracle service name
        :param schema: Database schema
        :param table: Table to extract
        :param base_directory: Output directory for extracted files
        :param batch_size: Number of rows to extract per batch
        :param file_format: Output file format (csv or json)
        :param where_clause: Optional SQL WHERE conditions
        :param order_by: Optional sorting column(s)
        :param timeout: Extraction timeout in seconds
        :param compress: Compress output files
        :param max_retries: Maximum retry attempts for extraction
        :param nls_date_format: NLS date format for consistent parsing
        :param additional_config: Additional configuration parameters
        """
        # Input validation
        if not all([username, password, host, service_name, schema, table]):
            raise ValueError("Missing required connection parameters")

        self.username = username
        self.password = self._sanitize_password(password)
        self.host = host
        self.port = port
        self.service_name = service_name
        self.schema = schema.strip().upper()
        self.table = table.strip().upper()
        
        # Configuration parameters
        self.base_directory = base_directory
        self.batch_size = batch_size
        self.file_format = file_format.lower()
        self.where_clause = where_clause or ""
        self.order_by = order_by
        self.timeout = timeout
        self.compress = compress
        self.max_retries = max_retries
        self.nls_date_format = nls_date_format
        self.additional_config = additional_config or {}

        # Validate file format
        if self.file_format not in {"csv", "json"}:
            raise ValueError("Supported output formats are 'csv' and 'json'")

        # Create output directory structure
        self.output_dir = Path(self.base_directory) / self.schema / self.table
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Metadata storage
        self.column_metadata: List[Dict[str, str]] = []
        self.timestamp_columns: List[str] = []

        # Logging and configuration
        logger.info(f"Initialized extractor for {self.schema}.{self.table}")
        logger.info(f"Configuration: batch_size={batch_size}, format={file_format}, compress={compress}")

    def _sanitize_password(self, password: str) -> str:
        """
        Sanitize password to handle special characters in shell commands.
        
        :param password: Raw password
        :return: Sanitized password
        """
        # Replace special characters that might break shell commands
        return re.sub(r'([&|;<>\(\)\[\]\{\}])', r'\\\1', password)

    def _build_dsn(self) -> str:
        """
        Construct Oracle Database Connection String (DSN).
        
        :return: Formatted DSN string
        """
        return (
            f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.host})(PORT={self.port}))"
            f"(CONNECT_DATA=(SERVICE_NAME={self.service_name})))"
        )

    def _get_column_metadata(self) -> List[Dict[str, str]]:
        """
        Retrieve comprehensive column metadata from Oracle system tables.
        
        :return: List of column metadata dictionaries
        """
        if self.column_metadata:
            return self.column_metadata

        query = f"""
        SELECT 
            column_name, 
            data_type, 
            data_length, 
            nullable,
            CASE 
                WHEN data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE')
                THEN 'true' 
                ELSE 'false' 
            END as is_timestamp
        FROM all_tab_columns
        WHERE owner = '{self.schema}' AND table_name = '{self.table}'
        ORDER BY column_id
        """

        # Temporary file for metadata extraction
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.sql') as temp_script, \
             tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.txt') as temp_output:
            
            # Write SQL script
            temp_script.write(f"""
            SET LINESIZE 32767
            SET PAGESIZE 0
            SET FEEDBACK OFF
            SET HEADING OFF
            SET COLSEP '|'
            SPOOL {temp_output.name}
            {query};
            SPOOL OFF
            EXIT
            """)
            temp_script.flush()

        try:
            # Execute SQL*Plus command
            dsn = self._build_dsn()
            command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script.name}"
            
            result = subprocess.run(
                command, 
                shell=True, 
                capture_output=True, 
                text=True, 
                timeout=self.timeout
            )

            # Process output
            metadata = []
            with open(temp_output.name, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith(('ORA-', 'SP2-')):
                        continue
                    
                    parts = line.split('|')
                    if len(parts) >= 5:
                        column_info = {
                            'name': parts[0].strip(),
                            'data_type': parts[1].strip(),
                            'data_length': parts[2].strip(),
                            'nullable': parts[3].strip() == 'Y',
                            'is_timestamp': parts[4].strip() == 'true'
                        }
                        metadata.append(column_info)

            # Store and return metadata
            self.column_metadata = metadata
            self.timestamp_columns = [
                col['name'] for col in metadata if col['is_timestamp']
            ]

            return metadata

        except Exception as e:
            logger.error(f"Error retrieving column metadata: {e}")
            raise
        finally:
            # Clean up temporary files
            os.unlink(temp_script.name)
            os.unlink(temp_output.name)

    def _build_extraction_query(self, offset: int) -> str:
        """
        Build a robust SQL query for data extraction with Snowflake compatibility.
        
        :param offset: Starting row for pagination
        :return: Fully formatted SQL query
        """
        metadata = self._get_column_metadata()
        
        # Construct column selection with type handling
        select_columns = []
        for col in metadata:
            if col['is_timestamp']:
                # Use TO_CHAR with microsecond precision for consistent timestamp handling
                select_columns.append(
                    f"TO_CHAR({col['name']}, '{self.nls_date_format}') AS {col['name']}"
                )
            else:
                select_columns.append(col['name'])

        # Base query construction
        base_query = f"""
        SELECT {', '.join(select_columns)}
        FROM {self.schema}.{self.table}
        """

        # Add optional WHERE and ORDER BY clauses
        if self.where_clause:
            base_query += f" WHERE {self.where_clause}"
        
        if self.order_by:
            base_query += f" ORDER BY {self.order_by}"

        # Pagination using OFFSET and FETCH
        paginated_query = f"""
        {base_query}
        OFFSET {offset} ROWS
        FETCH NEXT {self.batch_size} ROWS ONLY
        """

        return paginated_query

    def extract(self) -> int:
        """
        Execute comprehensive data extraction process.
        
        :return: Total number of rows extracted
        """
        logger.info("Starting data extraction process...")
        
        # Retrieve column names for output
        metadata = self._get_column_metadata()
        columns = [col['name'] for col in metadata]
        
        total_rows_extracted = 0
        current_offset = 0
        batch_number = 0

        while True:
            # Build extraction query
            query = self._build_extraction_query(current_offset)
            
            # Prepare output file
            output_filename = f"batch_{batch_number}.{self.file_format}"
            if self.compress:
                output_filename += ".gz"
            
            output_filepath = self.output_dir / output_filename
            
            # Open file based on format and compression
            if self.file_format == "csv":
                if self.compress:
                    out_file = gzip.open(output_filepath, 'wt', encoding='utf-8', newline='')
                else:
                    out_file = open(output_filepath, 'w', encoding='utf-8', newline='')
                
                csv_writer = csv.writer(
                    out_file, 
                    delimiter=',', 
                    quotechar='"', 
                    quoting=csv.QUOTE_ALL,
                    lineterminator='\n'
                )
                csv_writer.writerow(columns)  # Write header
            else:  # JSON
                batch_data = []

            # Execute SQL extraction
            try:
                with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.txt') as temp_output:
                    # Use SQL*Plus to extract data
                    dsn = self._build_dsn()
                    sql_script = f"""
                    SET LINESIZE 32767
                    SET PAGESIZE 0
                    SET FEEDBACK OFF
                    SET HEADING OFF
                    SET COLSEP '|'
                    SPOOL {temp_output.name}
                    {query};
                    SPOOL OFF
                    EXIT
                    """
                    
                    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as temp_script:
                        temp_script.write(sql_script)
                        temp_script.flush()

                    command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script.name}"
                    
                    subprocess.run(
                        command, 
                        shell=True, 
                        check=True, 
                        timeout=self.timeout
                    )

                    # Process extracted data
                    with open(temp_output.name, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if not line or line.startswith(('ORA-', 'SP2-')):
                                continue
                            
                            values = line.split('|')
                            if len(values) == len(columns):
                                if self.file_format == "csv":
                                    csv_writer.writerow(values)
                                else:
                                    batch_data.append(dict(zip(columns, values)))
                                total_rows_extracted += 1

                    # Close and handle CSV/JSON files
                    if self.file_format == "csv":
                        out_file.close()
                    else:
                        with (gzip.open(output_filepath, 'wt', encoding='utf-8') if self.compress 
                              else open(output_filepath, 'w', encoding='utf-8')) as out_file:
                            json.dump(batch_data, out_file, indent=2)

                    # Clean up temporary files
                    os.unlink(temp_output.name)
                    os.unlink(temp_script.name)

            except subprocess.CalledProcessError as e:
                logger.error(f"Data extraction failed: {e}")
                break
            except Exception as e:
                logger.error(f"Unexpected error during extraction: {e}")
                break

            # Check if we've extracted all data
            if not batch_data and self.file_format == "json":
                break
            elif self.file_format == "csv" and total_rows_extracted == 0:
                break

            current_offset += self.batch_size
            batch_number += 1

            logger.info(f"Batch {batch_number}: Extracted {len(batch_data or []) if self.file_format == 'json' else total_rows_extracted} rows")

        logger.info(f"Data extraction complete. Total rows extracted: {total_rows_extracted}")
        return total_rows_extracted

# Example Usage
if __name__ == "__main__":
    # Note: Replace with your actual secret management method
    extractor = OracleSnowflakeDataExtractor(
        username="your_username",
        password="your_password",
        host="your_host",
        port=1521,
        service_name="your_service",
        schema="YOUR_SCHEMA",
        table="YOUR_TABLE",
        batch_size=50000,
        file_format="csv",
        compress=True,
        where_clause="",  # Optional filtering
        order_by="",      # Optional sorting
    )

    # Execute extraction
    extractor.extract()
