import csv
import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
import gzip
import time
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class OracleSQLPlusExtractor:
    """
    Extracts data from Oracle using native SQL*Plus and writes the results to CSV or JSON.

    Key features:
      • Optimized for large tables with millions of rows
      • Uses array fetching and efficient batch processing
      • Handles DATE/TIMESTAMP columns with millisecond precision
      • Properly handles CSV field quoting and special characters
      • Supports compression for large outputs
      • Includes comprehensive error handling and recovery
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
        where_clause: str = "",
        base_directory: str = "OutputData",
        batch_size: int = 100000,     # Increased default batch size
        offset: int = 0,
        file_format: str = "csv",     # "csv" or "json"
        order_by: str = None,
        timeout: int = 1800,          # Increased timeout (30 minutes)
        compress: bool = False,
        max_retries: int = 3,
        array_size: int = 5000,       # Added array fetch size parameter
    ):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.schema = schema.strip()
        self.table = table.strip()
        self.where_clause = where_clause
        self.batch_size = batch_size
        self.offset = offset
        self.file_format = file_format.lower()
        if self.file_format not in {"csv", "json"}:
            raise ValueError("Supported output formats are 'csv' and 'json'.")
        self.order_by = order_by
        self.timeout = timeout
        self.compress = compress
        self.max_retries = max_retries
        self.array_size = array_size

        # Create structured folder: base_directory/schema/table/tna
        self.output_dir = Path(base_directory) / self.schema / self.table / "tna"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Custom spool delimiter that is unlikely to appear in the data
        self.spool_delimiter = "||~||"
        # List of timestamp columns (set dynamically below)
        self.timestamp_columns: List[str] = []
        # Store column metadata for reference
        self.column_metadata: List[Tuple[str, str]] = []

        logger.info(
            f"Initialized extractor for {self.schema}.{self.table} at {self.host}:{self.port}. "
            f"Batch size: {self.batch_size}, Format: {self.file_format}, Compressed: {self.compress}, "
            f"Array size: {self.array_size}"
        )

    def _build_dsn(self) -> str:
        """Constructs the Oracle DSN string."""
        return (
            f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.host})(PORT={self.port}))"
            f"(CONNECT_DATA=(SERVICE_NAME={self.service_name})))"
        )

    def _get_column_data(self) -> List[Tuple[str, str]]:
        """
        Retrieves column names and their data types from ALL_TAB_COLUMNS.
        Returns a list of tuples: (COLUMN_NAME, DATA_TYPE).
        Uses a custom delimiter to separate column name and data type.
        """
        query = (
            f"SELECT COLUMN_NAME || '{self.spool_delimiter}' || DATA_TYPE FROM ALL_TAB_COLUMNS "
            f"WHERE OWNER = '{self.schema.upper()}' AND TABLE_NAME = '{self.table.upper()}' "
            f"ORDER BY COLUMN_ID"
        )
        
        logger.info(f"Fetching column metadata for {self.schema}.{self.table}")
        temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".txt", dir=str(self.output_dir))
        temp_output.close()
        temp_script = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", dir=str(self.output_dir), encoding="utf-8"
        )
        temp_script.write(f"""SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET LINESIZE 32767
SPOOL {temp_output.name}
{query};
SPOOL OFF
EXIT;
""")
        temp_script.close()

        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script.name}"
        
        for retry in range(self.max_retries):
            try:
                logger.debug(f"Executing command to fetch column metadata (attempt {retry+1}/{self.max_retries})")
                subprocess.run(command, shell=True, check=True, 
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               timeout=self.timeout)
                
                with open(temp_output.name, "r", encoding="utf-8", errors="replace") as f:
                    data = [line.strip() for line in f if line.strip()]
                
                column_data: List[Tuple[str, str]] = []
                for entry in data:
                    if self.spool_delimiter in entry:
                        col, dt = entry.split(self.spool_delimiter, 1)
                        column_data.append((col.strip(), dt.strip()))
                    else:
                        column_data.append((entry.strip(), ""))
                
                if not column_data:
                    raise ValueError(f"No column data retrieved for {self.schema}.{self.table}. Check table schema or connection.")
                
                logger.info(f"Retrieved metadata for {len(column_data)} columns")
                self.column_metadata = column_data
                return column_data
                
            except subprocess.CalledProcessError as err:
                stderr = err.stderr.decode('utf-8') if err.stderr else "Unknown error"
                logger.error(f"Failed to fetch column data (attempt {retry+1}/{self.max_retries}): {stderr}")
                if retry == self.max_retries - 1:
                    raise ValueError(f"Failed to connect to database after {self.max_retries} attempts: {stderr}")
                time.sleep(5)  # Wait before retrying
                
            except Exception as e:
                logger.error(f"Unexpected error fetching column data: {str(e)}")
                if retry == self.max_retries - 1:
                    raise
                time.sleep(5)  # Wait before retrying
                
            finally:
                Path(temp_output.name).unlink(missing_ok=True)
                Path(temp_script.name).unlink(missing_ok=True)
                
        # This should never be reached due to the exception in the loop, but just in case
        raise ValueError(f"Failed to retrieve column data for {self.schema}.{self.table}")

    def _get_column_names(self) -> List[str]:
        """
        Returns a list of column names and sets self.timestamp_columns for DATE/TIMESTAMP columns.
        """
        col_data = self._get_column_data()
        columns = [col for col, _ in col_data]
        self.timestamp_columns = [
            col for col, dt in col_data 
            if "DATE" in dt.upper() or "TIMESTAMP" in dt.upper()
        ]
        logger.debug(f"Determined timestamp columns: {self.timestamp_columns}")
        return columns

    def _build_data_query(self, offset: int = None, columns: List[str] = None) -> str:
        """
        Dynamically builds the data query with proper formatting for timestamp columns.
        Includes millisecond precision for timestamp columns.
        """
        if columns is None:
            columns = self._get_column_names()
            
        select_list = []
        for col in columns:
            if col.upper() in (c.upper() for c in self.timestamp_columns):
                # Enhanced format with millisecond precision
                select_list.append(f"TO_CHAR({col}, 'YYYY-MM-DD HH24:MI:SS.FF3') AS {col}")
            else:
                select_list.append(col)
                
        select_clause = ", ".join(select_list)
        query = f"SELECT {select_clause} FROM {self.schema}.{self.table}"
        
        if self.where_clause:
            query += f" WHERE {self.where_clause}"
        if self.order_by:
            query += f" ORDER BY {self.order_by}"
        if self.batch_size is not None and offset is not None:
            query += f" OFFSET {offset} ROWS FETCH NEXT {self.batch_size} ROWS ONLY"
            
        logger.debug(f"Built data query with offset {offset}, batch size {self.batch_size}")
        return query

    def _create_sqlplus_script(self, query: str, output_file: Path) -> Path:
        """
        Creates an optimized SQL*Plus script with array fetching and proper formatting.
        """
        sql_script = f"""WHENEVER SQLERROR EXIT SQL.SQLCODE
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING ON
SET PAGESIZE 0
SET ARRAYSIZE {self.array_size}
SET LINESIZE 32767
SET WRAP OFF
SET COLSEP '{self.spool_delimiter}'
SET COLWIDTH _ALL_ 100000
SET LONG 100000
SET LONGCHUNKSIZE 100000
SET TRIMSPOOL ON
SET TIMING ON
-- Use custom column separator and ensure proper quoting
SET MARKUP CSV ON DELIMITER '{self.spool_delimiter}' QUOTE ON
SPOOL {output_file}
{query};
SPOOL OFF
EXIT;
"""
        temp_script = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", dir=str(self.output_dir), encoding="utf-8"
        )
        temp_script.write(sql_script)
        temp_script.close()
        logger.debug(f"Created SQL*Plus script at {temp_script.name}")
        return Path(temp_script.name)

    def _execute_sqlplus(self, query: str, output_file: Path, include_header: bool = False) -> int:
        """
        Executes SQL*Plus with the given query and returns the number of rows processed.
        Optimized with array fetching and better error handling.
        """
        temp_script = self._create_sqlplus_script(query, output_file)
        
        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script.name}"
        
        start_time = time.time()
        logger.info(f"Executing SQL*Plus command for batch extraction...")
        
        for retry in range(self.max_retries):
            try:
                result = subprocess.run(
                    command, 
                    shell=True, 
                    check=True, 
                    timeout=self.timeout,
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE
                )
                
                duration = time.time() - start_time
                logger.info(f"SQL*Plus execution completed in {duration:.2f} seconds")
                
                # Check if output file exists and has content
                if not output_file.exists():
                    logger.warning(f"Output file {output_file} was not created")
                    return 0
                    
                file_size = output_file.stat().st_size
                logger.info(f"Generated output file size: {file_size/1024/1024:.2f} MB")
                
                # Count actual rows in the file (quick estimation)
                row_count = 0
                with output_file.open("r", encoding="utf-8", errors="replace") as f:
                    # Skip header if it exists
                    if include_header:
                        next(f, None)
                    
                    # Sample first 1000 lines to get average line length
                    lines = []
                    for _ in range(1000):
                        try:
                            line = next(f)
                            lines.append(line)
                        except StopIteration:
                            break
                            
                    # If we have data, estimate total rows based on file size and average line length
                    if lines:
                        avg_line_length = sum(len(line) for line in lines) / len(lines)
                        estimated_rows = int(file_size / avg_line_length)
                        
                        # Get actual count for smaller files, estimate for larger ones
                        if file_size < 100 * 1024 * 1024:  # Less than 100MB
                            # Count actual lines
                            f.seek(0)
                            if include_header:
                                next(f, None)
                            row_count = sum(1 for _ in f)
                        else:
                            row_count = estimated_rows
                            logger.info(f"Large file detected, row count is estimated")
                
                return row_count
                
            except subprocess.CalledProcessError as err:
                stderr = err.stderr.decode('utf-8') if err.stderr else "Unknown error"
                logger.error(f"SQL*Plus execution failed (attempt {retry+1}/{self.max_retries}): {stderr}")
                if retry == self.max_retries - 1:
                    raise ValueError(f"SQL*Plus execution failed after {self.max_retries} attempts: {stderr}")
                time.sleep(10)  # Longer wait for query failures
                
            except subprocess.TimeoutExpired:
                logger.error(f"SQL*Plus execution timed out after {self.timeout} seconds (attempt {retry+1}/{self.max_retries})")
                if retry == self.max_retries - 1:
                    raise ValueError(f"SQL*Plus execution timed out after {self.max_retries} attempts")
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"Unexpected error during SQL*Plus execution: {str(e)}")
                if retry == self.max_retries - 1:
                    raise
                time.sleep(10)
                
            finally:
                Path(temp_script.name).unlink(missing_ok=True)
                
        # This should never be reached due to the exception in the loop, but just in case
        return 0

    def _process_csv_batch(self, batch_file: Path, header: List[str], output_writer) -> int:
        """
        Processes a CSV batch file and writes rows to the output writer.
        Returns the number of rows processed.
        """
        if not batch_file.exists() or batch_file.stat().st_size == 0:
            logger.warning(f"Batch file {batch_file} is empty or does not exist")
            return 0
            
        row_count = 0
        try:
            # Custom CSV reader with our delimiter
            with batch_file.open("r", encoding="utf-8", errors="replace") as f_in:
                # Skip header in batch files after the first one
                next(f_in, None)
                
                # Use a custom reader to handle our delimiter and special cases
                for line in f_in:
                    line = line.strip()
                    if not line or line.startswith(("ORA-", "SQL>", "SP2-", "----")):
                        continue
                        
                    # Parse the row using our delimiter, preserving quotes
                    fields = []
                    field_start = 0
                    in_quotes = False
                    i = 0
                    
                    while i < len(line):
                        if line[i] == '"':
                            in_quotes = not in_quotes
                        elif not in_quotes and i <= len(line) - len(self.spool_delimiter) and line[i:i+len(self.spool_delimiter)] == self.spool_delimiter:
                            fields.append(line[field_start:i].strip())
                            field_start = i + len(self.spool_delimiter)
                            i += len(self.spool_delimiter) - 1
                        i += 1
                    
                    # Add the last field
                    fields.append(line[field_start:].strip())
                    
                    # Clean up any remaining quotes
                    clean_row = []
                    for field in fields:
                        if field.startswith('"') and field.endswith('"'):
                            field = field[1:-1]
                        clean_row.append(field)
                    
                    # Make sure we have the right number of fields
                    while len(clean_row) < len(header):
                        clean_row.append("")
                    
                    if clean_row and any(clean_row):
                        output_writer.writerow(clean_row)
                        row_count += 1
        
        except Exception as e:
            logger.error(f"Error processing batch file {batch_file}: {str(e)}")
            raise
            
        return row_count

    def _create_batch_query(self, batch_num: int, header: List[str]) -> Tuple[Path, str]:
        """
        Creates the query for a batch and returns the output file path and query.
        """
        offset = self.offset + (batch_num * self.batch_size)
        query = self._build_data_query(offset=offset, columns=header)
        
        batch_file = self.output_dir / f"batch_{batch_num}.txt"
        return batch_file, query

    def extract(self) -> Path:
        """
        Main extraction process optimized for large datasets.
        Uses batched processing with array fetching for efficiency.
        Returns the path to the final output file.
        """
        logger.info(f"Starting extraction process for {self.schema}.{self.table}")
        
        # Get column metadata
        col_data = self._get_column_data()
        header = [col for col, _ in col_data]
        self.timestamp_columns = [
            col for col, dt in col_data 
            if "DATE" in dt.upper() or "TIMESTAMP" in dt.upper()
        ]
        
        logger.info(f"Retrieved {len(header)} columns, {len(self.timestamp_columns)} timestamp columns")
        
        # Define output file path
        final_output = self.output_dir / f"{self.schema}_{self.table}_full.{self.file_format}"
        if self.compress:
            final_output = final_output.with_suffix(final_output.suffix + ".gz")
        
        # Create a temporary directory for batch files
        batch_dir = self.output_dir / "batch_files"
        batch_dir.mkdir(exist_ok=True)
        
        total_rows = 0
        batch_number = 0
        
        try:
            if self.file_format == "csv":
                # Open the output file for writing
                output_mode = "wt"
                output_fp = gzip.open(final_output, output_mode, encoding="utf-8", newline="\n") if self.compress \
                         else final_output.open(output_mode, encoding="utf-8", newline="\n")
                
                with output_fp as f_out:
                    writer = csv.writer(f_out, delimiter=",", quoting=csv.QUOTE_ALL, lineterminator="\n")
                    # Write header row
                    writer.writerow(header)
                    
                    # Process batches
                    while True:
                        batch_file, query = self._create_batch_query(batch_number, header)
                        
                        # Execute SQLPlus for this batch
                        logger.info(f"Processing batch {batch_number}")
                        rows_in_batch = self._execute_sqlplus(query, batch_file, include_header=True)
                        
                        if rows_in_batch == 0:
                            logger.info(f"No more data in batch {batch_number}")
                            break
                            
                        # Process the batch file
                        processed_rows = self._process_csv_batch(batch_file, header, writer)
                        logger.info(f"Batch {batch_number}: Processed {processed_rows} rows")
                        
                        total_rows += processed_rows
                        
                        # Clean up batch file to save space
                        try:
                            batch_file.unlink()
                        except Exception as e:
                            logger.warning(f"Could not delete batch file {batch_file}: {e}")
                            
                        # If we didn't get a full batch, we're done
                        if processed_rows < self.batch_size:
                            logger.info(f"Batch {batch_number} not full ({processed_rows} < {self.batch_size}), extraction complete")
                            break
                            
                        # Move to next batch
                        batch_number += 1
                
            elif self.file_format == "json":
                # For JSON, we'll collect all data first
                json_rows = []
                
                # Process batches
                while True:
                    batch_file, query = self._create_batch_query(batch_number, header)
                    
                    # Execute SQLPlus for this batch
                    logger.info(f"Processing batch {batch_number}")
                    rows_in_batch = self._execute_sqlplus(query, batch_file, include_header=True)
                    
                    if rows_in_batch == 0:
                        logger.info(f"No more data in batch {batch_number}")
                        break
                        
                    # Process the batch file and convert to JSON records
                    batch_records = []
                    try:
                        with batch_file.open("r", encoding="utf-8", errors="replace") as f_in:
                            # Skip header
                            next(f_in, None)
                            
                            for line in f_in:
                                line = line.strip()
                                if not line or line.startswith(("ORA-", "SQL>", "SP2-", "----")):
                                    continue
                                    
                                # Split by delimiter and clean fields
                                fields = []
                                field_start = 0
                                in_quotes = False
                                i = 0
                                
                                while i < len(line):
                                    if line[i] == '"':
                                        in_quotes = not in_quotes
                                    elif not in_quotes and i <= len(line) - len(self.spool_delimiter) and line[i:i+len(self.spool_delimiter)] == self.spool_delimiter:
                                        fields.append(line[field_start:i].strip())
                                        field_start = i + len(self.spool_delimiter)
                                        i += len(self.spool_delimiter) - 1
                                    i += 1
                                
                                # Add the last field
                                fields.append(line[field_start:].strip())
                                
                                # Clean up fields and create record
                                clean_row = []
                                for field in fields:
                                    if field.startswith('"') and field.endswith('"'):
                                        field = field[1:-1]
                                    clean_row.append(field)
                                
                                # Make sure we have the right number of fields
                                while len(clean_row) < len(header):
                                    clean_row.append("")
                                
                                if clean_row and any(clean_row):
                                    record = dict(zip(header, clean_row))
                                    batch_records.append(record)
                                    
                    except Exception as e:
                        logger.error(f"Error processing JSON batch file {batch_file}: {str(e)}")
                        raise
                        
                    # Add batch records to main list
                    json_rows.extend(batch_records)
                    logger.info(f"Batch {batch_number}: Added {len(batch_records)} JSON records")
                    total_rows += len(batch_records)
                    
                    # Clean up batch file
                    try:
                        batch_file.unlink()
                    except Exception as e:
                        logger.warning(f"Could not delete batch file {batch_file}: {e}")
                        
                    # If we didn't get a full batch, we're done
                    if len(batch_records) < self.batch_size:
                        logger.info(f"Batch {batch_number} not full ({len(batch_records)} < {self.batch_size}), extraction complete")
                        break
                        
                    # Move to next batch
                    batch_number += 1
                
                                    # Write all JSON records to output file
                    output_mode = "wt"
                    output_fp = gzip.open(final_output, output_mode, encoding="utf-8", newline="") if self.compress \
                             else final_output.open(output_mode, encoding="utf-8", newline="")
                    
                    with output_fp as f_out:
                        json.dump(json_rows, f_out, indent=2)
                        logger.info(f"Written {total_rows} JSON records to {final_output}")
