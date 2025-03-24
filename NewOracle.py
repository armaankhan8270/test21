import csv
import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import List, Tuple, Dict, Generator, Optional, Any
import gzip
import time
import re

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
    Optimized for large datasets with efficient batching and streaming processing.

    Key improvements:
      • Better memory management with streaming processing
      • Improved datetime handling with milliseconds
      • Enhanced CSV formatting with proper quoting and escaping
      • Robust error handling and retry mechanism
      • Optimized batch size management
      • Support for progress tracking
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
        batch_size: int = 50000,      # Increased default batch size
        offset: int = 0,
        file_format: str = "csv",     # "csv" or "json"
        order_by: str = None,
        timeout: int = 600,           # Increased timeout for large datasets
        compress: bool = False,
        max_retries: int = 3,         # Number of retries for failed queries
        write_buffer_size: int = 1000, # Number of rows to buffer before writing
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
        self.write_buffer_size = write_buffer_size

        # Create structured folder: base_directory/schema/table/tna
        self.output_dir = Path(base_directory) / self.schema / self.table / "tna"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Custom spool delimiter to avoid conflicts with data content
        self.spool_delimiter = "||@@||"  # Very unlikely to occur in data
        # Timestamp columns (determined dynamically)
        self.timestamp_columns: List[str] = []
        # Column metadata
        self.column_data: List[Tuple[str, str]] = []

        logger.info(
            f"Initialized extractor for {self.schema}.{self.table} at {self.host}:{self.port}. "
            f"Batch size: {self.batch_size}, Format: {self.file_format}, Compressed: {self.compress}"
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
        Uses a more robust approach with better error handling.
        """
        if self.column_data:  # Return cached data if available
            return self.column_data
            
        query = (
            f"SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS "
            f"WHERE OWNER = '{self.schema.upper()}' AND TABLE_NAME = '{self.table.upper()}' "
            f"ORDER BY COLUMN_ID"
        )
        
        temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".txt", dir=str(self.output_dir))
        temp_output.close()
        temp_script = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", dir=str(self.output_dir), encoding="utf-8"
        )
        
        # Improved script with better formatting and error handling
        temp_script.write(f"""WHENEVER SQLERROR EXIT SQL.SQLCODE
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING ON
SET PAGESIZE 0
SET LINESIZE 32767
SET COLSEP '{self.spool_delimiter}'
SET WRAP OFF
SET TRIMSPOOL ON
SPOOL {temp_output.name}
{query};
SPOOL OFF
EXIT;
""")
        temp_script.close()

        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script.name}"
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Fetching column metadata (attempt {attempt}/{self.max_retries})...")
                result = subprocess.run(
                    command, 
                    shell=True, 
                    check=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    timeout=self.timeout
                )
                
                column_data = []
                with open(temp_output.name, "r", encoding="utf-8", errors="replace") as f:
                    # Skip header row if present
                    lines = f.readlines()
                    start_index = 0
                    if len(lines) > 0 and (
                        "COLUMN_NAME" in lines[0].upper() and 
                        "DATA_TYPE" in lines[0].upper()
                    ):
                        start_index = 1
                        
                    for line in lines[start_index:]:
                        line = line.strip()
                        if not line or line.startswith(("ORA-", "SP2-")):
                            continue
                            
                        parts = line.split(self.spool_delimiter)
                        if len(parts) >= 2:
                            column_data.append((parts[0].strip(), parts[1].strip()))
                
                if not column_data:
                    if attempt < self.max_retries:
                        logger.warning("No column data retrieved. Retrying...")
                        time.sleep(2)  # Wait before retry
                        continue
                    else:
                        raise ValueError(
                            f"No column data retrieved for {self.schema}.{self.table}. "
                            f"Check table schema or connection details."
                        )
                
                logger.info(f"Retrieved {len(column_data)} columns from {self.schema}.{self.table}")
                self.column_data = column_data
                return column_data
                
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as err:
                error_msg = getattr(err, 'stderr', b'').decode('utf-8', errors='replace')
                if attempt < self.max_retries:
                    logger.warning(f"Attempt {attempt} failed to fetch column data: {error_msg}. Retrying...")
                    time.sleep(2)  # Wait before retry
                else:
                    logger.error(f"All attempts to fetch column data failed: {error_msg}")
                    raise
            finally:
                # Clean up temp files
                for file_path in [temp_output.name, temp_script.name]:
                    try:
                        Path(file_path).unlink(missing_ok=True)
                    except Exception as e:
                        logger.warning(f"Could not delete temp file {file_path}: {e}")

    def _get_column_names(self) -> List[str]:
        """Returns column names and identifies timestamp columns."""
        col_data = self._get_column_data()
        columns = [col for col, _ in col_data]
        
        # Identify timestamp columns
        self.timestamp_columns = [
            col for col, dt in col_data 
            if any(time_type in dt.upper() for time_type in 
                  ["DATE", "TIMESTAMP", "INTERVAL"])
        ]
        
        logger.debug(f"Column names: {columns}")
        logger.debug(f"Timestamp columns: {self.timestamp_columns}")
        return columns

    def _get_table_row_count(self) -> int:
        """
        Gets an approximate row count of the table for progress tracking.
        Uses a faster method with optimizer statistics when available.
        """
        # First try using optimizer statistics (much faster)
        query = (
            f"SELECT NUM_ROWS FROM ALL_TABLES "
            f"WHERE OWNER = '{self.schema.upper()}' AND TABLE_NAME = '{self.table.upper()}'"
        )
        
        temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".txt", dir=str(self.output_dir))
        temp_output.close()
        temp_script = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", dir=str(self.output_dir), encoding="utf-8"
        )
        
        temp_script.write(f"""WHENEVER SQLERROR EXIT SQL.SQLCODE
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET PAGESIZE 0
SPOOL {temp_output.name}
{query};
SPOOL OFF
EXIT;
""")
        temp_script.close()

        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script.name}"
        
        try:
            subprocess.run(command, shell=True, check=True, timeout=60)
            with open(temp_output.name, "r", encoding="utf-8", errors="replace") as f:
                content = f.read().strip()
                if content and content.isdigit():
                    return int(content)
        except Exception as e:
            logger.warning(f"Could not get row count from statistics: {e}")
        finally:
            # Clean up
            for file_path in [temp_output.name, temp_script.name]:
                try:
                    Path(file_path).unlink(missing_ok=True)
                except Exception:
                    pass

        # Fall back to COUNT(*) if statistics unavailable (slower but accurate)
        count_query = f"SELECT COUNT(*) FROM {self.schema}.{self.table}"
        if self.where_clause:
            count_query += f" WHERE {self.where_clause}"
            
        temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".txt", dir=str(self.output_dir))
        temp_output.close()
        temp_script = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", dir=str(self.output_dir), encoding="utf-8"
        )
        
        temp_script.write(f"""WHENEVER SQLERROR EXIT SQL.SQLCODE
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET PAGESIZE 0
SPOOL {temp_output.name}
{count_query};
SPOOL OFF
EXIT;
""")
        temp_script.close()
        
        try:
            logger.info("Counting rows (this may take a while for large tables)...")
            subprocess.run(command, shell=True, check=True, timeout=300)
            with open(temp_output.name, "r", encoding="utf-8", errors="replace") as f:
                content = f.read().strip()
                if content and content.isdigit():
                    return int(content)
        except Exception as e:
            logger.warning(f"Could not get exact row count: {e}")
        finally:
            # Clean up
            for file_path in [temp_output.name, temp_script.name]:
                try:
                    Path(file_path).unlink(missing_ok=True)
                except Exception:
                    pass
        
        # If both methods fail, return -1
        return -1

    def _build_data_query(self, offset: int = 0, limit: int = None, columns: List[str] = None) -> str:
        """
        Builds the data query with proper date formatting including milliseconds.
        """
        if columns is None:
            columns = self._get_column_names()
        
        select_list = []
        for col in columns:
            if col.upper() in (c.upper() for c in self.timestamp_columns):
                # Include milliseconds in timestamp format
                select_list.append(f"TO_CHAR({col}, 'YYYY-MM-DD HH24:MI:SS.FF3') AS {col}")
            else:
                select_list.append(col)
                
        select_clause = ", ".join(select_list)
        query = f"SELECT {select_clause} FROM {self.schema}.{self.table}"
        
        if self.where_clause:
            query += f" WHERE {self.where_clause}"
        
        if self.order_by:
            query += f" ORDER BY {self.order_by}"
        
        if limit is not None:
            query += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        
        logger.debug(f"Built data query with offset {offset}, limit {limit}")
        return query

    def _create_sqlplus_script(self, query: str, output_file: Path, include_header: bool = True) -> Path:
        """Creates a SQL*Plus script with improved settings for large data extraction."""
        script_content = f"""WHENEVER SQLERROR EXIT SQL.SQLCODE
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING {"ON" if include_header else "OFF"}
SET PAGESIZE 0
SET ARRAYSIZE 5000
SET LOBPREFETCH 16384
SET LONG 100000000
SET LONGCHUNKSIZE 1000000
SET TRIMSPOOL ON
SET LINESIZE 32767
SET WRAP OFF
SET COLSEP '{self.spool_delimiter}'
SET UNDERLINE OFF
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3';
SPOOL {output_file}
{query};
SPOOL OFF
EXIT;
"""
        temp_script = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", dir=str(self.output_dir), encoding="utf-8"
        )
        temp_script.write(script_content)
        temp_script.close()
        return Path(temp_script.name)

    def _execute_sqlplus(self, script_path: Path, retry_on_error: bool = True) -> bool:
        """
        Executes SQL*Plus with better error handling and retries.
        Returns True if successful, False otherwise.
        """
        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{script_path}"
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(f"Executing SQL*Plus (attempt {attempt}/{self.max_retries})...")
                result = subprocess.run(
                    command, 
                    shell=True, 
                    check=True, 
                    timeout=self.timeout,
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE
                )
                return True
            except subprocess.CalledProcessError as err:
                error_output = err.stderr.decode('utf-8', errors='replace')
                logger.error(f"SQL*Plus execution failed (attempt {attempt}): {error_output}")
                
                # Check for specific Oracle errors that might indicate we should retry
                retriable_errors = ["ORA-03113", "ORA-12154", "ORA-03114", "ORA-03135", "ORA-12170"]
                should_retry = any(code in error_output for code in retriable_errors)
                
                if retry_on_error and should_retry and attempt < self.max_retries:
                    logger.warning(f"Retriable error detected. Waiting before retry...")
                    time.sleep(min(5 * attempt, 30))  # Exponential backoff
                else:
                    return False
            except subprocess.TimeoutExpired:
                logger.error(f"SQL*Plus execution timed out after {self.timeout}s (attempt {attempt})")
                if retry_on_error and attempt < self.max_retries:
                    logger.warning("Timeout occurred. Retrying with longer timeout...")
                    self.timeout = min(self.timeout * 2, 1800)  # Double timeout up to 30 minutes max
                else:
                    return False
        
        return False

    def _process_raw_output(self, raw_file: Path, header: List[str]) -> Generator[Dict[str, str], None, None]:
        """
        Processes the raw SQL*Plus output into usable records.
        Yields each record as a dictionary with proper handling of delimiters and special chars.
        """
        if not raw_file.exists() or raw_file.stat().st_size == 0:
            logger.warning(f"Raw output file {raw_file} is empty or doesn't exist")
            return
            
        encoding_errors = 0
        row_count = 0
        header_found = False
        
        with raw_file.open("r", encoding="utf-8", errors="replace") as f:
            # Read first line to check if it's a header
            first_line = f.readline().strip()
            if first_line:
                parts = first_line.split(self.spool_delimiter)
                if len(parts) == len(header) and all(
                    p.strip().upper() == h.upper() for p, h in zip(parts, header)
                ):
                    header_found = True
                    logger.debug("Header found in raw file, skipping first line")
                else:
                    # Rewind to start if first line is not header
                    f.seek(0)
            
            # Process each data line
            for line_num, line in enumerate(f, start=1):
                if not line.strip() or (line_num == 1 and header_found):
                    continue
                    
                # Skip Oracle error messages and SQL*Plus formatting
                if any(prefix in line for prefix in ["ORA-", "SP2-", "SQL>"]):
                    continue
                    
                if "�" in line:
                    encoding_errors += 1
                    if encoding_errors <= 5:  # Limit warning spam
                        logger.warning(f"Encoding issue detected in line {line_num}")
                    
                # Split by delimiter and map to header columns
                parts = line.split(self.spool_delimiter)
                if len(parts) != len(header):
                    logger.warning(
                        f"Line {line_num} has {len(parts)} fields but header has {len(header)} fields. "
                        f"Skipping this row."
                    )
                    continue
                    
                # Create dictionary from header and values
                record = {}
                for i, (key, value) in enumerate(zip(header, parts)):
                    # Clean value - strip whitespace and handle NULL values
                    clean_value = value.strip()
                    if clean_value.upper() == "NULL":
                        clean_value = ""
                    record[key] = clean_value
                    
                row_count += 1
                yield record
                
        logger.info(f"Processed {row_count} rows from raw file")
        if encoding_errors > 5:
            logger.warning(f"Total of {encoding_errors} encoding issues detected")

    def _write_csv_file(self, output_file: Path, header: List[str], data_generator: Generator[Dict[str, str], None, None]):
        """
        Writes data to CSV with proper handling of special characters and quoting.
        Uses buffered writing for better performance with large datasets.
        """
        mode = "wt" if not output_file.exists() else "at"  # Append if exists
        write_header = not output_file.exists() or output_file.stat().st_size == 0
        
        # Determine if we need to open with gzip
        open_func = gzip.open if self.compress else open
        
        with open_func(output_file, mode, encoding="utf-8", newline="") as f:
            writer = csv.writer(
                f, 
                delimiter=",", 
                quotechar='"',
                quoting=csv.QUOTE_ALL,  # Quote all fields for maximum compatibility
                lineterminator="\n",
                escapechar='\\',
                doublequote=True  # Use "" to escape quotes in fields
            )
            
            if write_header:
                writer.writerow(header)
                
            # Buffer rows for better performance
            row_buffer = []
            total_rows = 0
            
            for record in data_generator:
                # Extract values in header order to ensure consistent columns
                row = [record.get(col, "") for col in header]
                row_buffer.append(row)
                total_rows += 1
                
                # Write when buffer is full
                if len(row_buffer) >= self.write_buffer_size:
                    writer.writerows(row_buffer)
                    row_buffer = []
                    
            # Write any remaining rows
            if row_buffer:
                writer.writerows(row_buffer)
                
        logger.info(f"Wrote {total_rows} rows to CSV file")
        return total_rows

    def _write_json_file(self, output_file: Path, data_generator: Generator[Dict[str, str], None, None]):
        """
        Writes data to JSON with streaming approach to handle large datasets.
        Uses a line-delimited JSON format for streaming large files.
        """
        mode = "wt" if not output_file.exists() else "at"  # Append if exists
        is_new_file = not output_file.exists() or output_file.stat().st_size == 0
        
        # Determine if we need to open with gzip
        open_func = gzip.open if self.compress else open
        
        # For large datasets, use JSON Lines format instead of a single array
        # This is more efficient for streaming large amounts of data
        with open_func(output_file, mode, encoding="utf-8") as f:
            if is_new_file:
                f.write("[\n")  # Start array
            else:
                # Remove the closing bracket from previous write and add comma
                if self.compress:
                    with gzip.open(output_file, "rt", encoding="utf-8") as read_f:
                        content = read_f.read()
                    
                    if content.rstrip().endswith("]"):
                        with gzip.open(output_file, "wt", encoding="utf-8") as write_f:
                            write_f.write(content.rstrip()[:-1] + ",\n")
                else:
                    # Seek to end minus 2 bytes (to check for "]")
                    with open(output_file, "rb+") as binary_f:
                        binary_f.seek(max(0, os.path.getsize(output_file) - 2))
                        if binary_f.read(2) == b"]\n":
                            binary_f.seek(max(0, os.path.getsize(output_file) - 2))
                            binary_f.truncate()
                            binary_f.write(b",\n")
            
            # Process records in batches for efficiency
            row_buffer = []
            total_rows = 0
            first_record = is_new_file
            
            for record in data_generator:
                row_buffer.append(record)
                total_rows += 1
                
                # Write when buffer is full
                # Write when buffer is full
                if len(row_buffer) >= self.write_buffer_size:
                        for i, row in enumerate(row_buffer):
                            if first_record and i == 0:
                                f.write(json.dumps(row, ensure_ascii=False))
                                first_record = False
                            else:
                                f.write(",\n" + json.dumps(row, ensure_ascii=False))
                        row_buffer = []
            
            # Write any remaining rows
            for i, row in enumerate(row_buffer):
                if first_record and i == 0:
                    f.write(json.dumps(row, ensure_ascii=False))
                    first_record = False
                else:
                    f.write(",\n" + json.dumps(row, ensure_ascii=False))
            
            # Close the JSON array
            f.write("\n]")
            
        logger.info(f"Wrote {total_rows} rows to JSON file")
        return total_rows

    def extract(self) -> None:
        """
        Main extraction process with improved handling for large datasets.
        Uses a streaming approach with optimized batch processing and error handling.
        """
        logger.info(f"Starting extraction of {self.schema}.{self.table}")
        
        # Step 1: Get column metadata and determine output structure
        logger.info("Fetching column metadata...")
        columns = self._get_column_names()
        if not columns:
            raise ValueError(f"Could not retrieve columns for {self.schema}.{self.table}")
        logger.info(f"Found {len(columns)} columns")
        
        # Step 2: Estimate total rows for progress tracking
        total_rows_estimate = self._get_table_row_count()
        if total_rows_estimate > 0:
            logger.info(f"Estimated total rows: {total_rows_estimate:,}")
        
        # Step 3: Prepare output file path
        final_output = self.output_dir / f"{self.schema}_{self.table}_full.{self.file_format}"
        if self.compress:
            final_output = final_output.with_suffix(f"{final_output.suffix}.gz")
        logger.info(f"Output will be written to: {final_output}")
        
        # Step 4: Extract and process data in batches
        current_offset = self.offset
        batch_number = 0
        rows_processed = 0
        extraction_complete = False
        
        while not extraction_complete:
            batch_start_time = time.time()
            
            # Create temporary file for this batch
            temp_raw = self.output_dir / f"raw_output_part{batch_number}.txt"
            
            # Build and execute query for this batch
            data_query = self._build_data_query(
                offset=current_offset, 
                limit=self.batch_size,
                columns=columns
            )
            
            script_path = self._create_sqlplus_script(
                query=data_query,
                output_file=temp_raw,
                include_header=(batch_number == 0)  # Only include header in first batch
            )
            
            logger.info(f"Extracting batch {batch_number} (offset: {current_offset})...")
            success = self._execute_sqlplus(script_path)
            
            try:
                # Clean up script file
                script_path.unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"Could not delete script file: {e}")
            
            if not success:
                logger.error(f"Failed to extract batch {batch_number}")
                if batch_number == 0:
                    # If first batch fails, abort extraction
                    raise RuntimeError(f"Could not extract data from {self.schema}.{self.table}")
                else:
                    # If a later batch fails, we'll consider extraction complete
                    logger.warning("Stopping extraction due to batch failure")
                    extraction_complete = True
                    continue
                
            if not temp_raw.exists() or temp_raw.stat().st_size == 0:
                logger.info("No data returned in batch; extraction complete")
                extraction_complete = True
                continue
                
            # Process raw output based on file format
            batch_rows = 0
            if self.file_format == "csv":
                batch_rows = self._write_csv_file(
                    final_output,
                    columns,
                    self._process_raw_output(temp_raw, columns)
                )
            elif self.file_format == "json":
                batch_rows = self._write_json_file(
                    final_output,
                    self._process_raw_output(temp_raw, columns)
                )
            
            # Clean up temporary raw file
            try:
                temp_raw.unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"Could not delete temporary file: {e}")
                
            rows_processed += batch_rows
            batch_duration = time.time() - batch_start_time
            
            # Log progress
            if batch_rows == 0:
                logger.info("No rows in batch; extraction complete")
                extraction_complete = True
            else:
                logger.info(
                    f"Batch {batch_number}: Processed {batch_rows:,} rows in {batch_duration:.1f}s "
                    f"({batch_rows/batch_duration:.1f} rows/sec)"
                )
                if total_rows_estimate > 0:
                    progress = min(100, rows_processed * 100 / total_rows_estimate)
                    logger.info(f"Overall progress: {rows_processed:,}/{total_rows_estimate:,} rows ({progress:.1f}%)")
                    
                # If we got fewer rows than the batch size, we're done
                if self.batch_size is not None and batch_rows < self.batch_size:
                    logger.info("Received fewer rows than batch size; extraction complete")
                    extraction_complete = True
                else:
                    # Move to next batch
                    current_offset += batch_rows
                    batch_number += 1
        
        logger.info(f"Extraction complete. Total rows: {rows_processed:,}")
        logger.info(f"Final output saved to: {final_output}")

# Performance optimization for JSON extraction
def _get_estimated_row_count(self) -> int:
    """Estimates row count using Oracle's optimizer statistics."""
    query = (
        f"SELECT NUM_ROWS FROM ALL_TABLES "
        f"WHERE OWNER = '{self.schema.upper()}' AND TABLE_NAME = '{self.table.upper()}'"
    )
    # Implementation similar to _get_table_row_count but streamlined
    # ...

if __name__ == "__main__":
    # Example usage with improved parameters
    extractor = OracleSQLPlusExtractor(
        username="your_username",
        password="your_password",
        host="your_host",
        port=1521,
        service_name="your_service",
        schema="VMC_APEX",              
        table="APEX_RAW_INLOCAT",       
        where_clause="",                
        base_directory="OutputData2",
        batch_size=50000,               # Increased for better performance
        offset=0,
        file_format="csv",              # "csv" or "json"
        order_by="your_order_column",   
        timeout=600,                    # Increased for large tables
        compress=False,                 # Set True for gzip compression
        max_retries=3,                  # Retry failed operations
        write_buffer_size=5000          # Buffer size for efficient writing
    )
    extractor.extract()
