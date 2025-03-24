import csv
import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional, Iterator
import gzip
import time
import sys
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

    Key features:
      • Uses buffer-based processing to handle millions of rows efficiently
      • Retrieves exact column names and data types from Oracle's ALL_TAB_COLUMNS
      • Automatically detects DATE/TIMESTAMP columns and formats them with milliseconds
      • Uses SQL*Plus's SET MARKUP CSV with custom delimiter to prevent CSV field problems
      • Implements chunked file processing with proper resource cleanup
      • Recovers from common errors during extraction
      • Optimized for large table extractions
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
        batch_size: int = 100000,  # Increased default batch size
        offset: int = 0,
        file_format: str = "csv",  # "csv" or "json"
        order_by: str = None,
        timeout: int = 600,        # Increased timeout
        compress: bool = False,
        buffer_size: int = 8192,   # Buffer size for file operations
        max_retries: int = 3,      # Number of retry attempts
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
        self.buffer_size = buffer_size
        self.max_retries = max_retries

        # Create structured folder: base_directory/schema/table/tna
        self.output_dir = Path(base_directory) / self.schema / self.table / "tna"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Custom spool delimiter to avoid field-splitting by embedded commas
        self.spool_delimiter = "|"
        # List of timestamp columns (set dynamically below)
        self.timestamp_columns: List[str] = []
        # List of column data types
        self.column_types: Dict[str, str] = {}

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
        Uses a custom concatenation with a pipe delimiter.
        """
        logger.info(f"Retrieving column metadata for {self.schema}.{self.table}")
        query = (
            f"SELECT COLUMN_NAME || '|' || DATA_TYPE FROM ALL_TAB_COLUMNS "
            f"WHERE OWNER = '{self.schema.upper()}' AND TABLE_NAME = '{self.table.upper()}' "
            f"ORDER BY COLUMN_ID"
        )
        
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
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(f"Executing SQL*Plus to fetch column data (attempt {attempt}/{self.max_retries})")
                result = subprocess.run(
                    command, 
                    shell=True, 
                    check=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    timeout=30  # Shorter timeout just for metadata
                )
                
                with open(temp_output.name, "r", encoding="utf-8", errors="replace") as f:
                    data = [line.strip() for line in f if line.strip()]
                
                column_data: List[Tuple[str, str]] = []
                for entry in data:
                    if "|" in entry:
                        col, dt = entry.split("|", 1)
                        col = col.strip()
                        dt = dt.strip()
                        column_data.append((col, dt))
                        # Update column types dictionary
                        self.column_types[col] = dt
                    else:
                        column_data.append((entry.strip(), ""))
                
                if not column_data:
                    raise ValueError(f"No column data retrieved for {self.schema}.{self.table}. Check table exists and permissions.")
                
                logger.info(f"Retrieved {len(column_data)} columns from {self.schema}.{self.table}")
                return column_data
                
            except subprocess.CalledProcessError as err:
                stderr = err.stderr.decode('utf-8') if err.stderr else "Unknown error"
                logger.error(f"Failed to fetch column data (attempt {attempt}): {stderr}")
                if attempt == self.max_retries:
                    raise ValueError(f"Failed to fetch column data after {self.max_retries} attempts: {stderr}")
                time.sleep(2)  # Wait before retrying
                
            except Exception as e:
                logger.error(f"Error retrieving column data: {str(e)}")
                if attempt == self.max_retries:
                    raise
                time.sleep(2)  # Wait before retrying
                
            finally:
                Path(temp_output.name).unlink(missing_ok=True)
                Path(temp_script.name).unlink(missing_ok=True)

    def _get_column_names(self) -> List[str]:
        """
        Returns a list of column names and sets self.timestamp_columns for DATE/TIMESTAMP columns.
        """
        col_data = self._get_column_data()  # List of tuples (col, type)
        columns = [col for col, _ in col_data]
        self.timestamp_columns = [
            col for col, dt in col_data 
            if any(ts_type in dt.upper() for ts_type in ["DATE", "TIMESTAMP"])
        ]
        logger.debug(f"Determined timestamp columns: {self.timestamp_columns}")
        return columns

    def _build_data_query(self, offset: int = None, columns: List[str] = None) -> str:
        """
        Dynamically builds the data query.
        Each column in self.timestamp_columns is wrapped with TO_CHAR
        including milliseconds format for timestamps.
        """
        if columns is None:
            columns = self._get_column_names()
            
        select_list = []
        for col in columns:
            if col.upper() in (c.upper() for c in self.timestamp_columns):
                # Check if this is specifically a TIMESTAMP type (with potential fractional seconds)
                if self.column_types.get(col, "").upper().startswith("TIMESTAMP"):
                    select_list.append(f"TO_CHAR({col}, 'YYYY-MM-DD HH24:MI:SS.FF3') AS {col}")
                else:
                    select_list.append(f"TO_CHAR({col}, 'YYYY-MM-DD HH24:MI:SS') AS {col}")
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
            
        return query

    def _execute_sqlplus(self, query: str, output_file: Path) -> bool:
        """
        Executes SQL*Plus with the given query and spools data to output_file.
        Returns True if successful, False if no data was retrieved.
        """
        temp_script = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", dir=str(self.output_dir), encoding="utf-8"
        )
        
        # Enhanced SQL*Plus script with optimized settings
        temp_script.write(f"""WHENEVER SQLERROR EXIT SQL.SQLCODE
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET ARRAYSIZE 5000
SET LOBPREFETCH 16384
SET LONG 100000000
SET LONGCHUNKSIZE 1000000
SET PAGESIZE 0
SET COLSEP '{self.spool_delimiter}'
SET LINESIZE 32767
SET TRIMSPOOL ON
SET WRAP OFF
SET MARKUP CSV ON DELIMITER '{self.spool_delimiter}' QUOTE ON
SPOOL {output_file}
{query};
SPOOL OFF
EXIT;
""")
        temp_script.close()
        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script.name}"
        
        logger.info(f"Executing SQL*Plus to fetch data batch...")
        start_time = time.time()
        
        for attempt in range(1, self.max_retries + 1):
            try:
                result = subprocess.run(
                    command, 
                    shell=True, 
                    check=True, 
                    timeout=self.timeout,
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE
                )
                
                # Check if the output file exists and has content
                if not output_file.exists() or output_file.stat().st_size == 0:
                    logger.info("No data retrieved in this batch.")
                    Path(temp_script.name).unlink(missing_ok=True)
                    return False
                
                elapsed = time.time() - start_time
                file_size_mb = output_file.stat().st_size / (1024 * 1024)
                logger.info(f"Data batch retrieved: {file_size_mb:.2f} MB in {elapsed:.2f} seconds")
                Path(temp_script.name).unlink(missing_ok=True)
                return True
                
            except subprocess.CalledProcessError as err:
                stderr = err.stderr.decode('utf-8') if err.stderr else "Unknown error"
                logger.error(f"SQL*Plus execution failed (attempt {attempt}): {stderr}")
                if attempt == self.max_retries:
                    raise ValueError(f"SQL*Plus execution failed after {self.max_retries} attempts: {stderr}")
                time.sleep(2)  # Wait before retrying
                
            except subprocess.TimeoutExpired:
                logger.error(f"SQL*Plus execution timed out after {self.timeout} seconds (attempt {attempt})")
                if attempt == self.max_retries:
                    raise ValueError(f"SQL*Plus execution timed out after {self.max_retries} attempts")
                self.timeout *= 1.5  # Increase timeout for next attempt
                time.sleep(2)  # Wait before retrying
                
            except Exception as e:
                logger.error(f"Error executing SQL*Plus: {str(e)}")
                if attempt == self.max_retries:
                    raise
                time.sleep(2)  # Wait before retrying
                
            finally:
                if Path(temp_script.name).exists():
                    Path(temp_script.name).unlink(missing_ok=True)

    def _process_csv_batch(self, raw_file: Path, writer, delimiter: str = None) -> int:
        """
        Process a batch of raw CSV data and write to the output file.
        Returns the number of rows processed.
        Uses buffered reading for better performance.
        """
        if delimiter is None:
            delimiter = self.spool_delimiter
            
        row_count = 0
        # Use a large buffer for reading
        with raw_file.open("r", encoding="utf-8", errors="replace", buffering=self.buffer_size) as f_in:
            reader = csv.reader(f_in, delimiter=delimiter)
            for row in reader:
                if not row or all(not field.strip() for field in row):
                    continue
                    
                # Skip SQL*Plus error messages and other artifacts
                if any(row[0].startswith(prefix) for prefix in ("ORA-", "SP2-", "SQL>")):
                    continue
                    
                clean_row = [field.strip() for field in row]
                if clean_row and any(clean_row):
                    writer.writerow(clean_row)
                    row_count += 1
                    
                    # Log progress periodically for large batches
                    if row_count % 100000 == 0:
                        logger.info(f"Processed {row_count} rows...")
                        
        return row_count

    def _process_json_batch(self, raw_file: Path, header: List[str], delimiter: str = None) -> List[Dict[str, Any]]:
        """
        Process a batch of raw data into JSON objects.
        Returns a list of dictionaries.
        Uses buffered reading for better performance.
        """
        if delimiter is None:
            delimiter = self.spool_delimiter
            
        batch_rows = []
        row_count = 0
        
        with raw_file.open("r", encoding="utf-8", errors="replace", buffering=self.buffer_size) as f_in:
            reader = csv.reader(f_in, delimiter=delimiter)
            for row in reader:
                if not row or all(not field.strip() for field in row):
                    continue
                    
                # Skip SQL*Plus error messages and other artifacts
                if any(row[0].startswith(prefix) for prefix in ("ORA-", "SP2-", "SQL>")):
                    continue
                    
                clean_row = [field.strip() for field in row]
                if clean_row and any(clean_row):
                    # Handle cases where row length doesn't match header length
                    if len(clean_row) < len(header):
                        clean_row.extend([''] * (len(header) - len(clean_row)))
                    elif len(clean_row) > len(header):
                        clean_row = clean_row[:len(header)]
                        
                    # Create a dictionary mapping header to row values
                    row_dict = dict(zip(header, clean_row))
                    batch_rows.append(row_dict)
                    row_count += 1
                    
                    # Log progress periodically for large batches
                    if row_count % 100000 == 0:
                        logger.info(f"Processed {row_count} rows...")
                        
        return batch_rows

    def _estimate_row_count(self) -> Optional[int]:
        """
        Estimate the total number of rows in the table.
        This helps with progress reporting for large tables.
        """
        query = f"SELECT COUNT(*) FROM {self.schema}.{self.table}"
        if self.where_clause:
            query += f" WHERE {self.where_clause}"
            
        temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".txt", dir=str(self.output_dir))
        temp_output.close()
        temp_script = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", dir=str(self.output_dir), encoding="utf-8"
        )
        temp_script.write(f"""SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SPOOL {temp_output.name}
{query};
SPOOL OFF
EXIT;
""")
        temp_script.close()

        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script.name}"
        
        try:
            subprocess.run(command, shell=True, check=True, timeout=30, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            with open(temp_output.name, "r", encoding="utf-8", errors="replace") as f:
                count_str = f.read().strip()
                # Extract numeric part (handle possible commas or formatting)
                count_match = re.search(r'(\d[\d,]*)', count_str)
                if count_match:
                    count_str = count_match.group(1).replace(',', '')
                    return int(count_str)
            return None
        except Exception as e:
            logger.warning(f"Could not estimate row count: {e}")
            return None
        finally:
            Path(temp_output.name).unlink(missing_ok=True)
            Path(temp_script.name).unlink(missing_ok=True)

    def extract(self) -> None:
        """
        Main extraction process with improved handling for large tables:
          1. First estimates row count for better progress reporting
          2. Retrieves column metadata and creates proper output structure
          3. Processes data in optimized batches
          4. Uses buffered I/O for better performance
          5. Provides detailed progress updates
          6. Includes proper error handling and recovery
        """
        logger.info("Starting extraction process")
        start_time = time.time()
        
        # Estimate total rows for progress reporting
        total_rows = self._estimate_row_count()
        if total_rows:
            logger.info(f"Estimated row count: {total_rows:,}")
            
        # Fetch column metadata
        col_data = self._get_column_data()
        header = [col for col, _ in col_data]
        logger.info(f"Retrieved {len(header)} columns")
        
        current_offset = self.offset
        batch_number = 0
        total_processed = 0
        
        # Set up the final output file path
        final_output = self.output_dir / f"{self.schema}_{self.table}_full.{self.file_format}"
        if self.compress:
            final_output = final_output.with_suffix(final_output.suffix + ".gz")
            
        # Process based on the selected format
        if self.file_format == "csv":
            # Use context manager to ensure proper file closure
            with self._open_output_file(final_output) as f_out:
                writer = csv.writer(f_out, delimiter=",", quoting=csv.QUOTE_ALL, lineterminator="\n")
                # Write header row
                writer.writerow(header)
                
                # Process data in batches
                while True:
                    batch_start = time.time()
                    data_query = self._build_data_query(offset=current_offset, columns=header)
                    temp_raw = self.output_dir / f"raw_output_part{batch_number}.txt"
                    
                    try:
                        data_retrieved = self._execute_sqlplus(data_query, temp_raw)
                        if not data_retrieved:
                            logger.info("No more data to retrieve")
                            break
                            
                        row_count = self._process_csv_batch(temp_raw, writer)
                        batch_time = time.time() - batch_start
                        
                        if row_count == 0:
                            logger.info("No rows processed in this batch")
                            break
                            
                        total_processed += row_count
                        logger.info(f"Batch {batch_number}: Processed {row_count:,} rows in {batch_time:.2f} seconds")
                        
                        if total_rows:
                            progress = (total_processed / total_rows) * 100
                            logger.info(f"Overall progress: {total_processed:,}/{total_rows:,} rows ({progress:.2f}%)")
                            
                        # Clean up temp file to save disk space
                        temp_raw.unlink(missing_ok=True)
                        
                        if self.batch_size is not None:
                            current_offset += self.batch_size
                            batch_number += 1
                        else:
                            break
                            
                    except Exception as e:
                        logger.error(f"Error processing batch {batch_number}: {str(e)}")
                        if batch_number > 0:
                            # We've already processed some data, continue with next batch
                            logger.info("Continuing with next batch...")
                            current_offset += self.batch_size
                            batch_number += 1
                            continue
                        else:
                            # First batch failed, abort
                            raise
            
            total_time = time.time() - start_time
            logger.info(f"CSV extraction complete: {final_output}")
            logger.info(f"Total rows: {total_processed:,}, Total time: {total_time:.2f} seconds")
            
        elif self.file_format == "json":
            json_rows = []
            
            # Process data in batches
            while True:
                batch_start = time.time()
                data_query = self._build_data_query(offset=current_offset, columns=header)
                temp_raw = self.output_dir / f"raw_output_part{batch_number}.txt"
                
                try:
                    data_retrieved = self._execute_sqlplus(data_query, temp_raw)
                    if not data_retrieved:
                        logger.info("No more data to retrieve")
                        break
                        
                    batch_rows = self._process_json_batch(temp_raw, header)
                    batch_time = time.time() - batch_start
                    
                    if not batch_rows:
                        logger.info("No rows processed in this batch")
                        break
                        
                    # For large tables, write each batch to disk instead of keeping in memory
                    if total_rows and total_rows > 1000000:
                        # Write batch to temporary JSON file
                        temp_json = self.output_dir / f"json_part{batch_number}.json"
                        with self._open_output_file(temp_json) as f_out:
                            json.dump(batch_rows, f_out)
                        logger.info(f"Wrote batch {batch_number} to temporary file: {temp_json}")
                    else:
                        # Keep in memory for smaller datasets
                        json_rows.extend(batch_rows)
                        
                    row_count = len(batch_rows)
                    total_processed += row_count
                    logger.info(f"Batch {batch_number}: Processed {row_count:,} rows in {batch_time:.2f} seconds")
                    
                    if total_rows:
                        progress = (total_processed / total_rows) * 100
                        logger.info(f"Overall progress: {total_processed:,}/{total_rows:,} rows ({progress:.2f}%)")
                        
                    # Clean up temp file to save disk space
                    temp_raw.unlink(missing_ok=True)
                    
                    if self.batch_size is not None:
                        current_offset += self.batch_size
                        batch_number += 1
                    else:
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing batch {batch_number}: {str(e)}")
                    if batch_number > 0:
                        # Continue with next batch
                        current_offset += self.batch_size
                        batch_number += 1
                        continue
                    else:
                        # First batch failed, abort
                        raise
            
            # For large tables with batched JSON files
            # For large tables with batched JSON files
            if total_rows and total_rows > 1000000 and batch_number > 1:
                logger.info("Merging JSON batch files...")
                # Open the final output file
                with self._open_output_file(final_output) as f_out:
                    # Start the JSON array
                    f_out.write("[\n")
                    
                    # Process each batch file
                    for i in range(batch_number):
                        batch_file = self.output_dir / f"json_part{i}.json"
                        if not batch_file.exists():
                            continue
                            
                        # Read and write batch content
                        with batch_file.open("r", encoding="utf-8") as f_in:
                            # Skip the opening bracket
                            content = f_in.read().strip()
                            if content.startswith("["):
                                content = content[1:]
                            if content.endswith("]"):
                                content = content[:-1]
                                
                            # Write batch content
                            f_out.write(content)
                            
                            # Add comma if not the last batch
                            if i < batch_number - 1:
                                f_out.write(",\n")
                                
                        # Remove batch file
                        batch_file.unlink(missing_ok=True)
                        
                    # Close the JSON array
                    f_out.write("\n]")
            else:
                # For smaller datasets, write directly
                with self._open_output_file(final_output) as f_out:
                    json.dump(json_rows, f_out, indent=2)
            
            total_time = time.time() - start_time
            logger.info(f"JSON extraction complete: {final_output}")
            logger.info(f"Total rows: {total_processed:,}, Total time: {total_time:.2f} seconds")
            
        else:
            raise ValueError("Unsupported output format")

    def _open_output_file(self, output_file: Path):
        """
        Opens the target output file for writing with appropriate settings.
        Uses gzip compression if enabled.
        """
        if self.compress:
            logger.debug(f"Opening compressed output file: {output_file}")
            return gzip.open(output_file, mode="wt", encoding="utf-8", newline="")
        else:
            logger.debug(f"Opening output file: {output_file}")
            return open(output_file, "w", encoding="utf-8", newline="", buffering=self.buffer_size)


def main():
    """Command line interface for the extractor."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract data from Oracle using SQL*Plus")
    parser.add_argument("--username", required=True, help="Oracle username")
    parser.add_argument("--password", required=True, help="Oracle password")
    parser.add_argument("--host", required=True, help="Oracle host")
    parser.add_argument("--port", type=int, default=1521, help="Oracle port (default: 1521)")
    parser.add_argument("--service", required=True, help="Oracle service name")
    parser.add_argument("--schema", required=True, help="Schema name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--where", default="", help="WHERE clause (optional)")
    parser.add_argument("--output-dir", default="OutputData", help="Base output directory")
    parser.add_argument("--batch-size", type=int, default=100000, help="Batch size (default: 100000)")
    parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    parser.add_argument("--order-by", help="ORDER BY clause (optional)")
    parser.add_argument("--timeout", type=int, default=600, help="SQL*Plus timeout in seconds (default: 600)")
    parser.add_argument("--compress", action="store_true", help="Compress output with gzip")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    try:
        extractor = OracleSQLPlusExtractor(
            username=args.username,
            password=args.password,
            host=args.host,
            port=args.port,
            service_name=args.service,
            schema=args.schema,
            table=args.table,
            where_clause=args.where,
            base_directory=args.output_dir,
            batch_size=args.batch_size,
            file_format=args.format,
            order_by=args.order_by,
            timeout=args.timeout,
            compress=args.compress,
        )
        extractor.extract()
        return 0
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        return 1


if __name__ == "__main__":
    # Example usage
    """
    extractor = OracleSQLPlusExtractor(
        username="your_username",
        password="your_password",
        host="your_host",
        port=1521,
        service_name="your_service",
        schema="YOUR_SCHEMA",
        table="YOUR_TABLE",
        where_clause="",  # Optional filtering condition
        base_directory="OutputData",
        batch_size=100000,  # Increased for better performance
        offset=0,
        file_format="csv",  # "csv" or "json"
        order_by=None,  # Optional ORDER BY clause
        timeout=600,  # Increased timeout
        compress=False,  # Set True for gzip compression
    )
    extractor.extract()
    """
    # Or use the command-line interface
    sys.exit(main())
