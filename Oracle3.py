import csv
import json
import logging
import subprocess
import tempfile
import gzip
import os
from pathlib import Path
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class OracleSQLPlusExtractor:
    """
    Extracts data from Oracle using SQL*Plus and writes results to CSV or JSON.
    
    Key features:
      • Retrieves column names and data types from ALL_TAB_COLUMNS.
      • Detects DATE/TIMESTAMP columns and wraps them using TO_CHAR(..., 'YYYY-MM-DD HH24:MI:SS.FF6') to include milliseconds.
      • Uses SQL*Plus’s CSV markup to spool data with a custom delimiter.
      • Writes a final output file:
            - CSV: a header row (all fields double-quoted) followed by data rows.
            - JSON: a JSON array of dictionaries keyed by column names.
      • Supports batching via OFFSET/FETCH for large tables.
      • Handles Unicode errors using errors="replace".
      • Optionally compresses output using gzip.
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
        batch_size: int = None,
        offset: int = 0,
        file_format: str = "csv",  # "csv" or "json"
        order_by: str = None,
        timeout: int = 300,       # seconds
        compress: bool = False,
    ):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.schema = schema.strip().upper()
        self.table = table.strip().upper()
        self.where_clause = where_clause
        self.batch_size = batch_size
        self.offset = offset
        self.file_format = file_format.lower()
        if self.file_format not in {"csv", "json"}:
            raise ValueError("Supported output formats are 'csv' and 'json'.")
        self.order_by = order_by
        self.timeout = timeout
        self.compress = compress

        # Create output directory: base_directory/schema/table
        self.output_dir = Path(base_directory) / self.schema / self.table
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Use a custom spool delimiter to avoid conflicts with embedded commas.
        self.spool_delimiter = "|"
        # This list will be populated with detected timestamp columns.
        self.timestamp_columns: List[str] = []

        logger.info(
            f"Extractor initialized for {self.schema}.{self.table} at {self.host}:{self.port}. "
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
        """
        query = f"""
SELECT COLUMN_NAME, DATA_TYPE 
FROM ALL_TAB_COLUMNS 
WHERE OWNER = '{self.schema}' AND TABLE_NAME = '{self.table}' 
ORDER BY COLUMN_ID
"""
        # Use mkstemp to create temporary files (ensuring they’re closed before reuse).
        fd_output, temp_output_name = tempfile.mkstemp(suffix=".txt", dir=str(self.output_dir))
        os.close(fd_output)

        fd_script, temp_script_name = tempfile.mkstemp(suffix=".sql", dir=str(self.output_dir))
        os.close(fd_script)

        # Write SQL script to file.
        with open(temp_script_name, "w", encoding="utf-8") as temp_script:
            temp_script.write(f"""
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET LINESIZE 10000
SPOOL {temp_output_name}
{query}
SPOOL OFF
EXIT;
""")
        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script_name}"
        try:
            subprocess.run(
                command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            with open(temp_output_name, "r", encoding="utf-8", errors="replace") as f:
                # Each line should be: COLUMN_NAME|DATA_TYPE
                data = [line.strip().split("|") for line in f if line.strip()]
            # Create tuples for each valid line.
            column_data = [(col.strip(), dt.strip()) for col, dt in data if col and dt]
            # Determine timestamp columns based on DATA_TYPE.
            self.timestamp_columns = [
                col for col, dt in column_data if "DATE" in dt.upper() or "TIMESTAMP" in dt.upper()
            ]
            return column_data
        except subprocess.CalledProcessError as err:
            logger.error(f"Failed to fetch column data: {err.stderr.decode('utf-8')}")
            raise
        finally:
            try:
                os.unlink(temp_output_name)
            except Exception as e:
                logger.warning(f"Could not delete temporary file {temp_output_name}: {e}")
            try:
                os.unlink(temp_script_name)
            except Exception as e:
                logger.warning(f"Could not delete temporary file {temp_script_name}: {e}")

    def _build_data_query(self, offset: int = None) -> str:
        """
        Builds the data query. Timestamp columns are wrapped with TO_CHAR for millisecond precision.
        """
        col_data = self._get_column_data()
        select_list = []
        for col, _ in col_data:
            if col.upper() in (c.upper() for c in self.timestamp_columns):
                select_list.append(f"TO_CHAR({col}, 'YYYY-MM-DD HH24:MI:SS.FF6') AS {col}")
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
        logger.debug(f"Built data query: {query}")
        return query

    def _execute_sqlplus(self, query: str, output_file: Path) -> None:
        """
        Executes SQL*Plus with the given query and spools the results to output_file.
        """
        fd_script, temp_script_name = tempfile.mkstemp(suffix=".sql", dir=str(self.output_dir))
        os.close(fd_script)
        with open(temp_script_name, "w", encoding="utf-8") as temp_script:
            temp_script.write(f"""
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET COLSEP '{self.spool_delimiter}'
SET LINESIZE 32767
SET WRAP OFF
SET TRIMSPOOL ON
SET MARKUP CSV ON DELIMITER '{self.spool_delimiter}' QUOTE ON
SPOOL {output_file}
{query}
SPOOL OFF
EXIT;
""")
        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script_name}"
        logger.info("Executing SQL*Plus command for data extraction...")
        try:
            subprocess.run(
                command, shell=True, check=True, timeout=self.timeout,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
        except subprocess.CalledProcessError as err:
            logger.error(f"SQL*Plus execution error: {err.stderr.decode('utf-8')}")
            raise
        finally:
            try:
                os.unlink(temp_script_name)
            except Exception as e:
                logger.warning(f"Could not delete temporary file {temp_script_name}: {e}")

    def extract(self) -> None:
        """
        Main extraction process:
          1. Retrieves column names and writes the CSV header or JSON keys.
          2. Builds the data query with timestamp formatting.
          3. Executes the query in batches (if specified) and writes the results.
        """
        logger.info("Starting extraction process.")
        # Retrieve column information.
        col_data = self._get_column_data()
        header = [col for col, _ in col_data]
        logger.info(f"Retrieved columns: {header}")
        logger.info(f"Detected timestamp columns: {self.timestamp_columns}")

        current_offset = self.offset
        batch_number = 0
        final_output = self.output_dir / f"{self.schema}_{self.table}_full.{self.file_format}"
        if self.compress:
            final_output = final_output.with_suffix(final_output.suffix + ".gz")

        if self.file_format == "csv":
            out_fp = (
                gzip.open(final_output, "wt", encoding="utf-8", newline="\n")
                if self.compress
                else final_output.open("w", encoding="utf-8", newline="\n")
            )
            with out_fp as f_out:
                writer = csv.writer(
                    f_out, delimiter=",", quoting=csv.QUOTE_ALL, lineterminator="\n"
                )
                # Write header row.
                writer.writerow(header)
                while True:
                    query = self._build_data_query(offset=current_offset)
                    temp_raw = self.output_dir / f"raw_output_part{batch_number}.txt"
                    self._execute_sqlplus(query, temp_raw)
                    logger.info(f"Batch {batch_number} spooled to: {temp_raw}")

                    if not temp_raw.exists() or temp_raw.stat().st_size == 0:
                        logger.info("No more data. Extraction complete.")
                        break

                    with temp_raw.open("r", encoding="utf-8", errors="replace") as f_in:
                        reader = csv.reader(f_in, delimiter=self.spool_delimiter)
                        rows = [
                            [field.strip() for field in row]
                            for row in reader if any(field.strip() for field in row)
                        ]
                        if not rows:
                            logger.info("No data rows found in this batch; finishing extraction.")
                            break
                        writer.writerows(rows)
                        logger.info(f"Batch {batch_number}: Wrote {len(rows)} rows.")

                    try:
                        temp_raw.unlink()
                    except Exception as e:
                        logger.warning(f"Unable to delete temporary file {temp_raw}: {e}")

                    if self.batch_size is None:
                        break
                    current_offset += self.batch_size
                    batch_number += 1
            logger.info(f"CSV extraction complete: {final_output}")

        elif self.file_format == "json":
            json_rows = []
            while True:
                query = self._build_data_query(offset=current_offset)
                temp_raw = self.output_dir / f"raw_output_part{batch_number}.txt"
                self._execute_sqlplus(query, temp_raw)
                logger.info(f"Batch {batch_number} spooled to: {temp_raw}")

                if not temp_raw.exists() or temp_raw.stat().st_size == 0:
                    logger.info("No more data. Extraction complete.")
                    break

                with temp_raw.open("r", encoding="utf-8", errors="replace") as f_in:
                    reader = csv.reader(f_in, delimiter=self.spool_delimiter)
                    batch_data = [
                        dict(zip(header, [field.strip() for field in row]))
                        for row in reader if any(field.strip() for field in row)
                    ]
                    if not batch_data:
                        logger.info("No data rows found in this batch; finishing extraction.")
                        break
                    json_rows.extend(batch_data)
                    logger.info(f"Batch {batch_number}: Added {len(batch_data)} rows.")

                try:
                    temp_raw.unlink()
                except Exception as e:
                    logger.warning(f"Unable to delete temporary file {temp_raw}: {e}")

                if self.batch_size is None:
                    break
                current_offset += self.batch_size
                batch_number += 1

            out_fp = (
                gzip.open(final_output, "wt", encoding="utf-8", newline="")
                if self.compress
                else final_output.open("w", encoding="utf-8", newline="")
            )
            with out_fp as f_out:
                json.dump(json_rows, f_out, indent=4)
            logger.info(f"JSON extraction complete: {final_output}")
        else:
            raise ValueError("Unsupported output format.")


if __name__ == "__main__":
    # Replace the parameters with your actual Oracle connection details and desired options.
    extractor = OracleSQLPlusExtractor(
        username="your_username",
        password="your_password",
        host="your_host",
        port=1521,
        service_name="your_service",
        schema="VMC_APEX",              # Replace with your schema name.
        table="APEX_RAW_INLOCAT",         # Replace with your table name.
        where_clause="",                 # Optional filtering condition.
        base_directory="OutputData2",
        batch_size=10000,                # Set to None to extract in one batch.
        offset=0,
        file_format="csv",               # "csv" or "json"
        order_by="your_order_column",    # Optional ORDER BY clause.
        timeout=300,
        compress=False                 # Set True to enable gzip compression.
    )
    extractor.extract()
