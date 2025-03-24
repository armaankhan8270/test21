import csv
import json
import logging
import subprocess
import tempfile
import gzip
from pathlib import Path
from typing import List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

class OracleSQLPlusExtractor:
    def __init__(self, username, password, host, port, service_name, schema, table, 
                 where_clause="", base_directory="OutputData", batch_size=None, 
                 offset=0, file_format="csv", order_by=None, timeout=300, compress=False):
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
        self.order_by = order_by
        self.timeout = timeout
        self.compress = compress

        if self.file_format not in {"csv", "json"}:
            raise ValueError("Supported formats are 'csv' and 'json'.")

        self.output_dir = Path(base_directory) / self.schema / self.table
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.spool_delimiter = "|"
        self.timestamp_columns = []

        logger.info(f"Extractor initialized for {self.schema}.{self.table}. Batch: {self.batch_size}, Format: {self.file_format}, Compressed: {self.compress}")

    def _build_dsn(self):
        return f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.host})(PORT={self.port}))" \
               f"(CONNECT_DATA=(SERVICE_NAME={self.service_name})))"

    def _get_column_data(self) -> List[Tuple[str, str]]:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM ALL_TAB_COLUMNS 
        WHERE OWNER = '{self.schema}' AND TABLE_NAME = '{self.table}' 
        ORDER BY COLUMN_ID
        """

        temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".txt", dir=str(self.output_dir))
        temp_script = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".sql", dir=str(self.output_dir), encoding="utf-8")

        temp_script.write(f"""
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET LINESIZE 10000
SPOOL {temp_output.name}
{query}
SPOOL OFF
EXIT;
""")
        temp_script.close()

        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{temp_script.name}"

        try:
            subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            with open(temp_output.name, "r", encoding="utf-8", errors="replace") as f:
                data = [line.strip().split("|") for line in f if line.strip()]
            column_data = [(col.strip(), dt.strip()) for col, dt in data]
            self.timestamp_columns = [col for col, dt in column_data if "DATE" in dt.upper() or "TIMESTAMP" in dt.upper()]
            return column_data
        finally:
            Path(temp_output.name).unlink(missing_ok=True)
            Path(temp_script.name).unlink(missing_ok=True)

    def _build_data_query(self, offset=None) -> str:
        columns = [f"TO_CHAR({col}, 'YYYY-MM-DD HH24:MI:SS.FF6') AS {col}" if col in self.timestamp_columns else col
                   for col, _ in self._get_column_data()]
        query = f"SELECT {', '.join(columns)} FROM {self.schema}.{self.table}"
        if self.where_clause:
            query += f" WHERE {self.where_clause}"
        if self.order_by:
            query += f" ORDER BY {self.order_by}"
        if self.batch_size is not None and offset is not None:
            query += f" OFFSET {offset} ROWS FETCH NEXT {self.batch_size} ROWS ONLY"
        return query

    def _execute_sqlplus(self, query, output_file):
        sql_script = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".sql", dir=str(self.output_dir), encoding="utf-8")
        sql_script.write(f"""
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET COLSEP '{self.spool_delimiter}'
SET MARKUP CSV ON DELIMITER '{self.spool_delimiter}' QUOTE ON
SPOOL {output_file}
{query}
SPOOL OFF
EXIT;
""")
        sql_script.close()

        dsn = self._build_dsn()
        command = f"sqlplus -s {self.username}/{self.password}@{dsn} @{sql_script.name}"
        
        try:
            subprocess.run(command, shell=True, check=True, timeout=self.timeout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        finally:
            Path(sql_script.name).unlink(missing_ok=True)

    def extract(self):
        logger.info("Starting extraction...")
        final_output = self.output_dir / f"{self.schema}_{self.table}_full.{self.file_format}"
        if self.compress:
            final_output = final_output.with_suffix(final_output.suffix + ".gz")

        header = [col for col, _ in self._get_column_data()]
        current_offset = self.offset
        batch_number = 0

        with (gzip.open(final_output, "wt", encoding="utf-8", newline="") if self.compress else final_output.open("w", encoding="utf-8", newline="")) as f_out:
            writer = csv.writer(f_out, delimiter=",", quoting=csv.QUOTE_ALL, lineterminator="\n")
            writer.writerow(header)

            while True:
                query = self._build_data_query(offset=current_offset)
                temp_raw = self.output_dir / f"raw_output_part{batch_number}.txt"
                self._execute_sqlplus(query, temp_raw)

                if not temp_raw.exists() or temp_raw.stat().st_size == 0:
                    logger.info("No more data. Extraction complete.")
                    break

                with temp_raw.open("r", encoding="utf-8", errors="replace") as f_in:
                    writer.writerows(csv.reader(f_in, delimiter=self.spool_delimiter))

                logger.info(f"Batch {batch_number} processed.")

                temp_raw.unlink()
                if self.batch_size is None:
                    break
                current_offset += self.batch_size
                batch_number += 1

        logger.info(f"Extraction completed: {final_output}")
