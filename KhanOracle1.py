import csv
import os
import logging
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Optional

import gzip

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

class OracleDataExtractor:
    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        service_name: str,
        schema: str,
        table: str,
        output_dir: str = "extracted_data",
        batch_size: int = 20000,
        where_clause: str = "",
        order_by: Optional[str] = None
    ):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.schema = schema
        self.table = table
        self.output_dir = Path(output_dir) / f"{schema}_{table}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.where_clause = where_clause
        self.order_by = order_by
        
        self.delimiter = '|'
        
    def _build_dsn(self) -> str:
        """Construct Oracle DSN string."""
        return (
            f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.host})(PORT={self.port}))"
            f"(CONNECT_DATA=(SERVICE_NAME={self.service_name})))"
        )
    
    def _get_columns(self) -> List[str]:
        """Retrieve column names from the table."""
        query = f"""
        SELECT COLUMN_NAME 
        FROM ALL_TAB_COLUMNS 
        WHERE OWNER = '{self.schema.upper()}' 
        AND TABLE_NAME = '{self.table.upper()}'
        ORDER BY COLUMN_ID
        """
        return self._execute_query(query, header=True)
    
    def _execute_query(self, query: str, header: bool = False) -> List[str]:
        """Execute SQL*Plus query and return results."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as temp_script:
            script_content = f"""
            SET PAGESIZE 0
            SET FEEDBACK OFF
            SET HEADING {"ON" if header else "OFF"}
            SET COLSEP '{self.delimiter}'
            SET LINESIZE 32767
            {query};
            EXIT;
            """
            temp_script.write(script_content)
            temp_script.close()
        
        output_file = tempfile.mktemp(suffix='.txt')
        dsn = self._build_dsn()
        
        try:
            command = (
                f"sqlplus -s {self.username}/{self.password}@{dsn} "
                f"@{temp_script.name} > {output_file}"
            )
            subprocess.run(command, shell=True, check=True)
            
            with open(output_file, 'r', encoding='utf-8') as f:
                results = [line.strip().split(self.delimiter) for line in f if line.strip()]
            
            return results
        finally:
            os.unlink(temp_script.name)
            os.unlink(output_file)
    
    def _build_extract_query(self, columns: List[str], offset: int) -> str:
        """Build data extraction query with date/timestamp handling."""
        select_columns = []
        for col in columns:
            # Convert date/timestamp to standard format
            select_columns.append(
                f"TO_CHAR({col}, 'YYYY-MM-DD HH24:MI:SS') AS {col}"
                if self._is_date_column(col) 
                else col
            )
        
        select_clause = ", ".join(select_columns)
        base_query = f"SELECT {select_clause} FROM {self.schema}.{self.table}"
        
        if self.where_clause:
            base_query += f" WHERE {self.where_clause}"
        
        if self.order_by:
            base_query += f" ORDER BY {self.order_by}"
        
        query = f"""
        SELECT * FROM (
            SELECT a.*, ROWNUM rn FROM (
                {base_query}
            ) a WHERE ROWNUM <= {offset + self.batch_size}
        ) WHERE rn > {offset}
        """
        return query
    
    def _is_date_column(self, column: str) -> bool:
        """Determine if a column is a date/timestamp type."""
        query = f"""
        SELECT DATA_TYPE 
        FROM ALL_TAB_COLUMNS 
        WHERE OWNER = '{self.schema.upper()}' 
        AND TABLE_NAME = '{self.table.upper()}' 
        AND COLUMN_NAME = '{column.upper()}'
        """
        results = self._execute_query(query)
        return any('DATE' in str(result).upper() or 'TIMESTAMP' in str(result).upper() for result in results)
    
    def extract(self, max_batches: int = 100):
        """Main extraction method."""
        columns = [col[0] for col in self._get_columns()]
        
        for batch in range(max_batches):
            offset = batch * self.batch_size
            query = self._build_extract_query(columns, offset)
            
            output_file = self.output_dir / f"batch_{batch}.csv.gz"
            
            with gzip.open(output_file, 'wt', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(
                    csvfile, 
                    delimiter=',', 
                    quotechar='"', 
                    quoting=csv.QUOTE_ALL
                )
                
                # Write header
                writer.writerow(columns)
                
                # Execute query and write data
                results = self._execute_query(query)
                
                if not results:
                    logger.info(f"No more data. Stopping extraction after {batch} batches.")
                    break
                
                for row in results:
                    writer.writerow(row)
                
                logger.info(f"Batch {batch} completed. Extracted {len(results)} rows.")
        
        logger.info("Data extraction complete.")

# Example usage
if __name__ == "__main__":
    extractor = OracleDataExtractor(
        username="your_username",
        password="your_password",
        host="your_host",
        port=1521,
        service_name="your_service",
        schema="your_schema",
        table="your_table"
    )
    extractor.extract()
