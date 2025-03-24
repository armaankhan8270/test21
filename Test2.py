#!/usr/bin/env python3
"""
Oracle Database to CSV Extractor
Handles large datasets with proper encoding, batching, and formatting
"""

import os
import csv
import gzip
import logging
import cx_Oracle
import configparser
import pandas as pd
from datetime import datetime
from contextlib import contextmanager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('extraction.log'), logging.StreamHandler()]
)
logger = logging.getLogger('oracle_extractor')

class OracleExtractor:
    def __init__(self, config_file='extract_config.ini'):
        """Initialize the extractor with configuration parameters"""
        self.config = self._load_config(config_file)
        self.connection = None
        self.batch_size = int(self.config.get('extract', 'batch_size', fallback='500000'))
        self.output_dir = self.config.get('paths', 'output_dir', fallback='./output')
        self.compress = self.config.getboolean('extract', 'compress', fallback=True)
        self.max_file_size = int(self.config.get('extract', 'max_file_size', fallback='1073741824'))  # 1GB
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _load_config(self, config_file):
        """Load configuration from ini file"""
        if not os.path.exists(config_file):
            logger.error(f"Configuration file {config_file} not found!")
            raise FileNotFoundError(f"Configuration file {config_file} not found!")
        
        config = configparser.ConfigParser()
        config.read(config_file)
        return config
    
    @contextmanager
    def _get_connection(self):
        """Create a database connection context manager"""
        try:
            # Get connection parameters from config
            username = self.config.get('database', 'username')
            password = self.config.get('database', 'password')
            host = self.config.get('database', 'host')
            port = self.config.get('database', 'port')
            service = self.config.get('database', 'service')
            
            dsn = cx_Oracle.makedsn(host, port, service_name=service)
            connection = cx_Oracle.connect(username, password, dsn)
            logger.info(f"Connected to Oracle database: {host}:{port}/{service}")
            
            yield connection
        except cx_Oracle.Error as e:
            error, = e.args
            logger.error(f"Oracle Error: {error.message}")
            raise
        finally:
            if connection:
                connection.close()
                logger.info("Database connection closed")
    
    def _format_null_values(self, row):
        """Handle NULL values in the dataset"""
        null_placeholder = self.config.get('format', 'null_placeholder', fallback='NULL')
        return [null_placeholder if val is None else val for val in row]
    
    def _format_date_values(self, row, columns, cursor):
        """Format date and timestamp values according to the specified format"""
        date_format = self.config.get('format', 'date_format', fallback='%Y-%m-%d %H:%M:%S.%f')
        
        formatted_row = list(row)
        for i, col in enumerate(columns):
            col_type = cursor.description[i][1]
            # Oracle DATETIME and DATE types
            if col_type in (cx_Oracle.DB_TYPE_DATE, cx_Oracle.DB_TYPE_TIMESTAMP):
                if formatted_row[i] is not None:
                    try:
                        formatted_row[i] = formatted_row[i].strftime(date_format)
                    except AttributeError:
                        # If it's not a datetime object, leave it as is
                        pass
        
        return formatted_row
    
    def _clean_text_fields(self, row, columns, cursor):
        """Clean text fields: handle newlines, leading/trailing spaces, etc."""
        replace_newlines = self.config.getboolean('format', 'replace_newlines', fallback=True)
        strip_spaces = self.config.getboolean('format', 'strip_spaces', fallback=True)
        
        formatted_row = list(row)
        for i, col in enumerate(columns):
            if isinstance(formatted_row[i], str):
                # Replace newlines in text fields
                if replace_newlines:
                    formatted_row[i] = formatted_row[i].replace('\n', ' ').replace('\r', ' ')
                
                # Strip leading/trailing spaces
                if strip_spaces:
                    formatted_row[i] = formatted_row[i].strip()
        
        return formatted_row
    
    def _write_csv_chunk(self, cursor, file_path, start_row=0, max_rows=None):
        """Write a chunk of data to CSV file with proper formatting"""
        column_names = [col[0] for col in cursor.description]
        rows_written = 0
        
        # Determine if this is the first chunk (need headers)
        include_header = (start_row == 0)
        
        # Determine CSV dialect settings
        csv_dialect = {
            'delimiter': self.config.get('format', 'delimiter', fallback=','),
            'quotechar': self.config.get('format', 'quotechar', fallback='"'),
            'quoting': csv.QUOTE_ALL,  # Quote all fields
            'doublequote': True,       # Double quoting for escaping quotes within fields
            'lineterminator': '\n'     # Consistent newline character
        }
        
        # Open file in proper mode (write or append)
        mode = 'wt' if include_header else 'at'
        encoding = self.config.get('format', 'encoding', fallback='utf-8')
        
        with open(file_path, mode, newline='', encoding=encoding) as csv_file:
            writer = csv.writer(csv_file, **csv_dialect)
            
            # Write header row if this is the first chunk
            if include_header:
                writer.writerow(column_names)
            
            # Process and write rows
            for row in cursor:
                # Skip rows before start_row
                if rows_written < start_row:
                    rows_written += 1
                    continue
                
                # Apply formatting to the row
                formatted_row = self._format_null_values(row)
                formatted_row = self._format_date_values(formatted_row, column_names, cursor)
                formatted_row = self._clean_text_fields(formatted_row, column_names, cursor)
                
                # Write the formatted row
                writer.writerow(formatted_row)
                rows_written += 1
                
                # Stop if we've reached max_rows
                if max_rows and rows_written >= (start_row + max_rows):
                    break
        
        return rows_written - start_row  # Return number of rows actually written
    
    def _compress_file(self, file_path):
        """Compress a CSV file using gzip"""
        compressed_path = f"{file_path}.gz"
        with open(file_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                f_out.writelines(f_in)
        
        # Remove original file after successful compression
        os.remove(file_path)
        logger.info(f"Compressed {file_path} to {compressed_path}")
        return compressed_path
    
    def extract_table(self, table_name=None, query=None, output_file=None):
        """
        Extract data from an Oracle table or using a custom query
        
        Args:
            table_name: Name of the table to extract
            query: Custom SQL query (overrides table_name if provided)
            output_file: Base name for output file
        """
        if not table_name and not query:
            raise ValueError("Either table_name or query must be provided")
        
        # Determine SQL query
        if query:
            sql = query
        else:
            # Get columns explicitly instead of using *
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '{table_name.upper()}'")
                columns = [row[0] for row in cursor.fetchall()]
                
            # Form query with explicit columns
            column_list = ", ".join(columns)
            sql = f"SELECT {column_list} FROM {table_name}"
        
        # Determine output file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if not output_file:
            if table_name:
                base_name = f"{table_name}_{timestamp}"
            else:
                base_name = f"query_extract_{timestamp}"
        else:
            base_name = output_file
        
        with self._get_connection() as conn:
            # Get total row count for logging
            count_sql = f"SELECT COUNT(*) FROM ({sql})" if "COUNT(*)" not in sql.upper() else sql
            cursor = conn.cursor()
            cursor.execute(count_sql)
            total_rows = cursor.fetchone()[0]
            logger.info(f"Extracting {total_rows} rows from query: {sql[:100]}...")
            
            # Execute the main query
            cursor = conn.cursor()
            cursor.execute(sql)
            
            # Process in batches
            file_index = 1
            rows_processed = 0
            
            while True:
                # Create file name for this chunk
                if total_rows > self.batch_size:
                    file_name = f"{base_name}_part{file_index}.csv"
                else:
                    file_name = f"{base_name}.csv"
                
                file_path = os.path.join(self.output_dir, file_name)
                
                # Write chunk to CSV
                rows_written = self._write_csv_chunk(
                    cursor, 
                    file_path, 
                    start_row=rows_processed, 
                    max_rows=self.batch_size
                )
                
                rows_processed += rows_written
                logger.info(f"Wrote {rows_written} rows to {file_path} ({rows_processed}/{total_rows} total)")
                
                # Compress if needed
                if self.compress:
                    file_path = self._compress_file(file_path)
                
                # Check if we've processed all rows
                if rows_written < self.batch_size or rows_processed >= total_rows:
                    break
                
                file_index += 1
        
        logger.info(f"Extraction completed: {rows_processed} rows exported to CSV")
        return rows_processed

    def extract_with_pagination(self, query, output_file=None, id_column='ROWID'):
        """
        Extract data using pagination for very large datasets
        
        Args:
            query: SQL query to execute
            output_file: Base name for output file
            id_column: Column to use for pagination ordering
        """
        # Determine output file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if not output_file:
            base_name = f"paginated_extract_{timestamp}"
        else:
            base_name = output_file
        
        with self._get_connection() as conn:
            # Get total row count for logging
            count_sql = f"SELECT COUNT(*) FROM ({query})"
            cursor = conn.cursor()
            cursor.execute(count_sql)
            total_rows = cursor.fetchone()[0]
            logger.info(f"Extracting {total_rows} rows with pagination")
            
            # Initialize variables for pagination
            file_index = 1
            rows_processed = 0
            last_id = None
            
            while rows_processed < total_rows:
                # Construct paginated query
                if last_id is None:
                    paginated_query = f"""
                    SELECT * FROM (
                        SELECT a.*, ROWNUM as rn FROM (
                            {query}
                            ORDER BY {id_column}
                        ) a
                        WHERE ROWNUM <= {self.batch_size}
                    )
                    """
                else:
                    paginated_query = f"""
                    SELECT * FROM (
                        SELECT a.*, ROWNUM as rn FROM (
                            {query}
                            AND {id_column} > '{last_id}'
                            ORDER BY {id_column}
                        ) a
                        WHERE ROWNUM <= {self.batch_size}
                    )
                    """
                
                # Create file name for this chunk
                if total_rows > self.batch_size:
                    file_name = f"{base_name}_part{file_index}.csv"
                else:
                    file_name = f"{base_name}.csv"
                
                file_path = os.path.join(self.output_dir, file_name)
                
                # Execute query for this page
                cursor = conn.cursor()
                cursor.execute(paginated_query)
                
                # Get the last ID for next iteration
                all_rows = cursor.fetchall()
                if all_rows:
                    # Find the index of ID column
                    id_col_index = [col[0] for col in cursor.description].index(id_column)
                    last_id = all_rows[-1][id_col_index]
                    
                    # Write data to CSV
                    with open(file_path, 'w', newline='', encoding='utf-8') as csv_file:
                        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
                        
                        # Write header row
                        writer.writerow([col[0] for col in cursor.description if col[0] != 'RN'])
                        
                        # Write data rows
                        for row in all_rows:
                            # Skip the ROWNUM column which we added
                            formatted_row = [row[i] for i in range(len(row)) if cursor.description[i][0] != 'RN']
                            writer.writerow(formatted_row)
                    
                    rows_written = len(all_rows)
                    rows_processed += rows_written
                    logger.info(f"Wrote {rows_written} rows to {file_path} ({rows_processed}/{total_rows} total)")
                    
                    # Compress if needed
                    if self.compress:
                        file_path = self._compress_file(file_path)
                    
                    file_index += 1
                else:
                    # No more rows
                    break
        
        logger.info(f"Extraction completed: {rows_processed} rows exported to CSV")
        return rows_processed

def main():
    # Example usage
    try:
        extractor = OracleExtractor('extract_config.ini')
        
        # Extract using direct table query
        extractor.extract_table(table_name='EMPLOYEES')
        
        # Extract using custom query with date filtering
        query = """
        SELECT * FROM ORDERS 
        WHERE ORDER_DATE >= TO_DATE('2024-01-01', 'YYYY-MM-DD')
        """
        extractor.extract_table(query=query, output_file='recent_orders')
        
        # Extract very large table using pagination
        large_query = "SELECT * FROM TRANSACTION_HISTORY"
        extractor.extract_with_pagination(
            query=large_query, 
            output_file='transactions_history',
            id_column='TRANSACTION_ID'
        )
        
        logger.info("All extraction tasks completed successfully")
        
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())






[database]
username = oracle_user
password = oracle_password
host = localhost
port = 1521
service = ORCL

[paths]
output_dir = ./output
log_dir = ./logs

[extract]
batch_size = 500000
max_file_size = 1073741824  # 1GB in bytes
compress = true
parallel_extracts = 4

[format]
encoding = utf-8
delimiter = ,
quotechar = "
date_format = %Y-%m-%d %H:%M:%S.%f
null_placeholder = NULL
replace_newlines = true
strip_spaces = true
