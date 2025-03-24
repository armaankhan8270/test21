#!/usr/bin/env python3
"""
Oracle Database to CSV Extractor using SQL*Plus utility
Handles large datasets with proper formatting, batch processing and compression
"""

import os
import re
import gzip
import shutil
import logging
import subprocess
import configparser
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('sqlplus_extraction.log'), logging.StreamHandler()]
)
logger = logging.getLogger('oracle_sqlplus_extractor')

class OracleSQLPlusExtractor:
    def __init__(self, config_file='extract_config.ini'):
        """Initialize the extractor with configuration parameters"""
        self.config = self._load_config(config_file)
        self.sqlplus_path = self.config.get('sqlplus', 'sqlplus_path', fallback='sqlplus')
        self.output_dir = self.config.get('paths', 'output_dir', fallback='./output')
        self.temp_dir = self.config.get('paths', 'temp_dir', fallback='./temp')
        self.batch_size = int(self.config.get('extract', 'batch_size', fallback='500000'))
        self.compress = self.config.getboolean('extract', 'compress', fallback=True)
        
        # Ensure output and temp directories exist
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # SQL*Plus connection string
        self.db_user = self.config.get('database', 'username')
        self.db_password = self.config.get('database', 'password')
        self.db_connect = self.config.get('database', 'connect_string')
        self.connection_string = f"{self.db_user}/{self.db_password}@{self.db_connect}"

    def _load_config(self, config_file):
        """Load configuration from ini file"""
        if not os.path.exists(config_file):
            logger.error(f"Configuration file {config_file} not found!")
            raise FileNotFoundError(f"Configuration file {config_file} not found!")
        
        config = configparser.ConfigParser()
        config.read(config_file)
        return config
    
    def _create_sql_script(self, query, output_file, start_row=0, max_rows=None):
        """
        Create a SQL*Plus script file to extract data with proper formatting
        """
        # Set up SQL*Plus formatting for CSV output
        script_content = [
            "SET ECHO OFF",
            "SET FEEDBACK OFF",
            "SET HEADING OFF",
            "SET PAGESIZE 0",
            "SET LINESIZE 32767",
            "SET LONG 100000000",
            "SET LONGCHUNKSIZE 32767",
            "SET TRIMSPOOL ON",
            "SET TRIMOUT ON",
            "SET TERMOUT OFF",
            "SET VERIFY OFF",
            "SET WRAP OFF",
            "SET COLSEP ','",
            "SET NUMWIDTH 38",
            "SET NUMFORMAT 999999999999999999999999999.999999999",
            f"SET NLS_DATE_FORMAT '{self.config.get('format', 'date_format', fallback='YYYY-MM-DD HH24:MI:SS')}'",
            f"SET NLS_TIMESTAMP_FORMAT '{self.config.get('format', 'timestamp_format', fallback='YYYY-MM-DD HH24:MI:SS.FF3')}'",
        ]
        
        # Add column formatting if specified
        if self.config.has_section('column_formats'):
            for column, format_str in self.config.items('column_formats'):
                script_content.append(f"COLUMN {column} FORMAT {format_str}")
        
        # Configure NULL value representation
        null_placeholder = self.config.get('format', 'null_placeholder', fallback='NULL')
        script_content.append(f"SET NULL '{null_placeholder}'")
        
        # Modify query for pagination if needed
        if max_rows:
            # Wrap query with rownum filtering for pagination
            pagination_query = f"""
            SELECT * FROM (
                SELECT a.*, ROWNUM as rn FROM (
                    {query}
                ) a
                WHERE ROWNUM <= {start_row + max_rows}
            )
            WHERE rn > {start_row}
            """
            final_query = pagination_query
        else:
            final_query = query
        
        # Add spool command to direct output to file
        script_content.append(f"SPOOL {output_file}")
        
        # Add header query if this is the first batch
        if start_row == 0:
            # Extract column names for header
            header_query = f"""
            SELECT LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_id)
            FROM all_tab_cols
            WHERE table_name = (
                SELECT table_name 
                FROM (
                    SELECT table_name
                    FROM all_tables
                    WHERE table_name IN (
                        SELECT REGEXP_SUBSTR(UPPER('{query}'), '[A-Z0-9_$#]+', 1, LEVEL)
                        FROM dual
                        CONNECT BY REGEXP_INSTR(UPPER('{query}'), '[A-Z0-9_$#]+', 1, LEVEL) > 0
                    )
                    AND ROWNUM = 1
                )
            );
            """
            script_content.append(header_query)
        
        # Add the main query
        script_content.append(final_query)
        
        # End spooling and exit
        script_content.append("SPOOL OFF")
        script_content.append("EXIT")
        
        # Write script to file
        script_path = os.path.join(self.temp_dir, f"extract_{datetime.now().strftime('%Y%m%d%H%M%S')}.sql")
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(script_content))
        
        return script_path
    
    def _execute_sqlplus_script(self, script_path):
        """
        Execute a SQL*Plus script and return the result code
        """
        try:
            cmd = [self.sqlplus_path, "-S", self.connection_string, "@" + script_path]
            logger.info(f"Executing SQL*Plus command: {' '.join(cmd)}")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                logger.error(f"SQL*Plus execution failed with return code {process.returncode}")
                logger.error(f"STDERR: {stderr}")
                return False
            
            if stderr:
                logger.warning(f"SQL*Plus warnings: {stderr}")
            
            logger.info("SQL*Plus execution completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error executing SQL*Plus: {str(e)}")
            return False
    
    def _compress_file(self, file_path):
        """Compress a CSV file using gzip"""
        compressed_path = f"{file_path}.gz"
        with open(file_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Remove original file after successful compression
        os.remove(file_path)
        logger.info(f"Compressed {file_path} to {compressed_path}")
        return compressed_path
    
    def _clean_csv_file(self, input_file, output_file):
        """Clean the CSV file to handle special characters and ensure UTF-8 encoding"""
        try:
            with open(input_file, 'r', encoding='utf-8', errors='replace') as infile:
                with open(output_file, 'w', encoding='utf-8', newline='\n') as outfile:
                    for line in infile:
                        # Remove trailing whitespace
                        line = line.rstrip()
                        
                        # Replace internal newlines with spaces
                        line = re.sub(r'[\r\n]+', ' ', line)
                        
                        # Ensure all text fields are properly quoted
                        # This is a simplistic approach; for complex data, consider using the csv module
                        fields = line.split(',')
                        quoted_fields = []
                        
                        for field in fields:
                            # If field contains special characters or is not numeric, quote it
                            if not field.strip().replace('.', '').isdigit() and not field.strip() == self.config.get('format', 'null_placeholder', fallback='NULL'):
                                # Double any quotes inside the field
                                field = field.replace('"', '""')
                                quoted_fields.append(f'"{field}"')
                            else:
                                quoted_fields.append(field)
                        
                        outfile.write(','.join(quoted_fields) + '\n')
            
            logger.info(f"Cleaned and formatted CSV file: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning CSV file: {str(e)}")
            return False
    
    def _get_total_row_count(self, query):
        """Get the total number of rows that would be returned by the query"""
        count_query = f"SELECT COUNT(*) FROM ({query})"
        
        # Create a temporary script to get the count
        script_content = [
            "SET ECHO OFF",
            "SET FEEDBACK OFF",
            "SET HEADING OFF",
            "SET PAGESIZE 0",
            f"SELECT COUNT(*) FROM ({query});",
            "EXIT"
        ]
        
        count_script_path = os.path.join(self.temp_dir, f"count_{datetime.now().strftime('%Y%m%d%H%M%S')}.sql")
        with open(count_script_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(script_content))
        
        try:
            cmd = [self.sqlplus_path, "-S", self.connection_string, "@" + count_script_path]
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Error getting row count: {stderr}")
                return 0
            
            # Parse the output to get the count
            count_str = stdout.strip()
            try:
                return int(count_str)
            except ValueError:
                logger.error(f"Could not parse row count from: {count_str}")
                return 0
                
        except Exception as e:
            logger.error(f"Error executing count query: {str(e)}")
            return 0
        finally:
            # Clean up the temporary script
            if os.path.exists(count_script_path):
                os.remove(count_script_path)
    
    def extract_data(self, query, output_file=None):
        """
        Extract data from Oracle using SQL*Plus and save to CSV
        
        Args:
            query: SQL query to execute
            output_file: Base name for output file (without extension)
        """
        # Determine output file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if not output_file:
            output_base = f"extract_{timestamp}"
        else:
            output_base = output_file
        
        # Get total row count
        total_rows = self._get_total_row_count(query)
        logger.info(f"Query will return approximately {total_rows} rows")
        
        if total_rows == 0:
            logger.warning("Query would return zero rows. Aborting extraction.")
            return 0
        
        # Determine if batching is needed
        if total_rows > self.batch_size:
            # Process in batches
            batches_needed = (total_rows + self.batch_size - 1) // self.batch_size
            logger.info(f"Processing in {batches_needed} batches of {self.batch_size} rows")
            
            rows_processed = 0
            for batch_num in range(batches_needed):
                start_row = batch_num * self.batch_size
                
                # Create batch file names
                batch_raw_file = os.path.join(self.temp_dir, f"{output_base}_part{batch_num+1}_raw.csv")
                batch_output_file = os.path.join(self.output_dir, f"{output_base}_part{batch_num+1}.csv")
                
                # Create and execute SQL*Plus script for this batch
                script_path = self._create_sql_script(
                    query, 
                    batch_raw_file,
                    start_row=start_row,
                    max_rows=self.batch_size
                )
                
                if self._execute_sqlplus_script(script_path):
                    # Clean and format the CSV file
                    if self._clean_csv_file(batch_raw_file, batch_output_file):
                        batch_rows = min(self.batch_size, total_rows - start_row)
                        rows_processed += batch_rows
                        logger.info(f"Processed batch {batch_num+1}/{batches_needed}: {batch_rows} rows")
                        
                        # Compress if needed
                        if self.compress:
                            batch_output_file = self._compress_file(batch_output_file)
                    
                    # Remove raw file
                    if os.path.exists(batch_raw_file):
                        os.remove(batch_raw_file)
                
                # Remove script file
                if os.path.exists(script_path):
                    os.remove(script_path)
        else:
            # Process in a single batch
            raw_file = os.path.join(self.temp_dir, f"{output_base}_raw.csv")
            output_csv = os.path.join(self.output_dir, f"{output_base}.csv")
            
            # Create and execute SQL*Plus script
            script_path = self._create_sql_script(query, raw_file)
            
            if self._execute_sqlplus_script(script_path):
                # Clean and format the CSV file
                if self._clean_csv_file(raw_file, output_csv):
                    rows_processed = total_rows
                    logger.info(f"Processed {rows_processed} rows in a single batch")
                    
                    # Compress if needed
                    if self.compress:
                        output_csv = self._compress_file(output_csv)
                
                # Remove raw file
                if os.path.exists(raw_file):
                    os.remove(raw_file)
            
            # Remove script file
            if os.path.exists(script_path):
                os.remove(script_path)
        
        logger.info(f"Extraction completed: {rows_processed} rows exported to CSV")
        return rows_processed

def main():
    # Example usage
    try:
        extractor = OracleSQLPlusExtractor('extract_config.ini')
        
        # Extract using a single query
        query1 = """
        SELECT e.employee_id, e.first_name, e.last_name, e.email, e.phone_number, 
               e.hire_date, e.job_id, e.salary, e.commission_pct, e.manager_id, e.department_id
        FROM employees e
        ORDER BY e.employee_id
        """
        extractor.extract_data(query1, "employees_extract")
        
        # Extract with date filtering
        query2 = """
        SELECT o.order_id, o.customer_id, o.order_date, o.order_total, o.status
        FROM orders o
        WHERE o.order_date >= TO_DATE('2024-01-01', 'YYYY-MM-DD')
        ORDER BY o.order_id
        """
        extractor.extract_data(query2, "recent_orders")
        
        # Extract large transaction table
        query3 = """
        SELECT t.transaction_id, t.account_id, t.transaction_date, 
               t.amount, t.transaction_type, t.description
        FROM transaction_history t
        WHERE t.transaction_date >= ADD_MONTHS(SYSDATE, -6)
        ORDER BY t.transaction_id
        """
        extractor.extract_data(query3, "transaction_history")
        
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
connect_string = localhost:1521/ORCL

[sqlplus]
sqlplus_path = sqlplus
; Use full path if needed, e.g. C:\oracle\product\19c\bin\sqlplus.exe

[paths]
output_dir = ./output
temp_dir = ./temp
log_dir = ./logs

[extract]
batch_size = 500000
compress = true

[format]
date_format = YYYY-MM-DD HH24:MI:SS
timestamp_format = YYYY-MM-DD HH24:MI:SS.FF3
null_placeholder = NULL

; Optional column formatting
[column_formats]
; Format strings for specific columns, e.g.:
; salary = 999,999,999.99
; price = 999,999.99




                extractor = OracleSQLPlusExtractor('extract_config.ini')
extractor.extract_data("SELECT * FROM employees", "employee_data")
