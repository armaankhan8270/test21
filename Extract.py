#!/usr/bin/env python3
"""
Oracle Data Extractor - Extracts data from Oracle Database using SQLPlus 
and saves to CSV with proper encoding and error handling
"""

import os
import csv
import subprocess
import logging
import argparse
import tempfile
import concurrent.futures
from datetime import datetime
import pandas as pd
import re
from typing import List, Dict, Union, Tuple, Optional


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('oracle_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('OracleExtractor')


class OracleExtractor:
    """Class to handle Oracle data extraction via SQLPlus and export to CSV"""
    
    def __init__(
        self, 
        connection_string: str, 
        output_dir: str = 'output', 
        batch_size: int = 100000,
        max_workers: int = 4,
        delimiter: str = ',',
        quotechar: str = '"',
        encoding: str = 'utf-8',
        date_format: str = 'YYYY-MM-DD HH24:MI:SS.FF3'
    ):
        """
        Initialize the Oracle Extractor
        
        Args:
            connection_string: Oracle connection string (username/password@service)
            output_dir: Directory to save CSV files
            batch_size: Number of records to process in each batch
            max_workers: Maximum number of parallel workers for batch processing
            delimiter: CSV delimiter character
            quotechar: CSV quote character
            encoding: Character encoding for CSV files
            date_format: Oracle date format for timestamp conversion
        """
        self.connection_string = connection_string
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        self.date_format = date_format
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Validate connection
        self._validate_connection()
    
    def _validate_connection(self) -> None:
        """Test SQLPlus connection to ensure credentials are valid"""
        try:
            cmd = [
                'sqlplus', '-S', self.connection_string,
                '<<EOF\nSELECT 1 FROM DUAL;\nEXIT;\nEOF'
            ]
            result = subprocess.run(
                ' '.join(cmd), 
                shell=True,
                capture_output=True, 
                text=True
            )
            
            if "ORA-" in result.stdout or "SP2-" in result.stdout:
                logger.error(f"Connection test failed: {result.stdout}")
                raise ConnectionError(f"Oracle connection failed: {result.stdout}")
            
            logger.info("Oracle connection successful")
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {e}")
            raise
    
    def get_table_count(self, table_name: str) -> int:
        """
        Get the total count of records in a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            int: Count of records in the table
        """
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            cmd = [
                'sqlplus', '-S', self.connection_string,
                f'<<EOF\n{query};\nEXIT;\nEOF'
            ]
            
            result = subprocess.run(
                ' '.join(cmd), 
                shell=True,
                capture_output=True, 
                text=True
            )
            
            if "ORA-" in result.stdout:
                logger.error(f"Error getting table count: {result.stdout}")
                raise Exception(f"Oracle error: {result.stdout}")
            
            # Parse count from output
            count_match = re.search(r'\s*(\d+)\s*', result.stdout)
            if count_match:
                count = int(count_match.group(1))
                logger.info(f"Table {table_name} has {count} records")
                return count
            else:
                logger.error(f"Could not parse count from output: {result.stdout}")
                raise ValueError("Could not determine record count")
                
        except Exception as e:
            logger.error(f"Failed to get table count: {e}")
            raise
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get column information for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column dictionaries with name and data type
        """
        try:
            query = f"""
            SELECT 
                column_name, 
                data_type,
                data_length,
                data_precision,
                data_scale
            FROM 
                all_tab_columns 
            WHERE 
                table_name = UPPER('{table_name}')
            ORDER BY 
                column_id
            """
            
            cmd = [
                'sqlplus', '-S', self.connection_string,
                f'<<EOF\nSET PAGESIZE 0\nSET FEEDBACK OFF\nSET HEADING OFF\nSET ECHO OFF\n{query};\nEXIT;\nEOF'
            ]
            
            result = subprocess.run(
                ' '.join(cmd), 
                shell=True,
                capture_output=True, 
                text=True
            )
            
            if "ORA-" in result.stdout:
                logger.error(f"Error getting table columns: {result.stdout}")
                raise Exception(f"Oracle error: {result.stdout}")
            
            columns = []
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 2:
                        col_name = parts[0]
                        data_type = parts[1]
                        columns.append({
                            'name': col_name,
                            'type': data_type
                        })
            
            if not columns:
                logger.error(f"No columns found for table {table_name}")
                raise ValueError(f"No columns found for table {table_name}")
                
            logger.info(f"Retrieved {len(columns)} columns for table {table_name}")
            return columns
            
        except Exception as e:
            logger.error(f"Failed to get table columns: {e}")
            raise
            
    def _prepare_sqlplus_query(
        self, 
        table_name: str, 
        columns: List[Dict[str, str]], 
        offset: int, 
        limit: int
    ) -> str:
        """
        Prepare SQLPlus query with pagination, proper formatting for dates
        
        Args:
            table_name: Name of the table
            columns: List of column dictionaries
            offset: Offset for pagination
            limit: Limit for pagination
            
        Returns:
            str: SQL query with formatting
        """
        # Prepare column list with proper date formatting
        column_selects = []
        
        for col in columns:
            if col['type'].startswith('DATE') or col['type'].startswith('TIMESTAMP'):
                column_selects.append(
                    f"TO_CHAR({col['name']}, '{self.date_format}') AS {col['name']}"
                )
            else:
                column_selects.append(col['name'])
        
        column_list = ", ".join(column_selects)
        
        # Create paginated query
        query = f"""
        SELECT {column_list}
        FROM (
            SELECT a.*, ROWNUM rnum
            FROM (
                SELECT {column_list}
                FROM {table_name}
                ORDER BY ROWID
            ) a
            WHERE ROWNUM <= {offset + limit}
        )
        WHERE rnum > {offset}
        """
        
        return query
    
    def _execute_sqlplus_export(
        self, 
        table_name: str, 
        columns: List[Dict[str, str]], 
        offset: int, 
        limit: int,
        output_file: str
    ) -> bool:
        """
        Execute SQLPlus to export data to a temporary file
        
        Args:
            table_name: Name of the table
            columns: List of column dictionaries
            offset: Offset for pagination
            limit: Limit for pagination
            output_file: Path to output file
            
        Returns:
            bool: True if successful
        """
        query = self._prepare_sqlplus_query(table_name, columns, offset, limit)
        
        # Create temporary file for SQLPlus output
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            temp_filename = temp_file.name
        
        try:
            # Configure SQLPlus to output CSV format (using | as delimiter because commas can be in data)
            sql_script = f"""
            SET PAGESIZE 0
            SET FEEDBACK OFF
            SET HEADING OFF
            SET ECHO OFF
            SET TRIMSPOOL ON
            SET LINESIZE 32767
            SET LONG 1000000000
            SET LONGCHUNKSIZE 1000000000
            SET TERMOUT OFF
            SET COLSEP '|'
            SPOOL {temp_filename}
            {query};
            SPOOL OFF
            EXIT;
            """
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.sql') as script_file:
                script_file.write(sql_script.encode())
                script_path = script_file.name
            
            cmd = ['sqlplus', '-S', self.connection_string, f'@{script_path}']
            
            result = subprocess.run(
                cmd,
                capture_output=True, 
                text=True
            )
            
            if "ORA-" in result.stdout or "ORA-" in result.stderr:
                logger.error(f"Error executing SQLPlus: {result.stdout} {result.stderr}")
                raise Exception(f"Oracle error: {result.stdout} {result.stderr}")
            
            # Convert temp file with | delimiter to proper CSV
            self._convert_to_csv(temp_filename, output_file, len(columns))
            
            logger.info(f"Successfully exported batch {offset}-{offset+limit} to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error in SQLPlus export: {e}")
            return False
        finally:
            # Clean up temp files
            try:
                os.unlink(temp_filename)
                os.unlink(script_path)
            except Exception:
                pass
    
    def _convert_to_csv(
        self, 
        input_file: str, 
        output_file: str, 
        column_count: int
    ) -> None:
        """
        Convert SQLPlus output to proper CSV with handling for special characters
        
        Args:
            input_file: Path to input file (SQLPlus output with | delimiter)
            output_file: Path to output CSV file
            column_count: Number of columns expected
        """
        try:
            # Read data with | delimiter
            rows = []
            with open(input_file, 'r', encoding=self.encoding, errors='replace') as f:
                for line in f:
                    # Split on pipe but ensure we have the right number of columns
                    parts = line.strip().split('|')
                    
                    # Check if we have the expected number of columns
                    if len(parts) != column_count:
                        # This could be a row with embedded delimiters
                        # Try to reconstruct the correct data
                        logger.warning(f"Row has incorrect column count: {len(parts)} != {column_count}")
                        logger.warning(f"Problem row: {line}")
                        
                        # Skip malformed rows
                        continue
                    
                    rows.append(parts)
            
            # Write to CSV with proper escaping
            with open(output_file, 'w', newline='', encoding=self.encoding) as f:
                writer = csv.writer(
                    f, 
                    delimiter=self.delimiter, 
                    quotechar=self.quotechar, 
                    quoting=csv.QUOTE_MINIMAL
                )
                for row in rows:
                    writer.writerow(row)
                    
        except Exception as e:
            logger.error(f"Error converting to CSV: {e}")
            raise
    
    def _extract_batch(
        self, 
        table_name: str, 
        columns: List[Dict[str, str]], 
        batch_num: int
    ) -> Optional[str]:
        """
        Extract a single batch of data
        
        Args:
            table_name: Name of the table
            columns: List of column dictionaries
            batch_num: Batch number
            
        Returns:
            str: Path to output file if successful, None otherwise
        """
        offset = batch_num * self.batch_size
        batch_file = os.path.join(
            self.output_dir, 
            f"{table_name}_batch_{batch_num+1}.csv"
        )
        
        try:
            success = self._execute_sqlplus_export(
                table_name, 
                columns, 
                offset, 
                self.batch_size,
                batch_file
            )
            
            if success:
                return batch_file
            return None
        except Exception as e:
            logger.error(f"Error extracting batch {batch_num}: {e}")
            return None
    
    def extract_table(self, table_name: str) -> List[str]:
        """
        Extract all data from a table in batches
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[str]: List of output CSV files
        """
        try:
            start_time = datetime.now()
            logger.info(f"Starting extraction for table {table_name}")
            
            # Get column information
            columns = self.get_table_columns(table_name)
            
            # Get total count for batching
            total_count = self.get_table_count(table_name)
            num_batches = (total_count + self.batch_size - 1) // self.batch_size
            
            logger.info(f"Extracting {total_count} records in {num_batches} batches")
            
            output_files = []
            
            # Use ThreadPoolExecutor for parallel batch extraction
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self._extract_batch, table_name, columns, batch_num)
                    for batch_num in range(num_batches)
                ]
                
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            output_files.append(result)
                    except Exception as e:
                        logger.error(f"Error in batch extraction: {e}")
            
            # Check if all batches were successful
            if len(output_files) != num_batches:
                logger.warning(
                    f"Not all batches were successful. Expected {num_batches}, got {len(output_files)}"
                )
            
            # Create a combined file if requested
            combined_file = os.path.join(self.output_dir, f"{table_name}_combined.csv")
            self._combine_csv_files(output_files, combined_file, columns)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Extraction completed in {duration:.2f} seconds")
            
            return output_files
            
        except Exception as e:
            logger.error(f"Failed to extract table {table_name}: {e}")
            raise
    
    def _combine_csv_files(
        self, 
        csv_files: List[str], 
        output_file: str,
        columns: List[Dict[str, str]]
    ) -> None:
        """
        Combine multiple CSV files into one with headers
        
        Args:
            csv_files: List of CSV file paths
            output_file: Path to output combined file
            columns: List of column dictionaries
        """
        try:
            # Get column names
            column_names = [col['name'] for col in columns]
            
            # Create output file with headers
            with open(output_file, 'w', newline='', encoding=self.encoding) as outfile:
                writer = csv.writer(
                    outfile, 
                    delimiter=self.delimiter, 
                    quotechar=self.quotechar, 
                    quoting=csv.QUOTE_MINIMAL
                )
                
                # Write header
                writer.writerow(column_names)
                
                # Combine files
                for file_path in csv_files:
                    with open(file_path, 'r', encoding=self.encoding) as infile:
                        reader = csv.reader(
                            infile, 
                            delimiter=self.delimiter, 
                            quotechar=self.quotechar
                        )
                        for row in reader:
                            writer.writerow(row)
            
            logger.info(f"Combined {len(csv_files)} batch files into {output_file}")
            
        except Exception as e:
            logger.error(f"Error combining CSV files: {e}")
            raise
            
    def validate_csv(self, csv_file: str) -> Tuple[bool, Dict]:
        """
        Validate a CSV file for data integrity
        
        Args:
            csv_file: Path to CSV file
            
        Returns:
            Tuple of (is_valid, stats_dict)
        """
        try:
            stats = {
                'total_rows': 0,
                'empty_values': 0,
                'malformed_rows': 0
            }
            
            # Use pandas for validation
            df = pd.read_csv(
                csv_file, 
                encoding=self.encoding, 
                sep=self.delimiter, 
                quotechar=self.quotechar,
                low_memory=False,  # Avoid mixed type inference issues
                on_bad_lines='warn'  # Don't fail on bad lines
            )
            
            stats['total_rows'] = len(df)
            stats['empty_values'] = df.isna().sum().sum()
            
            # Check for timestamp format consistency if there are date columns
            date_cols = [col for col in df.columns if df[col].dtype == 'object' and 
                         df[col].astype(str).str.match(r'\d{4}-\d{2}-\d{2}').any()]
            
            for col in date_cols:
                invalid_dates = df[col].dropna().astype(str).str.match(
                    r'\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2}(\.\d{3})?)?'
                ) == False
                
                stats[f'invalid_dates_{col}'] = invalid_dates.sum()
            
            is_valid = (
                stats['malformed_rows'] == 0 and
                all(stats.get(f'invalid_dates_{col}', 0) == 0 for col in date_cols)
            )
            
            logger.info(f"Validation results for {csv_file}: {stats}")
            return is_valid, stats
            
        except Exception as e:
            logger.error(f"Error validating CSV file {csv_file}: {e}")
            return False, {'error': str(e)}


def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Extract data from Oracle to CSV')
    
    parser.add_argument(
        '--connection', '-c', 
        required=True, 
        help='Oracle connection string (username/password@service)'
    )
    
    parser.add_argument(
        '--table', '-t', 
        required=True, 
        help='Table name to extract'
    )
    
    parser.add_argument(
        '--output-dir', '-o', 
        default='output', 
        help='Output directory for CSV files'
    )
    
    parser.add_argument(
        '--batch-size', '-b', 
        type=int, 
        default=100000, 
        help='Batch size for extraction'
    )
    
    parser.add_argument(
        '--workers', '-w', 
        type=int, 
        default=4, 
        help='Number of parallel workers'
    )
    
    parser.add_argument(
        '--delimiter', '-d', 
        default=',', 
        help='CSV delimiter'
    )
    
    parser.add_argument(
        '--encoding', '-e', 
        default='utf-8', 
        help='CSV file encoding'
    )
    
    parser.add_argument(
        '--validate', 
        action='store_true', 
        help='Validate CSV files after extraction'
    )
    
    args = parser.parse_args()
    
    try:
        extractor = OracleExtractor(
            connection_string=args.connection,
            output_dir=args.output_dir,
            batch_size=args.batch_size,
            max_workers=args.workers,
            delimiter=args.delimiter,
            encoding=args.encoding
        )
        
        output_files = extractor.extract_table(args.table)
        
        logger.info(f"Extraction completed. {len(output_files)} CSV files created.")
        
        # Validate if requested
        if args.validate and output_files:
            combined_file = os.path.join(args.output_dir, f"{args.table}_combined.csv")
            is_valid, stats = extractor.validate_csv(combined_file)
            
            if is_valid:
                logger.info(f"Validation passed: {combined_file}")
            else:
                logger.warning(f"Validation failed: {combined_file}")
                logger.warning(f"Validation stats: {stats}")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())




python oracle_extractor.py --connection "username/password@service" --table "MY_TABLE" --batch-size 200000 --workers 8
