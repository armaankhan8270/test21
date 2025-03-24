def _create_sqlplus_script(self, query: str, output_file: Path, include_header: bool = True) -> Path:
    """Creates a SQL*Plus script with improved settings for handling large data and wide rows."""
    script_content = f"""WHENEVER SQLERROR EXIT SQL.SQLCODE
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING {"ON" if include_header else "OFF"}
SET PAGESIZE 0
SET ARRAYSIZE 1000
SET LOBPREFETCH 16384
SET LONG 1000000000
SET LONGCHUNKSIZE 1000000
SET LINESIZE 32767
SET TRIMSPOOL ON
SET WRAP OFF
SET COLSEP '{self.spool_delimiter}'
SET UNDERLINE OFF
-- Increase column length for large string values
SET SQLFORMAT CSV
SET SQLBLANKLINES ON
SET TRIMOUT ON
-- Set larger buffer sizes for character data
COLUMN SQL_TEXT FORMAT A32000
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3';
-- Set all columns to a large size
"""

    # Add dynamic COLUMN commands for all columns to handle wide data
    for col in self._get_column_names():
        script_content += f"COLUMN {col} FORMAT A32000\n"
    
    script_content += f"""
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
