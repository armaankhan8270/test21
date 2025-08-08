from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import json
import os
from datetime import datetime
import logging

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables to store cached data
cached_data = {}
snowflake_cursor = None

def set_snowflake_cursor(cursor):
    """Set the Snowflake cursor (to be called from main script)"""
    global snowflake_cursor
    snowflake_cursor = cursor

def execute_query_and_cache(table_name, query):
    """Execute query and cache results as JSON for faster retrieval"""
    global cached_data, snowflake_cursor
    
    try:
        logger.info(f"Executing query for {table_name}")
        snowflake_cursor.execute(query)
        results = snowflake_cursor.fetchall()
        columns = [desc[0] for desc in snowflake_cursor.description]
        
        # Convert to DataFrame
        df = pd.DataFrame(results, columns=columns)
        
        # Handle datetime and other non-JSON serializable types
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
        
        # Cache as JSON for faster API responses
        cached_data[table_name] = df.to_dict('records')
        logger.info(f"Cached {len(cached_data[table_name])} records for {table_name}")
        
        return True
    except Exception as e:
        logger.error(f"Error executing query for {table_name}: {str(e)}")
        return False

def refresh_all_tables():
    """Refresh all table data from Snowflake"""
    queries = {
        'query_history_summary': """
            SELECT * FROM QUERY_HISTORY_SUMMARY
        """,
        'query_details_complete': """
            SELECT * FROM QUERY_DETAILS_COMPLETE
        """,
        'warehouse_analytics': """
            SELECT * FROM WAREHOUSE_ANALYTICS_DASHBOARD_with_queries
        """,
        'user_performance_report': """
            SELECT * FROM user_query_performance_report
        """,
        # Add the 5th table query here if needed
        'account_summary': """
            SELECT 
                COUNT(*) as total_queries,
                COUNT(DISTINCT user_name) as unique_users,
                COUNT(DISTINCT warehouse_name) as unique_warehouses,
                SUM(credits_used_cloud_services) as total_credits,
                AVG(total_elapsed_time) as avg_execution_time,
                CURRENT_TIMESTAMP as last_updated
            FROM QUERY_HISTORY_SUMMARY
        """
    }
    
    success_count = 0
    for table_name, query in queries.items():
        if execute_query_and_cache(table_name, query):
            success_count += 1
    
    return success_count == len(queries)

# API Endpoints

@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    """Refresh all cached data from Snowflake"""
    if refresh_all_tables():
        return jsonify({"status": "success", "message": "All tables refreshed successfully"})
    else:
        return jsonify({"status": "error", "message": "Error refreshing some tables"}), 500

@app.route('/api/query-history-summary', methods=['GET'])
def get_query_history_summary():
    """Get query history summary data"""
    if 'query_history_summary' not in cached_data:
        return jsonify({"error": "Data not available. Please refresh first."}), 404
    return jsonify(cached_data['query_history_summary'])

@app.route('/api/query-details-complete', methods=['GET'])
def get_query_details_complete():
    """Get complete query details data"""
    if 'query_details_complete' not in cached_data:
        return jsonify({"error": "Data not available. Please refresh first."}), 404
    return jsonify(cached_data['query_details_complete'])

@app.route('/api/warehouse-analytics', methods=['GET'])
def get_warehouse_analytics():
    """Get warehouse analytics data"""
    if 'warehouse_analytics' not in cached_data:
        return jsonify({"error": "Data not available. Please refresh first."}), 404
    
    # Remove QUERY_IDS column for display
    data = cached_data['warehouse_analytics'].copy()
    for row in data:
        if 'QUERY_IDS' in row:
            del row['QUERY_IDS']
    
    return jsonify(data)

@app.route('/api/user-performance-report', methods=['GET'])
def get_user_performance_report():
    """Get user performance report data"""
    if 'user_performance_report' not in cached_data:
        return jsonify({"error": "Data not available. Please refresh first."}), 404
    
    # Remove sample_queries column for display
    data = cached_data['user_performance_report'].copy()
    for row in data:
        if 'sample_queries' in row:
            del row['sample_queries']
    
    return jsonify(data)

@app.route('/api/account-summary', methods=['GET'])
def get_account_summary():
    """Get account summary data"""
    if 'account_summary' not in cached_data:
        return jsonify({"error": "Data not available. Please refresh first."}), 404
    return jsonify(cached_data['account_summary'])

@app.route('/api/warehouse-drill-down', methods=['POST'])
def get_warehouse_drill_down():
    """Get drill-down data for warehouse queries"""
    data = request.json
    warehouse_id = data.get('warehouse_id')
    warehouse_name = data.get('warehouse_name')
    query_type = data.get('query_type')
    
    if not warehouse_id and not warehouse_name:
        return jsonify({"error": "warehouse_id or warehouse_name required"}), 400
    
    try:
        # Find the warehouse data
        warehouse_data = None
        for row in cached_data.get('warehouse_analytics', []):
            if (warehouse_id and str(row.get('WAREHOUSE_ID')) == str(warehouse_id)) or \
               (warehouse_name and row.get('WAREHOUSE_NAME') == warehouse_name):
                warehouse_data = row
                break
        
        if not warehouse_data:
            return jsonify({"error": "Warehouse not found"}), 404
        
        # Get original data with QUERY_IDS
        original_data = None
        for row in cached_data.get('warehouse_analytics', []):
            if (warehouse_id and str(row.get('WAREHOUSE_ID')) == str(warehouse_id)) or \
               (warehouse_name and row.get('WAREHOUSE_NAME') == warehouse_name):
                original_data = row
                break
        
        if not original_data or 'QUERY_IDS' not in original_data:
            return jsonify({"error": "Query IDs not found"}), 404
        
        query_ids_json = original_data['QUERY_IDS']
        if isinstance(query_ids_json, str):
            query_ids_data = json.loads(query_ids_json)
        else:
            query_ids_data = query_ids_json
        
        # Get query IDs for the specific type
        query_ids = query_ids_data.get(f"{query_type}_ids", [])
        query_ids = [qid for qid in query_ids if qid is not None]
        
        if not query_ids:
            return jsonify({"users": [], "query_count": 0})
        
        # Filter query details by these IDs and group by user
        user_queries = {}
        for query in cached_data.get('query_details_complete', []):
            if query.get('QUERY_ID') in query_ids:
                user_name = query.get('USER_NAME', 'Unknown')
                if user_name not in user_queries:
                    user_queries[user_name] = {
                        'user_name': user_name,
                        'query_count': 0,
                        'query_ids': []
                    }
                user_queries[user_name]['query_count'] += 1
                user_queries[user_name]['query_ids'].append(query.get('QUERY_ID'))
        
        return jsonify({
            "users": list(user_queries.values()),
            "total_queries": len(query_ids),
            "warehouse_name": warehouse_name or warehouse_data.get('WAREHOUSE_NAME'),
            "query_type": query_type
        })
        
    except Exception as e:
        logger.error(f"Error in warehouse drill-down: {str(e)}")
        return jsonify({"error": f"Internal error: {str(e)}"}), 500

@app.route('/api/user-drill-down', methods=['POST'])
def get_user_drill_down():
    """Get drill-down data for user queries"""
    data = request.json
    user_name = data.get('user_name')
    flag_type = data.get('flag_type')
    
    if not user_name or not flag_type:
        return jsonify({"error": "user_name and flag_type required"}), 400
    
    try:
        # Find user data with sample queries
        user_queries = []
        for row in cached_data.get('user_performance_report', []):
            if row.get('user_name') == user_name and row.get('flag_type') == flag_type:
                sample_queries_json = row.get('sample_queries', [])
                if isinstance(sample_queries_json, str):
                    sample_queries = json.loads(sample_queries_json)
                else:
                    sample_queries = sample_queries_json
                
                for query in sample_queries:
                    if query and isinstance(query, dict):
                        user_queries.append({
                            'query_id': query.get('query_id'),
                            'query_text_preview': query.get('query_text', '')[:100] + '...' if len(query.get('query_text', '')) > 100 else query.get('query_text', ''),
                            'execution_time_ms': query.get('execution_time_ms'),
                            'bytes_scanned': query.get('bytes_scanned'),
                            'start_time': query.get('start_time'),
                            'warehouse_size': query.get('warehouse_size')
                        })
                break
        
        return jsonify({
            "queries": user_queries,
            "user_name": user_name,
            "flag_type": flag_type,
            "query_count": len(user_queries)
        })
        
    except Exception as e:
        logger.error(f"Error in user drill-down: {str(e)}")
        return jsonify({"error": f"Internal error: {str(e)}"}), 500

@app.route('/api/query-details/<query_id>', methods=['GET'])
def get_query_details(query_id):
    """Get complete details for a specific query"""
    try:
        for query in cached_data.get('query_details_complete', []):
            if str(query.get('QUERY_ID')) == str(query_id):
                return jsonify(query)
        
        return jsonify({"error": "Query not found"}), 404
        
    except Exception as e:
        logger.error(f"Error getting query details: {str(e)}")
        return jsonify({"error": f"Internal error: {str(e)}"}), 500

@app.route('/api/queries-by-ids', methods=['POST'])
def get_queries_by_ids():
    """Get query details for multiple query IDs"""
    data = request.json
    query_ids = data.get('query_ids', [])
    
    if not query_ids:
        return jsonify({"error": "query_ids required"}), 400
    
    try:
        matching_queries = []
        for query in cached_data.get('query_details_complete', []):
            if query.get('QUERY_ID') in query_ids:
                matching_queries.append({
                    'query_id': query.get('QUERY_ID'),
                    'query_text_preview': query.get('QUERY_TEXT', '')[:100] + '...' if len(query.get('QUERY_TEXT', '')) > 100 else query.get('QUERY_TEXT', ''),
                    'execution_time_ms': query.get('TOTAL_ELAPSED_TIME'),
                    'user_name': query.get('USER_NAME'),
                    'warehouse_name': query.get('WAREHOUSE_NAME'),
                    'start_time': query.get('START_TIME'),
                    'execution_status': query.get('EXECUTION_STATUS')
                })
        
        return jsonify({"queries": matching_queries})
        
    except Exception as e:
        logger.error(f"Error getting queries by IDs: {str(e)}")
        return jsonify({"error": f"Internal error: {str(e)}"}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "cached_tables": list(cached_data.keys()),
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
