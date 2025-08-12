from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
from typing import Dict, List, Any, Optional
import json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

# Global variables to store cached data
cached_users_data = []
cached_warehouses_data = []
cached_queries_data = []
cursor = None  # Will be set when you initialize

# KPI column mappings
USER_CLICKABLE_COLS = [
    'SPILLED_QUERIES', 'OVER_PROVISIONED_QUERIES', 'PEAK_HOUR_LONG_RUNNING_QUERIES',
    'SELECT_STAR_QUERIES', 'UNPARTITIONED_SCAN_QUERIES', 'REPEATED_QUERIES',
    'COMPLEX_JOIN_QUERIES', 'ZERO_RESULT_QUERIES', 'HIGH_COMPILE_QUERIES'
]

WAREHOUSE_CLICKABLE_COLS = [
    'QUERIES_1_10_SEC', 'QUERIES_10_20_SEC', 'QUERIES_20_60_SEC', 'QUERIES_1_3_MIN',
    'QUERIES_3_5_MIN', 'QUERIES_5_PLUS_MIN', 'QUEUED_1_2_MIN', 'QUEUED_2_5_MIN',
    'QUEUED_5_10_MIN', 'QUEUED_10_20_MIN', 'QUEUED_20_PLUS_MIN', 'QUERIES_SPILLED_LOCAL',
    'QUERIES_SPILLED_REMOTE', 'FAILED_QUERIES', 'SUCCESSFUL_QUERIES', 'OVER_PROVISIONED_QUERIES',
    'PEAK_HOUR_LONG_RUNNING_QUERIES', 'SELECT_STAR_QUERIES', 'UNPARTITIONED_SCAN_QUERIES',
    'COMPLEX_JOIN_QUERIES', 'FAILED_CANCELLED_QUERIES', 'ZERO_RESULT_QUERIES',
    'HIGH_COMPILE_QUERIES', 'SPILLED_QUERIES'
]

def set_cursor(db_cursor):
    """Set the database cursor for executing queries"""
    global cursor
    cursor = db_cursor

def execute_query(query: str, params: tuple = None) -> List[Dict[str, Any]]:
    """
    Execute a query and return results as list of dictionaries
    """
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # Fetch all results
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        results = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                column_name = columns[i] if i < len(columns) else f"col_{i}"
                # Handle datetime objects
                if isinstance(value, datetime):
                    row_dict[column_name] = value.isoformat()
                else:
                    row_dict[column_name] = value
            results.append(row_dict)
        
        return results
        
    except Exception as e:
        logger.error(f"Database query error: {str(e)}")
        logger.error(f"Query: {query}")
        if params:
            logger.error(f"Parameters: {params}")
        raise

def load_cached_data():
    """Load and cache data from all three main tables"""
    global cached_users_data, cached_warehouses_data, cached_queries_data
    
    try:
        logger.info("Loading cached data from database tables...")
        
        # Load users data
        users_query = "SELECT * FROM user_depth_analysis_new ORDER BY WEIGHTED_SCORE DESC"
        cached_users_data = execute_query(users_query)
        logger.info(f"Loaded {len(cached_users_data)} users")
        
        # Load warehouses data  
        warehouses_query = "SELECT * FROM WAREHOUSE_ANALYTICS_DASHBOARD_with_queries_new ORDER BY TOTAL_QUERIES DESC"
        cached_warehouses_data = execute_query(warehouses_query)
        logger.info(f"Loaded {len(cached_warehouses_data)} warehouses")
        
        # Load queries data (limit to recent queries to avoid memory issues)
        queries_query = "SELECT * FROM query_360_analytics_enhanced_tbl ORDER BY START_TIME DESC"
        cached_queries_data = execute_query(queries_query)
        logger.info(f"Loaded {len(cached_queries_data)} queries")
        
        logger.info("Successfully cached all data")
        
    except Exception as e:
        logger.error(f"Error loading cached data: {str(e)}")
        raise

# ============================================================================
# BASIC DATA ENDPOINTS
# ============================================================================

@app.route('/users', methods=['GET'])
def get_users():
    """Get all users data"""
    try:
        logger.info("Fetching users data")
        return jsonify({
            'success': True,
            'data': cached_users_data,
            'count': len(cached_users_data),
            'message': f'Retrieved {len(cached_users_data)} users'
        })
    
    except Exception as e:
        logger.error(f"Error in get_users: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Failed to fetch users data',
            'message': str(e)
        }), 500

@app.route('/warehouses', methods=['GET'])
def get_warehouses():
    """Get all warehouses data"""
    try:
        logger.info("Fetching warehouses data")
        return jsonify({
            'success': True,
            'data': cached_warehouses_data,
            'count': len(cached_warehouses_data),
            'message': f'Retrieved {len(cached_warehouses_data)} warehouses'
        })
    
    except Exception as e:
        logger.error(f"Error in get_warehouses: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Failed to fetch warehouses data',
            'message': str(e)
        }), 500

@app.route('/query-details/<string:query_id>', methods=['GET'])
def get_query_details(query_id: str):
    """Get specific query details by query ID"""
    try:
        logger.info(f"Fetching query details for query_id: {query_id}")
        
        # Find query in cached data
        query_details = None
        for query in cached_queries_data:
            if query.get('QUERY_ID') == query_id:
                query_details = query
                break
        
        if not query_details:
            return jsonify({
                'success': False,
                'error': 'Query not found',
                'message': f'No query found with ID: {query_id}'
            }), 404
        
        return jsonify({
            'success': True,
            'data': query_details,
            'message': f'Retrieved query details for {query_id}'
        })
    
    except Exception as e:
        logger.error(f"Error in get_query_details: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Failed to fetch query details',
            'message': str(e)
        }), 500

# ============================================================================
# DRILL-DOWN ENDPOINTS
# ============================================================================

@app.route('/users/drill-down', methods=['POST'])
def user_drill_down():
    """
    Drill down into user queries by KPI column
    Expected payload: {"username": "USER1", "selected_kpi_column": "SPILLED_QUERIES"}
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'Invalid request',
                'message': 'JSON payload required'
            }), 400
        
        username = data.get('username')
        selected_kpi_column = data.get('selected_kpi_column')
        
        if not username or not selected_kpi_column:
            return jsonify({
                'success': False,
                'error': 'Missing parameters',
                'message': 'Both username and selected_kpi_column are required'
            }), 400
        
        if selected_kpi_column not in USER_CLICKABLE_COLS and selected_kpi_column not in WAREHOUSE_CLICKABLE_COLS:
            return jsonify({
                'success': False,
                'error': 'Invalid KPI column',
                'message': f'selected_kpi_column must be one of: {USER_CLICKABLE_COLS + WAREHOUSE_CLICKABLE_COLS}'
            }), 400
        
        logger.info(f"User drill-down: username={username}, kpi={selected_kpi_column}")
        
        # Filter queries for the user where the selected KPI column is True
        filtered_queries = []
        for query in cached_queries_data:
            if (query.get('USER_NAME') == username and 
                query.get(selected_kpi_column) is True):
                filtered_queries.append(query)
        
        return jsonify({
            'success': True,
            'data': filtered_queries,
            'count': len(filtered_queries),
            'filters': {
                'username': username,
                'selected_kpi_column': selected_kpi_column
            },
            'message': f'Found {len(filtered_queries)} queries for user {username} with {selected_kpi_column}=True'
        })
    
    except Exception as e:
        logger.error(f"Error in user_drill_down: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Failed to perform user drill-down',
            'message': str(e)
        }), 500

@app.route('/warehouses/drill-down', methods=['POST'])
def warehouse_drill_down():
    """
    Drill down into warehouse queries by KPI column, grouped by user
    Expected payload: {"warehouse_name": "WH1", "selected_kpi_column": "SPILLED_QUERIES"}
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'Invalid request',
                'message': 'JSON payload required'
            }), 400
        
        warehouse_name = data.get('warehouse_name')
        selected_kpi_column = data.get('selected_kpi_column')
        
        if not warehouse_name or not selected_kpi_column:
            return jsonify({
                'success': False,
                'error': 'Missing parameters',
                'message': 'Both warehouse_name and selected_kpi_column are required'
            }), 400
        
        if selected_kpi_column not in WAREHOUSE_CLICKABLE_COLS and selected_kpi_column not in USER_CLICKABLE_COLS:
            return jsonify({
                'success': False,
                'error': 'Invalid KPI column',
                'message': f'selected_kpi_column must be one of: {WAREHOUSE_CLICKABLE_COLS + USER_CLICKABLE_COLS}'
            }), 400
        
        logger.info(f"Warehouse drill-down: warehouse={warehouse_name}, kpi={selected_kpi_column}")
        
        # Filter and group queries
        user_query_counts = {}
        total_queries = 0
        
        for query in cached_queries_data:
            if (query.get('WAREHOUSE_NAME') == warehouse_name and 
                query.get(selected_kpi_column) is True):
                
                username = query.get('USER_NAME', 'Unknown')
                user_query_counts[username] = user_query_counts.get(username, 0) + 1
                total_queries += 1
        
        # Format results
        results = []
        for username, query_count in user_query_counts.items():
            results.append({
                'warehouse_name': warehouse_name,
                'selected_kpi_column': selected_kpi_column,
                'username': username,
                'query_count': query_count
            })
        
        # Sort by query count descending
        results.sort(key=lambda x: x['query_count'], reverse=True)
        
        return jsonify({
            'success': True,
            'data': results,
            'count': len(results),
            'total_queries': total_queries,
            'filters': {
                'warehouse_name': warehouse_name,
                'selected_kpi_column': selected_kpi_column
            },
            'message': f'Found {total_queries} queries across {len(results)} users for warehouse {warehouse_name} with {selected_kpi_column}=True'
        })
    
    except Exception as e:
        logger.error(f"Error in warehouse_drill_down: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Failed to perform warehouse drill-down',
            'message': str(e)
        }), 500

@app.route('/queries/batch', methods=['POST'])
def get_queries_batch():
    """
    Get multiple queries by their IDs
    Expected payload: {"query_ids": ["query1", "query2", "query3"]}
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'Invalid request',
                'message': 'JSON payload required'
            }), 400
        
        query_ids = data.get('query_ids', [])
        
        if not isinstance(query_ids, list):
            return jsonify({
                'success': False,
                'error': 'Invalid parameter',
                'message': 'query_ids must be an array'
            }), 400
        
        if not query_ids:
            return jsonify({
                'success': False,
                'error': 'Empty parameter',
                'message': 'query_ids array cannot be empty'
            }), 400
        
        logger.info(f"Batch query lookup for {len(query_ids)} query IDs")
        
        # Find all matching queries
        found_queries = []
        query_ids_set = set(query_ids)  # For faster lookup
        
        for query in cached_queries_data:
            if query.get('QUERY_ID') in query_ids_set:
                found_queries.append(query)
        
        # Find missing query IDs
        found_query_ids = {query.get('QUERY_ID') for query in found_queries}
        missing_query_ids = [qid for qid in query_ids if qid not in found_query_ids]
        
        return jsonify({
            'success': True,
            'data': found_queries,
            'requested_count': len(query_ids),
            'found_count': len(found_queries),
            'missing_count': len(missing_query_ids),
            'missing_query_ids': missing_query_ids,
            'message': f'Found {len(found_queries)} out of {len(query_ids)} requested queries'
        })
    
    except Exception as e:
        logger.error(f"Error in get_queries_batch: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Failed to fetch batch queries',
            'message': str(e)
        }), 500

# ============================================================================
# UTILITY ENDPOINTS
# ============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        return jsonify({
            'success': True,
            'message': 'Analytics API is running',
            'cached_data': {
                'users': len(cached_users_data),
                'warehouses': len(cached_warehouses_data),
                'queries': len(cached_queries_data)
            },
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in health_check: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Health check failed',
            'message': str(e)
        }), 500

@app.route('/refresh-cache', methods=['POST'])
def refresh_cache():
    """Manually refresh cached data"""
    try:
        logger.info("Manually refreshing cached data")
        load_cached_data()
        
        return jsonify({
            'success': True,
            'message': 'Cache refreshed successfully',
            'cached_data': {
                'users': len(cached_users_data),
                'warehouses': len(cached_warehouses_data),  
                'queries': len(cached_queries_data)
            },
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error in refresh_cache: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Failed to refresh cache',
            'message': str(e)
        }), 500

@app.route('/kpi-columns', methods=['GET'])
def get_kpi_columns():
    """Get available KPI columns for drill-down"""
    try:
        return jsonify({
            'success': True,
            'data': {
                'user_clickable_cols': USER_CLICKABLE_COLS,
                'warehouse_clickable_cols': WAREHOUSE_CLICKABLE_COLS,
                'all_clickable_cols': list(set(USER_CLICKABLE_COLS + WAREHOUSE_CLICKABLE_COLS))
            },
            'message': 'Retrieved available KPI columns'
        })
    
    except Exception as e:
        logger.error(f"Error in get_kpi_columns: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Failed to fetch KPI columns',
            'message': str(e)
        }), 500

# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Endpoint not found',
        'message': 'The requested endpoint does not exist'
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        'success': False,
        'error': 'Method not allowed',
        'message': 'The HTTP method is not allowed for this endpoint'
    }), 405

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'message': 'An unexpected error occurred'
    }), 500

# ============================================================================
# INITIALIZATION FUNCTION
# ============================================================================

def initialize_app(db_cursor):
    """
    Initialize the Flask app with database cursor and load cached data
    Call this function after setting up your Snowflake connection
    """
    try:
        logger.info("Initializing Analytics API...")
        
        # Set the cursor
        set_cursor(db_cursor)
        
        # Load cached data
        load_cached_data()
        
        logger.info("Analytics API initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize app: {str(e)}")
        return False

if __name__ == '__main__':
    # For development - you'll need to set up cursor before running
    print("Analytics API Server")
    print("Available endpoints:")
    print("  GET  /health")
    print("  GET  /users")
    print("  GET  /warehouses")  
    print("  GET  /query-details/<query_id>")
    print("  POST /users/drill-down")
    print("  POST /warehouses/drill-down")
    print("  POST /queries/batch")
    print("  POST /refresh-cache")
    print("  GET  /kpi-columns")
    print("\nCall initialize_app(cursor) before starting the server")
    
    # Uncomment to run in development
    # app.run(debug=True, host='0.0.0.0', port=5000)
