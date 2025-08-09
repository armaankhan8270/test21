from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_caching import Cache
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configure caching
app.config['CACHE_TYPE'] = 'simple'
app.config['CACHE_DEFAULT_TIMEOUT'] = 300  # 5 minutes
cache = Cache(app)

# Global variable to store Snowflake cursor
snowflake_cursor = None

def set_snowflake_cursor(cursor):
    """Set the Snowflake cursor for database operations"""
    global snowflake_cursor
    snowflake_cursor = cursor
    logger.info("Snowflake cursor initialized")

def execute_query(query: str, params: tuple = None) -> List[Dict]:
    """Execute a query safely with error handling"""
    try:
        if snowflake_cursor is None:
            raise Exception("Snowflake cursor not initialized")
        
        if params:
            snowflake_cursor.execute(query, params)
        else:
            snowflake_cursor.execute(query)
        
        columns = [desc[0] for desc in snowflake_cursor.description]
        results = []
        
        for row in snowflake_cursor.fetchall():
            row_dict = dict(zip(columns, row))
            # Handle datetime objects
            for key, value in row_dict.items():
                if isinstance(value, datetime):
                    row_dict[key] = value.isoformat()
            results.append(row_dict)
        
        return results
    
    except Exception as e:
        logger.error(f"Database query error: {str(e)}")
        raise

# API ENDPOINTS

@app.route('/api/warehouses', methods=['GET'])
@cache.cached(timeout=300)
def get_warehouses():
    """Get complete warehouse analytics data"""
    try:
        query = "SELECT * FROM WAREHOUSE_ANALYTICS_DASHBOARD_with_queries ORDER BY WAREHOUSE_NAME"
        results = execute_query(query)
        
        logger.info(f"Retrieved {len(results)} warehouse records")
        return jsonify({
            "status": "success",
            "data": results,
            "count": len(results),
            "timestamp": datetime.utcnow().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error fetching warehouses: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to fetch warehouse data",
            "error": str(e)
        }), 500

@app.route('/api/users', methods=['GET'])
@cache.cached(timeout=300)
def get_users():
    """Get complete user performance data"""
    try:
        query = "SELECT * FROM user_query_performance_report ORDER BY user_name"
        results = execute_query(query)
        
        # Parse query_samples JSON if it's a string
        for result in results:
            if 'query_samples' in result and isinstance(result['query_samples'], str):
                try:
                    result['query_samples'] = json.loads(result['query_samples'])
                except json.JSONDecodeError:
                    result['query_samples'] = {}
        
        logger.info(f"Retrieved {len(results)} user records")
        return jsonify({
            "status": "success",
            "data": results,
            "count": len(results),
            "timestamp": datetime.utcnow().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error fetching users: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to fetch user data",
            "error": str(e)
        }), 500

@app.route('/api/query/<query_id>', methods=['GET'])
def get_query_details(query_id: str):
    """Get complete query details for a specific query ID"""
    try:
        if not query_id:
            return jsonify({
                "status": "error",
                "message": "Query ID is required"
            }), 400
        
        query = "SELECT * FROM QUERY_DETAILS_COMPLETE WHERE QUERY_ID = %s"
        results = execute_query(query, (query_id,))
        
        if not results:
            return jsonify({
                "status": "error",
                "message": "Query not found"
            }), 404
        
        logger.info(f"Retrieved query details for {query_id}")
        return jsonify({
            "status": "success",
            "data": results[0],
            "timestamp": datetime.utcnow().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error fetching query details: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to fetch query details",
            "error": str(e)
        }), 500

@app.route('/api/queries/batch', methods=['POST'])
def get_queries_batch():
    """Get multiple query summaries by IDs"""
    try:
        data = request.get_json()
        if not data or 'query_ids' not in data:
            return jsonify({
                "status": "error",
                "message": "query_ids array is required"
            }), 400
        
        query_ids = data['query_ids']
        if not isinstance(query_ids, list) or not query_ids:
            return jsonify({
                "status": "error",
                "message": "query_ids must be a non-empty array"
            }), 400
        
        # Create placeholders for IN clause
        placeholders = ','.join(['%s'] * len(query_ids))
        query = f"SELECT * FROM QUERY_HISTORY_SUMMARY WHERE QUERY_ID IN ({placeholders}) ORDER BY START_TIME DESC"
        
        results = execute_query(query, tuple(query_ids))
        
        logger.info(f"Retrieved {len(results)} query summaries for {len(query_ids)} requested IDs")
        return jsonify({
            "status": "success",
            "data": results,
            "count": len(results),
            "requested_count": len(query_ids),
            "timestamp": datetime.utcnow().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error fetching batch queries: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to fetch query batch",
            "error": str(e)
        }), 500

@app.route('/api/warehouse/drill-down', methods=['POST'])
def warehouse_drill_down():
    """Get drill-down analysis for warehouse column"""
    try:
        data = request.get_json()
        if not data or 'warehouse_name' not in data or 'column_selected' not in data:
            return jsonify({
                "status": "error",
                "message": "warehouse_name and column_selected are required"
            }), 400
        
        warehouse_name = data['warehouse_name']
        column_selected = data['column_selected']
        
        # Map column names to query_ids keys
        column_mapping = {
            "QUERIES_1_10_SEC": "1-10_sec_ids",
            "QUERIES_10_20_SEC": "10-20_sec_ids",
            "QUERIES_20_60_SEC": "20-60_sec_ids",
            "QUERIES_1_3_MIN": "1-3_min_ids",
            "QUERIES_3_5_MIN": "3-5_min_ids",
            "QUERIES_5_PLUS_MIN": "5_plus_min_ids",
            "QUEUED_1_2_MIN": "queued_1-2_min_ids",
            "QUEUED_2_5_MIN": "queued_2-5_min_ids",
            "QUEUED_5_10_MIN": "queued_5-10_min_ids",
            "QUEUED_10_20_MIN": "queued_10-20_min_ids",
            "QUEUED_20_PLUS_MIN": "queued_20_plus_min_ids",
            "QUERIES_SPILLED_LOCAL": "spilled_local_ids",
            "QUERIES_SPILLED_REMOTE": "spilled_remote_ids",
            "FAILED_QUERIES": "failed_queries_ids",
            "SUCCESSFUL_QUERIES": "successful_queries_ids",
            "QUERIES_0_20_CENTS": "credit_0-20_cents_ids",
            "QUERIES_20_40_CENTS": "credit_20-40_cents_ids",
            "QUERIES_40_60_CENTS": "credit_40-60_cents_ids",
            "QUERIES_60_80_CENTS": "credit_60-80_cents_ids",
            "QUERIES_80_100_CENTS": "credit_80-100_cents_ids",
            "QUERIES_100_PLUS_CENTS": "credit_100_plus_cents_ids"
        }
        
        if column_selected not in column_mapping:
            return jsonify({
                "status": "error",
                "message": f"Invalid column selected: {column_selected}"
            }), 400
        
        # Get warehouse data with query IDs
        warehouse_query = "SELECT QUERY_IDS FROM WAREHOUSE_ANALYTICS_DASHBOARD_with_queries WHERE WAREHOUSE_NAME = %s"
        warehouse_results = execute_query(warehouse_query, (warehouse_name,))
        
        if not warehouse_results:
            return jsonify({
                "status": "error",
                "message": "Warehouse not found"
            }), 404
        
        query_ids_json = warehouse_results[0]['QUERY_IDS']
        if isinstance(query_ids_json, str):
            query_ids_data = json.loads(query_ids_json)
        else:
            query_ids_data = query_ids_json
        
        # Get specific column's query IDs
        ids_key = column_mapping[column_selected]
        query_ids = query_ids_data.get(ids_key, [])
        
        if not query_ids:
            return jsonify({
                "status": "success",
                "data": {
                    "warehouse_name": warehouse_name,
                    "column_selected": column_selected,
                    "user_analysis": []
                }
            })
        
        # Get query summaries and group by user
        placeholders = ','.join(['%s'] * len(query_ids))
        queries_query = f"SELECT USER_NAME, QUERY_ID FROM QUERY_HISTORY_SUMMARY WHERE QUERY_ID IN ({placeholders})"
        queries_results = execute_query(queries_query, tuple(query_ids))
        
        # Group by user
        user_analysis = {}
        for result in queries_results:
            username = result['USER_NAME']
            query_id = result['QUERY_ID']
            
            if username not in user_analysis:
                user_analysis[username] = {
                    "username": username,
                    "query_count": 0,
                    "query_ids": []
                }
            
            user_analysis[username]["query_count"] += 1
            user_analysis[username]["query_ids"].append(query_id)
        
        # Convert to list and sort by query count
        user_analysis_list = sorted(user_analysis.values(), key=lambda x: x["query_count"], reverse=True)
        
        logger.info(f"Drill-down analysis for {warehouse_name}.{column_selected}: {len(user_analysis_list)} users")
        return jsonify({
            "status": "success",
            "data": {
                "warehouse_name": warehouse_name,
                "column_selected": column_selected,
                "user_analysis": user_analysis_list
            },
            "timestamp": datetime.utcnow().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error in drill-down analysis: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to perform drill-down analysis",
            "error": str(e)
        }), 500

@app.route('/api/user/drill-down', methods=['POST'])
def user_drill_down():
    """Get drill-down analysis for user column"""
    try:
        data = request.get_json()
        if not data or 'user_name' not in data or 'column_selected' not in data:
            return jsonify({
                "status": "error",
                "message": "user_name and column_selected are required"
            }), 400
        
        user_name = data['user_name']
        column_selected = data['column_selected']
        
        # Get user data
        user_query = "SELECT query_samples FROM user_query_performance_report WHERE user_name = %s"
        user_results = execute_query(user_query, (user_name,))
        
        if not user_results:
            return jsonify({
                "status": "error",
                "message": "User not found"
            }), 404
        
        query_samples = user_results[0]['query_samples']
        if isinstance(query_samples, str):
            query_samples = json.loads(query_samples)
        
        # Get queries for the selected column
        queries = query_samples.get(column_selected, [])
        
        logger.info(f"User drill-down for {user_name}.{column_selected}: {len(queries)} queries")
        return jsonify({
            "status": "success",
            "data": {
                "user_name": user_name,
                "column_selected": column_selected,
                "queries": queries
            },
            "timestamp": datetime.utcnow().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error in user drill-down: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to perform user drill-down",
            "error": str(e)
        }), 500

@app.route('/api/cache/refresh', methods=['POST'])
def refresh_cache():
    """Refresh all cached data"""
    try:
        cache.clear()
        logger.info("Cache cleared successfully")
        return jsonify({
            "status": "success",
            "message": "Cache refreshed successfully",
            "refreshed_at": datetime.utcnow().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error refreshing cache: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to refresh cache",
            "error": str(e)
        }), 500

@app.route('/api/cache/status', methods=['GET'])
def get_cache_status():
    """Get cache status and statistics"""
    try:
        return jsonify({
            "status": "success",
            "cache_type": app.config['CACHE_TYPE'],
            "default_timeout": app.config['CACHE_DEFAULT_TIMEOUT'],
            "timestamp": datetime.utcnow().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error getting cache status: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to get cache status",
            "error": str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "cursor_initialized": snowflake_cursor is not None
    })

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "message": "Endpoint not found"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "status": "error",
        "message": "Internal server error"
    }), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
