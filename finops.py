from flask import Flask, jsonify, request
import json
import os
from datetime import datetime
import snowflake.connector
import pandas as pd
from uuid import uuid4

app = Flask(__name__)

# Configuration
CACHE_DIR = "cache"
TABLES = [
    "QUERY_HISTORY_SUMMARY",
    "QUERY_DETAILS_COMPLETE",
    "WAREHOUSE_ANALYTICS_DASHBOARD_with_queries",
    "user_query_performance_report"
]

# Ensure cache directory exists
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def execute_and_cache_query(cursor, table_name):
    """Execute SELECT * query and cache results as JSON"""
    try:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        # Convert to DataFrame and then to JSON
        df = pd.DataFrame(results, columns=columns)
        cache_file = os.path.join(CACHE_DIR, f"{table_name}.json")
        
        # Save to JSON
        with open(cache_file, 'w') as f:
            json.dump(df.to_dict(orient='records'), f, default=str)
            
        return {"status": "success", "table": table_name, "rows": len(results)}
    except Exception as e:
        return {"status": "error", "table": table_name, "error": str(e)}

def get_users_with_query_count_by_warehouse(warehouse_id, query_type, query_ids):
    """Fetch user query counts for a warehouse and query type"""
    try:
        # Load QUERY_HISTORY_SUMMARY from cache
        cache_file = os.path.join(CACHE_DIR, "QUERY_HISTORY_SUMMARY.json")
        if not os.path.exists(cache_file):
            return pd.DataFrame(columns=["user_name", "query_count", "query_ids"])
        
        with open(cache_file, 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        
        # Map query_type to the corresponding QUERY_IDS field
        query_type_mapping = {
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
            "RUNNING_QUERIES": "running_queries_ids",
            "QUERIES_0_20_CENTS": "credit_0-20_cents_ids",
            "QUERIES_20_40_CENTS": "credit_20-40_cents_ids",
            "QUERIES_40_60_CENTS": "credit_40-60_cents_ids",
            "QUERIES_60_80_CENTS": "credit_60-80_cents_ids",
            "QUERIES_80_100_CENTS": "credit_80-100_cents_ids",
            "QUERIES_100_PLUS_CENTS": "credit_100_plus_cents_ids"
        }
        
        # Filter by warehouse_id and extract query_ids
        filtered_df = df[df["WAREHOUSE_ID"] == warehouse_id]
        if filtered_df.empty or query_type not in query_type_mapping:
            return pd.DataFrame(columns=["user_name", "query_count", "query_ids"])
        
        # Extract query IDs for the specified query_type
        query_ids_field = query_type_mapping[query_type]
        query_ids_list = filtered_df[query_ids_field].iloc[0] if query_ids_field in filtered_df else []
        query_ids_list = [qid for qid in query_ids_list if qid]  # Remove nulls
        
        # Filter QUERY_HISTORY_SUMMARY by query_ids
        summary_df = pd.DataFrame(data)
        result_df = summary_df[summary_df["QUERY_ID"].isin(query_ids_list)][["USER_NAME", "QUERY_ID"]]
        
        # Group by user_name
        result = result_df.groupby("USER_NAME").agg(
            query_count=pd.NamedAgg(column="QUERY_ID", aggfunc="count"),
            query_ids=pd.NamedAgg(column="QUERY_ID", aggfunc=list)
        ).reset_index()
        
        return result
    except Exception as e:
        print(f"Error in get_users_with_query_count_by_warehouse: {str(e)}")
        return pd.DataFrame(columns=["user_name", "query_count", "query_ids"])

def get_query_preview_by_ids(query_ids):
    """Fetch query previews for a list of query IDs"""
    try:
        cache_file = os.path.join(CACHE_DIR, "QUERY_DETAILS_COMPLETE.json")
        if not os.path.exists(cache_file):
            return pd.DataFrame(columns=["QUERY_ID", "QUERY_TEXT_PREVIEW"])
        
        with open(cache_file, 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        
        # Filter by query_ids and select relevant columns
        result = df[df["QUERY_ID"].isin(query_ids)][["QUERY_ID", "QUERY_TEXT"]]
        result = result.rename(columns={"QUERY_TEXT": "QUERY_TEXT_PREVIEW"})
        return result
    except Exception as e:
        print(f"Error in get_query_preview_by_ids: {str(e)}")
        return pd.DataFrame(columns=["QUERY_ID", "QUERY_TEXT_PREVIEW"])

def get_query_details_by_id(query_id):
    """Fetch full details for a single query ID"""
    try:
        cache_file = os.path.join(CACHE_DIR, "QUERY_DETAILS_COMPLETE.json")
        if not os.path.exists(cache_file):
            return pd.DataFrame(columns=[])
        
        with open(cache_file, 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        
        # Filter by query_id
        result = df[df["QUERY_ID"] == query_id]
        return result
    except Exception as e:
        print(f"Error in get_query_details_by_id: {str(e)}")
        return pd.DataFrame(columns=[])

@app.route('/refresh-cache', methods=['POST'])
def refresh_cache():
    """Execute queries for all tables and refresh cache"""
    results = []
    for table in TABLES:
        result = execute_and_cache_query(cursor, table)  # cursor needs to be defined
        results.append(result)
    return jsonify({
        "timestamp": datetime.utcnow().isoformat(),
        "results": results
    })

@app.route('/get-data/<table_name>', methods=['GET'])
def get_data(table_name):
    """Serve cached data for a specific table"""
    if table_name not in TABLES:
        return jsonify({"error": "Invalid table name"}), 400
    
    cache_file = os.path.join(CACHE_DIR, f"{table_name}.json")
    if not os.path.exists(cache_file):
        return jsonify({"error": "Data not cached. Please refresh cache first."}), 404
    
    try:
        with open(cache_file, 'r') as f:
            data = json.load(f)
        return jsonify({
            "table": table_name,
            "data": data,
            "row_count": len(data),
            "timestamp": datetime.utcnow().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/users-by-warehouse', methods=['POST'])
def users_by_warehouse():
    """Get user query counts for a warehouse and query type"""
    data = request.json
    warehouse_id = data.get('warehouse_id')
    query_type = data.get('query_type')
    query_ids = data.get('query_ids', [])
    
    if not warehouse_id or not query_type:
        return jsonify({"error": "warehouse_id and query_type are required"}), 400
    
    result_df = get_users_with_query_count_by_warehouse(warehouse_id, query_type, query_ids)
    return jsonify(result_df.to_dict(orient='records'))

@app.route('/query-previews', methods=['POST'])
def query_previews():
    """Get query previews for a list of query IDs"""
    data = request.json
    query_ids = data.get('query_ids', [])
    
    if not query_ids:
        return jsonify({"error": "query_ids are required"}), 400
    
    result_df = get_query_preview_by_ids(query_ids)
    return jsonify(result_df.to_dict(orient='records'))

@app.route('/query-details/<query_id>', methods=['GET'])
def query_details(query_id):
    """Get full details for a single query ID"""
    result_df = get_query_details_by_id(query_id)
    if result_df.empty:
        return jsonify({"error": "Query ID not found"}), 404
    return jsonify(result_df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
