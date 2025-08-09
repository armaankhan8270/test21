import React, { useState, useEffect, useMemo } from 'react';
import { Search, Filter, Download, RefreshCw, ChevronDown, ChevronUp, Eye, ArrowLeft } from 'lucide-react';

// API Service
const API_BASE_URL = 'http://localhost:5000/api';

const apiService = {
  async fetchWarehouses() {
    const response = await fetch(`${API_BASE_URL}/warehouses`);
    if (!response.ok) throw new Error('Failed to fetch warehouses');
    return response.json();
  },
  
  async fetchUsers() {
    const response = await fetch(`${API_BASE_URL}/users`);
    if (!response.ok) throw new Error('Failed to fetch users');
    return response.json();
  },
  
  async fetchQueryDetails(queryId) {
    const response = await fetch(`${API_BASE_URL}/query/${queryId}`);
    if (!response.ok) throw new Error('Failed to fetch query details');
    return response.json();
  },
  
  async fetchQueriesBatch(queryIds) {
    const response = await fetch(`${API_BASE_URL}/queries/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query_ids: queryIds })
    });
    if (!response.ok) throw new Error('Failed to fetch queries batch');
    return response.json();
  },
  
  async warehouseDrillDown(warehouseName, columnSelected) {
    const response = await fetch(`${API_BASE_URL}/warehouse/drill-down`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ warehouse_name: warehouseName, column_selected: columnSelected })
    });
    if (!response.ok) throw new Error('Failed to perform warehouse drill-down');
    return response.json();
  },
  
  async userDrillDown(userName, columnSelected) {
    const response = await fetch(`${API_BASE_URL}/user/drill-down`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_name: userName, column_selected: columnSelected })
    });
    if (!response.ok) throw new Error('Failed to perform user drill-down');
    return response.json();
  }
};

// Enhanced Data Table Component
const DataTable = ({ 
  data = [], 
  columns = [], 
  onCellClick, 
  clickableColumns = [],
  isLoading = false,
  title = "Data Table"
}) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 50;

  // Filter and search data
  const filteredData = useMemo(() => {
    if (!searchTerm) return data;
    
    return data.filter(row => 
      Object.values(row).some(value => 
        String(value).toLowerCase().includes(searchTerm.toLowerCase())
      )
    );
  }, [data, searchTerm]);

  // Sort data
  const sortedData = useMemo(() => {
    if (!sortConfig.key) return filteredData;
    
    return [...filteredData].sort((a, b) => {
      const aValue = a[sortConfig.key];
      const bValue = b[sortConfig.key];
      
      if (aValue === bValue) return 0;
      
      const comparison = aValue < bValue ? -1 : 1;
      return sortConfig.direction === 'desc' ? -comparison : comparison;
    });
  }, [filteredData, sortConfig]);

  // Paginate data
  const paginatedData = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    return sortedData.slice(startIndex, startIndex + itemsPerPage);
  }, [sortedData, currentPage]);

  const totalPages = Math.ceil(sortedData.length / itemsPerPage);

  const handleSort = (key) => {
    setSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc'
    }));
  };

  const handleCellClick = (rowData, columnKey, value) => {
    if (clickableColumns.includes(columnKey) && onCellClick) {
      onCellClick(rowData, columnKey, value);
    }
  };

  const exportToCSV = () => {
    const csv = [
      columns.map(col => col.key).join(','),
      ...sortedData.map(row => 
        columns.map(col => `"${row[col.key] || ''}"`).join(',')
      )
    ].join('\n');
    
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${title.replace(/\s+/g, '_')}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  if (isLoading) {
    return (
      <div className="w-full p-6">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/3 mb-4"></div>
          <div className="space-y-3">
            {[...Array(5)].map((_, i) => (
              <div key={i} className="h-6 bg-gray-200 rounded"></div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="w-full bg-white rounded-lg shadow-lg">
      {/* Header */}
      <div className="p-6 border-b border-gray-200">
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
          <h2 className="text-2xl font-bold text-gray-900">{title}</h2>
          
          <div className="flex flex-col sm:flex-row gap-3">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
              <input
                type="text"
                placeholder="Search..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
            
            <button
              onClick={exportToCSV}
              className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
            >
              <Download className="w-4 h-4" />
              Export CSV
            </button>
          </div>
        </div>
        
        <div className="mt-4 text-sm text-gray-600">
          Showing {paginatedData.length} of {sortedData.length} records
          {searchTerm && ` (filtered from ${data.length})`}
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-50">
            <tr>
              {columns.map((column) => (
                <th
                  key={column.key}
                  onClick={() => handleSort(column.key)}
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                >
                  <div className="flex items-center gap-2">
                    {column.label}
                    {sortConfig.key === column.key && (
                      sortConfig.direction === 'asc' ? 
                      <ChevronUp className="w-4 h-4" /> : 
                      <ChevronDown className="w-4 h-4" />
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {paginatedData.map((row, rowIndex) => (
              <tr key={rowIndex} className="hover:bg-gray-50 transition-colors">
                {columns.map((column) => {
                  const value = row[column.key];
                  const isClickable = clickableColumns.includes(column.key) && value > 0;
                  
                  return (
                    <td
                      key={column.key}
                      onClick={() => handleCellClick(row, column.key, value)}
                      className={`px-6 py-4 whitespace-nowrap text-sm ${
                        isClickable 
                          ? 'text-blue-600 cursor-pointer hover:text-blue-800 hover:bg-blue-50' 
                          : 'text-gray-900'
                      }`}
                    >
                      {column.render ? column.render(value, row) : (value || '-')}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="px-6 py-4 border-t border-gray-200">
          <div className="flex items-center justify-between">
            <div className="text-sm text-gray-700">
              Page {currentPage} of {totalPages}
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                disabled={currentPage === 1}
                className="px-3 py-2 border border-gray-300 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
              >
                Previous
              </button>
              <button
                onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                disabled={currentPage === totalPages}
                className="px-3 py-2 border border-gray-300 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
              >
                Next
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// Query Details Modal
const QueryDetailsModal = ({ queryId, onClose, isOpen }) => {
  const [queryDetails, setQueryDetails] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (isOpen && queryId) {
      setIsLoading(true);
      setError(null);
      
      apiService.fetchQueryDetails(queryId)
        .then(response => {
          setQueryDetails(response.data);
        })
        .catch(err => {
          setError(err.message);
        })
        .finally(() => {
          setIsLoading(false);
        });
    }
  }, [isOpen, queryId]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex min-h-screen items-center justify-center p-4">
        <div className="fixed inset-0 bg-black bg-opacity-25" onClick={onClose}></div>
        
        <div className="relative bg-white rounded-lg shadow-xl max-w-4xl w-full max-h-[90vh] overflow-y-auto">
          <div className="p-6">
            <div className="flex justify-between items-center mb-6">
              <h2 className="text-2xl font-bold text-gray-900">Query Details</h2>
              <button
                onClick={onClose}
                className="text-gray-400 hover:text-gray-600"
              >
                ✕
              </button>
            </div>

            {isLoading && (
              <div className="text-center py-8">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
                <p className="mt-4 text-gray-600">Loading query details...</p>
              </div>
            )}

            {error && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
                <p className="text-red-800">Error: {error}</p>
              </div>
            )}

            {queryDetails && (
              <div className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <h3 className="font-semibold text-gray-900 mb-2">Basic Information</h3>
                    <div className="space-y-2 text-sm">
                      <div><strong>Query ID:</strong> {queryDetails.QUERY_ID}</div>
                      <div><strong>Type:</strong> {queryDetails.QUERY_TYPE}</div>
                      <div><strong>Status:</strong> {queryDetails.EXECUTION_STATUS}</div>
                      <div><strong>User:</strong> {queryDetails.USER_NAME}</div>
                      <div><strong>Warehouse:</strong> {queryDetails.WAREHOUSE_NAME}</div>
                      <div><strong>Database:</strong> {queryDetails.DATABASE_NAME}</div>
                    </div>
                  </div>
                  
                  <div>
                    <h3 className="font-semibold text-gray-900 mb-2">Performance Metrics</h3>
                    <div className="space-y-2 text-sm">
                      <div><strong>Total Time:</strong> {queryDetails.TOTAL_ELAPSED_TIME}ms</div>
                      <div><strong>Execution Time:</strong> {queryDetails.EXECUTION_TIME}ms</div>
                      <div><strong>Compilation Time:</strong> {queryDetails.COMPILATION_TIME}ms</div>
                      <div><strong>Rows Produced:</strong> {queryDetails.ROWS_PRODUCED}</div>
                      <div><strong>Bytes Scanned:</strong> {(queryDetails.BYTES_SCANNED / 1024 / 1024).toFixed(2)} MB</div>
                      <div><strong>Credits Used:</strong> {queryDetails.CREDITS_USED_CLOUD_SERVICES}</div>
                    </div>
                  </div>
                </div>

                <div>
                  <h3 className="font-semibold text-gray-900 mb-2">Query Text</h3>
                  <div className="bg-gray-50 p-4 rounded-lg overflow-x-auto">
                    <pre className="text-sm text-gray-800 whitespace-pre-wrap">
                      {queryDetails.QUERY_TEXT}
                    </pre>
                  </div>
                </div>

                {queryDetails.ERROR_MESSAGE && (
                  <div>
                    <h3 className="font-semibold text-red-900 mb-2">Error Details</h3>
                    <div className="bg-red-50 p-4 rounded-lg">
                      <p className="text-sm text-red-800">{queryDetails.ERROR_MESSAGE}</p>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

// Main Dashboard Component
const Dashboard = () => {
  const [currentView, setCurrentView] = useState('warehouses');
  const [warehouses, setWarehouses] = useState([]);
  const [users, setUsers] = useState([]);
  const [drillDownData, setDrillDownData] = useState(null);
  const [queriesData, setQueriesData] = useState([]);
  const [selectedQueryId, setSelectedQueryId] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showQueryModal, setShowQueryModal] = useState(false);

  // Fetch initial data
  useEffect(() => {
    loadWarehouses();
    loadUsers();
  }, []);

  const loadWarehouses = async () => {
    try {
      setIsLoading(true);
      const response = await apiService.fetchWarehouses();
      setWarehouses(response.data);
    } catch (err) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  const loadUsers = async () => {
    try {
      const response = await apiService.fetchUsers();
      setUsers(response.data);
    } catch (err) {
      console.error('Failed to load users:', err);
    }
  };

  // Handle warehouse cell click
  const handleWarehouseCellClick = async (rowData, columnKey, value) => {
    if (value === 0 || value === null) return;
    
    try {
      setIsLoading(true);
      const response = await apiService.warehouseDrillDown(rowData.WAREHOUSE_NAME, columnKey);
      setDrillDownData(response.data);
      setCurrentView('drill-down');
    } catch (err) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  // Handle user cell click
  const handleUserCellClick = async (rowData, columnKey, value) => {
    try {
      setIsLoading(true);
      const response = await apiService.userDrillDown(rowData.user_name, columnKey);
      setQueriesData(response.data.queries);
      setCurrentView('queries');
    } catch (err) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  // Handle drill-down user click
  const handleDrillDownUserClick = async (queryIds) => {
    try {
      setIsLoading(true);
      const response = await apiService.fetchQueriesBatch(queryIds);
      setQueriesData(response.data);
      setCurrentView('queries');
    } catch (err) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  // Handle query detail view
  const handleQueryDetailClick = (queryId) => {
    setSelectedQueryId(queryId);
    setShowQueryModal(true);
  };

  // Define columns for different tables
  const warehouseColumns = [
    { key: 'WAREHOUSE_NAME', label: 'Warehouse Name' },
    { key: 'WAREHOUSE_SIZE', label: 'Size' },
    { key: 'WAREHOUSE_TYPE', label: 'Type' },
    { key: 'TOTAL_QUERIES', label: 'Total Queries' },
    { key: 'QUERIES_1_10_SEC', label: '1-10 Sec' },
    { key: 'QUERIES_10_20_SEC', label: '10-20 Sec' },
    { key: 'QUERIES_20_60_SEC', label: '20-60 Sec' },
    { key: 'QUERIES_1_3_MIN', label: '1-3 Min' },
    { key: 'QUERIES_3_5_MIN', label: '3-5 Min' },
    { key: 'QUERIES_5_PLUS_MIN', label: '5+ Min' },
    { key: 'FAILED_QUERIES', label: 'Failed' },
    { key: 'SUCCESSFUL_QUERIES', label: 'Successful' },
    { key: 'QUERIES_SPILLED_LOCAL', label: 'Spilled Local' },
    { key: 'QUERIES_SPILLED_REMOTE', label: 'Spilled Remote' },
    { key: 'TOTAL_CREDITS_USED', label: 'Total Credits', render: (value) => value?.toFixed(2) }
  ];

  const userColumns = [
    { key: 'user_name', label: 'User Name' },
    { key: 'total_queries', label: 'Total Queries' },
    { key: 'warehouses_used', label: 'Warehouses Used' },
    { key: 'total_credits', label: 'Total Credits', render: (value) => value?.toFixed(2) },
    { key: 'failure_cancellation_rate_pct', label: 'Failure Rate %', render: (value) => value?.toFixed(1) + '%' },
    { key: 'spilled_queries', label: 'Spilled Queries' },
    { key: 'slow_queries', label: 'Slow Queries' },
    { key: 'select_star_queries', label: 'Select * Queries' },
    { key: 'untagged_queries', label: 'Untagged Queries' },
    { key: 'weighted_score', label: 'Score', render: (value) => value?.toFixed(1) },
    { key: 'cost_status', label: 'Cost Status' }
  ];

  const drillDownColumns = [
    { key: 'username', label: 'User Name' },
    { key: 'query_count', label: 'Query Count' },
    { 
      key: 'query_ids', 
      label: 'Actions',
      render: (value, row) => (
        <button
          onClick={() => handleDrillDownUserClick(value)}
          className="px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors text-xs"
        >
          View Queries ({value.length})
        </button>
      )
    }
  ];

  const queryColumns = [
    { key: 'QUERY_ID', label: 'Query ID', render: (value) => value?.substring(0, 8) + '...' },
    { key: 'QUERY_TEXT_PREVIEW', label: 'Query Preview', render: (value) => value?.substring(0, 50) + '...' },
    { key: 'USER_NAME', label: 'User' },
    { key: 'EXECUTION_STATUS', label: 'Status' },
    { key: 'TOTAL_ELAPSED_TIME', label: 'Duration (ms)' },
    { key: 'START_TIME', label: 'Start Time', render: (value) => new Date(value).toLocaleString() },
    { 
      key: 'actions', 
      label: 'Actions',
      render: (value, row) => (
        <button
          onClick={() => handleQueryDetailClick(row.QUERY_ID)}
          className="flex items-center gap-1 px-3 py-1 bg-green-600 text-white rounded hover:bg-green-700 transition-colors text-xs"
        >
          <Eye className="w-3 h-3" />
          View Details
        </button>
      )
    }
  ];

  const clickableWarehouseColumns = [
    'QUERIES_1_10_SEC', 'QUERIES_10_20_SEC', 'QUERIES_20_60_SEC', 
    'QUERIES_1_3_MIN', 'QUERIES_3_5_MIN', 'QUERIES_5_PLUS_MIN',
    'QUEUED_1_2_MIN', 'QUEUED_2_5_MIN', 'QUEUED_5_10_MIN', 
    'QUEUED_10_20_MIN', 'QUEUED_20_PLUS_MIN',
    'QUERIES_SPILLED_LOCAL', 'QUERIES_SPILLED_REMOTE', 
    'FAILED_QUERIES', 'SUCCESSFUL_QUERIES', 'RUNNING_QUERIES',
    'QUERIES_0_20_CENTS', 'QUERIES_20_40_CENTS', 'QUERIES_40_60_CENTS',
    'QUERIES_60_80_CENTS', 'QUERIES_80_100_CENTS', 'QUERIES_100_PLUS_CENTS'
  ];

  const clickableUserColumns = [
    'spilled_queries', 'slow_queries', 'select_star_queries', 'untagged_queries',
    'failed_queries', 'zero_result_queries', 'high_compile_queries'
  ];

  const renderContent = () => {
    if (error) {
      return (
        <div className="bg-red-50 border border-red-200 rounded-lg p-6">
          <h3 className="text-red-800 font-semibold mb-2">Error</h3>
          <p className="text-red-700">{error}</p>
          <button
            onClick={() => {
              setError(null);
              if (currentView === 'warehouses') loadWarehouses();
              else if (currentView === 'users') loadUsers();
            }}
            className="mt-4 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
          >
            Retry
          </button>
        </div>
      );
    }

    switch (currentView) {
      case 'warehouses':
        return (
          <DataTable
            data={warehouses}
            columns={warehouseColumns}
            onCellClick={handleWarehouseCellClick}
            clickableColumns={clickableWarehouseColumns}
            isLoading={isLoading}
            title="Warehouse Analytics Dashboard"
          />
        );
      
      case 'users':
        return (
          <DataTable
            data={users}
            columns={userColumns}
            onCellClick={handleUserCellClick}
            clickableColumns={clickableUserColumns}
            isLoading={isLoading}
            title="User Performance Report"
          />
        );
      
      case 'drill-down':
        return (
          <div>
            <div className="mb-6 p-4 bg-blue-50 rounded-lg">
              <h3 className="font-semibold text-blue-900 mb-2">
                Drill-down Analysis: {drillDownData?.warehouse_name} - {drillDownData?.column_selected}
              </h3>
              <button
                onClick={() => setCurrentView('warehouses')}
                className="flex items-center gap-2 text-blue-600 hover:text-blue-800"
              >
                <ArrowLeft className="w-4 h-4" />
                Back to Warehouses
              </button>
            </div>
            
            <DataTable
              data={drillDownData?.user_analysis || []}
              columns={drillDownColumns}
              isLoading={isLoading}
              title="User Analysis"
            />
          </div>
        );
      
      case 'queries':
        return (
          <div>
            <div className="mb-6 p-4 bg-green-50 rounded-lg">
              <h3 className="font-semibold text-green-900 mb-2">Query Details</h3>
              <button
                onClick={() => setCurrentView(drillDownData ? 'drill-down' : 'warehouses')}
                className="flex items-center gap-2 text-green-600 hover:text-green-800"
              >
                <ArrowLeft className="w-4 h-4" />
                Back
              </button>
            </div>
            
            <DataTable
              data={queriesData}
              columns={queryColumns}
              isLoading={isLoading}
              title="Query History"
            />
          </div>
        );
      
      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">Snowflake FinOps Dashboard</h1>
              <p className="text-gray-600">Query Performance Monitoring & Cost Analysis</p>
            </div>
            
            <div className="flex items-center gap-4">
              <button
                onClick={() => {
                  setError(null);
                  if (currentView === 'warehouses') loadWarehouses();
                  else if (currentView === 'users') loadUsers();
                }}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                disabled={isLoading}
              >
                <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
                Refresh
              </button>
            </div>
          </div>

          {/* Navigation Tabs */}
          <div className="flex space-x-8 border-b">
            <button
              onClick={() => setCurrentView('warehouses')}
              className={`py-2 px-1 border-b-2 font-medium text-sm ${
                currentView === 'warehouses'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              Warehouses
            </button>
            <button
              onClick={() => setCurrentView('users')}
              className={`py-2 px-1 border-b-2 font-medium text-sm ${
                currentView === 'users'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              Users
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {renderContent()}
      </main>

      {/* Query Details Modal */}
      <QueryDetailsModal
        queryId={selectedQueryId}
        isOpen={showQueryModal}
        onClose={() => {
          setShowQueryModal(false);
          setSelectedQueryId(null);
        }}
      />
    </div>
  );
};

export default Dashboard;
