import React, { useState, useEffect } from 'react';
import { ChevronRight, Database, Users, Activity, Eye, ArrowLeft, Search, BarChart3, Clock, AlertTriangle, CheckCircle, XCircle } from 'lucide-react';

// Toast Component
const Toast = ({ message, type, onClose }) => {
  useEffect(() => {
    const timer = setTimeout(onClose, 4000);
    return () => clearTimeout(timer);
  }, [onClose]);

  const bgColor = type === 'error' ? 'bg-red-500' : type === 'success' ? 'bg-green-500' : 'bg-blue-500';
  const icon = type === 'error' ? <XCircle size={16} /> : type === 'success' ? <CheckCircle size={16} /> : <Activity size={16} />;

  return (
    <div className={`fixed top-4 right-4 ${bgColor} text-white px-4 py-3 rounded-lg shadow-lg flex items-center gap-2 z-50 min-w-64 animate-pulse`}>
      {icon}
      <span className="text-sm font-medium">{message}</span>
    </div>
  );
};

// Loading Spinner
const LoadingSpinner = () => (
  <div className="flex items-center justify-center p-8">
    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
  </div>
);

// Error Message Component
const ErrorMessage = ({ message, onRetry }) => (
  <div className="flex flex-col items-center justify-center p-8 text-gray-600">
    <AlertTriangle className="h-12 w-12 text-red-500 mb-4" />
    <p className="text-lg font-medium mb-2">Oops! Something went wrong</p>
    <p className="text-sm text-gray-500 mb-4">{message}</p>
    {onRetry && (
      <button
        onClick={onRetry}
        className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
      >
        Try Again
      </button>
    )}
  </div>
);

// Table Component
const DataTable = ({ data, columns, onCellClick, title, searchable = true }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });

  if (!data || data.length === 0) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8 text-center">
        <Database className="h-12 w-12 text-gray-400 mx-auto mb-4" />
        <p className="text-gray-500">No data available</p>
      </div>
    );
  }

  const filteredData = searchable ? data.filter(row =>
    Object.values(row).some(value =>
      String(value).toLowerCase().includes(searchTerm.toLowerCase())
    )
  ) : data;

  const sortedData = sortConfig.key
    ? [...filteredData].sort((a, b) => {
        if (a[sortConfig.key] < b[sortConfig.key]) {
          return sortConfig.direction === 'asc' ? -1 : 1;
        }
        if (a[sortConfig.key] > b[sortConfig.key]) {
          return sortConfig.direction === 'asc' ? 1 : -1;
        }
        return 0;
      })
    : filteredData;

  const handleSort = (key) => {
    setSortConfig({
      key,
      direction: sortConfig.key === key && sortConfig.direction === 'asc' ? 'desc' : 'asc'
    });
  };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200">
      <div className="p-6 border-b border-gray-200">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
            <BarChart3 className="h-5 w-5 text-blue-600" />
            {title}
          </h3>
          {searchable && (
            <div className="relative">
              <Search className="h-4 w-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Search..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-9 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
          )}
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-50">
            <tr>
              {columns.map(col => (
                <th
                  key={col.key}
                  className={`px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider ${
                    col.sortable ? 'cursor-pointer hover:bg-gray-100' : ''
                  }`}
                  onClick={() => col.sortable && handleSort(col.key)}
                >
                  <div className="flex items-center gap-2">
                    {col.title}
                    {col.sortable && sortConfig.key === col.key && (
                      <ChevronRight className={`h-4 w-4 transform ${sortConfig.direction === 'desc' ? 'rotate-90' : '-rotate-90'}`} />
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {sortedData.map((row, idx) => (
              <tr key={idx} className="hover:bg-gray-50 transition-colors">
                {columns.map(col => (
                  <td
                    key={col.key}
                    className={`px-6 py-4 whitespace-nowrap text-sm ${
                      col.clickable 
                        ? 'text-blue-600 hover:text-blue-800 cursor-pointer hover:bg-blue-50 font-medium' 
                        : col.fixed 
                          ? 'text-gray-900 font-medium' 
                          : 'text-gray-600'
                    }`}
                    onClick={() => col.clickable && onCellClick && onCellClick(row, col)}
                  >
                    {col.render ? col.render(row[col.key], row) : (row[col.key] || 0)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      <div className="px-6 py-3 bg-gray-50 border-t border-gray-200 text-sm text-gray-600">
        Showing {sortedData.length} of {data.length} records
      </div>
    </div>
  );
};

// Navigation Component
const NavigationBreadcrumb = ({ breadcrumbs, onNavigate }) => (
  <div className="flex items-center gap-2 mb-6">
    {breadcrumbs.map((item, idx) => (
      <React.Fragment key={idx}>
        {idx > 0 && <ChevronRight className="h-4 w-4 text-gray-400" />}
        <button
          onClick={() => onNavigate(item.view)}
          className={`text-sm font-medium ${
            idx === breadcrumbs.length - 1
              ? 'text-gray-900'
              : 'text-blue-600 hover:text-blue-800'
          }`}
        >
          {item.title}
        </button>
      </React.Fragment>
    ))}
  </div>
);

// Header Component
const Header = () => (
  <div className="bg-gradient-to-r from-blue-600 to-purple-600 text-white p-6 rounded-xl shadow-lg mb-6">
    <div className="flex items-center gap-3">
      <Database className="h-8 w-8" />
      <div>
        <h1 className="text-2xl font-bold">Snowflake Analytics Dashboard</h1>
        <p className="text-blue-100">Monitor warehouse performance and query analytics</p>
      </div>
    </div>
  </div>
);

// Main App Component
const App = () => {
  const [currentView, setCurrentView] = useState('warehouses');
  const [loading, setLoading] = useState(false);
  const [toast, setToast] = useState(null);
  
  // Data states
  const [warehousesData, setWarehousesData] = useState([]);
  const [usersData, setUsersData] = useState([]);
  const [drillDownData, setDrillDownData] = useState([]);
  const [queriesData, setQueriesData] = useState([]);
  const [queryDetails, setQueryDetails] = useState(null);
  
  // Context states
  const [selectedWarehouse, setSelectedWarehouse] = useState(null);
  const [selectedUser, setSelectedUser] = useState(null);
  const [selectedKPI, setSelectedKPI] = useState(null);
  const [breadcrumbs, setBreadcrumbs] = useState([{ title: 'Warehouses', view: 'warehouses' }]);

  const API_BASE_URL = 'http://localhost:5000';

  const showToast = (message, type = 'info') => {
    setToast({ message, type });
  };

  const closeToast = () => setToast(null);

  // API call wrapper with error handling
  const apiCall = async (url, options = {}) => {
    try {
      const response = await fetch(`${API_BASE_URL}${url}`, {
        headers: {
          'Content-Type': 'application/json',
          ...options.headers
        },
        ...options
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      
      if (!data.success) {
        throw new Error(data.message || 'API request failed');
      }

      return data;
    } catch (error) {
      console.error('API Error:', error);
      throw error;
    }
  };

  // Load initial data
  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    setLoading(true);
    try {
      const [warehousesRes, usersRes] = await Promise.all([
        apiCall('/warehouses'),
        apiCall('/users')
      ]);

      setWarehousesData(warehousesRes.data || []);
      setUsersData(usersRes.data || []);
      showToast('Data loaded successfully', 'success');
    } catch (error) {
      showToast(`Failed to load data: ${error.message}`, 'error');
    } finally {
      setLoading(false);
    }
  };

  // Navigate between views
  const navigate = (view, context = {}) => {
    setCurrentView(view);
    
    switch (view) {
      case 'warehouses':
        setBreadcrumbs([{ title: 'Warehouses', view: 'warehouses' }]);
        break;
      case 'users':
        setBreadcrumbs([{ title: 'Users', view: 'users' }]);
        break;
      case 'warehouse-drill':
        setBreadcrumbs([
          { title: 'Warehouses', view: 'warehouses' },
          { title: `${context.warehouse} - ${context.kpi}`, view: 'warehouse-drill' }
        ]);
        break;
      case 'user-queries':
        setBreadcrumbs([
          { title: context.fromView === 'users' ? 'Users' : 'Warehouses', view: context.fromView || 'warehouses' },
          ...(context.fromView !== 'users' ? [{ title: `${context.warehouse} - ${context.kpi}`, view: 'warehouse-drill' }] : []),
          { title: `${context.user} Queries`, view: 'user-queries' }
        ]);
        break;
      case 'query-details':
        setBreadcrumbs(prev => [...prev, { title: 'Query Details', view: 'query-details' }]);
        break;
    }
  };

  // Warehouse KPI click handler
  const handleWarehouseKPIClick = async (row, col) => {
    if (col.key === 'WAREHOUSE_NAME') return;
    
    setLoading(true);
    try {
      const response = await apiCall('/warehouses/drill-down', {
        method: 'POST',
        body: JSON.stringify({
          warehouse_name: row.WAREHOUSE_NAME,
          selected_kpi_column: col.key
        })
      });

      setDrillDownData(response.data || []);
      setSelectedWarehouse(row.WAREHOUSE_NAME);
      setSelectedKPI(col.key);
      navigate('warehouse-drill', { warehouse: row.WAREHOUSE_NAME, kpi: col.key });
      showToast(`Found ${response.total_queries || 0} queries`, 'success');
    } catch (error) {
      showToast(`Failed to load drill-down data: ${error.message}`, 'error');
    } finally {
      setLoading(false);
    }
  };

  // User KPI click handler
  const handleUserKPIClick = async (row, col) => {
    if (col.key === 'USER_NAME') return;
    
    setLoading(true);
    try {
      const response = await apiCall('/users/drill-down', {
        method: 'POST',
        body: JSON.stringify({
          username: row.USER_NAME,
          selected_kpi_column: col.key
        })
      });

      setQueriesData(response.data || []);
      setSelectedUser(row.USER_NAME);
      setSelectedKPI(col.key);
      navigate('user-queries', { user: row.USER_NAME, kpi: col.key, fromView: 'users' });
      showToast(`Found ${response.count || 0} queries`, 'success');
    } catch (error) {
      showToast(`Failed to load user queries: ${error.message}`, 'error');
    } finally {
      setLoading(false);
    }
  };

  // View user queries from warehouse drill-down
  const handleViewUserQueries = async (username) => {
    setLoading(true);
    try {
      const response = await apiCall('/users/drill-down', {
        method: 'POST',
        body: JSON.stringify({
          username: username,
          selected_kpi_column: selectedKPI
        })
      });

      setQueriesData(response.data || []);
      setSelectedUser(username);
      navigate('user-queries', { 
        user: username, 
        kpi: selectedKPI, 
        warehouse: selectedWarehouse,
        fromView: 'warehouse-drill'
      });
      showToast(`Found ${response.count || 0} queries for ${username}`, 'success');
    } catch (error) {
      showToast(`Failed to load user queries: ${error.message}`, 'error');
    } finally {
      setLoading(false);
    }
  };

  // View query details
  const handleViewQueryDetails = async (queryId) => {
    setLoading(true);
    try {
      const response = await apiCall(`/query-details/${queryId}`);
      
      setQueryDetails(response.data);
      navigate('query-details');
      showToast('Query details loaded', 'success');
    } catch (error) {
      showToast(`Failed to load query details: ${error.message}`, 'error');
    } finally {
      setLoading(false);
    }
  };

  // Column definitions
  const warehouseColumns = [
    { key: 'WAREHOUSE_NAME', title: 'Warehouse Name', fixed: true, sortable: true },
    { key: 'WAREHOUSE_SIZE', title: 'Size', sortable: true },
    { key: 'TOTAL_QUERIES', title: 'Total Queries', clickable: true, sortable: true },
    { key: 'QUERIES_1_10_SEC', title: '1-10 Sec', clickable: true, sortable: true },
    { key: 'QUERIES_10_20_SEC', title: '10-20 Sec', clickable: true, sortable: true },
    { key: 'QUERIES_20_60_SEC', title: '20-60 Sec', clickable: true, sortable: true },
    { key: 'QUERIES_1_3_MIN', title: '1-3 Min', clickable: true, sortable: true },
    { key: 'QUERIES_3_5_MIN', title: '3-5 Min', clickable: true, sortable: true },
    { key: 'QUERIES_5_PLUS_MIN', title: '5+ Min', clickable: true, sortable: true },
    { key: 'QUEUED_1_2_MIN', title: 'Queued 1-2 Min', clickable: true, sortable: true },
    { key: 'QUEUED_2_5_MIN', title: 'Queued 2-5 Min', clickable: true, sortable: true },
    { key: 'QUEUED_5_10_MIN', title: 'Queued 5-10 Min', clickable: true, sortable: true },
    { key: 'QUERIES_SPILLED_LOCAL', title: 'Spilled Local', clickable: true, sortable: true },
    { key: 'QUERIES_SPILLED_REMOTE', title: 'Spilled Remote', clickable: true, sortable: true },
    { key: 'FAILED_QUERIES', title: 'Failed', clickable: true, sortable: true },
    { key: 'SUCCESSFUL_QUERIES', title: 'Successful', clickable: true, sortable: true },
    { 
      key: 'TOTAL_CREDITS_USED', 
      title: 'Credits Used', 
      sortable: true,
      render: (value) => (value || 0).toFixed(2)
    }
  ];

  const userColumns = [
    { key: 'USER_NAME', title: 'User Name', fixed: true, sortable: true },
    { key: 'TOTAL_QUERIES', title: 'Total Queries', sortable: true },
    { key: 'TOTAL_CREDITS', title: 'Credits', sortable: true, render: (value) => (value || 0).toFixed(2) },
    { key: 'SPILLED_QUERIES', title: 'Spilled', clickable: true, sortable: true },
    { key: 'OVER_PROVISIONED_QUERIES', title: 'Over Provisioned', clickable: true, sortable: true },
    { key: 'PEAK_HOUR_LONG_RUNNING_QUERIES', title: 'Peak Long Running', clickable: true, sortable: true },
    { key: 'SELECT_STAR_QUERIES', title: 'Select *', clickable: true, sortable: true },
    { key: 'UNPARTITIONED_SCAN_QUERIES', title: 'Unpartitioned', clickable: true, sortable: true },
    { key: 'REPEATED_QUERIES', title: 'Repeated', clickable: true, sortable: true },
    { key: 'COMPLEX_JOIN_QUERIES', title: 'Complex Joins', clickable: true, sortable: true },
    { key: 'ZERO_RESULT_QUERIES', title: 'Zero Results', clickable: true, sortable: true },
    { key: 'HIGH_COMPILE_QUERIES', title: 'High Compile', clickable: true, sortable: true },
    { key: 'WEIGHTED_SCORE', title: 'Score', sortable: true, render: (value) => (value || 0).toFixed(1) }
  ];

  const drillDownColumns = [
    { key: 'warehouse_name', title: 'Warehouse', sortable: true },
    { key: 'selected_kpi_column', title: 'KPI', sortable: true },
    { key: 'username', title: 'User', sortable: true },
    { key: 'query_count', title: 'Query Count', sortable: true },
    {
      key: 'actions',
      title: 'Actions',
      render: (_, row) => (
        <button
          onClick={() => handleViewUserQueries(row.username)}
          className="px-3 py-1 bg-blue-600 text-white text-xs rounded-md hover:bg-blue-700 transition-colors flex items-center gap-1"
        >
          <Eye size={12} />
          View All Queries
        </button>
      )
    }
  ];

  const queryColumns = [
    { key: 'QUERY_ID', title: 'Query ID', sortable: true },
    { key: 'USER_NAME', title: 'User', sortable: true },
    { key: 'WAREHOUSE_NAME', title: 'Warehouse', sortable: true },
    { key: 'QUERY_TYPE', title: 'Type', sortable: true },
    { key: 'START_TIME', title: 'Start Time', sortable: true, render: (value) => new Date(value).toLocaleString() },
    { key: 'TOTAL_ELAPSED_TIME_SECONDS', title: 'Duration (s)', sortable: true, render: (value) => (value || 0).toFixed(2) },
    { key: 'EXECUTION_STATUS', title: 'Status', sortable: true },
    { key: 'CREDITS_USED_CLOUD_SERVICES', title: 'Credits', sortable: true, render: (value) => (value || 0).toFixed(3) },
    {
      key: 'actions',
      title: 'Actions',
      render: (_, row) => (
        <button
          onClick={() => handleViewQueryDetails(row.QUERY_ID)}
          className="px-3 py-1 bg-green-600 text-white text-xs rounded-md hover:bg-green-700 transition-colors flex items-center gap-1"
        >
          <Eye size={12} />
          View Details
        </button>
      )
    }
  ];

  // Render current view
  const renderCurrentView = () => {
    if (loading) return <LoadingSpinner />;

    switch (currentView) {
      case 'warehouses':
        return (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-semibold text-gray-900 flex items-center gap-2">
                <Database className="h-5 w-5 text-blue-600" />
                Warehouse Analytics
              </h2>
              <button
                onClick={() => navigate('users')}
                className="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 transition-colors flex items-center gap-2"
              >
                <Users size={16} />
                View Users
              </button>
            </div>
            <DataTable
              data={warehousesData}
              columns={warehouseColumns}
              onCellClick={handleWarehouseKPIClick}
              title="Warehouse Performance Metrics"
            />
          </div>
        );

      case 'users':
        return (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-semibold text-gray-900 flex items-center gap-2">
                <Users className="h-5 w-5 text-purple-600" />
                User Analytics
              </h2>
              <button
                onClick={() => navigate('warehouses')}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2"
              >
                <Database size={16} />
                View Warehouses
              </button>
            </div>
            <DataTable
              data={usersData}
              columns={userColumns}
              onCellClick={handleUserKPIClick}
              title="User Performance Analysis"
            />
          </div>
        );

      case 'warehouse-drill':
        return (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-semibold text-gray-900">
                {selectedWarehouse} - {selectedKPI} Breakdown
              </h2>
              <button
                onClick={() => navigate('warehouses')}
                className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors flex items-center gap-2"
              >
                <ArrowLeft size={16} />
                Back to Warehouses
              </button>
            </div>
            <DataTable
              data={drillDownData}
              columns={drillDownColumns}
              title={`Users with ${selectedKPI} queries in ${selectedWarehouse}`}
            />
          </div>
        );

      case 'user-queries':
        return (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-semibold text-gray-900">
                {selectedUser} - {selectedKPI} Queries
              </h2>
              <button
                onClick={() => window.history.back()}
                className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors flex items-center gap-2"
              >
                <ArrowLeft size={16} />
                Back
              </button>
            </div>
            <DataTable
              data={queriesData}
              columns={queryColumns}
              title={`Query Details for ${selectedUser} (${selectedKPI})`}
            />
          </div>
        );

      case 'query-details':
        return (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-semibold text-gray-900 flex items-center gap-2">
                <Activity className="h-5 w-5 text-green-600" />
                Query Details
              </h2>
              <button
                onClick={() => window.history.back()}
                className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors flex items-center gap-2"
              >
                <ArrowLeft size={16} />
                Back
              </button>
            </div>
            
            {queryDetails && (
              <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  <div className="space-y-4">
                    <h3 className="text-lg font-semibold text-gray-900 border-b pb-2">Basic Information</h3>
                    <div className="space-y-2">
                      <div><strong>Query ID:</strong> <span className="font-mono text-sm">{queryDetails.QUERY_ID}</span></div>
                      <div><strong>User:</strong> {queryDetails.USER_NAME}</div>
                      <div><strong>Warehouse:</strong> {queryDetails.WAREHOUSE_NAME}</div>
                      <div><strong>Type:</strong> {queryDetails.QUERY_TYPE}</div>
                      <div><strong>Status:</strong> 
                        <span className={`ml-2 px-2 py-1 rounded-full text-xs ${
                          queryDetails.EXECUTION_STATUS === 'SUCCESS' 
                            ? 'bg-green-100 text-green-800' 
                            : 'bg-red-100 text-red-800'
                        }`}>
                          {queryDetails.EXECUTION_STATUS}
                        </span>
                      </div>
                    </div>
                  </div>

                  <div className="space-y-4">
                    <h3 className="text-lg font-semibold text-gray-900 border-b pb-2">Performance Metrics</h3>
                    <div className="space-y-2">
                      <div><strong>Duration:</strong> {(queryDetails.TOTAL_ELAPSED_TIME_SECONDS || 0).toFixed(2)} seconds</div>
                      <div><strong>Execution Time:</strong> {(queryDetails.EXECUTION_TIME_SECONDS || 0).toFixed(2)} seconds</div>
                      <div><strong>Compilation Time:</strong> {(queryDetails.COMPILATION_TIME_SECONDS || 0).toFixed(2)} seconds</div>
                      <div><strong>Credits Used:</strong> {(queryDetails.CREDITS_USED_CLOUD_SERVICES || 0).toFixed(6)}</div>
                      <div><strong>Bytes Scanned:</strong> {(queryDetails.BYTES_SCANNED_GB || 0).toFixed(2)} GB</div>
                      <div><strong>Rows Produced:</strong> {(queryDetails.ROWS_PRODUCED || 0).toLocaleString()}</div>
                    </div>
                  </div>

                  <div className="lg:col-span-2 space-y-4">
                    <h3 className="text-lg font-semibold text-gray-900 border-b pb-2">Query Text</h3>
                    <div className="bg-gray-50 p-4 rounded-lg">
                      <pre className="text-sm text-gray-700 whitespace-pre-wrap overflow-x-auto">
                        {queryDetails.QUERY_TEXT_SAMPLE || 'Query text not available'}
                      </pre>
                    </div>
                  </div>

                  {queryDetails.ERROR_MESSAGE && (
                    <div className="lg:col-span-2 space-y-4">
                      <h3 className="text-lg font-semibold text-red-600 border-b pb-2">Error Information</h3>
                      <div className="bg-red-50 p-4 rounded-lg">
                        <div className="text-red-800">
                          <strong>Error Code:</strong> {queryDetails.ERROR_CODE}<br />
                          <strong>Message:</strong> {queryDetails.ERROR_MESSAGE}
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        );

      default:
        return <ErrorMessage message="Unknown view" onRetry={() => navigate('warehouses')} />;
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 p-4">
      <div className="max-w-7xl mx-auto">
        <Header />
        
        <NavigationBreadcrumb 
          breadcrumbs={breadcrumbs} 
          onNavigate={navigate}
        />
        
        <main>
          {renderCurrentView()}
        </main>

        {/* Toast Notifications */}
        {toast && (
          <Toast
            message={toast.message}
            type={toast.type}
            onClose={closeToast}
          />
        )}

        {/* Loading Overlay */}
        {loading && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-40">
            <div className="bg-white p-6 rounded-lg shadow-xl">
              <div className="flex items-center gap-3">
                <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
                <span className="text-gray-700 font-medium">Loading...</span>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default App;
