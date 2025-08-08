import React, { useState, useEffect, useMemo } from 'react';
import { 
  Search, 
  Filter, 
  SortAsc, 
  SortDesc, 
  Eye, 
  RefreshCw, 
  Database, 
  Users, 
  Server, 
  Activity,
  BarChart3,
  ChevronDown,
  ChevronUp,
  ArrowLeft,
  Clock,
  User,
  HardDrive,
  Zap
} from 'lucide-react';

const API_BASE_URL = 'http://localhost:5000/api';

// Utility functions
const formatNumber = (num) => {
  if (num === null || num === undefined) return 'N/A';
  if (typeof num === 'string') return num;
  return num.toLocaleString();
};

const formatBytes = (bytes) => {
  if (!bytes) return '0 B';
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(2) + ' ' + sizes[i];
};

const formatDuration = (ms) => {
  if (!ms) return '0ms';
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60000).toFixed(1)}m`;
};

// Loading spinner component
const LoadingSpinner = () => (
  <div className="flex items-center justify-center p-8">
    <RefreshCw className="w-6 h-6 animate-spin text-blue-600" />
    <span className="ml-2 text-gray-600">Loading...</span>
  </div>
);

// Interactive table component
const InteractiveTable = ({ 
  data, 
  columns, 
  title, 
  onRowClick,
  searchable = true,
  sortable = true 
}) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 10;

  const filteredData = useMemo(() => {
    if (!searchTerm) return data;
    return data.filter(row =>
      Object.values(row).some(value =>
        String(value).toLowerCase().includes(searchTerm.toLowerCase())
      )
    );
  }, [data, searchTerm]);

  const sortedData = useMemo(() => {
    if (!sortConfig.key) return filteredData;
    
    return [...filteredData].sort((a, b) => {
      const aVal = a[sortConfig.key];
      const bVal = b[sortConfig.key];
      
      if (aVal === null || aVal === undefined) return 1;
      if (bVal === null || bVal === undefined) return -1;
      
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        return sortConfig.direction === 'asc' ? aVal - bVal : bVal - aVal;
      }
      
      const aStr = String(aVal).toLowerCase();
      const bStr = String(bVal).toLowerCase();
      
      if (aStr < bStr) return sortConfig.direction === 'asc' ? -1 : 1;
      if (aStr > bStr) return sortConfig.direction === 'asc' ? 1 : -1;
      return 0;
    });
  }, [filteredData, sortConfig]);

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

  const getSortIcon = (columnKey) => {
    if (sortConfig.key !== columnKey) return null;
    return sortConfig.direction === 'asc' ? 
      <SortAsc className="w-4 h-4 ml-1" /> : 
      <SortDesc className="w-4 h-4 ml-1" />;
  };

  return (
    <div className="bg-white rounded-lg shadow-lg overflow-hidden">
      <div className="bg-gray-50 px-6 py-4 border-b">
        <h3 className="text-lg font-semibold text-gray-800">{title}</h3>
        {searchable && (
          <div className="mt-3 relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
            <input
              type="text"
              placeholder="Search..."
              className="pl-10 pr-4 py-2 border border-gray-300 rounded-md w-full focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          </div>
        )}
      </div>
      
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              {columns.map((column) => (
                <th
                  key={column.key}
                  className={`px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider ${
                    sortable && column.sortable !== false ? 'cursor-pointer hover:bg-gray-100' : ''
                  }`}
                  onClick={() => sortable && column.sortable !== false && handleSort(column.key)}
                >
                  <div className="flex items-center">
                    {column.label}
                    {sortable && column.sortable !== false && getSortIcon(column.key)}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {paginatedData.map((row, index) => (
              <tr 
                key={index}
                className={`hover:bg-gray-50 ${onRowClick ? 'cursor-pointer' : ''}`}
                onClick={() => onRowClick && onRowClick(row)}
              >
                {columns.map((column) => (
                  <td key={column.key} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {column.render ? column.render(row[column.key], row) : formatNumber(row[column.key])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      {totalPages > 1 && (
        <div className="bg-white px-6 py-3 border-t border-gray-200 flex items-center justify-between">
          <div className="text-sm text-gray-700">
            Showing {Math.min((currentPage - 1) * itemsPerPage + 1, sortedData.length)} to{' '}
            {Math.min(currentPage * itemsPerPage, sortedData.length)} of {sortedData.length} results
          </div>
          <div className="flex space-x-2">
            <button
              className="px-3 py-1 border border-gray-300 rounded text-sm disabled:opacity-50"
              disabled={currentPage === 1}
              onClick={() => setCurrentPage(prev => Math.max(prev - 1, 1))}
            >
              Previous
            </button>
            <button
              className="px-3 py-1 border border-gray-300 rounded text-sm disabled:opacity-50"
              disabled={currentPage === totalPages}
              onClick={() => setCurrentPage(prev => Math.min(prev + 1, totalPages))}
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

// Main dashboard component
const AnalyticsDashboard = () => {
  const [activeTab, setActiveTab] = useState('overview');
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState({});
  const [drillDownData, setDrillDownData] = useState(null);
  const [selectedQuery, setSelectedQuery] = useState(null);
  
  const tabs = [
    { id: 'overview', label: 'Overview', icon: BarChart3 },
    { id: 'queries', label: 'Query History', icon: Database },
    { id: 'warehouses', label: 'Warehouses', icon: Server },
    { id: 'users', label: 'Users', icon: Users },
    { id: 'details', label: 'Query Details', icon: Activity }
  ];

  // API functions
  const fetchData = async (endpoint) => {
    try {
      const response = await fetch(`${API_BASE_URL}${endpoint}`);
      if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
      return await response.json();
    } catch (error) {
      console.error(`Error fetching ${endpoint}:`, error);
      return [];
    }
  };

  const refreshData = async () => {
    setLoading(true);
    try {
      const [
        queryHistory,
        queryDetails,
        warehouseAnalytics,
        userPerformance,
        accountSummary
      ] = await Promise.all([
        fetchData('/query-history-summary'),
        fetchData('/query-details-complete'),
        fetchData('/warehouse-analytics'),
        fetchData('/user-performance-report'),
        fetchData('/account-summary')
      ]);

      setData({
        queryHistory,
        queryDetails,
        warehouseAnalytics,
        userPerformance,
        accountSummary
      });
    } catch (error) {
      console.error('Error refreshing data:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleWarehouseDrillDown = async (warehouse, queryType) => {
    setLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/warehouse-drill-down`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          warehouse_id: warehouse.WAREHOUSE_ID,
          warehouse_name: warehouse.WAREHOUSE_NAME,
          query_type: queryType
        })
      });
      
      const result = await response.json();
      setDrillDownData({
        type: 'warehouse',
        data: result,
        warehouse,
        queryType
      });
    } catch (error) {
      console.error('Error in warehouse drill-down:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleUserDrillDown = async (user, flagType) => {
    setLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/user-drill-down`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user_name: user.user_name,
          flag_type: flagType
        })
      });
      
      const result = await response.json();
      setDrillDownData({
        type: 'user',
        data: result,
        user,
        flagType
      });
    } catch (error) {
      console.error('Error in user drill-down:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleQueryDetails = async (queryId) => {
    setLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/query-details/${queryId}`);
      const result = await response.json();
      setSelectedQuery(result);
    } catch (error) {
      console.error('Error fetching query details:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refreshData();
  }, []);

  // Overview Tab
  const OverviewTab = () => {
    const summary = data.accountSummary?.[0] || {};
    
    const metrics = [
      { label: 'Total Queries', value: formatNumber(summary.TOTAL_QUERIES), icon: Database, color: 'blue' },
      { label: 'Unique Users', value: formatNumber(summary.UNIQUE_USERS), icon: Users, color: 'green' },
      { label: 'Warehouses', value: formatNumber(summary.UNIQUE_WAREHOUSES), icon: Server, color: 'purple' },
      { label: 'Total Credits', value: formatNumber(summary.TOTAL_CREDITS), icon: Zap, color: 'yellow' }
    ];

    return (
      <div className="space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {metrics.map((metric, index) => (
            <div key={index} className={`bg-white p-6 rounded-lg shadow-lg border-l-4 border-${metric.color}-500`}>
              <div className="flex items-center">
                <div className={`p-2 rounded-lg bg-${metric.color}-100`}>
                  <metric.icon className={`w-6 h-6 text-${metric.color}-600`} />
                </div>
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-600">{metric.label}</p>
                  <p className="text-2xl font-bold text-gray-900">{metric.value}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
        
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-white p-6 rounded-lg shadow-lg">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">Recent Query Performance</h3>
            <div className="space-y-3">
              {data.queryHistory?.slice(0, 5).map((query, index) => (
                <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded">
                  <div>
                    <p className="font-medium text-sm">{query.QUERY_ID}</p>
                    <p className="text-xs text-gray-600">{query.USER_NAME}</p>
                  </div>
                  <span className={`px-2 py-1 text-xs rounded ${
                    query.EXECUTION_STATUS === 'SUCCESS' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                  }`}>
                    {query.EXECUTION_STATUS}
                  </span>
                </div>
              ))}
            </div>
          </div>
          
          <div className="bg-white p-6 rounded-lg shadow-lg">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">Top Warehouses by Usage</h3>
            <div className="space-y-3">
              {data.warehouseAnalytics?.slice(0, 5).map((warehouse, index) => (
                <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded">
                  <div>
                    <p className="font-medium text-sm">{warehouse.WAREHOUSE_NAME}</p>
                    <p className="text-xs text-gray-600">{warehouse.WAREHOUSE_SIZE}</p>
                  </div>
                  <span className="text-sm font-bold text-blue-600">
                    {formatNumber(warehouse.TOTAL_QUERIES)} queries
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  };

  // Query Details Modal
  const QueryDetailsModal = () => {
    if (!selectedQuery) return null;

    const detailSections = [
      {
        title: 'Basic Information',
        fields: [
          { label: 'Query ID', value: selectedQuery.QUERY_ID },
          { label: 'Query Type', value: selectedQuery.QUERY_TYPE },
          { label: 'User Name', value: selectedQuery.USER_NAME },
          { label: 'Warehouse', value: selectedQuery.WAREHOUSE_NAME },
          { label: 'Database', value: selectedQuery.DATABASE_NAME },
          { label: 'Schema', value: selectedQuery.SCHEMA_NAME }
        ]
      },
      {
        title: 'Performance Metrics',
        fields: [
          { label: 'Total Elapsed Time', value: formatDuration(selectedQuery.TOTAL_ELAPSED_TIME) },
          { label: 'Execution Time', value: formatDuration(selectedQuery.EXECUTION_TIME) },
          { label: 'Compilation Time', value: formatDuration(selectedQuery.COMPILATION_TIME) },
          { label: 'Bytes Scanned', value: formatBytes(selectedQuery.BYTES_SCANNED) },
          { label: 'Rows Produced', value: formatNumber(selectedQuery.ROWS_PRODUCED) },
          { label: 'Credits Used', value: formatNumber(selectedQuery.CREDITS_USED_CLOUD_SERVICES) }
        ]
      },
      {
        title: 'Execution Details',
        fields: [
          { label: 'Execution Status', value: selectedQuery.EXECUTION_STATUS },
          { label: 'Start Time', value: selectedQuery.START_TIME },
          { label: 'End Time', value: selectedQuery.END_TIME },
          { label: 'Error Code', value: selectedQuery.ERROR_CODE || 'None' },
          { label: 'Cache Hit %', value: `${selectedQuery.PERCENTAGE_SCANNED_FROM_CACHE || 0}%` },
          { label: 'Partitions Scanned', value: `${selectedQuery.PARTITIONS_SCANNED || 0}/${selectedQuery.PARTITIONS_TOTAL || 0}` }
        ]
      }
    ];

    return (
      <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
        <div className="bg-white rounded-lg shadow-xl max-w-6xl max-h-[90vh] overflow-auto">
          <div className="sticky top-0 bg-white border-b px-6 py-4 flex justify-between items-center">
            <h2 className="text-xl font-bold text-gray-800">Query Details</h2>
            <button
              onClick={() => setSelectedQuery(null)}
              className="text-gray-500 hover:text-gray-700"
            >
              ×
            </button>
          </div>
          
          <div className="p-6 space-y-6">
            {/* Query Text */}
            <div>
              <h3 className="text-lg font-semibold text-gray-800 mb-3">Query Text</h3>
              <div className="bg-gray-100 p-4 rounded-lg overflow-x-auto">
                <pre className="text-sm text-gray-800 whitespace-pre-wrap">
                  {selectedQuery.QUERY_TEXT}
                </pre>
              </div>
            </div>

            {/* Detail Sections */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {detailSections.map((section, index) => (
                <div key={index} className="bg-gray-50 p-4 rounded-lg">
                  <h4 className="font-semibold text-gray-800 mb-3">{section.title}</h4>
                  <div className="space-y-2">
                    {section.fields.map((field, fieldIndex) => (
                      <div key={fieldIndex} className="flex justify-between">
                        <span className="text-sm text-gray-600">{field.label}:</span>
                        <span className="text-sm font-medium text-gray-800">{field.value}</span>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  };

  // Drill Down View
  const DrillDownView = () => {
    if (!drillDownData) return null;

    const handleBack = () => {
      setDrillDownData(null);
    };

    if (drillDownData.type === 'warehouse') {
      const columns = [
        { key: 'user_name', label: 'User Name', sortable: true },
        { key: 'query_count', label: 'Query Count', sortable: true },
        { 
          key: 'actions', 
          label: 'Actions', 
          sortable: false,
          render: (_, row) => (
            <button
              onClick={() => handleQueryDetails(row.query_ids[0])}
              className="bg-blue-600 text-white px-3 py-1 rounded text-sm hover:bg-blue-700 flex items-center"
            >
              <Eye className="w-4 h-4 mr-1" />
              View Queries
            </button>
          )
        }
      ];

      return (
        <div className="space-y-6">
          <div className="flex items-center space-x-4">
            <button
              onClick={handleBack}
              className="flex items-center text-blue-600 hover:text-blue-800"
            >
              <ArrowLeft className="w-4 h-4 mr-1" />
              Back to Warehouses
            </button>
            <h2 className="text-xl font-bold text-gray-800">
              {drillDownData.data.warehouse_name} - {drillDownData.queryType.replace('_', ' ')}
            </h2>
          </div>
          
          <InteractiveTable
            data={drillDownData.data.users}
            columns={columns}
            title={`Users with ${drillDownData.queryType.replace('_', ' ')} queries`}
          />
        </div>
      );
    }

    if (drillDownData.type === 'user') {
      const columns = [
        { key: 'query_id', label: 'Query ID', sortable: true },
        { key: 'query_text_preview', label: 'Query Preview', sortable: false },
        { 
          key: 'execution_time_ms', 
          label: 'Duration', 
          sortable: true,
          render: (value) => formatDuration(value)
        },
        { 
          key: 'bytes_scanned', 
          label: 'Bytes Scanned', 
          sortable: true,
          render: (value) => formatBytes(value)
        },
        { key: 'start_time', label: 'Start Time', sortable: true },
        { 
          key: 'actions', 
          label: 'Actions', 
          sortable: false,
          render: (_, row) => (
            <button
              onClick={() => handleQueryDetails(row.query_id)}
              className="bg-green-600 text-white px-3 py-1 rounded text-sm hover:bg-green-700 flex items-center"
            >
              <Eye className="w-4 h-4 mr-1" />
              View Details
            </button>
          )
        }
      ];

      return (
        <div className="space-y-6">
          <div className="flex items-center space-x-4">
            <button
              onClick={handleBack}
              className="flex items-center text-blue-600 hover:text-blue-800"
            >
              <ArrowLeft className="w-4 h-4 mr-1" />
              Back to Users
            </button>
            <h2 className="text-xl font-bold text-gray-800">
              {drillDownData.data.user_name} - {drillDownData.flagType.replace('_', ' ')}
            </h2>
          </div>
          
          <InteractiveTable
            data={drillDownData.data.queries}
            columns={columns}
            title={`${drillDownData.flagType.replace('_', ' ')} queries`}
          />
        </div>
      );
    }

    return null;
  };

  // Tab Content Components
  const QueryHistoryTab = () => {
    const columns = [
      { key: 'QUERY_ID', label: 'Query ID', sortable: true },
      { key: 'USER_NAME', label: 'User', sortable: true },
      { key: 'WAREHOUSE_NAME', label: 'Warehouse', sortable: true },
      { key: 'QUERY_TYPE', label: 'Type', sortable: true },
      { 
        key: 'TOTAL_ELAPSED_TIME', 
        label: 'Duration', 
        sortable: true,
        render: (value) => formatDuration(value)
      },
      { key: 'EXECUTION_STATUS', label: 'Status', sortable: true },
      { key: 'START_TIME', label: 'Start Time', sortable: true },
      { 
        key: 'actions', 
        label: 'Actions', 
        sortable: false,
        render: (_, row) => (
          <button
            onClick={() => handleQueryDetails(row.QUERY_ID)}
            className="bg-blue-600 text-white px-3 py-1 rounded text-sm hover:bg-blue-700 flex items-center"
          >
            <Eye className="w-4 h-4 mr-1" />
            Details
          </button>
        )
      }
    ];

    return (
      <InteractiveTable
        data={data.queryHistory || []}
        columns={columns}
        title="Query History Summary"
        onRowClick={(row) => handleQueryDetails(row.QUERY_ID)}
      />
    );
  };

  const WarehousesTab = () => {
    if (drillDownData?.type === 'warehouse') {
      return <DrillDownView />;
    }

    const columns = [
      { key: 'WAREHOUSE_NAME', label: 'Warehouse Name', sortable: true },
      { key: 'WAREHOUSE_SIZE', label: 'Size', sortable: true },
      { key: 'TOTAL_QUERIES', label: 'Total Queries', sortable: true },
      { key: 'SUCCESSFUL_QUERIES', label: 'Success', sortable: true },
      { key: 'FAILED_QUERIES', label: 'Failed', sortable: true },
      { 
        key: 'QUERIES_1_10_SEC', 
        label: '1-10s', 
        sortable: true,
        render: (value, row) => (
          <button
            onClick={() => handleWarehouseDrillDown(row, '1-10_sec')}
            className="text-blue-600 hover:text-blue-800 underline"
          >
            {formatNumber(value)}
          </button>
        )
      },
      { 
        key: 'QUERIES_10_20_SEC', 
        label: '10-20s', 
        sortable: true,
        render: (value, row) => (
          <button
            onClick={() => handleWarehouseDrillDown(row, '10-20_sec')}
            className="text-blue-600 hover:text-blue-800 underline"
          >
            {formatNumber(value)}
          </button>
        )
      },
      { 
        key: 'QUERIES_SPILLED_LOCAL', 
        label: 'Spilled Local', 
        sortable: true,
        render: (value, row) => (
          <button
            onClick={() => handleWarehouseDrillDown(row, 'spilled_local')}
            className="text-red-600 hover:text-red-800 underline"
          >
            {formatNumber(value)}
          </button>
        )
      },
      { 
        key: 'TOTAL_CREDITS_USED', 
        label: 'Credits Used', 
        sortable: true,
        render: (value) => formatNumber(value)
      }
    ];

    return (
      <InteractiveTable
        data={data.warehouseAnalytics || []}
        columns={columns}
        title="Warehouse Analytics"
      />
    );
  };

  const UsersTab = () => {
    if (drillDownData?.type === 'user') {
      return <DrillDownView />;
    }

    // Group user performance data by user_name
    const groupedUsers = useMemo(() => {
      const userMap = {};
      (data.userPerformance || []).forEach(row => {
        const userName = row.user_name;
        if (!userMap[userName]) {
          userMap[userName] = { 
            user_name: userName,
            flags: {}
          };
        }
        userMap[userName].flags[row.flag_type] = row;
      });
      return Object.values(userMap);
    }, [data.userPerformance]);

    const columns = [
      { key: 'user_name', label: 'User Name', sortable: true },
      { 
        key: 'over_provisioned', 
        label: 'Over Provisioned', 
        sortable: true,
        render: (_, row) => {
          const count = row.flags.over_provisioned?.over_provisioned || 0;
          return count > 0 ? (
            <button
              onClick={() => handleUserDrillDown(row, 'over_provisioned')}
              className="text-orange-600 hover:text-orange-800 underline"
            >
              {formatNumber(count)}
            </button>
          ) : (
            <span className="text-gray-500">0</span>
          );
        }
      },
      { 
        key: 'slow_query', 
        label: 'Slow Queries', 
        sortable: true,
        render: (_, row) => {
          const count = row.flags.slow_query?.slow_query || 0;
          return count > 0 ? (
            <button
              onClick={() => handleUserDrillDown(row, 'slow_query')}
              className="text-red-600 hover:text-red-800 underline"
            >
              {formatNumber(count)}
            </button>
          ) : (
            <span className="text-gray-500">0</span>
          );
        }
      },
      { 
        key: 'spilled', 
        label: 'Spilled Queries', 
        sortable: true,
        render: (_, row) => {
          const count = row.flags.spilled?.spilled || 0;
          return count > 0 ? (
            <button
              onClick={() => handleUserDrillDown(row, 'spilled')}
              className="text-red-600 hover:text-red-800 underline"
            >
              {formatNumber(count)}
            </button>
          ) : (
            <span className="text-gray-500">0</span>
          );
        }
      },
      { 
        key: 'failed_cancelled', 
        label: 'Failed/Cancelled', 
        sortable: true,
        render: (_, row) => {
          const count = row.flags.failed_cancelled?.failed_cancelled || 0;
          return count > 0 ? (
            <button
              onClick={() => handleUserDrillDown(row, 'failed_cancelled')}
              className="text-red-600 hover:text-red-800 underline"
            >
              {formatNumber(count)}
            </button>
          ) : (
            <span className="text-gray-500">0</span>
          );
        }
      },
      { 
        key: 'select_star', 
        label: 'SELECT * Queries', 
        sortable: true,
        render: (_, row) => {
          const count = row.flags.select_star?.select_star || 0;
          return count > 0 ? (
            <button
              onClick={() => handleUserDrillDown(row, 'select_star')}
              className="text-yellow-600 hover:text-yellow-800 underline"
            >
              {formatNumber(count)}
            </button>
          ) : (
            <span className="text-gray-500">0</span>
          );
        }
      }
    ];

    return (
      <InteractiveTable
        data={groupedUsers}
        columns={columns}
        title="User Performance Report"
      />
    );
  };

  const QueryDetailsTab = () => {
    const columns = [
      { key: 'QUERY_ID', label: 'Query ID', sortable: true },
      { key: 'USER_NAME', label: 'User', sortable: true },
      { key: 'WAREHOUSE_NAME', label: 'Warehouse', sortable: true },
      { key: 'DATABASE_NAME', label: 'Database', sortable: true },
      { key: 'QUERY_TYPE', label: 'Type', sortable: true },
      { 
        key: 'TOTAL_ELAPSED_TIME', 
        label: 'Duration', 
        sortable: true,
        render: (value) => formatDuration(value)
      },
      { 
        key: 'BYTES_SCANNED', 
        label: 'Bytes Scanned', 
        sortable: true,
        render: (value) => formatBytes(value)
      },
      { key: 'ROWS_PRODUCED', label: 'Rows', sortable: true },
      { key: 'EXECUTION_STATUS', label: 'Status', sortable: true },
      { 
        key: 'actions', 
        label: 'Actions', 
        sortable: false,
        render: (_, row) => (
          <button
            onClick={() => handleQueryDetails(row.QUERY_ID)}
            className="bg-blue-600 text-white px-3 py-1 rounded text-sm hover:bg-blue-700 flex items-center"
          >
            <Eye className="w-4 h-4 mr-1" />
            Full Details
          </button>
        )
      }
    ];

    return (
      <InteractiveTable
        data={data.queryDetails || []}
        columns={columns}
        title="Complete Query Details"
      />
    );
  };

  const renderTabContent = () => {
    switch (activeTab) {
      case 'overview':
        return <OverviewTab />;
      case 'queries':
        return <QueryHistoryTab />;
      case 'warehouses':
        return <WarehousesTab />;
      case 'users':
        return <UsersTab />;
      case 'details':
        return <QueryDetailsTab />;
      default:
        return <OverviewTab />;
    }
  };

  if (loading) {
    return <LoadingSpinner />;
  }

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-4">
            <h1 className="text-2xl font-bold text-gray-900">Snowflake Analytics Dashboard</h1>
            <button
              onClick={refreshData}
              disabled={loading}
              className="bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 disabled:opacity-50 flex items-center"
            >
              <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
              Refresh Data
            </button>
          </div>
        </div>
      </div>

      {/* Navigation Tabs */}
      <div className="bg-white border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <nav className="flex space-x-8">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => {
                  setActiveTab(tab.id);
                  setDrillDownData(null);
                }}
                className={`py-4 px-1 border-b-2 font-medium text-sm flex items-center ${
                  activeTab === tab.id
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                <tab.icon className="w-5 h-5 mr-2" />
                {tab.label}
              </button>
            ))}
          </nav>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {renderTabContent()}
      </div>

      {/* Query Details Modal */}
      {selectedQuery && <QueryDetailsModal />}
    </div>
  );
};

export default AnalyticsDashboard;
