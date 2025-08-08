import React, { useState, useEffect, useMemo } from 'react';
import { useTable, useSortBy, useFilters, useGlobalFilter } from 'react-table';
import { useNavigate, useParams } from 'react-router-dom';
import { Search, ChevronDown, ChevronUp, Filter } from 'lucide-react';

// Column Filter Component
const DefaultColumnFilter = ({ column: { filterValue, setFilter } }) => (
  <input
    value={filterValue || ''}
    onChange={e => setFilter(e.target.value || undefined)}
    placeholder="Filter..."
    className="mt-1 p-2 border rounded w-full text-sm"
  />
);

// Global Filter Component
const GlobalFilter = ({ globalFilter, setGlobalFilter }) => (
  <div className="flex items-center mb-4">
    <Search className="w-5 h-5 mr-2 text-gray-500" />
    <input
      value={globalFilter || ''}
      onChange={e => setGlobalFilter(e.target.value || undefined)}
      placeholder="Search all columns..."
      className="p-2 border rounded w-full max-w-md text-sm"
    />
  </div>
);

// DataTable Component
const DataTable = ({ columns, data, onRowClick }) => {
  const defaultColumn = useMemo(
    () => ({
      Filter: DefaultColumnFilter,
    }),
    []
  );

  const {
    getTableProps,
    getTableBodyProps,
    headerGroups,
    rows,
    prepareRow,
    state,
    setGlobalFilter,
  } = useTable(
    {
      columns,
      data,
      defaultColumn,
    },
    useFilters,
    useGlobalFilter,
    useSortBy
  );

  return (
    <div className="overflow-x-auto">
      <GlobalFilter globalFilter={state.globalFilter} setGlobalFilter={setGlobalFilter} />
      <table {...getTableProps()} className="min-w-full bg-white border">
        <thead>
          {headerGroups.map(headerGroup => (
            <tr {...headerGroup.getHeaderGroupProps()}>
              {headerGroup.headers.map(column => (
                <th
                  {...column.getHeaderProps(column.getSortByToggleProps())}
                  className="px-4 py-2 border-b text-left text-sm font-medium text-gray-700"
                >
                  <div className="flex items-center">
                    {column.render('Header')}
                    <span className="ml-2">
                      {column.isSorted ? (
                        column.isSortedDesc ? (
                          <ChevronDown className="w-4 h-4" />
                        ) : (
                          <ChevronUp className="w-4 h-4" />
                        )
                      ) : null}
                    </span>
                  </div>
                  <div>{column.canFilter ? column.render('Filter') : null}</div>
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody {...getTableBodyProps()}>
          {rows.map(row => {
            prepareRow(row);
            return (
              <tr
                {...row.getRowProps()}
                className="hover:bg-gray-100 cursor-pointer"
                onClick={() => onRowClick && onRowClick(row.original)}
              >
                {row.cells.map(cell => (
                  <td {...cell.getCellProps()} className="px-4 py-2 border-b text-sm">
                    {cell.render('Cell')}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

// Query Details Page
const QueryDetailsPage = () => {
  const { queryId } = useParams();
  const [queryDetails, setQueryDetails] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`http://localhost:5000/query-details/${queryId}`)
      .then(res => res.json())
      .then(data => {
        setQueryDetails(data[0]);
        setLoading(false);
      })
      .catch(err => {
        console.error(err);
        setLoading(false);
      });
  }, [queryId]);

  if (loading) return <div className="p-4">Loading...</div>;
  if (!queryDetails) return <div className="p-4">Query not found</div>;

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <h2 className="text-2xl font-bold mb-4">Query Details: {queryId}</h2>
      <div className="bg-white p-4 rounded shadow">
        {Object.entries(queryDetails).map(([key, value]) => (
          <div key={key} className="mb-2">
            <strong className="text-gray-700">{key}:</strong>{' '}
            {typeof value === 'object' ? JSON.stringify(value) : value}
          </div>
        ))}
      </div>
    </div>
  );
};

// Warehouse Drill-Down Page
const WarehouseDrillDown = () => {
  const { warehouseId, queryType } = useParams();
  const navigate = useNavigate();
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Fetch QUERY_IDS for the warehouse and query type
    fetch('http://localhost:5000/get-data/WAREHOUSE_ANALYTICS_DASHBOARD_with_queries')
      .then(res => res.json())
      .then(data => {
        const warehouse = data.data.find(w => w.WAREHOUSE_ID === warehouseId);
        if (warehouse) {
          const queryTypeMapping = {
            QUERIES_1_10_SEC: '1-10_sec_ids',
            QUERIES_10_20_SEC: '10-20_sec_ids',
            QUERIES_20_60_SEC: '20-60_sec_ids',
            QUERIES_1_3_MIN: '1-3_min_ids',
            QUERIES_3_5_MIN: '3-5_min_ids',
            QUERIES_5_PLUS_MIN: '5_plus_min_ids',
            QUEUED_1_2_MIN: 'queued_1-2_min_ids',
            QUEUED_2_5_MIN: 'queued_2-5_min_ids',
            QUEUED_5_10_MIN: 'queued_5-10_min_ids',
            QUEUED_10_20_MIN: 'queued_10-20_min_ids',
            QUEUED_20_PLUS_MIN: 'queued_20_plus_min_ids',
            QUERIES_SPILLED_LOCAL: 'spilled_local_ids',
            QUERIES_SPILLED_REMOTE: 'spilled_remote_ids',
            FAILED_QUERIES: 'failed_queries_ids',
            SUCCESSFUL_QUERIES: 'successful_queries_ids',
            RUNNING_QUERIES: 'running_queries_ids',
            QUERIES_0_20_CENTS: 'credit_0-20_cents_ids',
            QUERIES_20_40_CENTS: 'credit_20-40_cents_ids',
            QUERIES_40_60_CENTS: 'credit_40-60_cents_ids',
            QUERIES_60_80_CENTS: 'credit_60-80_cents_ids',
            QUERIES_80_100_CENTS: 'credit_80-100_cents_ids',
            QUERIES_100_PLUS_CENTS: 'credit_100_plus_cents_ids'
          };
          const queryIds = warehouse.QUERY_IDS[queryTypeMapping[queryType]] || [];
          fetch('http://localhost:5000/users-by-warehouse', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ warehouse_id: warehouseId, query_type, query_ids: queryIds }),
          })
            .then(res => res.json())
            .then(data => {
              setUsers(data);
              setLoading(false);
            });
        } else {
          setLoading(false);
        }
      });
  }, [warehouseId, queryType]);

  const columns = useMemo(
    () => [
      { Header: 'User Name', accessor: 'user_name' },
      { Header: 'Query Count', accessor: 'query_count' },
      {
        Header: 'Queries',
        accessor: 'query_ids',
        Cell: ({ value }) => (
          <button
            onClick={() => navigate(`/warehouse/${warehouseId}/${queryType}/queries`, { state: { queryIds: value } })}
            className="text-blue-600 hover:underline"
          >
            View Queries
          </button>
        ),
      },
    ],
    [navigate, warehouseId, queryType]
  );

  if (loading) return <div className="p-4">Loading...</div>;

  return (
    <div className="p-6">
      <h2 className="text-2xl font-bold mb-4">Users for Warehouse {warehouseId} - {queryType}</h2>
      <DataTable columns={columns} data={users} />
    </div>
  );
};

// Warehouse Queries Page
const WarehouseQueries = () => {
  const { warehouseId, queryType } = useParams();
  const { queryIds } = useLocation().state || { queryIds: [] };
  const navigate = useNavigate();
  const [queries, setQueries] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('http://localhost:5000/query-previews', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query_ids: queryIds }),
    })
      .then(res => res.json())
      .then(data => {
        setQueries(data);
        setLoading(false);
      });
  }, [queryIds]);

  const columns = useMemo(
    () => [
      { Header: 'Query ID', accessor: 'QUERY_ID' },
      { Header: 'Query Text Preview', accessor: 'QUERY_TEXT_PREVIEW' },
      {
        Header: 'Actions',
        accessor: 'QUERY_ID',
        Cell: ({ value }) => (
          <button
            onClick={() => navigate(`/query/${value}`)}
            className="text-blue-600 hover:underline"
          >
            View Details
          </button>
        ),
      },
    ],
    [navigate]
  );

  if (loading) return <div className="p-4">Loading...</div>;

  return (
    <div className="p-6">
      <h2 className="text-2xl font-bold mb-4">Queries for Warehouse {warehouseId} - {queryType}</h2>
      <DataTable columns={columns} data={queries} />
    </div>
  );
};

// User Drill-Down Page
const UserDrillDown = () => {
  const { userName, flagType } = useParams();
  const navigate = useNavigate();
  const [queries, setQueries] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('http://localhost:5000/get-data/user_query_performance_report')
      .then(res => res.json())
      .then(data => {
        const userData = data.data.find(u => u.user_name === userName && u.flag_type === flagType);
        const queryIds = userData ? userData.sample_queries.map(q => q.query_id) : [];
        fetch('http://localhost:5000/query-previews', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query_ids: queryIds }),
        })
          .then(res => res.json())
          .then(data => {
            setQueries(data);
            setLoading(false);
          });
      });
  }, [userName, flagType]);

  const columns = useMemo(
    () => [
      { Header: 'Query ID', accessor: 'QUERY_ID' },
      { Header: 'Query Text Preview', accessor: 'QUERY_TEXT_PREVIEW' },
      {
        Header: 'Actions',
        accessor: 'QUERY_ID',
        Cell: ({ value }) => (
          <button
            onClick={() => navigate(`/query/${value}`)}
            className="text-blue-600 hover:underline"
          >
            View Details
          </button>
        ),
      },
    ],
    [navigate]
  );

  if (loading) return <div className="p-4">Loading...</div>;

  return (
    <div className="p-6">
      <h2 className="text-2xl font-bold mb-4">Queries for User {userName} - {flagType}</h2>
      <DataTable columns={columns} data={queries} />
    </div>
  );
};

// Main Dashboard Component
const Dashboard = () => {
  const [activeTab, setActiveTab] = useState('QUERY_HISTORY_SUMMARY');
  const [data, setData] = useState({});
  const navigate = useNavigate();

  useEffect(() => {
    const tables = [
      'QUERY_HISTORY_SUMMARY',
      'QUERY_DETAILS_COMPLETE',
      'WAREHOUSE_ANALYTICS_DASHBOARD_with_queries',
      'user_query_performance_report',
    ];

    tables.forEach(table => {
      fetch(`http://localhost:5000/get-data/${table}`)
        .then(res => res.json())
        .then(result => {
          setData(prev => ({ ...prev, [table]: result.data }));
        })
        .catch(err => console.error(err));
    });
  }, []);

  const getColumns = (tableName) => {
    if (!data[tableName] || data[tableName].length === 0) return [];

    const columns = Object.keys(data[tableName][0]).map(key => {
      if (
        (tableName === 'WAREHOUSE_ANALYTICS_DASHBOARD_with_queries' && key === 'QUERY_IDS') ||
        (tableName === 'user_query_performance_report' && ['sample_queries', 'recommendation'].includes(key))
      ) {
        return null; // Exclude these columns
      }

      // For warehouse query count columns, make them clickable
      if (
        tableName === 'WAREHOUSE_ANALYTICS_DASHBOARD_with_queries' &&
        [
          'QUERIES_1_10_SEC',
          'QUERIES_10_20_SEC',
          'QUERIES_20_60_SEC',
          'QUERIES_1_3_MIN',
          'QUERIES_3_5_MIN',
          'QUERIES_5_PLUS_MIN',
          'QUEUED_1_2_MIN',
          'QUEUED_2_5_MIN',
          'QUEUED_5_10_MIN',
          'QUEUED_10_20_MIN',
          'QUEUED_20_PLUS_MIN',
          'QUERIES_SPILLED_LOCAL',
          'QUERIES_SPILLED_REMOTE',
          'FAILED_QUERIES',
          'SUCCESSFUL_QUERIES',
          'RUNNING_QUERIES',
          'QUERIES_0_20_CENTS',
          'QUERIES_20_40_CENTS',
          'QUERIES_40_60_CENTS',
          'QUERIES_60_80_CENTS',
          'QUERIES_80_100_CENTS',
          'QUERIES_100_PLUS_CENTS',
        ].includes(key)
      ) {
        return {
          Header: key,
          accessor: key,
          Cell: ({ value, row }) => (
            <button
              onClick={() => navigate(`/warehouse/${row.original.WAREHOUSE_ID}/${key}`)}
              className="text-blue-600 hover:underline"
            >
              {value}
            </button>
          ),
        };
      }

      // For user query count columns, make them clickable
      if (
        tableName === 'user_query_performance_report' &&
        [
          'over_provisioned',
          'peak_hour_long_running',
          'select_star',
          'unpartitioned_scan',
          'spilled',
          'failed_cancelled',
          'zero_result_query',
          'high_compile_time',
          'slow_query',
          'cartesian_join',
          'unlimited_order_by',
          'large_group_by',
          'expensive_distinct',
          'inefficient_like',
          'no_results_with_scan',
          'high_compile_ratio',
        ].includes(key)
      ) {
        return {
          Header: key,
          accessor: key,
          Cell: ({ value, row }) => (
            <button
              onClick={() => navigate(`/user/${row.original.user_name}/${key}`)}
              className="text-blue-600 hover:underline"
            >
              {value}
            </button>
          ),
        };
      }

      return { Header: key, accessor: key };
    });

    return columns.filter(col => col !== null);
  };

  const tabs = [
    { name: 'QUERY_HISTORY_SUMMARY', label: 'Query History' },
    { name: 'QUERY_DETAILS_COMPLETE', label: 'Query Details' },
    { name: 'WAREHOUSE_ANALYTICS_DASHBOARD_with_queries', label: 'Warehouse Analytics' },
    { name: 'user_query_performance_report', label: 'User Performance' },
  ];

  return (
    <div className="min-h-screen bg-gray-100">
      <div className="max-w-7xl mx-auto py-6 sm:px-6 lg:px-8">
        <div className="px-4 py-6 sm:px-0">
          <div className="border-b border-gray-200">
            <nav className="-mb-px flex space-x-8">
              {tabs.map(tab => (
                <button
                  key={tab.name}
                  onClick={() => setActiveTab(tab.name)}
                  className={`${
                    activeTab === tab.name
                      ? 'border-indigo-500 text-indigo-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  } whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm`}
                >
                  {tab.label}
                </button>
              ))}
            </nav>
          </div>
          <div className="mt-6">
            {data[activeTab] ? (
              <DataTable columns={getColumns(activeTab)} data={data[activeTab]} />
            ) : (
              <div>Loading...</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

// App Component with Router
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
const App = () => (
  <Router>
    <Routes>
      <Route path="/" element={<Dashboard />} />
      <Route path="/warehouse/:warehouseId/:queryType" element={<WarehouseDrillDown />} />
      <Route path="/warehouse/:warehouseId/:queryType/queries" element={<WarehouseQueries />} />
      <Route path="/user/:userName/:flagType" element={<UserDrillDown />} />
      <Route path="/query/:queryId" element={<QueryDetailsPage />} />
    </Routes>
  </Router>
);

export default App;
