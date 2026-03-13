import React, { useState, useEffect } from 'react';
import { Zap, Calendar, Tag, Copy, CheckCircle, Filter, Download, FileJson, FileSpreadsheet } from 'lucide-react';
import toast from 'react-hot-toast';
import { contributionAPI } from '../../services/api';

// Export utilities
const exportToCSV = (data, filename) => {
  if (!data || data.length === 0) {
    toast.error('No data to export');
    return;
  }

  const headers = Object.keys(data[0]);
  const csv = [
    headers.join(','),
    ...data.map(row => 
      headers.map(header => {
        const value = row[header];
        // Handle special characters in CSV
        if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return value;
      }).join(',')
    )
  ].join('\n');

  const blob = new Blob([csv], { type: 'text/csv' });
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${filename}.csv`;
  document.body.appendChild(a);
  a.click();
  window.URL.revokeObjectURL(url);
  document.body.removeChild(a);
  toast.success(`Exported ${filename}.csv`);
};

const exportToExcel = (data, filename) => {
  if (!data || data.length === 0) {
    toast.error('No data to export');
    return;
  }

  const headers = Object.keys(data[0]);
  let xlsxContent = '\uFEFF'; // BOM for Excel UTF-8
  
  // Add header row
  xlsxContent += headers.join('\t') + '\n';
  
  // Add data rows
  data.forEach(row => {
    xlsxContent += headers.map(header => {
      const value = row[header];
      return typeof value === 'number' ? value : `"${value}"`;
    }).join('\t') + '\n';
  });

  const blob = new Blob([xlsxContent], { type: 'application/vnd.ms-excel' });
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${filename}.xlsx`;
  document.body.appendChild(a);
  a.click();
  window.URL.revokeObjectURL(url);
  document.body.removeChild(a);
  toast.success(`Exported ${filename}.xlsx`);
};

export default function ExecutionPanel({ presets }) {
  const [selectedPresets, setSelectedPresets] = useState([]);
  const [selectedMajcats, setSelectedMajcats] = useState([]);
  const [availableMajcats, setAvailableMajcats] = useState([]);
  const [masterColumns, setMasterColumns] = useState([]);
  const [selectedMasterColumn, setSelectedMasterColumn] = useState(null);
  const [dateRange, setDateRange] = useState({ start: '', end: '' });
  const [groupingColumn, setGroupingColumn] = useState('MACRO_MVGR');
  const [availableColumns, setAvailableColumns] = useState([]);
  const [sequenceExecution, setSequenceExecution] = useState(false);
  const [saveToDb, setSaveToDb] = useState(true);
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState(null);
  const [optionsLoading, setOptionsLoading] = useState(true);

  // Auto-set date range to 26th of last month to 26th of current/next month
  useEffect(() => {
    const today = new Date();
    const currentMonth = today.getMonth();
    const currentYear = today.getFullYear();
    
    // Get 26th of previous month
    let startMonth = currentMonth - 1;
    let startYear = currentYear;
    if (startMonth < 0) {
      startMonth = 11;
      startYear = currentYear - 1;
    }
    const startDate = new Date(startYear, startMonth, 26);
    
    // Get 26th of current month (or next month if today is before 26th)
    let endDate;
    if (today.getDate() < 26) {
      // If before 26th, use 26th of previous month as end
      endDate = new Date(currentYear, currentMonth, 26);
    } else {
      // If 26th or after, use 26th of next month as end
      let endMonth = currentMonth + 1;
      let endYear = currentYear;
      if (endMonth > 11) {
        endMonth = 0;
        endYear = currentYear + 1;
      }
      endDate = new Date(endYear, endMonth, 26);
    }
    
    // Format dates as YYYY-MM-DD
    const formatDate = (date) => date.toISOString().split('T')[0];
    
    setDateRange({
      start: formatDate(startDate),
      end: formatDate(endDate)
    });
  }, []);

  // Load execution options on component mount and when grouping_column changes
  useEffect(() => {
    const loadExecutionOptions = async () => {
      try {
        setOptionsLoading(true);
        // Fetch execution options with current grouping column ONLY
        // majcats are loaded from Master_HIER_{grouping_column} with SEG filter
        // Frontend handles user selection
        const response = await contributionAPI.getExecutionOptions(groupingColumn);
        
        // Parse API response - response.data contains the actual data
        const data = response.data || response;
        
        if (data?.status === 'success') {
          // Set available columns from the API
          setAvailableColumns(data.grouping_columns || ['MACRO_MVGR', 'M_VND_CD', 'CATEGORY', 'SEGMENT']);
          
          // Set available major categories from the API
          // These are ALL majcats from Master_HIER_{grouping_column} with SEG IN ('APP', 'GM')
          setAvailableMajcats(data.majcats || []);
          
          // Set master columns from the API for context/selection
          setMasterColumns(data.master_columns || []);
          
          console.log('✅ Execution options loaded:', {
            groupingColumn: groupingColumn,
            availableMajcats: data.majcats?.length,
            masterColumns: data.master_columns?.length
          });
        } else {
          console.warn('Unexpected response structure:', data);
          setAvailableMajcats([]);
          setMasterColumns([]);
        }
      } catch (error) {
        console.error('Failed to load execution options:', error);
        // Set defaults if API fails
        setAvailableMajcats([]);
        setMasterColumns([]);
        setAvailableColumns(['MACRO_MVGR', 'M_VND_CD', 'CATEGORY', 'SEGMENT']);
        toast.error('Failed to load execution options - using defaults');
      } finally {
        setOptionsLoading(false);
      }
    };
    
    loadExecutionOptions();
  }, [groupingColumn]);

  const togglePreset = (presetName) => {
    setSelectedPresets((prev) =>
      prev.includes(presetName)
        ? prev.filter((p) => p !== presetName)
        : [...prev, presetName]
    );
  };

  const toggleMajcat = (majcat) => {
    setSelectedMajcats((prev) =>
      prev.includes(majcat)
        ? prev.filter((m) => m !== majcat)
        : [...prev, majcat]
    );
  };

  const handleSelectAllMajcats = () => {
    if (selectedMajcats.length === availableMajcats.length) {
      setSelectedMajcats([]);
    } else {
      setSelectedMajcats([...availableMajcats]);
    }
  };

  const handleSelectAll = () => {
    if (selectedPresets.length === presets.length) {
      setSelectedPresets([]);
    } else {
      setSelectedPresets(presets.map((p) => p.preset_name));
    }
  };

  const handleExecute = async () => {
    if (selectedPresets.length === 0) {
      toast.error('Please select at least one preset');
      return;
    }

    setLoading(true);
    try {
      const response = await contributionAPI.calculate({
        presets: selectedPresets,
        major_categories: selectedMajcats.length > 0 ? selectedMajcats : undefined,
        group_by: groupingColumn,
        sequence_execution: sequenceExecution,
        save_to_db: saveToDb,
      });
      
      setResults(response.data);
      toast.success('Calculation completed successfully');
    } catch (error) {
      toast.error('Calculation failed: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-6 space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Left Panel - Configuration */}
        <div className="space-y-4">
          <h3 className="font-semibold text-lg mb-4">Execution Configuration</h3>

          {/* Preset Selection */}
          <div className="space-y-3">
            <div className="flex justify-between items-center">
              <label className="label font-semibold">Select Presets*</label>
              <button
                onClick={handleSelectAll}
                className="text-xs text-primary-600 hover:underline"
              >
                {selectedPresets.length === presets.length ? 'Deselect All' : 'Select All'}
              </button>
            </div>
            <div className="space-y-2">
              {presets.length === 0 ? (
                <p className="text-gray-400 text-sm">No presets available</p>
              ) : (
                presets.map((preset) => (
                  <label key={preset.preset_name} className="flex items-center gap-3 p-2 hover:bg-gray-50 rounded cursor-pointer">
                    <input
                      type="checkbox"
                      checked={selectedPresets.includes(preset.preset_name)}
                      onChange={() => togglePreset(preset.preset_name)}
                      className="rounded"
                    />
                    <div className="flex-1">
                      <div className="text-sm font-medium">{preset.preset_name}</div>
                      <div className="text-xs text-gray-500">{preset.description}</div>
                    </div>
                    <span className={`text-xs px-2 py-0.5 rounded ${
                      preset.preset_type === 'formula'
                        ? 'bg-purple-100 text-purple-700'
                        : 'bg-blue-100 text-blue-700'
                    }`}>
                      {preset.preset_type}
                    </span>
                  </label>
                ))
              )}
            </div>
          </div>

          {/* Date Range */}
          <div className="space-y-2">
            <label className="label font-semibold flex items-center gap-2">
              <Calendar size={14} /> Date Range (Optional)
            </label>
            <div className="grid grid-cols-2 gap-2">
              <input
                type="date"
                value={dateRange.start}
                onChange={(e) => setDateRange({ ...dateRange, start: e.target.value })}
                className="input"
              />
              <input
                type="date"
                value={dateRange.end}
                onChange={(e) => setDateRange({ ...dateRange, end: e.target.value })}
                className="input"
              />
            </div>
          </div>

          {/* Product Attributes Selection - Context Only */}
          {masterColumns && masterColumns.length > 0 && (
            <div className="space-y-2">
              <label className="label font-semibold flex items-center gap-2">
                <Tag size={14} /> Product Attribute
              </label>
              <select 
                value={selectedMasterColumn || ''} 
                onChange={(e) => setSelectedMasterColumn(e.target.value || null)} 
                className="input"
                disabled={optionsLoading}
              >
                <option value="">-- None Selected --</option>
                {masterColumns.map((col) => (
                  <option key={col} value={col}>
                    {col}
                  </option>
                ))}
              </select>
              <p className="text-xs text-gray-500">
                Select for context (CLR, SZ, RNG_SEG, M_VND_CD, MACRO_MVGR, MICRO_MVGR, FAB)
              </p>
            </div>
          )}

          {/* Major Categories Selection */}
          <div className="space-y-2">
            <div className="flex justify-between items-center">
              <label className="label font-semibold flex items-center gap-2">
                <Filter size={14} /> Major Categories (Optional)
              </label>
              {availableMajcats.length > 0 && (
                <button
                  onClick={handleSelectAllMajcats}
                  className="text-xs text-primary-600 hover:underline"
                >
                  {selectedMajcats.length === availableMajcats.length ? 'Deselect All' : 'Select All'}
                </button>
              )}
            </div>
            {optionsLoading ? (
              <p className="text-sm text-gray-500">Loading major categories...</p>
            ) : availableMajcats.length === 0 ? (
              <p className="text-sm text-gray-400">No major categories available</p>
            ) : (
              <div className="grid grid-cols-2 gap-2 max-h-48 overflow-y-auto border rounded p-2">
                {availableMajcats.map((majcat) => (
                  <label key={majcat} className="flex items-center gap-2 p-2 hover:bg-gray-50 rounded cursor-pointer">
                    <input
                      type="checkbox"
                      checked={selectedMajcats.includes(majcat)}
                      onChange={() => toggleMajcat(majcat)}
                      className="rounded"
                    />
                    <span className="text-sm">{majcat}</span>
                  </label>
                ))}
              </div>
            )}
            <p className="text-xs text-gray-500">Leave empty to include all categories</p>
          </div>

          {/* Execution Options */}
          <div className="space-y-2 pt-2 border-t">
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={sequenceExecution}
                onChange={(e) => setSequenceExecution(e.target.checked)}
                className="rounded"
              />
              <span>Execute in Sequence Order</span>
            </label>
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={saveToDb}
                onChange={(e) => setSaveToDb(e.target.checked)}
                className="rounded"
              />
              <span>Save Results to Database</span>
            </label>
          </div>

          <button
            onClick={handleExecute}
            disabled={loading || selectedPresets.length === 0}
            className="btn-primary w-full mt-6"
          >
            <Zap size={16} /> {loading ? 'Executing...' : 'Execute Analysis'}
          </button>
        </div>

        {/* Right Panel - Results Summary */}
        <div className="bg-gradient-to-br from-primary-50 to-blue-50 rounded-lg p-6 border border-primary-200">
          <h3 className="font-semibold text-lg mb-4">Execution Summary</h3>

          <div className="space-y-4">
            <div className="bg-white rounded p-3">
              <div className="text-xs text-gray-600">Presets Selected</div>
              <div className="text-xl font-bold text-gray-900">{selectedPresets.length} / {presets.length}</div>
            </div>

            <div className="bg-white rounded p-3">
              <div className="text-xs text-gray-600">Grouping Column</div>
              <div className="text-lg font-semibold text-gray-900 font-mono">
                {groupingColumn}
              </div>
            </div>

            <div className="bg-white rounded p-3">
              <div className="text-xs text-gray-600">Major Categories Available</div>
              <div className="text-lg font-semibold text-gray-900">
                {availableMajcats.length} total
              </div>
              <p className="text-xs text-gray-500 mt-1">
                {selectedMajcats.length > 0 ? `${selectedMajcats.length} selected` : 'All selected by default'}
              </p>
            </div>

            <div className="bg-white rounded p-3">
              <div className="text-xs text-gray-600">Product Attribute</div>
              <div className="text-sm font-semibold text-gray-900">
                {selectedMasterColumn || '(None)'}
              </div>
              <p className="text-xs text-gray-500 mt-1">
                Context only - does not filter categories
              </p>
            </div>

            <div className="bg-white rounded p-3">
              <div className="text-xs text-gray-600">Output Level</div>
              <div className="text-sm font-semibold text-gray-900">Store & Company</div>
            </div>

            {selectedMajcats.length > 0 && (
              <div className="bg-white rounded p-3">
                <div className="text-xs text-gray-600 mb-2">Filtered Major Categories</div>
                <div className="flex flex-wrap gap-1 max-h-16 overflow-y-auto">
                  {selectedMajcats.map((cat) => (
                    <span key={cat} className="badge-secondary text-xs">{cat}</span>
                  ))}
                </div>
              </div>
            )}

            {results && (
              <div className="bg-white rounded p-3 border border-green-200">
                <div className="flex items-center gap-2 text-green-700 mb-2">
                  <CheckCircle size={16} />
                  <div className="text-sm font-semibold">Execution Successful</div>
                </div>
                <div className="text-xs space-y-1">
                  <p>Presets executed: {results.presets_executed?.length || 0}</p>
                  <p>Total duration: {results.total_duration}s</p>
                  {results.saved_tables && (
                    <p>Saved tables: {results.saved_tables.length}</p>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Results Details */}
      {results && (
        <div className="border-t pt-6 space-y-6">
          <h3 className="font-semibold text-lg mb-4">Results Details</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <div className="card">
              <div className="card-header">Presets Executed</div>
              <div className="p-4">
                <ul className="space-y-2">
                  {results.presets_executed.map((p) => (
                    <li key={p} className="flex items-center gap-2 text-sm">
                      <CheckCircle size={14} className="text-green-600" />
                      {p}
                    </li>
                  ))}
                </ul>
              </div>
            </div>

            {results.saved_tables && results.saved_tables.length > 0 && (
              <div className="card">
                <div className="card-header">Saved Database Tables</div>
                <div className="p-4">
                  <ul className="space-y-2">
                    {results.saved_tables.map((table) => (
                      <li key={table} className="flex items-center gap-2 text-xs bg-gray-50 p-2 rounded">
                        <Copy size={12} className="text-gray-500 flex-shrink-0" />
                        <span className="font-mono">{table}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            )}
          </div>

          {/* Store-Level Results Table */}
          {results.store_results && (
            <div className="card">
              <div className="card-header flex items-center justify-between">
                <span>Store-Level Results - Detailed Data</span>
                {results.store_results.full && results.store_results.full.length > 0 && (
                  <div className="flex gap-2">
                    <button
                      onClick={() => exportToCSV(results.store_results.full, 'store_detailed_results')}
                      className="btn-secondary text-xs flex items-center gap-1"
                      title="Export all data to CSV"
                    >
                      <Download size={12} /> CSV ({results.store_results.full.length} rows)
                    </button>
                    <button
                      onClick={() => exportToExcel(results.store_results.full, 'store_detailed_results')}
                      className="btn-secondary text-xs flex items-center gap-1"
                      title="Export all data to Excel"
                    >
                      <FileSpreadsheet size={12} /> Excel
                    </button>
                  </div>
                )}
              </div>
              <div className="p-4 overflow-x-auto">
                <div className="text-xs text-gray-500 mb-3">
                  Showing {results.store_results.sample?.length || 0} of {results.store_results.count} rows
                </div>
                <table className="w-full text-xs">
                  <thead className="bg-gray-50 border-b sticky top-0">
                    <tr>
                      {results.store_results.columns && results.store_results.columns.slice(0, 15).map((col) => (
                        <th key={col} className="text-left px-2 py-2 font-semibold text-gray-700 whitespace-nowrap">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {results.store_results.sample && results.store_results.sample.length > 0 ? (
                      results.store_results.sample.map((row, idx) => (
                        <tr key={idx} className="border-b hover:bg-gray-50">
                          {results.store_results.columns && results.store_results.columns.slice(0, 15).map((col) => {
                            const value = row[col];
                            const isNumeric = typeof value === 'number';
                            return (
                              <td key={col} className={`px-2 py-2 ${isNumeric ? 'text-right' : 'text-left'} text-gray-700`}>
                                {isNumeric ? typeof value === 'number' && value % 1 !== 0 ? value.toFixed(2) : value : value}
                              </td>
                            );
                          })}
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan={Math.min(15, results.store_results.columns?.length || 6)} className="px-2 py-2 text-center text-gray-500 text-sm">
                          No data available
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
                {results.store_results.columns && (
                  <p className="text-xs text-gray-500 mt-2">
                    Showing first 15 columns of {results.store_results.columns.length} total columns
                  </p>
                )}
              </div>
            </div>
          )}

          {/* Column Information */}
          {results.store_results && results.store_results.columns && (
            <div className="card">
              <div className="card-header">Available Columns ({results.store_results.columns.length})</div>
              <div className="p-4">
                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2 max-h-40 overflow-y-auto">
                  {results.store_results.columns.map((col) => (
                    <div key={col} className="text-xs bg-gray-100 rounded px-2 py-1 font-mono">
                      {col}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Company-Level Results Table */}
          {results.company_results && (
            <div className="card">
              <div className="card-header flex items-center justify-between">
                <span>Company-Level Summary</span>
                {(results.company_results.sample || results.company_results).length > 0 && (
                  <div className="flex gap-2">
                    <button
                      onClick={() => exportToCSV(results.company_results.sample || results.company_results, 'company_summary')}
                      className="btn-secondary text-xs flex items-center gap-1"
                      title="Export to CSV"
                    >
                      <Download size={12} /> CSV
                    </button>
                    <button
                      onClick={() => exportToExcel(results.company_results.sample || results.company_results, 'company_summary')}
                      className="btn-secondary text-xs flex items-center gap-1"
                      title="Export to Excel"
                    >
                      <FileSpreadsheet size={12} /> Excel
                    </button>
                  </div>
                )}
              </div>
              <div className="p-4 overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 border-b">
                    <tr>
                      <th className="text-left px-3 py-2 font-semibold text-gray-700">Preset</th>
                      <th className="text-left px-3 py-2 font-semibold text-gray-700">Major Category</th>
                      <th className="text-right px-3 py-2 font-semibold text-gray-700">Stock Cont%</th>
                      <th className="text-right px-3 py-2 font-semibold text-gray-700">Sale Cont%</th>
                      <th className="text-right px-3 py-2 font-semibold text-gray-700">Sales PSF Ach%</th>
                      <th className="text-right px-3 py-2 font-semibold text-gray-700">GM PSF Ach%</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(results.company_results.sample || results.company_results).length > 0 ? (
                      (results.company_results.sample || results.company_results).map((row, idx) => (
                        <tr key={idx} className="border-b hover:bg-gray-50">
                          <td className="px-3 py-2 text-gray-900 font-mono">{row.PRESET || row.preset || '--'}</td>
                          <td className="px-3 py-2 text-gray-900">{row.MAJ_CAT}</td>
                          <td className="px-3 py-2 text-right text-gray-700 font-mono">
                            {typeof row['STOCK_CONT%'] === 'number' ? row['STOCK_CONT%'].toFixed(2) : row['STOCK_CONT%']}%
                          </td>
                          <td className="px-3 py-2 text-right text-gray-700 font-mono">
                            {typeof row['SALE_CONT%'] === 'number' ? row['SALE_CONT%'].toFixed(2) : row['SALE_CONT%']}%
                          </td>
                          <td className="px-3 py-2 text-right text-gray-700 font-mono">
                            {typeof row['SALES_PSF_ACH%'] === 'number' ? row['SALES_PSF_ACH%'].toFixed(2) : row['SALES_PSF_ACH%']}%
                          </td>
                          <td className="px-3 py-2 text-right text-gray-700 font-mono">
                            {typeof row['GM_PSF_ACH%'] === 'number' ? row['GM_PSF_ACH%'].toFixed(2) : row['GM_PSF_ACH%']}%
                          </td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan="6" className="px-3 py-2 text-center text-gray-500 text-sm">
                          No data available
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
                {results.company_results.count && (
                  <p className="text-xs text-gray-500 mt-2">
                    Total: {results.company_results.count} rows
                  </p>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
