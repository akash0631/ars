import React, { useState, useEffect } from 'react';
import { Download, Trash2, Eye, FileJson, CheckCircle, Lock } from 'lucide-react';
import toast from 'react-hot-toast';
import { contributionAPI } from '../../services/api';

export default function ReviewExport() {
  const [savedResults, setSavedResults] = useState([]);
  const [tempResults, setTempResults] = useState(null);
  const [expandedTable, setExpandedTable] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadSavedResults();
  }, []);

  const loadSavedResults = async () => {
    try {
      const response = await contributionAPI.getSavedResults();
      setSavedResults(response.data || []);

      // Mock temp results - in real scenario would be from session store
      setTempResults({
        store_level: {
          record_count: 1250,
          columns: ['ST_CD', 'ST_NM', 'STOCK_CONT_%', 'SALE_CONT_%', 'STR', 'SALES_PSF', 'GM_PSF'],
        },
        company_level: {
          record_count: 45,
          columns: ['COMP_CD', 'COMP_NM', 'STOCK_CONT_%', 'SALE_CONT_%', 'TOTAL_STR', 'AVG_SALES_PSF'],
        },
      });
    } catch (error) {
      toast.error('Failed to load results');
    }
  };

  const handleExport = async (format = 'csv') => {
    setLoading(true);
    try {
      if (!tempResults) {
        toast.error('No temporary results to export');
        return;
      }

      await contributionAPI.export({
        format: format,
        include_store_level: true,
        include_company_level: true,
      });

      toast.success(`Export started (${format.toUpperCase()})`);
    } catch (error) {
      toast.error('Export failed');
    } finally {
      setLoading(false);
    }
  };

  const handleDownloadTable = async (tableName) => {
    try {
      const url = contributionAPI.downloadTable(tableName);
      window.open(url, '_blank');
      toast.success('Download started');
    } catch (error) {
      toast.error('Download failed');
    }
  };

  const handleDeleteTable = async (tableName) => {
    if (!window.confirm(`Delete table "${tableName}"?`)) return;

    try {
      await contributionAPI.deleteTable(tableName);
      setSavedResults((prev) => prev.filter((r) => r.table_name !== tableName));
      toast.success('Table deleted');
    } catch (error) {
      toast.error('Failed to delete table');
    }
  };

  const handleClearTemp = () => {
    if (!window.confirm('Clear temporary results?')) return;
    setTempResults(null);
    toast.success('Temporary results cleared');
  };

  return (
    <div className="p-6 space-y-8">
      {/* Temporary Results Section */}
      <div>
        <h3 className="text-lg font-semibold mb-4">Temporary Results (Current Session)</h3>
        
        {tempResults ? (
          <div className="space-y-4">
            {/* Store Level */}
            <div className="border rounded-lg p-4 hover:bg-gray-50">
              <div className="flex justify-between items-start mb-3">
                <div>
                  <h4 className="font-semibold text-gray-900">Store Level</h4>
                  <p className="text-sm text-gray-600 mt-1">
                    {tempResults.store_level.record_count} records
                  </p>
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => handleExport('csv')}
                    disabled={loading}
                    className="btn-secondary btn-sm"
                  >
                    <Download size={14} /> CSV
                  </button>
                  <button
                    onClick={() => setExpandedTable(expandedTable === 'store' ? null : 'store')}
                    className="btn-ghost btn-sm"
                  >
                    <Eye size={14} /> Preview
                  </button>
                </div>
              </div>

              {expandedTable === 'store' && (
                <div className="mt-3 max-h-64 overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="bg-gray-50">
                        {tempResults.store_level.columns.map((col) => (
                          <th key={col} className="px-2 py-2 text-left font-semibold">
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      <tr className="border-t">
                        <td colSpan={tempResults.store_level.columns.length} className="px-2 py-4 text-center text-gray-400">
                          Sample data (loading...)
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            {/* Company Level */}
            <div className="border rounded-lg p-4 hover:bg-gray-50">
              <div className="flex justify-between items-start mb-3">
                <div>
                  <h4 className="font-semibold text-gray-900">Company Level</h4>
                  <p className="text-sm text-gray-600 mt-1">
                    {tempResults.company_level.record_count} records
                  </p>
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => handleExport('csv')}
                    disabled={loading}
                    className="btn-secondary btn-sm"
                  >
                    <Download size={14} /> CSV
                  </button>
                  <button
                    onClick={() => setExpandedTable(expandedTable === 'company' ? null : 'company')}
                    className="btn-ghost btn-sm"
                  >
                    <Eye size={14} /> Preview
                  </button>
                </div>
              </div>

              {expandedTable === 'company' && (
                <div className="mt-3 max-h-64 overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="bg-gray-50">
                        {tempResults.company_level.columns.map((col) => (
                          <th key={col} className="px-2 py-2 text-left font-semibold">
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      <tr className="border-t">
                        <td colSpan={tempResults.company_level.columns.length} className="px-2 py-4 text-center text-gray-400">
                          Sample data (loading...)
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            <div className="flex justify-end gap-2 pt-2 border-t">
              <button onClick={handleClearTemp} className="btn-secondary btn-sm">
                <Trash2 size={14} /> Clear Temporary
              </button>
              <button onClick={() => handleExport('zip')} disabled={loading} className="btn-primary btn-sm">
                <Download size={14} /> Export as ZIP
              </button>
            </div>
          </div>
        ) : (
          <div className="border rounded-lg p-8 text-center text-gray-400">
            <FileJson size={32} className="mx-auto mb-2 opacity-50" />
            <p>No temporary results available</p>
            <p className="text-xs mt-1">Run an analysis to generate results</p>
          </div>
        )}
      </div>

      {/* Saved Database Results Section */}
      <div className="border-t pt-8">
        <h3 className="text-lg font-semibold mb-4">Saved Database Results</h3>

        {savedResults.length === 0 ? (
          <div className="border rounded-lg p-8 text-center text-gray-400">
            <Lock size={32} className="mx-auto mb-2 opacity-50" />
            <p>No saved results in database</p>
            <p className="text-xs mt-1">Enable "Save Results to Database" during execution</p>
          </div>
        ) : (
          <div className="space-y-3">
            {savedResults.map((result) => (
              <div key={result.table_name} className="border rounded-lg p-4 hover:bg-gray-50">
                <div className="flex justify-between items-start">
                  <div className="flex-1">
                    <div className="flex items-center gap-2">
                      <CheckCircle size={16} className="text-green-600" />
                      <h4 className="font-semibold text-gray-900">{result.table_name}</h4>
                    </div>
                    <div className="grid grid-cols-3 gap-4 mt-2 text-sm text-gray-600">
                      <div>
                        <span className="text-xs font-semibold">Records:</span>
                        <div className="text-lg font-bold text-gray-900">{result.record_count.toLocaleString()}</div>
                      </div>
                      <div>
                        <span className="text-xs font-semibold">Size:</span>
                        <div className="text-lg font-bold text-gray-900">{result.size_kb} KB</div>
                      </div>
                      <div>
                        <span className="text-xs font-semibold">Created:</span>
                        <div className="text-sm text-gray-600">{result.created_date}</div>
                      </div>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <button
                      onClick={() => handleDownloadTable(result.table_name)}
                      className="btn-secondary btn-sm"
                    >
                      <Download size={14} />
                    </button>
                    <button
                      onClick={() => handleDeleteTable(result.table_name)}
                      className="btn-ghost btn-sm text-red-600 hover:bg-red-100"
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Export Options Info */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mt-8">
        <h4 className="font-semibold text-blue-900 mb-2">Export Information</h4>
        <ul className="text-sm text-blue-800 space-y-1">
          <li>✓ CSV exports are optimized for Excel with proper formatting</li>
          <li>✓ Large results are automatically split into multiple files (~800 KB each)</li>
          <li>✓ ZIP archives include both store and company level data</li>
          <li>✓ Database tables are preserved for audit trail and long-term storage</li>
        </ul>
      </div>
    </div>
  );
}
