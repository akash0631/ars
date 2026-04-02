import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import { useParams, Link, useSearchParams } from 'react-router-dom'
import { ArrowLeft, Download, Columns, RefreshCw } from 'lucide-react'
import { tablesAPI, dataAPI, checklistAPI } from '@/services/api'
import { AgGridReact } from 'ag-grid-react'
import 'ag-grid-community/styles/ag-grid.css'
import 'ag-grid-community/styles/ag-theme-alpine.css'
import toast from 'react-hot-toast'
import useAuthStore from '@/store/authStore'

export default function TableDataPage() {
  const { tableName } = useParams()
  const [searchParams] = useSearchParams()
  const fromChecklist = searchParams.get('from') === 'checklist'
  const [schema, setSchema] = useState(null)
  const [rowData, setRowData] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [pageSize] = useState(100)
  const [loading, setLoading] = useState(true)
  const [serverFilters, setServerFilters] = useState(null)
  const { hasPermission } = useAuthStore()
  const filterTimer = useRef(null)

  const loadSchema = async () => {
    try {
      const { data } = await tablesAPI.schema(tableName)
      setSchema(data.data)
    } catch {}
  }

  const loadData = async (p = page, filters = serverFilters) => {
    setLoading(true)
    try {
      const params = { page: p, page_size: pageSize }
      if (filters && Object.keys(filters).length > 0) {
        params.filters = JSON.stringify(filters)
      }
      const { data } = await tablesAPI.data(tableName, params)
      setRowData(data.data?.data || [])
      setTotal(data.data?.total || 0)
    } finally { setLoading(false) }
  }

  useEffect(() => {
    loadSchema(); loadData(1, null)
    if (fromChecklist) checklistAPI.stamp(tableName).catch(() => {})
  }, [tableName])
  useEffect(() => { loadData(page) }, [page])

  // Convert ag-grid filter model to server filter format
  const onFilterChanged = useCallback((params) => {
    const model = params.api.getFilterModel()
    if (!model || Object.keys(model).length === 0) {
      setServerFilters(null)
      setPage(1)
      clearTimeout(filterTimer.current)
      filterTimer.current = setTimeout(() => loadData(1, null), 300)
      return
    }
    const filters = {}
    Object.entries(model).forEach(([col, filterDef]) => {
      if (filterDef.filterType === 'text') {
        const type = filterDef.type || 'contains'
        filters[col] = { type, filter: filterDef.filter }
      }
    })
    setServerFilters(filters)
    setPage(1)
    clearTimeout(filterTimer.current)
    filterTimer.current = setTimeout(() => loadData(1, filters), 400)
  }, [tableName, pageSize])

  const columnDefs = useMemo(() => {
    if (!schema?.columns) return []
    return schema.columns.map(col => ({
      field: col.column_name,
      headerName: col.display_name || col.column_name,
      sortable: true,
      filter: true,
      resizable: true,
      editable: hasPermission('DATA_EDIT') && !col.is_primary_key,
      cellStyle: col.is_primary_key ? { fontWeight: 600, background: '#f8fafc' } : null,
      minWidth: 100,
    }))
  }, [schema, hasPermission])

  const defaultColDef = useMemo(() => ({
    flex: 1,
    minWidth: 100,
    filter: 'agTextColumnFilter',
    floatingFilter: true,
  }), [])

  const onCellValueChanged = useCallback(async (params) => {
    const pkCols = schema?.columns?.filter(c => c.is_primary_key).map(c => c.column_name) || []
    if (pkCols.length === 0) return toast.error('No PK defined — cannot save edit')
    const pkValues = {}
    pkCols.forEach(pk => { pkValues[pk] = params.data[pk] })
    try {
      await dataAPI.update({
        table_name: tableName,
        primary_key_columns: pkCols,
        primary_key_values: pkValues,
        updates: { [params.colDef.field]: params.newValue },
      })
      toast.success('Cell updated')
      if (fromChecklist) checklistAPI.stamp(tableName).catch(() => {})
    } catch { params.api.undoCellEditing() }
  }, [schema, tableName, fromChecklist])

  const exportCSV = () => {
    const headers = schema?.columns?.map(c => c.column_name) || []
    const csv = [headers.join(','), ...rowData.map(r => headers.map(h => `"${r[h] ?? ''}"`).join(','))].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = `${tableName}.csv`; a.click()
  }

  const totalPages = Math.ceil(total / pageSize)

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Link to={fromChecklist ? "/data-validation/checklist" : "/tables"} className="p-2 hover:bg-gray-100 rounded-lg"><ArrowLeft size={18} /></Link>
          <div>
            <h1 className="text-xl font-bold text-gray-900">{schema?.display_name || tableName}</h1>
            <div className="flex items-center gap-3 text-sm text-gray-500">
              <span>{total.toLocaleString()} rows</span>
              <span>{schema?.columns?.length || 0} columns</span>
              {schema?.module && <span className="badge-gray">{schema.module}</span>}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={() => loadData(page)} className="btn-ghost btn-sm"><RefreshCw size={14} /> Refresh</button>
          <button onClick={exportCSV} className="btn-secondary btn-sm"><Download size={14} /> Export CSV</button>
        </div>
      </div>

      <div className="ag-theme-alpine" style={{ width: '100%', height: 'calc(100vh - 250px)' }}>
        <AgGridReact
          rowData={rowData}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          onCellValueChanged={onCellValueChanged}
          onFilterChanged={onFilterChanged}
          undoRedoCellEditing
          enableCellChangeFlash
          pagination={false}
          suppressRowClickSelection
          rowSelection="multiple"
          loading={loading}
        />
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between">
        <div className="text-sm text-gray-500">
          Showing {((page - 1) * pageSize) + 1}–{Math.min(page * pageSize, total)} of {total.toLocaleString()}
        </div>
        <div className="flex items-center gap-2">
          <button disabled={page <= 1} onClick={() => setPage(p => p - 1)} className="btn-secondary btn-sm">Previous</button>
          <span className="text-sm text-gray-600">Page {page} of {totalPages}</span>
          <button disabled={page >= totalPages} onClick={() => setPage(p => p + 1)} className="btn-secondary btn-sm">Next</button>
        </div>
      </div>
    </div>
  )
}
