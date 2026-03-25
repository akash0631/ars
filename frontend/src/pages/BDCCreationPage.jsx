import { useState, useRef, useCallback } from 'react'
import { Upload, FileSpreadsheet, X, Loader2, AlertCircle, CheckCircle2, Table2, Download, Trash2, ArrowRight } from 'lucide-react'
import { bdcAPI } from '@/services/api'
import toast from 'react-hot-toast'

export default function BDCCreationPage() {
  const [file, setFile] = useState(null)
  const [fileContent, setFileContent] = useState(null) // store raw file for download
  const [sheets, setSheets] = useState([])
  const [selectedSheet, setSelectedSheet] = useState('')
  const [allocationNo, setAllocationNo] = useState('')
  const [uploading, setUploading] = useState(false)
  const [downloading, setDownloading] = useState(false)
  const [result, setResult] = useState(null)
  const [dragOver, setDragOver] = useState(false)
  const fileInputRef = useRef()

  const isExcel = (f) => /\.(xlsx|xls)$/i.test(f?.name || '')

  const handleFileSelect = useCallback(async (selectedFile) => {
    if (!selectedFile) return

    const ext = selectedFile.name.split('.').pop().toLowerCase()
    if (!['csv', 'xlsx', 'xls'].includes(ext)) {
      toast.error('Please upload a CSV or Excel (.xlsx/.xls) file')
      return
    }

    setFile(selectedFile)
    setFileContent(selectedFile)
    setResult(null)
    setSheets([])
    setSelectedSheet('')

    if (isExcel(selectedFile)) {
      try {
        const formData = new FormData()
        formData.append('file', selectedFile)
        const { data } = await bdcAPI.getSheets(formData)
        if (data.sheets?.length > 1) {
          setSheets(data.sheets)
          setSelectedSheet(data.sheets[0])
        } else if (data.sheets?.length === 1) {
          setSelectedSheet(data.sheets[0])
        }
      } catch {
        // ignore
      }
    }
  }, [])

  const handleDrop = useCallback((e) => {
    e.preventDefault()
    setDragOver(false)
    const droppedFile = e.dataTransfer.files?.[0]
    if (droppedFile) handleFileSelect(droppedFile)
  }, [handleFileSelect])

  const handleUpload = async () => {
    if (!file) return

    setUploading(true)
    setResult(null)
    try {
      const formData = new FormData()
      formData.append('file', file)
      if (selectedSheet) formData.append('sheet_name', selectedSheet)
      formData.append('allocation_no', allocationNo.trim())

      const { data } = await bdcAPI.upload(formData)
      setResult(data)
      toast.success(`BDC processed: ${data.total_rows} rows generated`)
    } catch (err) {
      const msg = err.response?.data?.detail || err.message
      toast.error(msg)
    } finally {
      setUploading(false)
    }
  }

  const handleDownload = async () => {
    if (!fileContent) return

    setDownloading(true)
    try {
      const formData = new FormData()
      formData.append('file', fileContent)
      if (selectedSheet) formData.append('sheet_name', selectedSheet)
      formData.append('allocation_no', allocationNo.trim())

      const response = await bdcAPI.download(formData)
      const url = window.URL.createObjectURL(new Blob([response.data]))
      const link = document.createElement('a')
      link.href = url
      link.download = 'BDC_Output.csv'
      document.body.appendChild(link)
      link.click()
      link.remove()
      window.URL.revokeObjectURL(url)
      toast.success('BDC file downloaded')
    } catch (err) {
      const msg = err.response?.data?.detail || err.message
      toast.error(msg)
    } finally {
      setDownloading(false)
    }
  }

  const handleClear = () => {
    setFile(null)
    setFileContent(null)
    setSheets([])
    setSelectedSheet('')
    setAllocationNo('')
    setResult(null)
    if (fileInputRef.current) fileInputRef.current.value = ''
  }

  const formatFileSize = (bytes) => {
    if (bytes < 1024) return `${bytes} B`
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">BDC Creation</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Upload allocation quantity data to generate BDC output
        </p>
      </div>

      {/* Upload Section */}
      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2">
            <Upload size={20} />
            Upload Allocation Data
          </h2>
          <button
            onClick={() => {
              const headers = ['ALLOC-DATE', 'RDC', 'VAR-ART', 'ST-CD', 'ALLOC-QTY', 'PICKING_DATE']
              const csv = headers.join(',') + '\n'
              const blob = new Blob([csv], { type: 'text/csv' })
              const url = window.URL.createObjectURL(blob)
              const link = document.createElement('a')
              link.href = url
              link.download = 'BDC_Upload_Template.csv'
              document.body.appendChild(link)
              link.click()
              link.remove()
              window.URL.revokeObjectURL(url)
            }}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium text-primary-600 dark:text-primary-400 border border-primary-300 dark:border-primary-700 hover:bg-primary-50 dark:hover:bg-primary-900/20 transition-colors"
          >
            <Download size={14} />
            Download Template
          </button>
        </div>

        {/* Drop Zone */}
        <div
          onDragOver={(e) => { e.preventDefault(); setDragOver(true) }}
          onDragLeave={() => setDragOver(false)}
          onDrop={handleDrop}
          onClick={() => fileInputRef.current?.click()}
          className={`
            relative border-2 border-dashed rounded-xl p-10 text-center cursor-pointer transition-all duration-200
            ${dragOver
              ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/20'
              : 'border-gray-300 dark:border-gray-600 hover:border-primary-400 hover:bg-gray-50 dark:hover:bg-gray-700/50'
            }
          `}
        >
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv,.xlsx,.xls"
            onChange={(e) => handleFileSelect(e.target.files?.[0])}
            className="hidden"
          />
          <FileSpreadsheet size={48} className="mx-auto mb-3 text-gray-400 dark:text-gray-500" />
          <p className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Drag & drop your file here, or <span className="text-primary-600 dark:text-primary-400">browse</span>
          </p>
          <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
            Supports CSV, XLS, XLSX — Required columns: ALLOC-DATE, RDC, VAR-ART, ST-CD, ALLOC-QTY, PICKING_DATE
          </p>
        </div>

        {/* Selected File Info */}
        {file && (
          <div className="mt-4 bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <FileSpreadsheet size={20} className="text-primary-500" />
                <div>
                  <p className="text-sm font-medium text-gray-900 dark:text-white">{file.name}</p>
                  <p className="text-xs text-gray-500 dark:text-gray-400">{formatFileSize(file.size)}</p>
                </div>
              </div>
              <button
                onClick={(e) => { e.stopPropagation(); handleClear() }}
                className="p-1.5 rounded-lg text-gray-400 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
              >
                <X size={16} />
              </button>
            </div>

            {/* Sheet selector for multi-sheet Excel */}
            {sheets.length > 1 && (
              <div className="mt-3">
                <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                  Select Sheet
                </label>
                <select
                  value={selectedSheet}
                  onChange={(e) => setSelectedSheet(e.target.value)}
                  className="w-full text-sm rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-2 focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
                >
                  {sheets.map((s) => (
                    <option key={s} value={s}>{s}</option>
                  ))}
                </select>
              </div>
            )}

            {/* Allocation Number Input */}
            <div className="mt-3">
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                Allocation Number <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={allocationNo}
                onChange={(e) => setAllocationNo(e.target.value.toUpperCase())}
                placeholder="e.g. N345"
                className="w-full text-sm rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-2 focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
              />
            </div>

            {/* Process Button */}
            <button
              onClick={handleUpload}
              disabled={uploading || !allocationNo.trim()}
              className="mt-4 w-full flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {uploading ? (
                <>
                  <Loader2 size={16} className="animate-spin" />
                  Processing BDC...
                </>
              ) : (
                <>
                  <ArrowRight size={16} />
                  Process BDC
                </>
              )}
            </button>
          </div>
        )}
      </div>

      {/* Processing Stats */}
      {result?.stats && (
        <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-6">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Processing Summary</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4">
            <StatCard label="Input Rows" value={result.stats.input_rows} color="blue" />
            <StatCard label="After Master Join" value={result.stats.after_master_join} color="indigo" />
            <StatCard label="Hold Article Removed" value={result.stats.hold_article_removed} color="red" />
            <StatCard label="Division (KIDS) Removed" value={result.stats.division_delete_removed} color="orange" />
            <StatCard label="MAJ_CAT Removed" value={result.stats.majcat_delete_removed} color="amber" />
            <StatCard label="Duplicates Removed" value={result.stats.duplicates_removed} color="red" />
            <StatCard label="Final BDC Rows" value={result.stats.final_rows} color="green" />
          </div>
        </div>
      )}

      {/* Results / Preview Table */}
      {result && (
        <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-6">
          {/* Header with download */}
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2">
              <Table2 size={20} />
              BDC Output Preview
            </h2>
            <div className="flex items-center gap-3">
              <span className="flex items-center gap-1.5 text-sm text-green-600 dark:text-green-400">
                <CheckCircle2 size={14} />
                {result.total_rows} rows
              </span>
              <button
                onClick={handleDownload}
                disabled={downloading}
                className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white bg-green-600 hover:bg-green-700 disabled:opacity-50 transition-colors"
              >
                {downloading ? (
                  <Loader2 size={14} className="animate-spin" />
                ) : (
                  <Download size={14} />
                )}
                Download CSV
              </button>
            </div>
          </div>

          {result.total_rows > 100 && (
            <div className="flex items-center gap-2 text-xs text-amber-600 dark:text-amber-400 bg-amber-50 dark:bg-amber-900/20 rounded-lg px-3 py-2 mb-4">
              <AlertCircle size={14} />
              Showing first 100 rows of {result.total_rows} total rows. Download Excel for complete data.
            </div>
          )}

          {/* Scrollable Table */}
          <div className="overflow-auto max-h-[500px] rounded-lg border border-gray-200 dark:border-gray-700">
            <table className="min-w-full text-sm">
              <thead className="bg-gray-50 dark:bg-gray-700 sticky top-0 z-10">
                <tr>
                  {result.columns.map((col) => (
                    <th
                      key={col}
                      className="px-3 py-2 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-600 whitespace-nowrap"
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
                {result.preview.map((row, idx) => (
                  <tr key={idx} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                    {result.columns.map((col) => (
                      <td
                        key={col}
                        className="px-3 py-1.5 text-xs text-gray-700 dark:text-gray-300 whitespace-nowrap"
                        title={String(row[col] ?? '')}
                      >
                        {row[col] !== null && row[col] !== undefined && row[col] !== '' ? String(row[col]) : '-'}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

function StatCard({ label, value, color }) {
  const colorMap = {
    blue: 'bg-blue-50 dark:bg-blue-900/20 text-blue-700 dark:text-blue-300 border-blue-200 dark:border-blue-800',
    indigo: 'bg-indigo-50 dark:bg-indigo-900/20 text-indigo-700 dark:text-indigo-300 border-indigo-200 dark:border-indigo-800',
    red: 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 border-red-200 dark:border-red-800',
    orange: 'bg-orange-50 dark:bg-orange-900/20 text-orange-700 dark:text-orange-300 border-orange-200 dark:border-orange-800',
    amber: 'bg-amber-50 dark:bg-amber-900/20 text-amber-700 dark:text-amber-300 border-amber-200 dark:border-amber-800',
    green: 'bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-300 border-green-200 dark:border-green-800',
  }
  return (
    <div className={`rounded-lg border p-3 text-center ${colorMap[color] || colorMap.blue}`}>
      <p className="text-2xl font-bold">{value?.toLocaleString()}</p>
      <p className="text-[10px] font-medium mt-1 opacity-80">{label}</p>
    </div>
  )
}
