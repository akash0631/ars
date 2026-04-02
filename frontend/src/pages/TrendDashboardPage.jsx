/**
 * TrendDashboardPage — Visual trend charts and summary tables
 */
import { useState, useEffect, useMemo } from 'react'
import { trendsAPI } from '@/services/api'
import toast from 'react-hot-toast'
import {
  TrendingUp, Database, Calendar, BarChart3, RefreshCw,
  Loader2, Table2, LineChart as LineChartIcon, ArrowUpRight, ArrowDownRight, Minus
} from 'lucide-react'
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, Area, AreaChart
} from 'recharts'

const HIDDEN_SYS = new Set(['VERSION','UPLOAD_DATETIME','SYSTEM_IP','SYSTEM_NAME','SYSTEM_LOGIN_ID','REPORT_DATE'])
const COLORS = ['#4f46e5','#06b6d4','#f59e0b','#10b981','#ef4444','#8b5cf6','#ec4899','#14b8a6','#f97316','#6366f1']

export default function TrendDashboardPage() {
  const [tables, setTables] = useState([])
  const [sel, setSel] = useState('')
  const [schema, setSchema] = useState(null)
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)
  const [chartType, setChartType] = useState('line') // line | bar | area
  const [groupCol, setGroupCol] = useState('')
  const [metricCols, setMetricCols] = useState([])
  const [summaryData, setSummaryData] = useState([])

  useEffect(() => {
    trendsAPI.listTables().then(r => { const d = r.data?.data; setTables(d?.tables || (Array.isArray(d) ? d : [])) }).catch(() => {})
  }, [])

  useEffect(() => {
    if (!sel) { setSchema(null); setData([]); setSummaryData([]); return }
    trendsAPI.getSchema(sel).then(r => {
      const s = r.data?.data || r.data
      setSchema(s)
      const cols = (s?.columns || s || []).map(c => typeof c === 'string' ? c : c.column_name || c.name)
      const visible = cols.filter(c => !HIDDEN_SYS.has(c))
      // Auto-detect: first text column as group, first 3 numeric-looking as metrics
      if (visible.length > 0) setGroupCol(visible[0])
      setMetricCols([])
      setData([]); setSummaryData([])
    }).catch(() => toast.error('Failed to load schema'))
  }, [sel])

  const colNames = useMemo(() => {
    const cols = schema?.columns || schema || []
    return cols.map(c => typeof c === 'string' ? c : c.column_name || c.name)
  }, [schema])
  const visCols = useMemo(() => colNames.filter(c => !HIDDEN_SYS.has(c) && c !== groupCol), [colNames, groupCol])

  const fetchData = async () => {
    if (!sel) return
    setLoading(true)
    try {
      const r = await trendsAPI.review({ table_name: sel, limit: 5000, filters: {} })
      const o = r.data?.data || r.data || {}
      const rows = o?.data || o?.rows || (Array.isArray(o) ? o : [])
      setData(rows)

      // Build summary by groupCol
      if (groupCol && metricCols.length > 0) buildSummary(rows)
      toast.success(`${rows.length.toLocaleString()} rows loaded`)
    } catch { toast.error('Fetch failed') }
    finally { setLoading(false) }
  }

  const buildSummary = (rows) => {
    if (!groupCol || !metricCols.length) { setSummaryData([]); return }
    const grouped = {}
    rows.forEach(row => {
      const key = String(row[groupCol] ?? 'Unknown')
      if (!grouped[key]) grouped[key] = { [groupCol]: key }
      metricCols.forEach(mc => {
        const val = Number(row[mc])
        if (!isNaN(val)) {
          grouped[key][mc] = (grouped[key][mc] || 0) + val
        }
      })
    })
    const summary = Object.values(grouped).map(row => {
      const cleaned = { ...row }
      metricCols.forEach(mc => { if (typeof cleaned[mc] === 'number') cleaned[mc] = Math.round(cleaned[mc] * 100) / 100 })
      return cleaned
    })
    // Sort by first metric desc
    if (metricCols[0]) summary.sort((a, b) => (b[metricCols[0]] || 0) - (a[metricCols[0]] || 0))
    setSummaryData(summary)
  }

  useEffect(() => { if (data.length) buildSummary(data) }, [groupCol, metricCols])

  const toggleMetric = (col) => {
    setMetricCols(prev => prev.includes(col) ? prev.filter(c => c !== col) : prev.length < 5 ? [...prev, col] : prev)
  }

  // Stats cards
  const stats = useMemo(() => {
    if (!metricCols.length || !summaryData.length) return []
    return metricCols.map(mc => {
      const vals = summaryData.map(r => r[mc] || 0)
      const total = vals.reduce((a, b) => a + b, 0)
      const avg = total / vals.length
      const max = Math.max(...vals)
      const min = Math.min(...vals)
      return { col: mc, total: Math.round(total * 100) / 100, avg: Math.round(avg * 100) / 100, max: Math.round(max * 100) / 100, min: Math.round(min * 100) / 100 }
    })
  }, [summaryData, metricCols])

  const inp = { height:22, fontSize:9, padding:'0 5px', borderRadius:3, border:'1px solid #e2e8f0', outline:'none', background:'#fff' }

  return (
    <div style={{ display:'flex', flexDirection:'column', gap:6 }}>
      {/* Header + Controls */}
      <div style={{ background:'#fff', borderRadius:6, border:'1px solid #e2e8f0', padding:'6px 10px',
        display:'flex', gap:8, alignItems:'center', flexWrap:'wrap' }}>
        <div style={{ display:'flex', alignItems:'center', gap:5, marginRight:6 }}>
          <div style={{ width:24, height:24, borderRadius:5, background:'linear-gradient(135deg,#4f46e5,#7c3aed)',
            display:'flex', alignItems:'center', justifyContent:'center' }}>
            <BarChart3 size={12} style={{ color:'#fff' }}/>
          </div>
          <span style={{ fontSize:12, fontWeight:700, color:'#0f172a' }}>Trend Dashboard</span>
        </div>

        <select value={sel} onChange={e => setSel(e.target.value)} style={{ ...inp, flex:'1 1 130px', minWidth:100, cursor:'pointer' }}>
          <option value="">Select table...</option>
          {tables.map(t => { const n = t.table_name || t, rc = t.row_count
            return <option key={n} value={n}>{n}{rc != null ? ` (${Number(rc).toLocaleString()})` : ''}</option> })}
        </select>

        {colNames.length > 0 && (
          <>
            <div style={{ display:'flex', alignItems:'center', gap:3 }}>
              <span style={{ fontSize:8, fontWeight:700, color:'#64748b' }}>GROUP BY:</span>
              <select value={groupCol} onChange={e => setGroupCol(e.target.value)} style={{ ...inp, width:100, cursor:'pointer' }}>
                {colNames.filter(c => !HIDDEN_SYS.has(c)).map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>

            <div style={{ display:'flex', alignItems:'center', gap:3 }}>
              <span style={{ fontSize:8, fontWeight:700, color:'#64748b' }}>CHART:</span>
              {['line','bar','area'].map(t => (
                <button key={t} onClick={() => setChartType(t)}
                  style={{ height:20, padding:'0 6px', fontSize:8, fontWeight:600, borderRadius:3, cursor:'pointer',
                    background: chartType === t ? '#4f46e5' : '#fff', color: chartType === t ? '#fff' : '#64748b',
                    border:`1px solid ${chartType === t ? '#4f46e5' : '#e2e8f0'}` }}>
                  {t.charAt(0).toUpperCase() + t.slice(1)}
                </button>
              ))}
            </div>
          </>
        )}

        <button onClick={fetchData} disabled={loading || !sel}
          style={{ height:22, padding:'0 10px', borderRadius:3, fontSize:9, fontWeight:700, color:'#fff',
            background: loading || !sel ? '#94a3b8' : '#4f46e5', border:'none',
            cursor: loading || !sel ? 'not-allowed' : 'pointer', display:'inline-flex', alignItems:'center', gap:3 }}>
          {loading ? <Loader2 size={9} className="animate-spin"/> : <RefreshCw size={9}/>} Load
        </button>
      </div>

      {/* Metric selector */}
      {visCols.length > 0 && (
        <div style={{ background:'#fff', borderRadius:6, border:'1px solid #e2e8f0', padding:'4px 10px' }}>
          <span style={{ fontSize:8, fontWeight:700, color:'#64748b', marginRight:6 }}>METRICS (select up to 5):</span>
          <div style={{ display:'inline-flex', gap:3, flexWrap:'wrap' }}>
            {visCols.map(col => {
              const active = metricCols.includes(col)
              const colorIdx = metricCols.indexOf(col)
              return (
                <button key={col} onClick={() => toggleMetric(col)}
                  style={{ height:18, padding:'0 6px', fontSize:8, fontWeight: active ? 700 : 400, borderRadius:3, cursor:'pointer',
                    background: active ? COLORS[colorIdx] + '18' : '#fff',
                    color: active ? COLORS[colorIdx] : '#94a3b8',
                    border:`1px solid ${active ? COLORS[colorIdx] : '#e2e8f0'}` }}>
                  {active && <span style={{ display:'inline-block', width:6, height:6, borderRadius:3, background:COLORS[colorIdx], marginRight:3 }}/>}
                  {col}
                </button>
              )
            })}
          </div>
        </div>
      )}

      {/* Stats cards */}
      {stats.length > 0 && (
        <div style={{ display:'flex', gap:6, flexWrap:'wrap' }}>
          {stats.map((st, i) => (
            <div key={st.col} style={{ flex:'1 1 140px', background:'#fff', borderRadius:6, border:'1px solid #e2e8f0', padding:'8px 10px' }}>
              <div style={{ fontSize:8, fontWeight:600, color:COLORS[i], textTransform:'uppercase', marginBottom:4, display:'flex', alignItems:'center', gap:3 }}>
                <span style={{ width:6, height:6, borderRadius:3, background:COLORS[i] }}/>
                {st.col}
              </div>
              <div style={{ fontSize:16, fontWeight:800, color:'#0f172a', lineHeight:1 }}>{st.total.toLocaleString()}</div>
              <div style={{ display:'flex', gap:8, marginTop:4 }}>
                <span style={{ fontSize:8, color:'#64748b' }}>Avg: <b>{st.avg.toLocaleString()}</b></span>
                <span style={{ fontSize:8, color:'#059669' }}>Max: <b>{st.max.toLocaleString()}</b></span>
                <span style={{ fontSize:8, color:'#dc2626' }}>Min: <b>{st.min.toLocaleString()}</b></span>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Chart */}
      {summaryData.length > 0 && metricCols.length > 0 && (
        <div style={{ background:'#fff', borderRadius:6, border:'1px solid #e2e8f0', padding:'10px' }}>
          <div style={{ fontSize:9, fontWeight:700, color:'#475569', marginBottom:6, display:'flex', alignItems:'center', gap:4 }}>
            <LineChartIcon size={10}/> {metricCols.join(', ')} by {groupCol}
            <span style={{ fontSize:8, fontWeight:400, color:'#94a3b8' }}>({summaryData.length} groups)</span>
          </div>
          <ResponsiveContainer width="100%" height={280}>
            {chartType === 'bar' ? (
              <BarChart data={summaryData.slice(0, 50)} margin={{ top:5, right:10, left:10, bottom:40 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                <XAxis dataKey={groupCol} tick={{ fontSize:8 }} angle={-35} textAnchor="end" height={50} interval={0} />
                <YAxis tick={{ fontSize:8 }} tickFormatter={v => v >= 1000 ? (v/1000).toFixed(0)+'K' : v} />
                <Tooltip contentStyle={{ fontSize:10 }} formatter={v => typeof v === 'number' ? v.toFixed(2) : v} />
                <Legend wrapperStyle={{ fontSize:9 }} />
                {metricCols.map((mc, i) => <Bar key={mc} dataKey={mc} fill={COLORS[i]} radius={[2,2,0,0]} />)}
              </BarChart>
            ) : chartType === 'area' ? (
              <AreaChart data={summaryData.slice(0, 50)} margin={{ top:5, right:10, left:10, bottom:40 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                <XAxis dataKey={groupCol} tick={{ fontSize:8 }} angle={-35} textAnchor="end" height={50} interval={0} />
                <YAxis tick={{ fontSize:8 }} tickFormatter={v => v >= 1000 ? (v/1000).toFixed(0)+'K' : v} />
                <Tooltip contentStyle={{ fontSize:10 }} formatter={v => typeof v === 'number' ? v.toFixed(2) : v} />
                <Legend wrapperStyle={{ fontSize:9 }} />
                {metricCols.map((mc, i) => <Area key={mc} type="monotone" dataKey={mc} fill={COLORS[i]+'30'} stroke={COLORS[i]} strokeWidth={2} />)}
              </AreaChart>
            ) : (
              <LineChart data={summaryData.slice(0, 50)} margin={{ top:5, right:10, left:10, bottom:40 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                <XAxis dataKey={groupCol} tick={{ fontSize:8 }} angle={-35} textAnchor="end" height={50} interval={0} />
                <YAxis tick={{ fontSize:8 }} tickFormatter={v => v >= 1000 ? (v/1000).toFixed(0)+'K' : v} />
                <Tooltip contentStyle={{ fontSize:10 }} formatter={v => typeof v === 'number' ? v.toFixed(2) : v} />
                <Legend wrapperStyle={{ fontSize:9 }} />
                {metricCols.map((mc, i) => <Line key={mc} type="monotone" dataKey={mc} stroke={COLORS[i]} strokeWidth={2} dot={{ r:2 }} />)}
              </LineChart>
            )}
          </ResponsiveContainer>
        </div>
      )}

      {/* Summary table */}
      {summaryData.length > 0 && (
        <div style={{ background:'#fff', borderRadius:6, border:'1px solid #e2e8f0', overflow:'hidden' }}>
          <div style={{ padding:'4px 10px', background:'#f8fafc', borderBottom:'1px solid #e2e8f0', display:'flex', alignItems:'center', gap:4 }}>
            <Table2 size={9} style={{ color:'#64748b' }}/>
            <span style={{ fontSize:9, fontWeight:700, color:'#475569' }}>Summary Table ({summaryData.length} groups)</span>
          </div>
          <div style={{ overflowX:'auto', maxHeight:300 }}>
            <table style={{ width:'100%', borderCollapse:'collapse', fontSize:9 }}>
              <thead>
                <tr style={{ background:'#f8fafc' }}>
                  <th style={{ padding:'4px 8px', textAlign:'left', borderBottom:'1px solid #e2e8f0', color:'#475569', fontWeight:700, position:'sticky', top:0, background:'#f8fafc' }}>{groupCol}</th>
                  {metricCols.map((mc, i) => (
                    <th key={mc} style={{ padding:'4px 8px', textAlign:'right', borderBottom:'1px solid #e2e8f0', fontWeight:700, position:'sticky', top:0, background:'#f8fafc', color:COLORS[i] }}>{mc}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {summaryData.map((row, idx) => (
                  <tr key={idx} style={{ background: idx % 2 ? '#fafbfc' : '#fff' }}>
                    <td style={{ padding:'3px 8px', borderBottom:'1px solid #f1f5f9', fontWeight:500, color:'#0f172a' }}>{row[groupCol]}</td>
                    {metricCols.map(mc => (
                      <td key={mc} style={{ padding:'3px 8px', textAlign:'right', borderBottom:'1px solid #f1f5f9', color:'#334155', fontFamily:'monospace' }}>
                        {typeof row[mc] === 'number' ? row[mc].toLocaleString(undefined, { minimumFractionDigits:2, maximumFractionDigits:2 }) : row[mc] ?? '—'}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Empty state */}
      {!data.length && sel && !loading && (
        <div style={{ background:'#fff', borderRadius:6, border:'1px solid #e2e8f0', padding:'30px', textAlign:'center' }}>
          <BarChart3 size={24} style={{ color:'#c7d2fe', margin:'0 auto 8px' }}/>
          <div style={{ fontSize:11, fontWeight:600, color:'#475569' }}>Select metrics and click Load</div>
          <div style={{ fontSize:9, color:'#94a3b8', marginTop:2 }}>Pick a group-by column and metric columns, then load data to see charts</div>
        </div>
      )}
    </div>
  )
}
