import { useState, useEffect, useCallback } from 'react'
import { storeStockAPI } from '@/services/api'
import toast from 'react-hot-toast'
import { RefreshCw, Save, Search, CheckCircle2, XCircle, AlertTriangle, Database, Sparkles } from 'lucide-react'
import clsx from 'clsx'

/* ── Status badge ─────────────────────────────────────────────────────────── */
const StatusBadge = ({ status }) =>
  status === 'Active' ? (
    <span style={{ display:'inline-flex', alignItems:'center', gap:4,
      padding:'2px 10px', borderRadius:999, fontSize:11, fontWeight:700,
      background:'rgba(16,185,129,0.18)', color:'#34d399', border:'1px solid rgba(52,211,153,0.4)' }}>
      <CheckCircle2 size={10} /> Active
    </span>
  ) : (
    <span style={{ display:'inline-flex', alignItems:'center', gap:4,
      padding:'2px 10px', borderRadius:999, fontSize:11, fontWeight:700,
      background:'rgba(239,68,68,0.18)', color:'#f87171', border:'1px solid rgba(248,113,113,0.4)' }}>
      <XCircle size={10} /> Inactive
    </span>
  )

const NewBadge = () => (
  <span style={{ display:'inline-flex', alignItems:'center', gap:3,
    padding:'1px 7px', borderRadius:999, fontSize:10, fontWeight:700,
    background:'rgba(245,158,11,0.18)', color:'#fbbf24', border:'1px solid rgba(251,191,36,0.4)' }}>
    <Sparkles size={9} /> New
  </span>
)

/* ── Main page ───────────────────────────────────────────────────────────── */
export default function StoreStockPage() {
  const [rows,        setRows]        = useState([])
  const [dirty,       setDirty]       = useState({})   // { sloc: { kpi?, status? } }
  const [loading,     setLoading]     = useState(false)
  const [syncing,     setSyncing]     = useState(false)
  const [saving,      setSaving]      = useState(false)
  const [search,      setSearch]      = useState('')
  const [filterTab,   setFilterTab]   = useState('all')

  /* ── load ────────────────────────────────────────────────────────────── */
  const loadData = useCallback(async () => {
    setLoading(true)
    try {
      const { data } = await storeStockAPI.getSlocSettings()
      setRows(data.data.items || [])
      setDirty({})
    } catch { /* interceptor shows toast */ }
    finally { setLoading(false) }
  }, [])

  useEffect(() => { loadData() }, [loadData])

  /* ── sync ────────────────────────────────────────────────────────────── */
  const handleSync = async () => {
    setSyncing(true)
    try {
      const { data } = await storeStockAPI.syncSlocs()
      toast.success(data.message)
      await loadData()
    } catch { } finally { setSyncing(false) }
  }

  /* ── edit helpers ────────────────────────────────────────────────────── */
  const setField = (sloc, field, val) =>
    setDirty(prev => ({ ...prev, [sloc]: { ...(prev[sloc] || {}), [field]: val } }))

  const getVal = (row, field) =>
    dirty[row.sloc]?.[field] !== undefined ? dirty[row.sloc][field] : row[field]

  const toggleStatus = (sloc) => {
    const row = rows.find(r => r.sloc === sloc)
    const cur = getVal(row, 'status')
    setField(sloc, 'status', cur === 'Active' ? 'Inactive' : 'Active')
  }

  /* ── save ────────────────────────────────────────────────────────────── */
  const handleSave = async () => {
    const keys = Object.keys(dirty)
    if (!keys.length) { toast('Nothing to save.'); return }
    setSaving(true)
    try {
      const items = keys.map(sloc => {
        const base = rows.find(r => r.sloc === sloc) || {}
        return {
          sloc,
          kpi:    dirty[sloc]?.kpi    !== undefined ? dirty[sloc].kpi    : base.kpi,
          status: dirty[sloc]?.status !== undefined ? dirty[sloc].status : base.status,
        }
      })
      const { data } = await storeStockAPI.bulkUpdate(items)
      toast.success(data.message)
      await loadData()
    } catch { } finally { setSaving(false) }
  }

  /* ── filter ──────────────────────────────────────────────────────────── */
  const visible = rows.filter(r => {
    const q = search.toLowerCase()
    const match = r.sloc.toLowerCase().includes(q) ||
      (getVal(r,'kpi') || '').toLowerCase().includes(q)
    if (!match) return false
    if (filterTab === 'active')   return getVal(r,'status') === 'Active'
    if (filterTab === 'inactive') return getVal(r,'status') === 'Inactive'
    if (filterTab === 'new')      return r.is_new
    return true
  })

  const dirtyCount = Object.keys(dirty).length
  const newCount   = rows.filter(r => r.is_new).length
  const activeCount   = rows.filter(r => getVal(r,'status') === 'Active').length
  const inactiveCount = rows.filter(r => getVal(r,'status') === 'Inactive').length

  /* ── render ──────────────────────────────────────────────────────────── */
  return (
    <div style={{ padding:24, display:'flex', flexDirection:'column', gap:20, color:'#f1f5f9' }}>

      {/* Header */}
      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', flexWrap:'wrap', gap:12 }}>
        <div>
          <h1 style={{ fontSize:20, fontWeight:700, color:'#f8fafc', display:'flex', alignItems:'center', gap:8, margin:0 }}>
            <Database size={20} color="#818cf8" />
            Store Stock – SLOC Settings
          </h1>
          <p style={{ fontSize:13, color:'#94a3b8', marginTop:4 }}>
            Manage <strong style={{color:'#e2e8f0'}}>KPI</strong> labels and&nbsp;
            <strong style={{color:'#e2e8f0'}}>Active / Inactive</strong> status saved in&nbsp;
            <code style={{ background:'#1e293b', color:'#fbbf24', padding:'1px 6px', borderRadius:4, fontSize:12 }}>
              ARS_SLOC_SETTINGS
            </code>
            &nbsp;for each distinct SLOC from&nbsp;
            <code style={{ background:'#1e293b', color:'#fbbf24', padding:'1px 6px', borderRadius:4, fontSize:12 }}>
              ET_STORE_STOCK
            </code>
          </p>
        </div>

        <div style={{ display:'flex', gap:8, flexWrap:'wrap' }}>
          {/* Sync button */}
          <button onClick={handleSync} disabled={syncing || loading}
            style={{ display:'flex', alignItems:'center', gap:6, padding:'7px 14px', borderRadius:8,
              fontSize:13, fontWeight:600, cursor:'pointer', border:'1px solid rgba(245,158,11,0.4)',
              background:'rgba(245,158,11,0.1)', color:'#fbbf24',
              opacity: (syncing||loading) ? 0.5 : 1 }}>
            <RefreshCw size={14} style={{ animation: syncing ? 'spin 1s linear infinite' : 'none' }} />
            Sync New SLOCs
            {newCount > 0 && (
              <span style={{ background:'#f59e0b', color:'#000', borderRadius:999,
                padding:'1px 7px', fontSize:10, fontWeight:800 }}>{newCount}</span>
            )}
          </button>

          {/* Save button */}
          <button onClick={handleSave} disabled={saving || dirtyCount === 0}
            style={{ display:'flex', alignItems:'center', gap:6, padding:'7px 14px', borderRadius:8,
              fontSize:13, fontWeight:600, cursor: dirtyCount > 0 ? 'pointer' : 'not-allowed',
              border:'none',
              background: dirtyCount > 0 ? '#4f46e5' : '#334155',
              color: dirtyCount > 0 ? '#fff' : '#64748b',
              opacity: saving ? 0.6 : 1,
              boxShadow: dirtyCount > 0 ? '0 0 16px rgba(79,70,229,0.4)' : 'none' }}>
            <Save size={14} />
            Save Changes
            {dirtyCount > 0 && (
              <span style={{ background:'#fff', color:'#4f46e5', borderRadius:999,
                padding:'1px 7px', fontSize:10, fontWeight:800 }}>{dirtyCount}</span>
            )}
          </button>
        </div>
      </div>

      {/* Stats */}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(4,1fr)', gap:12 }}>
        {[
          { label:'Total SLOCs',   value: rows.length,   color:'#f8fafc' },
          { label:'Active',        value: activeCount,   color:'#34d399' },
          { label:'Inactive',      value: inactiveCount, color:'#f87171' },
          { label:'Unsaved Edits', value: dirtyCount,    color:'#fbbf24' },
        ].map(s => (
          <div key={s.label} style={{ background:'rgba(30,41,59,0.8)', border:'1px solid #334155',
            borderRadius:12, padding:'12px 16px' }}>
            <div style={{ fontSize:26, fontWeight:800, color: s.color, lineHeight:1 }}>{s.value}</div>
            <div style={{ fontSize:11, color:'#94a3b8', marginTop:4 }}>{s.label}</div>
          </div>
        ))}
      </div>

      {/* Search + filter tabs */}
      <div style={{ display:'flex', gap:10, flexWrap:'wrap', alignItems:'center' }}>
        <div style={{ position:'relative', flex:1, minWidth:220 }}>
          <Search size={14} style={{ position:'absolute', left:10, top:'50%', transform:'translateY(-50%)', color:'#64748b' }} />
          <input type="text" placeholder="Search SLOC or KPI…" value={search}
            onChange={e => setSearch(e.target.value)}
            style={{ width:'100%', padding:'8px 12px 8px 32px', background:'#1e293b',
              border:'1px solid #334155', borderRadius:8, color:'#f1f5f9', fontSize:13,
              outline:'none', boxSizing:'border-box' }} />
        </div>

        <div style={{ display:'flex', background:'#1e293b', border:'1px solid #334155', borderRadius:8, padding:4, gap:2 }}>
          {[
            { key:'all',      label:'All' },
            { key:'active',   label:'Active' },
            { key:'inactive', label:'Inactive' },
            { key:'new',      label: newCount > 0 ? `New (${newCount})` : 'New' },
          ].map(f => (
            <button key={f.key} onClick={() => setFilterTab(f.key)}
              style={{ padding:'4px 12px', borderRadius:6, fontSize:12, fontWeight:600,
                border:'none', cursor:'pointer', transition:'all .15s',
                background: filterTab === f.key ? '#4f46e5' : 'transparent',
                color:       filterTab === f.key ? '#fff'   : '#94a3b8' }}>
              {f.label}
            </button>
          ))}
        </div>
      </div>

      {/* New-SLOC banner */}
      {newCount > 0 && (
        <div style={{ display:'flex', alignItems:'center', gap:10, padding:'12px 16px',
          background:'rgba(245,158,11,0.08)', border:'1px solid rgba(245,158,11,0.3)',
          borderRadius:10, fontSize:13, color:'#fcd34d' }}>
          <AlertTriangle size={15} style={{ flexShrink:0 }} />
          <span>
            <strong>{newCount} new SLOC{newCount>1?'s':''}</strong> detected in ET_STORE_STOCK but not yet saved.
            Click <strong>Sync New SLOCs</strong> to persist them, then set their KPI and Status.
          </span>
        </div>
      )}

      {/* Table */}
      <div style={{ background:'rgba(15,23,42,0.7)', border:'1px solid #1e293b', borderRadius:12, overflow:'hidden' }}>
        <table style={{ width:'100%', borderCollapse:'collapse', fontSize:13 }}>
          <thead>
            <tr style={{ borderBottom:'2px solid #1e293b', background:'rgba(30,41,59,0.9)' }}>
              {[
                { label:'SLOC',              width:180  },
                { label:'KPI',               width:'auto' },
                { label:'ACTIVE / INACTIVE', width:200, center:true },
                { label:'STATUS',            width:120, center:true },
              ].map(h => (
                <th key={h.label}
                  style={{ padding:'10px 16px', textAlign: h.center ? 'center' : 'left',
                    fontSize:11, fontWeight:700, color:'#94a3b8',
                    letterSpacing:'0.07em', textTransform:'uppercase',
                    width: h.width !== 'auto' ? h.width : undefined }}>
                  {h.label}
                </th>
              ))}
            </tr>
          </thead>

          <tbody>
            {loading ? (
              <tr><td colSpan={4} style={{ textAlign:'center', padding:60, color:'#64748b' }}>
                <RefreshCw size={20} style={{ display:'block', margin:'0 auto 8px', animation:'spin 1s linear infinite' }} />
                Loading SLOC data…
              </td></tr>
            ) : visible.length === 0 ? (
              <tr><td colSpan={4} style={{ textAlign:'center', padding:60, color:'#64748b' }}>
                No SLOC records found.
              </td></tr>
            ) : visible.map((row, idx) => {
              const isDirty  = !!dirty[row.sloc]
              const kpiVal   = getVal(row, 'kpi') ?? ''
              const statusVal = getVal(row, 'status') ?? 'Active'
              const isActive  = statusVal === 'Active'

              return (
                <tr key={row.sloc}
                  style={{
                    borderBottom:'1px solid #1e293b',
                    background: isDirty
                      ? 'rgba(79,70,229,0.08)'
                      : idx % 2 === 0 ? 'transparent' : 'rgba(30,41,59,0.3)',
                    transition:'background .15s',
                  }}>

                  {/* SLOC */}
                  <td style={{ padding:'10px 16px' }}>
                    <div style={{ display:'flex', alignItems:'center', gap:8 }}>
                      <code style={{ fontFamily:'monospace', fontWeight:700, fontSize:13,
                        color:'#e2e8f0', letterSpacing:'0.04em' }}>
                        {row.sloc}
                      </code>
                      {row.is_new && <NewBadge />}
                      {isDirty && (
                        <span title="Unsaved change"
                          style={{ width:6, height:6, borderRadius:'50%', background:'#818cf8', flexShrink:0 }} />
                      )}
                    </div>
                  </td>

                  {/* KPI input */}
                  <td style={{ padding:'8px 16px' }}>
                    <input
                      type="text"
                      value={kpiVal}
                      onChange={e => setField(row.sloc, 'kpi', e.target.value)}
                      placeholder="Enter KPI label…"
                      style={{
                        width:'100%', padding:'7px 12px', borderRadius:7, fontSize:13,
                        background: isDirty && dirty[row.sloc]?.kpi !== undefined
                          ? 'rgba(79,70,229,0.15)' : '#1e293b',
                        border: isDirty && dirty[row.sloc]?.kpi !== undefined
                          ? '1px solid rgba(129,140,248,0.6)' : '1px solid #334155',
                        color:'#f1f5f9',          /* ← WHITE text - always visible */
                        outline:'none',
                        boxSizing:'border-box',
                        caretColor:'#818cf8',
                        fontFamily:'inherit',
                      }}
                    />
                  </td>

                  {/* Toggle */}
                  <td style={{ padding:'8px 16px', textAlign:'center' }}>
                    <button onClick={() => toggleStatus(row.sloc)}
                      style={{
                        display:'inline-flex', alignItems:'center', gap:8,
                        padding:'6px 14px', borderRadius:8, fontSize:12, fontWeight:700,
                        cursor:'pointer', transition:'all .15s',
                        background: isActive ? 'rgba(16,185,129,0.12)' : 'rgba(239,68,68,0.12)',
                        border:     isActive ? '1px solid rgba(52,211,153,0.4)' : '1px solid rgba(248,113,113,0.4)',
                        color:      isActive ? '#34d399' : '#f87171',
                      }}>
                      {/* Pill */}
                      <span style={{
                        width:34, height:18, borderRadius:9, position:'relative', display:'inline-block', flexShrink:0,
                        background: isActive ? '#10b981' : '#475569', transition:'background .2s',
                      }}>
                        <span style={{
                          position:'absolute', top:3, width:12, height:12, borderRadius:'50%',
                          background:'#fff', boxShadow:'0 1px 3px rgba(0,0,0,0.4)',
                          transition:'left .2s', left: isActive ? 19 : 3,
                        }} />
                      </span>
                      {/* Label — always clearly readable */}
                      <span style={{ color: isActive ? '#34d399' : '#f87171', fontWeight:700, fontSize:12 }}>
                        {statusVal}
                      </span>
                    </button>
                  </td>

                  {/* Status badge */}
                  <td style={{ padding:'8px 16px', textAlign:'center' }}>
                    <StatusBadge status={statusVal} />
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>

        {/* Footer */}
        {!loading && visible.length > 0 && (
          <div style={{ padding:'10px 16px', background:'rgba(15,23,42,0.6)',
            borderTop:'1px solid #1e293b', fontSize:12, color:'#64748b',
            display:'flex', justifyContent:'space-between' }}>
            <span>Showing {visible.length} of {rows.length} records</span>
            {dirtyCount > 0 && (
              <span style={{ color:'#fbbf24', fontWeight:600 }}>
                ● {dirtyCount} unsaved change{dirtyCount>1?'s':''} — click Save Changes
              </span>
            )}
          </div>
        )}
      </div>

      {/* CSS keyframe for spinner */}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  )
}
