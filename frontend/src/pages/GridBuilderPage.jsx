/**
 * GridBuilderPage — Dynamic Pivot Grid Builder
 * Light theme matching ARS app (bg-gray-50 layout).
 */
import { useState, useEffect, useCallback } from 'react'
import { gridBuilderAPI } from '@/services/api'
import toast from 'react-hot-toast'
import {
  Plus, Play, PlayCircle, Trash2, Edit3, X, Save,
  CheckCircle2, XCircle, Clock, AlertTriangle, Loader,
  LayoutGrid, ChevronDown, ChevronUp, RefreshCw, Database
} from 'lucide-react'

/* ── colour tokens (light theme) ─────────────────────────────────────────── */
const C = {
  text:       '#0f172a',
  textSub:    '#475569',
  textMuted:  '#94a3b8',
  card:       '#ffffff',
  cardBorder: '#e2e8f0',
  headerBg:   '#f8fafc',
  rowAlt:     '#f8fafc',
  inputBg:    '#ffffff',
  inputBd:    '#cbd5e1',
  primary:    '#4f46e5',
  primaryLt:  '#eef2ff',
  primaryBd:  '#c7d2fe',
  green:      '#059669', greenBg: '#ecfdf5', greenBd: '#a7f3d0',
  red:        '#dc2626', redBg:   '#fef2f2', redBd:   '#fecaca',
  amber:      '#d97706', amberBg: '#fffbeb', amberBd: '#fde68a',
  blue:       '#2563eb', blueBg:  '#eff6ff', blueBd:  '#bfdbfe',
  gray:       '#64748b', grayBg:  '#f1f5f9', grayBd:  '#e2e8f0',
}

/* ── tiny helpers ─────────────────────────────────────────────────────────── */
const StatusBadge = ({ s }) => {
  const map = {
    Active:   [C.green,  C.greenBg,  C.greenBd],
    Inactive: [C.red,    C.redBg,    C.redBd],
    Success:  [C.green,  C.greenBg,  C.greenBd],
    Failed:   [C.red,    C.redBg,    C.redBd],
    Running:  [C.blue,   C.blueBg,   C.blueBd],
  }
  const [col, bg, bd] = map[s] || [C.gray, C.grayBg, C.grayBd]
  return (
    <span style={{ display:'inline-flex', alignItems:'center', gap:4,
      padding:'2px 9px', borderRadius:20, fontSize:11, fontWeight:700,
      background:bg, color:col, border:`1px solid ${bd}`, whiteSpace:'nowrap' }}>
      {s === 'Running' && <Loader size={9} style={{ animation:'spin 1s linear infinite' }} />}
      {s}
    </span>
  )
}

const Btn = ({ onClick, disabled, color='primary', children, style={} }) => {
  const map = {
    primary: { bg:C.primary,  text:'#fff',    bd:C.primary  },
    green:   { bg:C.greenBg,  text:C.green,   bd:C.greenBd  },
    red:     { bg:C.redBg,    text:C.red,     bd:C.redBd    },
    amber:   { bg:C.amberBg,  text:C.amber,   bd:C.amberBd  },
    gray:    { bg:C.grayBg,   text:C.textSub, bd:C.grayBd   },
    blue:    { bg:C.blueBg,   text:C.blue,    bd:C.blueBd   },
  }
  const t = map[color] || map.primary
  return (
    <button onClick={onClick} disabled={disabled}
      style={{ display:'inline-flex', alignItems:'center', gap:6,
        padding:'7px 14px', borderRadius:8, fontSize:12, fontWeight:600,
        cursor: disabled ? 'not-allowed' : 'pointer',
        border:`1px solid ${t.bd}`, background:t.bg, color:t.text,
        opacity: disabled ? .5 : 1, transition:'all .15s', ...style }}>
      {children}
    </button>
  )
}

const Field = ({ label, children, required }) => (
  <div style={{ display:'flex', flexDirection:'column', gap:4 }}>
    <label style={{ fontSize:12, fontWeight:600, color:C.textSub }}>
      {label}{required && <span style={{ color:C.red }}> *</span>}
    </label>
    {children}
  </div>
)

const Input = ({ value, onChange, placeholder, ...rest }) => (
  <input value={value} onChange={onChange} placeholder={placeholder} {...rest}
    style={{ padding:'7px 11px', borderRadius:7, fontSize:13,
      background:C.inputBg, border:`1px solid ${C.inputBd}`,
      color:C.text, outline:'none', fontFamily:'inherit', ...rest.style }} />
)

/* ── Column multi-selector ────────────────────────────────────────────────── */
const ColPicker = ({ available, selected, onChange }) => {
  const toggle = (col) => {
    if (selected.includes(col)) onChange(selected.filter(c => c !== col))
    else onChange([...selected, col])
  }
  const moveUp   = (i) => { if (i === 0) return; const a = [...selected]; [a[i-1],a[i]]=[a[i],a[i-1]]; onChange(a) }
  const moveDown = (i) => { if (i === selected.length-1) return; const a=[...selected]; [a[i],a[i+1]]=[a[i+1],a[i]]; onChange(a) }
  const remove   = (col) => onChange(selected.filter(c => c !== col))

  return (
    <div style={{ border:`1px solid ${C.cardBorder}`, borderRadius:8, overflow:'hidden' }}>
      {/* Available columns */}
      <div style={{ padding:10, background:C.headerBg, borderBottom:`1px solid ${C.cardBorder}` }}>
        <div style={{ fontSize:11, fontWeight:600, color:C.textSub, marginBottom:6 }}>
          Available columns (click to add)
        </div>
        <div style={{ display:'flex', flexWrap:'wrap', gap:5 }}>
          {available.filter(c => !selected.includes(c)).map(col => (
            <button key={col} onClick={() => toggle(col)}
              style={{ padding:'3px 10px', borderRadius:6, fontSize:11, fontWeight:600,
                cursor:'pointer', background:C.primaryLt, color:C.primary,
                border:`1px solid ${C.primaryBd}` }}>
              + {col}
            </button>
          ))}
          {available.filter(c => !selected.includes(c)).length === 0 &&
            <span style={{ fontSize:11, color:C.textMuted }}>All columns selected</span>}
        </div>
      </div>

      {/* Selected (ordered) */}
      <div style={{ padding:10 }}>
        <div style={{ fontSize:11, fontWeight:600, color:C.textSub, marginBottom:6 }}>
          Selected hierarchy (drag order matters for GROUP BY)
        </div>
        {selected.length === 0 ? (
          <div style={{ fontSize:12, color:C.textMuted, fontStyle:'italic' }}>
            No columns selected — default: MATNR, WERKS
          </div>
        ) : selected.map((col, i) => (
          <div key={col} style={{ display:'flex', alignItems:'center', gap:6,
            padding:'5px 8px', borderRadius:6, background:C.grayBg,
            border:`1px solid ${C.cardBorder}`, marginBottom:4 }}>
            <span style={{ flex:1, fontSize:12, fontWeight:600, color:C.text, fontFamily:'monospace' }}>{col}</span>
            <button onClick={() => moveUp(i)} disabled={i===0}
              style={{ border:'none', background:'none', cursor: i===0 ? 'not-allowed' : 'pointer',
                color: i===0 ? C.textMuted : C.primary, padding:'1px 3px' }}>
              <ChevronUp size={13}/>
            </button>
            <button onClick={() => moveDown(i)} disabled={i===selected.length-1}
              style={{ border:'none', background:'none', cursor: i===selected.length-1 ? 'not-allowed' : 'pointer',
                color: i===selected.length-1 ? C.textMuted : C.primary, padding:'1px 3px' }}>
              <ChevronDown size={13}/>
            </button>
            <button onClick={() => remove(col)}
              style={{ border:'none', background:'none', cursor:'pointer', color:C.red, padding:'1px 3px' }}>
              <X size={13}/>
            </button>
          </div>
        ))}
      </div>
    </div>
  )
}

/* ── Create / Edit Modal ──────────────────────────────────────────────────── */
const EMPTY_FORM = { grid_name:'', description:'', hierarchy_columns:[], kpi_filter:'', output_table:'', status:'Active' }

const GridModal = ({ open, onClose, onSave, availableCols, editing }) => {
  const [form, setForm] = useState(EMPTY_FORM)

  useEffect(() => {
    if (editing) setForm({ ...editing, hierarchy_columns: editing.hierarchy_columns || [] })
    else setForm(EMPTY_FORM)
  }, [editing, open])

  const set = (k,v) => setForm(p => ({ ...p, [k]: v }))

  // Auto-generate output table name from grid name
  const autoTable = (name) => {
    const safe = name.toUpperCase().replace(/[^A-Z0-9]/g, '_').replace(/^_+|_+$/g,'')
    return safe ? `ARS_GRID_${safe}` : ''
  }

  const handleNameChange = (v) => {
    set('grid_name', v)
    if (!editing) set('output_table', autoTable(v))
  }

  const handleSave = async () => {
    if (!form.grid_name.trim()) { toast.error('Grid name is required'); return }
    if (!form.output_table.trim()) { toast.error('Output table is required'); return }
    await onSave(form)
  }

  if (!open) return null

  return (
    <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,.5)',
      display:'flex', alignItems:'center', justifyContent:'center', zIndex:1000 }}>
      <div style={{ background:C.card, border:`1px solid ${C.cardBorder}`, borderRadius:14,
        width:'min(700px, 95vw)', maxHeight:'90vh', overflow:'auto',
        boxShadow:'0 20px 60px rgba(0,0,0,.2)' }}>

        {/* Modal header */}
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center',
          padding:'16px 20px', borderBottom:`1px solid ${C.cardBorder}`, background:C.headerBg }}>
          <h2 style={{ margin:0, fontSize:16, fontWeight:700, color:C.text }}>
            {editing ? `Edit Grid: ${editing.grid_name}` : 'Create New Grid'}
          </h2>
          <button onClick={onClose} style={{ border:'none', background:'none',
            cursor:'pointer', color:C.textSub, padding:4 }}><X size={18}/></button>
        </div>

        {/* Modal body */}
        <div style={{ padding:20, display:'flex', flexDirection:'column', gap:16 }}>

          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:14 }}>
            <Field label="Grid Name" required>
              <Input value={form.grid_name} onChange={e => handleNameChange(e.target.value)}
                placeholder="e.g. STK Summary" />
            </Field>
            <Field label="Status">
              <select value={form.status} onChange={e => set('status', e.target.value)}
                style={{ padding:'7px 11px', borderRadius:7, fontSize:13,
                  background:C.inputBg, border:`1px solid ${C.inputBd}`, color:C.text, outline:'none' }}>
                <option value="Active">Active</option>
                <option value="Inactive">Inactive</option>
              </select>
            </Field>
          </div>

          <Field label="Description">
            <Input value={form.description || ''} onChange={e => set('description', e.target.value)}
              placeholder="Optional description" />
          </Field>

          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:14 }}>
            <Field label="KPI Filter"
              title="Leave blank to include all active SLOCs. Enter a KPI value (e.g. STK) to only include SLOCs where KPI matches.">
              <Input value={form.kpi_filter || ''} onChange={e => set('kpi_filter', e.target.value)}
                placeholder="e.g. STK (leave blank for all)" />
              <span style={{ fontSize:10, color:C.textMuted }}>
                Filters ARS_STORE_SLOC_SETTINGS.KPI column
              </span>
            </Field>
            <Field label="Output Table" required>
              <Input value={form.output_table || ''} onChange={e => set('output_table', e.target.value.toUpperCase())}
                placeholder="e.g. ARS_GRID_STK" style={{ fontFamily:'monospace', fontSize:12 }} />
              <span style={{ fontSize:10, color:C.textMuted }}>
                Created/truncated on each run in Rep_data
              </span>
            </Field>
          </div>

          <Field label="Hierarchy Columns (from vw_master_product)">
            <ColPicker
              available={availableCols}
              selected={form.hierarchy_columns}
              onChange={v => set('hierarchy_columns', v)}
            />
          </Field>

        </div>

        {/* Modal footer */}
        <div style={{ display:'flex', justifyContent:'flex-end', gap:10,
          padding:'14px 20px', borderTop:`1px solid ${C.cardBorder}`, background:C.headerBg }}>
          <Btn onClick={onClose} color="gray"><X size={13}/> Cancel</Btn>
          <Btn onClick={handleSave} color="primary"><Save size={13}/> {editing ? 'Save Changes' : 'Create Grid'}</Btn>
        </div>
      </div>
    </div>
  )
}

/* ── Run Results Modal ────────────────────────────────────────────────────── */
const RunResultsModal = ({ results, onClose }) => {
  if (!results) return null
  return (
    <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,.5)',
      display:'flex', alignItems:'center', justifyContent:'center', zIndex:1000 }}>
      <div style={{ background:C.card, border:`1px solid ${C.cardBorder}`, borderRadius:14,
        width:'min(560px, 95vw)', maxHeight:'80vh', overflow:'auto',
        boxShadow:'0 20px 60px rgba(0,0,0,.2)' }}>
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center',
          padding:'16px 20px', borderBottom:`1px solid ${C.cardBorder}`, background:C.headerBg }}>
          <h2 style={{ margin:0, fontSize:16, fontWeight:700, color:C.text }}>Run All — Results</h2>
          <button onClick={onClose} style={{ border:'none', background:'none', cursor:'pointer', color:C.textSub }}><X size={18}/></button>
        </div>
        <div style={{ padding:20 }}>
          {results.map((r, i) => (
            <div key={i} style={{ display:'flex', alignItems:'center', justifyContent:'space-between',
              padding:'10px 14px', borderRadius:8, marginBottom:8,
              background: r.status === 'Success' ? C.greenBg : C.redBg,
              border:`1px solid ${r.status === 'Success' ? C.greenBd : C.redBd}` }}>
              <div>
                <div style={{ fontWeight:700, color:C.text, fontSize:13 }}>{r.grid_name}</div>
                {r.error && <div style={{ fontSize:11, color:C.red, marginTop:2 }}>{r.error}</div>}
              </div>
              <div style={{ textAlign:'right' }}>
                <StatusBadge s={r.status}/>
                {r.status === 'Success' && (
                  <div style={{ fontSize:11, color:C.textSub, marginTop:3 }}>{r.rows.toLocaleString()} rows</div>
                )}
              </div>
            </div>
          ))}
        </div>
        <div style={{ padding:'12px 20px', borderTop:`1px solid ${C.cardBorder}`, textAlign:'right' }}>
          <Btn onClick={onClose} color="gray"><X size={13}/> Close</Btn>
        </div>
      </div>
    </div>
  )
}

/* ── Main Page ────────────────────────────────────────────────────────────── */
export default function GridBuilderPage() {
  const [grids,       setGrids]      = useState([])
  const [availCols,   setAvailCols]  = useState(['MATNR','WERKS'])
  const [loading,     setLoading]    = useState(false)
  const [runningId,   setRunningId]  = useState(null)   // grid id currently running
  const [runningAll,  setRunningAll] = useState(false)
  const [modalOpen,   setModalOpen]  = useState(false)
  const [editing,     setEditing]    = useState(null)
  const [seqChanged,  setSeqChanged] = useState(false)
  const [savingSeq,   setSavingSeq]  = useState(false)
  const [runResults,  setRunResults] = useState(null)
  const [deleteConf,  setDeleteConf] = useState(null)   // id to confirm delete

  /* load */
  const load = useCallback(async () => {
    setLoading(true)
    try {
      const [gRes, cRes] = await Promise.all([
        gridBuilderAPI.listGrids(),
        gridBuilderAPI.getColumns(),
      ])
      setGrids(gRes.data.data.grids || [])
      setAvailCols(cRes.data.data.columns || ['MATNR','WERKS'])
      setSeqChanged(false)
    } catch {} finally { setLoading(false) }
  }, [])
  useEffect(() => { load() }, [load])

  /* sequence management */
  const moveGrid = (idx, dir) => {
    const newIdx = idx + dir
    if (newIdx < 0 || newIdx >= grids.length) return
    const updated = [...grids]
    const [moved] = updated.splice(idx, 1)
    updated.splice(newIdx, 0, moved)
    setGrids(updated)
    setSeqChanged(true)
  }

  const saveSequence = async () => {
    setSavingSeq(true)
    try {
      const sequence = grids.map((g, i) => ({ id: g.id, seq: i + 1 }))
      await gridBuilderAPI.reorder(sequence)
      setSeqChanged(false)
      toast.success('Sequence saved')
    } catch { toast.error('Failed to save sequence') }
    finally { setSavingSeq(false) }
  }

  /* create / update */
  const handleSave = async (form) => {
    try {
      if (editing) {
        await gridBuilderAPI.updateGrid(editing.id, form)
        toast.success(`Grid '${form.grid_name || editing.grid_name}' updated.`)
      } else {
        await gridBuilderAPI.createGrid(form)
        toast.success(`Grid '${form.grid_name}' created.`)
      }
      setModalOpen(false); setEditing(null)
      await load()
    } catch {}
  }

  /* toggle status */
  const handleToggleStatus = async (grid) => {
    const newStatus = grid.status === 'Active' ? 'Inactive' : 'Active'
    try {
      await gridBuilderAPI.updateGrid(grid.id, { status: newStatus })
      toast.success(`Grid '${grid.grid_name}' marked ${newStatus}.`)
      await load()
    } catch {}
  }

  /* delete */
  const handleDelete = async (id) => {
    try {
      await gridBuilderAPI.deleteGrid(id)
      toast.success('Grid deleted.')
      setDeleteConf(null)
      await load()
    } catch {}
  }

  /* run single */
  const handleRun = async (grid) => {
    setRunningId(grid.id)
    try {
      const { data } = await gridBuilderAPI.runGrid(grid.id)
      toast.success(data.message)
      await load()
    } catch {} finally { setRunningId(null) }
  }

  /* run all */
  const handleRunAll = async () => {
    setRunningAll(true)
    try {
      const { data } = await gridBuilderAPI.runAll()
      toast.success(data.message)
      setRunResults(data.data.results || [])
      await load()
    } catch {} finally { setRunningAll(false) }
  }

  const activeCount = grids.filter(g => g.status === 'Active').length

  /* ── render ──────────────────────────────────────────────────────────── */
  return (
    <div style={{ color:C.text, fontFamily:'inherit' }}>
      {/* Page title */}
      <div style={{ marginBottom:20 }}>
        <h1 style={{ fontSize:18, fontWeight:700, color:C.text, margin:0,
          display:'flex', alignItems:'center', gap:8 }}>
          <LayoutGrid size={20} color={C.primary}/>
          Store Stock Grid Builder
        </h1>
        <p style={{ fontSize:13, color:C.textSub, marginTop:4 }}>
          Build dynamic pivot grids from{' '}
          <code style={{ fontSize:11, background:C.primaryLt, color:C.primary,
            padding:'1px 6px', borderRadius:4, border:`1px solid ${C.primaryBd}` }}>
            ET_STORE_STOCK
          </code>
          {' '}joined with{' '}
          <code style={{ fontSize:11, background:C.primaryLt, color:C.primary,
            padding:'1px 6px', borderRadius:4, border:`1px solid ${C.primaryBd}` }}>
            vw_master_product
          </code>
          . Each run creates / truncates / inserts into the output table.
        </p>
      </div>

      {/* Main card */}
      <div style={{ background:C.card, border:`1px solid ${C.cardBorder}`, borderRadius:12,
        overflow:'hidden', boxShadow:'0 1px 3px rgba(0,0,0,.08)' }}>

        {/* Card header */}
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center',
          flexWrap:'wrap', gap:10, padding:'14px 18px',
          background:C.headerBg, borderBottom:`1px solid ${C.cardBorder}` }}>
          <span style={{ fontSize:13, fontWeight:600, color:C.textSub }}>
            {grids.length} grid{grids.length!==1?'s':''} &nbsp;·&nbsp;
            <span style={{ color:C.green }}>{activeCount} active</span>
          </span>
          <div style={{ display:'flex', gap:8, flexWrap:'wrap' }}>
            {seqChanged && (
              <Btn onClick={saveSequence} disabled={savingSeq} color="amber">
                {savingSeq ? <Loader size={13} style={{ animation:'spin 1s linear infinite' }}/> : <Save size={13}/>}
                Save Sequence
              </Btn>
            )}
            <Btn onClick={() => { setEditing(null); setModalOpen(true) }} color="primary">
              <Plus size={13}/> New Grid
            </Btn>
            <Btn onClick={handleRunAll} disabled={runningAll || activeCount===0} color="green">
              {runningAll
                ? <><Loader size={13} style={{ animation:'spin 1s linear infinite' }}/> Running…</>
                : <><PlayCircle size={13}/> Run All Active ({activeCount})</>}
            </Btn>
            <Btn onClick={load} disabled={loading} color="gray">
              <RefreshCw size={13} style={{ animation:loading?'spin 1s linear infinite':'none' }}/>
            </Btn>
          </div>
        </div>

        {/* Grid list */}
        {loading ? (
          <div style={{ textAlign:'center', padding:60, color:C.textMuted }}>
            <RefreshCw size={20} style={{ display:'block', margin:'0 auto 8px',
              animation:'spin 1s linear infinite' }}/>
            Loading grids…
          </div>
        ) : grids.length === 0 ? (
          <div style={{ textAlign:'center', padding:60, color:C.textMuted }}>
            <LayoutGrid size={32} style={{ display:'block', margin:'0 auto 10px', opacity:.3 }}/>
            No grids yet. Click <strong>New Grid</strong> to create one.
          </div>
        ) : (
          <div style={{ overflowX:'auto' }}>
            <table style={{ width:'100%', borderCollapse:'collapse', fontSize:13, minWidth:800 }}>
              <thead>
                <tr style={{ background:'#f1f5f9', borderBottom:`2px solid ${C.cardBorder}` }}>
                  {['#','Grid Name','Output Table','Hierarchy Cols','KPI Filter',
                    'Last Run','Rows','Status','Actions'].map(h => (
                    <th key={h} style={{ padding:'9px 14px', textAlign:'left',
                      fontSize:11, fontWeight:700, color:C.textSub,
                      textTransform:'uppercase', letterSpacing:'.06em',
                      whiteSpace:'nowrap' }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {grids.map((g, idx) => {
                  const isRunning = runningId === g.id
                  return (
                    <tr key={g.id} style={{
                      borderBottom:`1px solid ${C.cardBorder}`,
                      background: idx%2===0 ? C.card : C.rowAlt,
                      transition:'background .1s',
                    }}>
                      {/* Sequence */}
                      <td style={{ padding:'6px 8px', textAlign:'center', width:60 }}>
                        <div style={{ display:'flex', alignItems:'center', gap:2, justifyContent:'center' }}>
                          <div style={{ display:'flex', flexDirection:'column', gap:0 }}>
                            <button onClick={() => moveGrid(idx, -1)} disabled={idx === 0}
                              style={{ background:'none', border:'none', cursor: idx === 0 ? 'default' : 'pointer', padding:0, opacity: idx === 0 ? .2 : .6 }}>
                              <ChevronUp size={12} />
                            </button>
                            <button onClick={() => moveGrid(idx, 1)} disabled={idx === grids.length - 1}
                              style={{ background:'none', border:'none', cursor: idx === grids.length - 1 ? 'default' : 'pointer', padding:0, opacity: idx === grids.length - 1 ? .2 : .6 }}>
                              <ChevronDown size={12} />
                            </button>
                          </div>
                          <span style={{ fontSize:12, fontWeight:700, color:C.textMuted }}>{idx + 1}</span>
                        </div>
                      </td>
                      {/* Grid name */}
                      <td style={{ padding:'10px 14px' }}>
                        <div style={{ fontWeight:700, color:C.text }}>{g.grid_name}</div>
                        {g.description && (
                          <div style={{ fontSize:11, color:C.textMuted, marginTop:1 }}>{g.description}</div>
                        )}
                      </td>

                      {/* Output table */}
                      <td style={{ padding:'10px 14px' }}>
                        <code style={{ fontSize:11, color:C.primary, background:C.primaryLt,
                          padding:'2px 6px', borderRadius:4, border:`1px solid ${C.primaryBd}`,
                          fontFamily:'monospace', fontWeight:600 }}>
                          {g.output_table}
                        </code>
                      </td>

                      {/* Hierarchy cols */}
                      <td style={{ padding:'10px 14px' }}>
                        <div style={{ display:'flex', flexWrap:'wrap', gap:3 }}>
                          {(g.hierarchy_columns.length ? g.hierarchy_columns : ['MATNR','WERKS']).map(c => (
                            <span key={c} style={{ fontSize:10, fontWeight:600, color:C.textSub,
                              background:C.grayBg, border:`1px solid ${C.grayBd}`,
                              padding:'1px 6px', borderRadius:4, fontFamily:'monospace' }}>{c}</span>
                          ))}
                        </div>
                      </td>

                      {/* KPI filter */}
                      <td style={{ padding:'10px 14px' }}>
                        {g.kpi_filter
                          ? <span style={{ fontSize:11, fontWeight:700, color:C.amber,
                              background:C.amberBg, border:`1px solid ${C.amberBd}`,
                              padding:'2px 8px', borderRadius:4 }}>{g.kpi_filter}</span>
                          : <span style={{ fontSize:11, color:C.textMuted }}>All SLOCs</span>}
                      </td>

                      {/* Last run */}
                      <td style={{ padding:'10px 14px' }}>
                        {g.last_run_at ? (
                          <div>
                            <div style={{ fontSize:11, color:C.text }}>
                              {new Date(g.last_run_at).toLocaleString()}
                            </div>
                            {g.last_run_status && <StatusBadge s={g.last_run_status}/>}
                            {g.last_run_error && (
                              <div style={{ fontSize:10, color:C.red, marginTop:2,
                                maxWidth:180, overflow:'hidden', textOverflow:'ellipsis',
                                whiteSpace:'nowrap' }} title={g.last_run_error}>
                                {g.last_run_error}
                              </div>
                            )}
                          </div>
                        ) : (
                          <span style={{ fontSize:11, color:C.textMuted }}>Never run</span>
                        )}
                      </td>

                      {/* Row count */}
                      <td style={{ padding:'10px 14px' }}>
                        {g.last_run_rows != null
                          ? <strong style={{ color:C.text }}>{g.last_run_rows.toLocaleString()}</strong>
                          : <span style={{ color:C.textMuted }}>—</span>}
                      </td>

                      {/* Status toggle */}
                      <td style={{ padding:'10px 14px' }}>
                        <button onClick={() => handleToggleStatus(g)} style={{
                          display:'inline-flex', alignItems:'center', gap:6,
                          padding:'4px 12px', borderRadius:7, fontSize:11, fontWeight:700,
                          cursor:'pointer', transition:'all .15s',
                          border:`1px solid ${g.status==='Active' ? C.greenBd : C.redBd}`,
                          background: g.status==='Active' ? C.greenBg : C.redBg,
                          color:      g.status==='Active' ? C.green   : C.red }}>
                          {/* pill */}
                          <span style={{ width:26, height:14, borderRadius:7, position:'relative',
                            display:'inline-block', flexShrink:0,
                            background: g.status==='Active' ? '#10b981' : '#e2e8f0', transition:'background .2s' }}>
                            <span style={{ position:'absolute', top:2, width:10, height:10,
                              borderRadius:'50%', background:'#fff',
                              boxShadow:'0 1px 3px rgba(0,0,0,.3)', transition:'left .2s',
                              left: g.status==='Active' ? 14 : 2 }}/>
                          </span>
                          {g.status}
                        </button>
                      </td>

                      {/* Actions */}
                      <td style={{ padding:'10px 14px' }}>
                        <div style={{ display:'flex', gap:6, alignItems:'center' }}>
                          {/* Run */}
                          <button onClick={() => handleRun(g)}
                            disabled={isRunning || runningAll}
                            title="Run this grid"
                            style={{ display:'flex', alignItems:'center', gap:4,
                              padding:'5px 10px', borderRadius:7, fontSize:11, fontWeight:600,
                              cursor: isRunning ? 'not-allowed' : 'pointer',
                              border:`1px solid ${C.greenBd}`, background:C.greenBg, color:C.green,
                              opacity: (isRunning||runningAll) ? .5 : 1 }}>
                            {isRunning
                              ? <Loader size={12} style={{ animation:'spin 1s linear infinite' }}/>
                              : <Play size={12}/>}
                            Run
                          </button>
                          {/* Edit */}
                          <button onClick={() => { setEditing(g); setModalOpen(true) }}
                            title="Edit grid"
                            style={{ display:'flex', alignItems:'center', gap:4,
                              padding:'5px 10px', borderRadius:7, fontSize:11, fontWeight:600,
                              cursor:'pointer', border:`1px solid ${C.primaryBd}`,
                              background:C.primaryLt, color:C.primary }}>
                            <Edit3 size={12}/> Edit
                          </button>
                          {/* Delete */}
                          {deleteConf === g.id ? (
                            <div style={{ display:'flex', gap:4 }}>
                              <button onClick={() => handleDelete(g.id)}
                                style={{ padding:'5px 10px', borderRadius:7, fontSize:11, fontWeight:700,
                                  cursor:'pointer', border:`1px solid ${C.redBd}`,
                                  background:C.red, color:'#fff' }}>
                                Confirm
                              </button>
                              <button onClick={() => setDeleteConf(null)}
                                style={{ padding:'5px 8px', borderRadius:7, fontSize:11,
                                  cursor:'pointer', border:`1px solid ${C.grayBd}`,
                                  background:C.grayBg, color:C.textSub }}>
                                <X size={11}/>
                              </button>
                            </div>
                          ) : (
                            <button onClick={() => setDeleteConf(g.id)}
                              title="Delete grid"
                              style={{ display:'flex', alignItems:'center',
                                padding:'5px 8px', borderRadius:7, fontSize:11,
                                cursor:'pointer', border:`1px solid ${C.redBd}`,
                                background:C.redBg, color:C.red }}>
                              <Trash2 size={12}/>
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}

        {/* Footer */}
        {grids.length > 0 && (
          <div style={{ padding:'9px 18px', borderTop:`1px solid ${C.cardBorder}`,
            background:C.headerBg, fontSize:12, color:C.textMuted }}>
            Each grid: <strong style={{color:C.textSub}}>CREATE TABLE IF NOT EXISTS</strong> →{' '}
            <strong style={{color:C.textSub}}>TRUNCATE</strong> →{' '}
            <strong style={{color:C.textSub}}>INSERT</strong> on every run.
            Active SLOCs from <code style={{fontSize:11}}>ARS_STORE_SLOC_SETTINGS</code>.
          </div>
        )}
      </div>

      {/* Modals */}
      <GridModal open={modalOpen} onClose={() => { setModalOpen(false); setEditing(null) }}
        onSave={handleSave} availableCols={availCols} editing={editing}/>
      <RunResultsModal results={runResults} onClose={() => setRunResults(null)}/>

      <style>{`@keyframes spin{to{transform:rotate(360deg);}}`}</style>
    </div>
  )
}
