/**
 * ContribReviewPage – List result tables, preview, download, delete.
 */
import { useState, useEffect } from 'react'
import { contribAPI } from '@/services/api'
import toast from 'react-hot-toast'
import { ClipboardCheck, Download, Trash2, RefreshCw, Table2, Eye, Search } from 'lucide-react'

const C = {
  cardBg:'#fff', cardBorder:'#e2e8f0', headerBg:'#f8fafc',
  text:'#0f172a', textSub:'#475569', textMuted:'#94a3b8',
  primary:'#4f46e5', primaryLight:'#eef2ff', primaryBd:'#c7d2fe',
  green:'#059669', greenBg:'#ecfdf5', greenBd:'#a7f3d0',
  red:'#dc2626',
  inputBg:'#fff', inputBorder:'#cbd5e1',
}

export default function ContribReviewPage() {
  const [tables, setTables] = useState([])
  const [loading, setLoading] = useState(false)
  const [selected, setSelected] = useState(null)
  const [preview, setPreview] = useState(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [downloading, setDownloading] = useState({})
  const [search, setSearch] = useState('')

  const load = async () => {
    setLoading(true)
    try {
      const { data } = await contribAPI.listTables()
      setTables(data.data?.tables || [])
    } catch { toast.error('Failed to load tables') }
    finally { setLoading(false) }
  }
  useEffect(() => { load() }, [])

  const handlePreview = async (name) => {
    setSelected(name); setPreviewLoading(true); setPreview(null)
    try {
      const { data } = await contribAPI.previewTable(name, 200)
      setPreview(data.data)
    } catch { toast.error('Preview failed') }
    finally { setPreviewLoading(false) }
  }

  const handleDownload = async (name) => {
    if (downloading[name]) return
    setDownloading(d => ({...d, [name]: true}))
    toast('Preparing download…')
    try {
      const res = await contribAPI.downloadTable(name)
      const ct = res.headers?.['content-type'] || ''
      const ext = ct.includes('zip') ? 'zip' : 'csv'
      const a = document.createElement('a')
      a.href = URL.createObjectURL(new Blob([res.data]))
      a.download = `${name}.${ext}`; a.click()
      toast.success('Download complete')
    } catch { toast.error('Download failed — try again') }
    finally { setDownloading(d => ({...d, [name]: false})) }
  }

  const handleDelete = async (name) => {
    if (!confirm(`Delete table "${name}"? This cannot be undone.`)) return
    try {
      await contribAPI.deleteTable(name); toast.success('Deleted')
      if (selected === name) { setSelected(null); setPreview(null) }
      load()
    } catch { toast.error('Delete failed') }
  }

  const filtered = tables.filter(t => t.toLowerCase().includes(search.toLowerCase()))

  return (
    <div style={{ color:C.text }}>
      <h1 style={{ fontSize:20, fontWeight:800, margin:'0 0 20px', display:'flex', alignItems:'center', gap:10 }}>
        <ClipboardCheck size={20} color={C.primary}/> Contribution % — Review & Export
      </h1>

      <div style={{ display:'grid', gridTemplateColumns:'350px 1fr', gap:16 }}>
        {/* Left: Table list */}
        <div style={{ background:C.cardBg, border:`1px solid ${C.cardBorder}`, borderRadius:12, overflow:'hidden' }}>
          <div style={{ padding:'12px 14px', background:C.headerBg, borderBottom:`1px solid ${C.cardBorder}`, display:'flex', justifyContent:'space-between', alignItems:'center' }}>
            <span style={{ fontSize:13, fontWeight:700 }}>{tables.length} Result Tables</span>
            <button onClick={load} style={{ background:'none', border:'none', cursor:'pointer' }}>
              <RefreshCw size={14} color={C.textMuted} style={{ animation:loading?'spin 1s linear infinite':'none' }}/>
            </button>
          </div>

          <div style={{ padding:'8px 10px', borderBottom:`1px solid ${C.cardBorder}` }}>
            <div style={{ position:'relative' }}>
              <Search size={13} style={{ position:'absolute', left:8, top:'50%', transform:'translateY(-50%)', color:C.textMuted }}/>
              <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Search tables…"
                style={{ width:'100%', padding:'6px 8px 6px 28px', borderRadius:6, border:`1px solid ${C.inputBorder}`, background:C.inputBg, color:C.text, fontSize:12, boxSizing:'border-box' }}/>
            </div>
          </div>

          <div style={{ maxHeight:'calc(100vh - 280px)', overflowY:'auto' }}>
            {filtered.map(t => (
              <div key={t} style={{
                padding:'10px 14px', borderBottom:`1px solid ${C.cardBorder}`, cursor:'pointer',
                background:selected===t ? C.primaryLight : '#fff',
              }} onClick={() => handlePreview(t)}>
                <div style={{ fontSize:12, fontWeight:600, color:C.text, wordBreak:'break-all' }}>{t}</div>
                <div style={{ display:'flex', gap:6, marginTop:6 }}>
                  <button onClick={e => { e.stopPropagation(); handlePreview(t) }}
                    style={{ padding:'3px 8px', borderRadius:5, fontSize:10, fontWeight:600, border:`1px solid ${C.primaryBd}`, background:C.primaryLight, color:C.primary, cursor:'pointer', display:'flex', alignItems:'center', gap:3 }}>
                    <Eye size={10}/> Preview
                  </button>
                  <button onClick={e => { e.stopPropagation(); handleDownload(t) }} disabled={downloading[t]}
                    style={{ padding:'3px 8px', borderRadius:5, fontSize:10, fontWeight:600, border:`1px solid ${C.greenBd}`, background:downloading[t]?'#fef9c3':C.greenBg, color:downloading[t]?'#a16207':C.green, cursor:downloading[t]?'wait':'pointer', display:'flex', alignItems:'center', gap:3 }}>
                    {downloading[t] ? <><RefreshCw size={10} style={{animation:'spin 1s linear infinite'}}/> Downloading…</> : <><Download size={10}/> Excel</>}
                  </button>
                  <button onClick={e => { e.stopPropagation(); handleDelete(t) }}
                    style={{ padding:'3px 8px', borderRadius:5, fontSize:10, fontWeight:600, border:'1px solid #fecaca', background:'#fef2f2', color:C.red, cursor:'pointer', display:'flex', alignItems:'center', gap:3 }}>
                    <Trash2 size={10}/> Delete
                  </button>
                </div>
              </div>
            ))}
            {filtered.length === 0 && (
              <div style={{ padding:30, textAlign:'center', color:C.textMuted, fontSize:13 }}>
                {tables.length === 0 ? 'No result tables yet. Run Execute first.' : 'No tables match search.'}
              </div>
            )}
          </div>
        </div>

        {/* Right: Preview */}
        <div style={{ background:C.cardBg, border:`1px solid ${C.cardBorder}`, borderRadius:12, overflow:'hidden' }}>
          {!selected ? (
            <div style={{ padding:60, textAlign:'center', color:C.textMuted }}>
              <Table2 size={32} color={C.cardBorder} style={{ margin:'0 auto 12px' }}/>
              <div style={{ fontSize:14, fontWeight:600 }}>Select a table to preview</div>
            </div>
          ) : previewLoading ? (
            <div style={{ padding:60, textAlign:'center', color:C.textMuted }}>
              <RefreshCw size={20} style={{ animation:'spin 1s linear infinite', margin:'0 auto 10px', display:'block' }}/>
              Loading preview…
            </div>
          ) : preview ? (
            <>
              <div style={{ padding:'10px 18px', background:C.headerBg, borderBottom:`1px solid ${C.cardBorder}`, display:'flex', alignItems:'center', gap:8 }}>
                <Table2 size={14} color={C.primary}/>
                <span style={{ fontSize:13, fontWeight:700 }}>{selected}</span>
                <span style={{ fontSize:11, color:C.textMuted, marginLeft:'auto' }}>
                  {preview.total_rows?.toLocaleString()} total rows · {preview.columns?.length} columns
                </span>
              </div>
              <div style={{ overflowX:'auto', maxHeight:'calc(100vh - 280px)' }}>
                <table style={{ width:'100%', borderCollapse:'collapse', fontSize:11 }}>
                  <thead style={{ position:'sticky', top:0 }}>
                    <tr style={{ background:'#f1f5f9' }}>
                      <th style={{ padding:'6px 10px', fontSize:10, fontWeight:700, color:C.textMuted, borderBottom:`2px solid ${C.cardBorder}`, width:40 }}>#</th>
                      {preview.columns?.map(c => (
                        <th key={c} style={{ padding:'6px 10px', textAlign:'left', fontSize:10, fontWeight:700, color:C.textSub, whiteSpace:'nowrap', borderBottom:`2px solid ${C.cardBorder}` }}>{c}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {preview.preview?.map((row, i) => (
                      <tr key={i} style={{ borderBottom:`1px solid ${C.cardBorder}`, background:i%2===0?'#fff':'#fafbfc' }}>
                        <td style={{ padding:'4px 10px', fontSize:10, color:C.textMuted, textAlign:'center' }}>{i+1}</td>
                        {preview.columns?.map(c => (
                          <td key={c} style={{ padding:'4px 10px', whiteSpace:'nowrap', maxWidth:180, overflow:'hidden', textOverflow:'ellipsis' }}>
                            {row[c] != null ? String(row[c]) : ''}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          ) : null}
        </div>
      </div>
      <style>{`@keyframes spin{to{transform:rotate(360deg);}}`}</style>
    </div>
  )
}
