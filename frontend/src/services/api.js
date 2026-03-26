import axios from 'axios'
import toast from 'react-hot-toast'

const API_BASE = import.meta.env.VITE_API_URL || '/api/v1'

// Log actual API configuration
console.log(`🔌 API Configuration:
  Base URL: ${API_BASE}
  Environment: ${import.meta.env.MODE}
  Dev: ${import.meta.env.DEV}
  Prod: ${import.meta.env.PROD}
`);

const api = axios.create({ baseURL: API_BASE, timeout: 300000 }) // 5 minute timeout for complex calculations

// Request interceptor: attach JWT
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token')
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
})

// Response interceptor: handle 401, errors
api.interceptors.response.use(
  (res) => res,
  async (error) => {
    const status = error.response?.status
    if (status === 401) {
      // Try refresh
      const refresh = localStorage.getItem('refresh_token')
      if (refresh && !error.config._retry) {
        error.config._retry = true
        try {
          const { data } = await axios.post(`${API_BASE}/auth/refresh`, { refresh_token: refresh })
          localStorage.setItem('access_token', data.access_token)
          localStorage.setItem('refresh_token', data.refresh_token)
          error.config.headers.Authorization = `Bearer ${data.access_token}`
          return api(error.config)
        } catch {
          localStorage.clear()
          window.location.href = '/login'
        }
      } else {
        localStorage.clear()
        window.location.href = '/login'
      }
    }
    const msg = error.response?.data?.detail || error.message
    if (status !== 401) toast.error(msg)
    return Promise.reject(error)
  }
)

// ============== Auth ==============
export const authAPI = {
  login: (username, password) => api.post('/auth/login', { username, password }),
  me: () => api.get('/auth/me'),
  updateProfile: (data) => api.put('/auth/profile', data),
  changePassword: (data) => api.post('/auth/change-password', data),
}

// ============== Users ==============
export const usersAPI = {
  list: (params) => api.get('/users', { params }),
  get: (id) => api.get(`/users/${id}`),
  create: (data) => api.post('/users', data),
  update: (id, data) => api.put(`/users/${id}`, data),
  unlock: (id) => api.post(`/users/${id}/unlock`),
}

// ============== Roles ==============
export const rolesAPI = {
  list: () => api.get('/roles'),
  create: (data) => api.post('/roles', data),
  update: (id, data) => api.put(`/roles/${id}`, data),
  permissions: () => api.get('/roles/permissions'),
  assignPermissions: (id, data) => api.post(`/roles/${id}/permissions`, data),
}

// ============== RLS ==============
export const rlsAPI = {
  stores: () => api.get('/rls/stores'),
  storeAccess: (uid) => api.get(`/rls/store-access/${uid}`),
  addStoreAccess: (data) => api.post('/rls/store-access', data),
  deleteStoreAccess: (uid, code) => api.delete(`/rls/store-access/${uid}/${code}`),
  regionAccess: (uid) => api.get(`/rls/region-access/${uid}`),
  addRegionAccess: (data) => api.post('/rls/region-access', data),
  columnRestrictions: (table) => api.get(`/rls/column-restrictions/${table}`),
  addColumnRestrictions: (data) => api.post('/rls/column-restrictions', data),
}

// ============== Tables ==============
export const tablesAPI = {
  list: (params) => api.get('/tables', { params }),
  listAll: (params) => api.get('/tables/database/all', { params }),
  listAllVisible: () => api.get('/tables/database/all', { params: { visible_only: true } }),
  schema: (name) => api.get(`/tables/${name}/schema`),
  create: (data) => api.post('/tables', data),
  alter: (name, data) => api.put(`/tables/${name}/alter`, data),
  delete: (name) => api.delete(`/tables/${name}`),
  data: (name, params) => api.get(`/tables/${name}/data`, { params }),
  truncate: (name) => api.delete(`/tables/${name}/data`),
  rowCount: (name) => api.get(`/tables/${name}/row-count`),
  settings: (name) => api.get(`/tables/${name}/settings`),
  updateSettings: (name, params) => api.put(`/tables/settings/${name}`, null, { params }),
  allSettings: () => api.get('/tables/settings/all'),
  distinct: (name, column, params) => api.get(`/tables/${name}/distinct/${column}`, { params }),
  exportSettings: () => api.get('/tables/export/settings'),
  updateExportSettings: (settings) => api.post('/tables/export/settings', settings),
  // Table permissions
  tablePermissions: () => api.get('/tables/permissions'),
  allowedTables: (action) => api.get('/tables/permissions/allowed', { params: { action } }),
  saveTablePermissions: (permissions) => api.post('/tables/permissions', permissions),
  // Export jobs
  startExportJob: (data) => api.post('/tables/export/jobs/start', data),
  listExportJobs: (limit = 20) => api.get('/tables/export/jobs', { params: { limit } }),
  getExportJobStatus: (jobId) => api.get(`/tables/export/jobs/${jobId}`),
  deleteExportJob: (jobId) => api.delete(`/tables/export/jobs/${jobId}`),
  downloadExportJob: (jobId) => `/api/v1/tables/export/jobs/${jobId}/download`,
}

// ============== Data Operations ==============
export const dataAPI = {
  upsert: (data) => api.post('/data/upsert', data),
  update: (data) => api.put('/data/update', data),
  delete: (data) => api.post('/data/delete', data),
}

// ============== Upload ==============
export const uploadAPI = {
  upload: (formData, onProgress) =>
    api.post('/upload/', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: onProgress,
      timeout: 300000,
    }),
  uploadAsync: (formData, onProgress) =>
    api.post('/upload/async', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: onProgress,
      timeout: 60000,  // Shorter timeout since it's async
    }),
  preview: (formData) =>
    api.post('/upload/preview', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }),
  sheets: (formData) =>
    api.post('/upload/sheets', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }),
  // Job management
  listJobs: (limit = 20) => api.get('/upload/jobs', { params: { limit } }),
  listAllJobs: (limit = 50) => api.get('/upload/jobs/all', { params: { limit } }),
  getJob: (jobId) => api.get(`/upload/jobs/${jobId}`),
  cancelJob: (jobId, force = false) => api.post(`/upload/jobs/${jobId}/cancel`, null, { params: { force } }),
  deleteJob: (jobId) => api.delete(`/upload/jobs/${jobId}`),
  queueStatus: () => api.get('/upload/queue/status'),
}

// ============== Allocations ==============
export const allocAPI = {
  run: (data) => api.post('/allocations/run', data),
  list: (params) => api.get('/allocations', { params }),
  details: (id, params) => api.get(`/allocations/${id}/details`, { params }),
  summary: (id) => api.get(`/allocations/${id}/summary`),
  overrides: (id, data) => api.post(`/allocations/${id}/overrides`, data),
  approve: (id) => api.post(`/allocations/${id}/approve`),
  execute: (id) => api.post(`/allocations/${id}/execute`),
  cancel: (id) => api.post(`/allocations/${id}/cancel`),
  grid: (id, storeCode, params) => api.get(`/allocations/${id}/grid/${storeCode}`, { params }),
}

// ============== MSA Analysis ==============
export const msaAPI = {
  // Configuration & Discovery
  getColumns: () => api.get('/msa/columns'),
  getDistinct: (column, date, filters) => api.get('/msa/distinct', { 
    params: { 
      column, 
      ...(date && { date }),
      ...(filters && { filters })
    } 
  }),
  loadConfig: (configName) => api.get(`/msa/load/${configName}`),
  saveConfig: (payload) => api.post('/msa/config', payload),
  
  // Filtering & Data Loading
  applyFilters: (payload) => api.post('/msa/filter', payload),
  
  // Debug endpoints
  debugTestDate: (date) => api.get(`/msa/debug/test-date`, { params: { date } }),
  
  // Calculation
  calculate: (payload) => api.post('/msa/calculate', payload),
  
  // Pivot Tables
  generatePivot: (payload) => api.post('/msa/pivot', payload),
  
  // Save Results
  save: (payload) => api.post('/msa/save', payload),
  
  // Stored Results Management (with Sequence Tracking)
  getStoredSequences: (limit = 10) => api.get('/msa/results/sequences', { params: { limit } }),
  getStoredResults: (sequenceId, table = 'msa') => api.get(`/msa/results/${sequenceId}`, { params: { table } }),
  getSequenceSummary: (sequenceId) => api.get(`/msa/results/${sequenceId}/summary`),
  
  // Legacy
  run: (payload) => api.post('/msa/run', payload),
}

// ============== Audit ==============
export const auditAPI = {
  list: (params) => api.get('/audit', { params }),
  get: (id) => api.get(`/audit/${id}`),
}

// ============== Settings ==============
export const settingsAPI = {
  getAll: () => api.get('/settings'),
  get: (category) => api.get(`/settings/${category}`),
  update: (category, settings) => api.put('/settings', { category, settings }),
  testConnection: () => api.post('/settings/test-connection'),
  testEmail: (to) => api.post('/settings/test-email', { to_address: to }),
  systemInfo: () => api.get('/settings/system/info'),
  // Backup
  listBackups: () => api.get('/settings/backup/list'),
  createBackup: (database) => api.post('/settings/backup/create', { database }),
  deleteBackup: (filename) => api.delete(`/settings/backup/${filename}`),
}

// ============== Contribution Percentage Analysis ==============
export const contributionAPI = {
  // Presets
  listPresets: () => api.get('/contribution/presets'),
  getPreset: (name) => api.get(`/contribution/presets/${name}`),
  createPreset: (data) => api.post('/contribution/presets', data),
  updatePreset: (name, data) => api.put(`/contribution/presets/${name}`, data),
  deletePreset: (name) => api.delete(`/contribution/presets/${name}`),
  
  // Mappings
  listMappings: () => api.get('/contribution/mappings'),
  getMapping: (name) => api.get(`/contribution/mappings/${name}`),
  createMapping: (data) => api.post('/contribution/mappings', data),
  updateMapping: (name, data) => api.put(`/contribution/mappings/${name}`, data),
  deleteMapping: (name) => api.delete(`/contribution/mappings/${name}`),
  
  // Assignments
  listAssignments: () => api.get('/contribution/assignments'),
  getAssignment: (id) => api.get(`/contribution/assignments/${id}`),
  createAssignment: (data) => api.post('/contribution/assignments', data),
  updateAssignment: (id, data) => api.put(`/contribution/assignments/${id}`, data),
  deleteAssignment: (id) => api.delete(`/contribution/assignments/${id}`),
  
  // Execution
  calculate: (data) => api.post('/contribution/calculate', data),
  getDynamicQuery: (params) => api.get('/contribution/dynamic-query', { params }),
  getExecutionOptions: (groupingColumn) => api.get('/contribution/execution/options', { 
    params: { 
      grouping_column: groupingColumn
    } 
  }),
  
  // Export
  export: (data) => api.post('/contribution/export', data),
  downloadTable: (tableName) => `${API_BASE}/contribution/download/${tableName}`,
  deleteTable: (tableName) => api.delete(`/contribution/tables/${tableName}`),
  getSavedResults: () => api.get('/contribution/results'),
}

// ============== BDC Creation ==============
export const bdcAPI = {
  upload: (formData) =>
    api.post('/bdc/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 300000,
    }),
  save: (formData) =>
    api.post('/bdc/save', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 300000,
    }),
  download: (formData) =>
    api.post('/bdc/download', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      responseType: 'blob',
      timeout: 300000,
    }),
  getSheets: (formData) =>
    api.post('/bdc/sheets', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }),
  getSequences: () => api.get('/bdc/sequences'),
  deleteSequence: (allocationNo) => api.delete(`/bdc/sequences/${allocationNo}`),
  deliveryOrderUpload: (formData) =>
    api.post('/bdc/delivery-order-upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 300000,
    }),
}

// ============== Store Stock (Data Preparation) ==============
// DB columns: kpi (NVARCHAR) and status ('Active' | 'Inactive')
export const storeStockAPI = {
  getSlocSettings: ()           => api.get('/store-stock/sloc-settings'),
  syncSlocs:       ()           => api.post('/store-stock/sync'),
  updateSloc:      (sloc, data) => api.put(`/store-stock/sloc-settings/${encodeURIComponent(sloc)}`, data),
  bulkUpdate:      (items)      => api.put('/store-stock/sloc-settings', { items }),
}

// ============== Grid Builder (Data Preparation > Store Stock) ==============
export const gridBuilderAPI = {
  getColumns:  ()           => api.get('/grid-builder/columns'),
  listGrids:   ()           => api.get('/grid-builder/grids'),
  createGrid:  (data)       => api.post('/grid-builder/grids', data),
  updateGrid:  (id, data)   => api.put(`/grid-builder/grids/${id}`, data),
  deleteGrid:  (id)         => api.delete(`/grid-builder/grids/${id}`),
  runGrid:     (id)         => api.post(`/grid-builder/grids/${id}/run`),
  runAll:      ()           => api.post('/grid-builder/run-all'),
}

// ============== Lookup Art Master (Data Preparation) ==============
export const lookupArtMasterAPI = {
  getColumns: () => api.get('/lookup-art-master/columns'),
  preview: (formData) =>
    api.post('/lookup-art-master/preview', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 300000,
    }),
  run: (formData) =>
    api.post('/lookup-art-master/run', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 300000,
    }),
  download: (formData) =>
    api.post('/lookup-art-master/download', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      responseType: 'blob',
      timeout: 300000,
    }),
}

export default api
