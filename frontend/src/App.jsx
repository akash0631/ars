import { Routes, Route, Navigate } from 'react-router-dom'
import { useEffect, Component } from 'react'
import useAuthStore from '@/store/authStore'
import Layout from '@/components/layout/Layout'
import LoginPage from '@/pages/LoginPage'
import DashboardPage from '@/pages/DashboardPage'
import TablesPage from '@/pages/TablesPage'
import TableDataPage from '@/pages/TableDataPage'
import CreateTablePage from '@/pages/CreateTablePage'
import UploadPage from '@/pages/UploadPage'
import ExportPage from '@/pages/ExportPage'
import DataEditorPage from '@/pages/DataEditorPage'
import AllocationsPage from '@/pages/AllocationsPage'
import AllocationDetailPage from '@/pages/AllocationDetailPage'
import NewAllocationPage from '@/pages/NewAllocationPage'
import UsersPage from '@/pages/UsersPage'
import RolesPage from '@/pages/RolesPage'
import AuditPage from '@/pages/AuditPage'
import RLSPage from '@/pages/RLSPage'
import TableManagementPage from '@/pages/TableManagementPage'
import SettingsPage from '@/pages/SettingsPage'
import MSAStockCalculationPage from '@/pages/MSAStockCalculationPage'
import ContribPresetsPage from '@/pages/ContribPresetsPage'
import ContribMappingsPage from '@/pages/ContribMappingsPage'
import ContribExecutePage from '@/pages/ContribExecutePage'
import ContribReviewPage from '@/pages/ContribReviewPage'
import JobsDashboardPage from '@/pages/JobsDashboardPage'
import BDCCreationPage from '@/pages/BDCCreationPage'
import StoreStockPage from '@/pages/StoreStockPage'
import GridBuilderPage from '@/pages/GridBuilderPage'
import LookupArtMasterPage from '@/pages/LookupArtMasterPage'
import PendAlcReportPage from '@/pages/PendAlcReportPage'
import ChecklistPage from '@/pages/ChecklistPage'
import TrendUploadPage from '@/pages/TrendUploadPage'
import TrendReviewPage from '@/pages/TrendReviewPage'
import TrendAdminPage from '@/pages/TrendAdminPage'
import TrendDashboardPage from '@/pages/TrendDashboardPage'

class ErrorBoundary extends Component {
  constructor(props) { super(props); this.state = { error: null } }
  static getDerivedStateFromError(error) { return { error } }
  componentDidCatch(error, info) { console.error('ErrorBoundary caught:', error, info) }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 40, color: '#dc2626' }}>
          <h2 style={{ marginBottom: 10 }}>Page Error</h2>
          <pre style={{ whiteSpace: 'pre-wrap', fontSize: 13, background: '#fef2f2', padding: 16, borderRadius: 8 }}>
            {this.state.error.message}{'\n'}{this.state.error.stack}
          </pre>
          <button onClick={() => this.setState({ error: null })} style={{ marginTop: 10, padding: '6px 16px', cursor: 'pointer' }}>
            Retry
          </button>
        </div>
      )
    }
    return this.props.children
  }
}

function ProtectedRoute({ children, permission }) {
  const { isAuthenticated, hasPermission } = useAuthStore()
  if (!isAuthenticated) return <Navigate to="/login" replace />
  if (permission && !hasPermission(permission)) {
    return <div className="p-10 text-center text-gray-500">Access denied. You don't have the required permission.</div>
  }
  return children
}

export default function App() {
  const { isAuthenticated, fetchUser } = useAuthStore()

  useEffect(() => {
    if (isAuthenticated) fetchUser()
  }, [])

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/" element={<ProtectedRoute><Layout /></ProtectedRoute>}>
        <Route index element={<DashboardPage />} />
        {/* Data Management */}
        <Route path="tables" element={<TablesPage />} />
        <Route path="tables/create" element={<ProtectedRoute permission="TABLE_CREATE"><CreateTablePage /></ProtectedRoute>} />
        <Route path="tables/:tableName" element={<TableDataPage />} />
        <Route path="upload" element={<UploadPage />} />
        <Route path="export" element={<ExportPage />} />
        <Route path="jobs" element={<JobsDashboardPage />} />
        <Route path="editor" element={<DataEditorPage />} />
        {/* Pending */}
        {/* Data Preparation */}
        <Route path="msa" element={<MSAStockCalculationPage />} />
        <Route path="contribution/presets" element={<ContribPresetsPage />} />
        <Route path="contribution/mappings" element={<ContribMappingsPage />} />
        <Route path="contribution/execute" element={<ContribExecutePage />} />
        <Route path="contribution/review" element={<ContribReviewPage />} />
        <Route path="bdc" element={<BDCCreationPage />} />
        <Route path="data-validation/store-sloc" element={<StoreStockPage />} />
        <Route path="data-validation/checklist" element={<ChecklistPage />} />
        {/* Data Preparation - Store Stock Grid Builder */}
        <Route path="data-prep/store-stock" element={<GridBuilderPage />} />
        <Route path="data-prep/lookup-art-master" element={<LookupArtMasterPage />} />
        {/* Trends */}
        <Route path="trends/dashboard" element={<ErrorBoundary><TrendDashboardPage /></ErrorBoundary>} />
        <Route path="trends/upload" element={<ErrorBoundary><TrendUploadPage /></ErrorBoundary>} />
        <Route path="trends/review" element={<ErrorBoundary><TrendReviewPage /></ErrorBoundary>} />
        <Route path="trends/admin" element={<ErrorBoundary><TrendAdminPage /></ErrorBoundary>} />
        {/* Reports */}
        <Route path="reports/pend-alc" element={<PendAlcReportPage />} />
        {/* Allocations */}
        <Route path="allocations" element={<AllocationsPage />} />
        <Route path="allocations/new" element={<NewAllocationPage />} />
        <Route path="allocations/:id" element={<AllocationDetailPage />} />
        {/* Settings / Admin */}
        <Route path="settings" element={<ProtectedRoute permission="ADMIN_SETTINGS"><SettingsPage /></ProtectedRoute>} />
        <Route path="settings/tables" element={<ProtectedRoute permission="TABLE_CREATE"><TableManagementPage /></ProtectedRoute>} />
        <Route path="settings/users" element={<ProtectedRoute permission="ADMIN_USERS_READ"><UsersPage /></ProtectedRoute>} />
        <Route path="settings/roles" element={<ProtectedRoute permission="ADMIN_ROLES_MANAGE"><RolesPage /></ProtectedRoute>} />
        <Route path="settings/rls" element={<ProtectedRoute permission="ADMIN_RLS_MANAGE"><RLSPage /></ProtectedRoute>} />
        <Route path="settings/audit" element={<ProtectedRoute permission="ADMIN_AUDIT_READ"><AuditPage /></ProtectedRoute>} />
        {/* Legacy routes - redirect to new paths */}
        <Route path="admin/users" element={<Navigate to="/settings/users" replace />} />
        <Route path="admin/roles" element={<Navigate to="/settings/roles" replace />} />
        <Route path="admin/rls" element={<Navigate to="/settings/rls" replace />} />
        <Route path="admin/audit" element={<Navigate to="/settings/audit" replace />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
