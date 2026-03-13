import { Routes, Route, Navigate } from 'react-router-dom'
import { useEffect } from 'react'
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
import ContributionPercentageCalculationPage from '@/pages/ContributionPercentageCalculationPage'
import JobsDashboardPage from '@/pages/JobsDashboardPage'
// Duplicate imports removed

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
        {/* Data Preparation */}
        <Route path="msa" element={<MSAStockCalculationPage />} />
        <Route path="contribution" element={<ContributionPercentageCalculationPage />} />
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
