import React, { useState } from 'react';
import { Plus, Edit2, Trash2, X, Save } from 'lucide-react';
import toast from 'react-hot-toast';
import { contributionAPI } from '../../services/api';

export default function AssignmentManager({ mappings, assignments, setAssignments, onRefresh }) {
  const [showModal, setShowModal] = useState(false);
  const [editingAssignment, setEditingAssignment] = useState(null);
  const [form, setForm] = useState({
    col_name: '',
    mapping_name: '',
    prefix: '',
    target: 'Both',
  });

  const handleAdd = () => {
    setEditingAssignment(null);
    setForm({
      col_name: '',
      mapping_name: '',
      prefix: '',
      target: 'Both',
    });
    setShowModal(true);
  };

  const handleEdit = (assignment) => {
    setEditingAssignment(assignment);
    setForm(assignment);
    setShowModal(true);
  };

  const handleSave = async () => {
    try {
      if (!form.col_name.trim() || !form.mapping_name) {
        toast.error('Column name and mapping are required');
        return;
      }

      if (editingAssignment) {
        await contributionAPI.updateAssignment(editingAssignment.id, form);
      } else {
        await contributionAPI.createAssignment(form);
      }

      toast.success(editingAssignment ? 'Assignment updated' : 'Assignment created');
      setShowModal(false);
      onRefresh && onRefresh();
    } catch (error) {
      toast.error('Failed to save assignment');
    }
  };

  const handleDelete = async (assignmentId) => {
    if (!window.confirm('Delete this assignment?')) return;

    try {
      await contributionAPI.deleteAssignment(assignmentId);
      toast.success('Assignment deleted');
      onRefresh && onRefresh();
    } catch (error) {
      toast.error('Failed to delete assignment');
    }
  };

  return (
    <div className="p-6 space-y-4">
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-lg font-semibold">Mapping Assignments ({assignments.length})</h3>
        <button onClick={handleAdd} className="btn-primary btn-sm">
          <Plus size={14} /> New Assignment
        </button>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b">
              <th className="px-4 py-3 text-left font-semibold">Column</th>
              <th className="px-4 py-3 text-left font-semibold">Mapping</th>
              <th className="px-4 py-3 text-left font-semibold">Prefix</th>
              <th className="px-4 py-3 text-left font-semibold">Target</th>
              <th className="px-4 py-3 text-left font-semibold">Actions</th>
            </tr>
          </thead>
          <tbody>
            {assignments.length === 0 ? (
              <tr>
                <td colSpan="5" className="px-4 py-8 text-center text-gray-400">
                  No assignments created yet
                </td>
              </tr>
            ) : (
              assignments.map((assignment) => (
                <tr key={assignment.id} className="border-b hover:bg-gray-50">
                  <td className="px-4 py-3 font-medium">{assignment.col_name}</td>
                  <td className="px-4 py-3">
                    <span className="badge-primary">{assignment.mapping_name}</span>
                  </td>
                  <td className="px-4 py-3 text-gray-600">{assignment.prefix || '-'}</td>
                  <td className="px-4 py-3">
                    <span className={`px-2 py-1 rounded text-xs font-medium ${
                      assignment.target === 'Both' ? 'bg-blue-100 text-blue-700' :
                      assignment.target === 'Store' ? 'bg-green-100 text-green-700' :
                      'bg-purple-100 text-purple-700'
                    }`}>
                      {assignment.target}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex gap-2">
                      <button onClick={() => handleEdit(assignment)} className="p-1 hover:bg-gray-200 rounded">
                        <Edit2 size={14} />
                      </button>
                      <button onClick={() => handleDelete(assignment.id)} className="p-1 hover:bg-red-100 text-red-600 rounded">
                        <Trash2 size={14} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Modal */}
      {showModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white rounded-xl shadow-xl w-full max-w-xl m-4">
            <div className="flex items-center justify-between px-6 py-4 border-b">
              <h2 className="text-lg font-semibold">
                {editingAssignment ? 'Edit Assignment' : 'Create Assignment'}
              </h2>
              <button onClick={() => setShowModal(false)} className="p-1 hover:bg-gray-100 rounded">
                <X size={18} />
              </button>
            </div>

            <div className="p-6 space-y-4">
              <div>
                <label className="label">Column Name*</label>
                <input
                  type="text"
                  value={form.col_name}
                  onChange={(e) => setForm({ ...form, col_name: e.target.value })}
                  className="input"
                  placeholder="e.g., SKU"
                />
              </div>

              <div>
                <label className="label">Mapping*</label>
                <select
                  value={form.mapping_name}
                  onChange={(e) => setForm({ ...form, mapping_name: e.target.value })}
                  className="input"
                >
                  <option value="">Select a mapping...</option>
                  {mappings.map((m) => (
                    <option key={m.mapping_name} value={m.mapping_name}>
                      {m.mapping_name}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="label">Prefix (Optional)</label>
                <input
                  type="text"
                  value={form.prefix}
                  onChange={(e) => setForm({ ...form, prefix: e.target.value })}
                  className="input"
                  placeholder="e.g., PRE_"
                />
              </div>

              <div>
                <label className="label">Target</label>
                <select
                  value={form.target}
                  onChange={(e) => setForm({ ...form, target: e.target.value })}
                  className="input"
                >
                  <option value="Both">Both (Store & Company)</option>
                  <option value="Store">Store Only</option>
                  <option value="Company">Company Only</option>
                </select>
              </div>
            </div>

            <div className="flex gap-3 px-6 py-4 border-t bg-gray-50">
              <button onClick={() => setShowModal(false)} className="btn-secondary flex-1">
                Cancel
              </button>
              <button onClick={handleSave} className="btn-primary flex-1">
                <Save size={14} /> Save Assignment
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
