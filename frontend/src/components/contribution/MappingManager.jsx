import React, { useState } from 'react';
import { Plus, Edit2, Trash2, X, Save } from 'lucide-react';
import toast from 'react-hot-toast';
import { contributionAPI } from '../../services/api';

export default function MappingManager({ mappings, setMappings, onRefresh }) {
  const [showModal, setShowModal] = useState(false);
  const [editingMapping, setEditingMapping] = useState(null);
  const [form, setForm] = useState({
    mapping_name: '',
    mapping_json: '{"suffix_mapping": {}}',
    fallback_json: '{"default": ""}',
    description: '',
  });

  const handleAdd = () => {
    setEditingMapping(null);
    setForm({
      mapping_name: '',
      mapping_json: '{"suffix_mapping": {}}',
      fallback_json: '{"default": ""}',
      description: '',
    });
    setShowModal(true);
  };

  const handleEdit = (mapping) => {
    setEditingMapping(mapping);
    setForm(mapping);
    setShowModal(true);
  };

  const handleSave = async () => {
    try {
      // Validate JSON
      JSON.parse(form.mapping_json);
      JSON.parse(form.fallback_json);

      if (!form.mapping_name.trim()) {
        toast.error('Mapping name is required');
        return;
      }

      if (editingMapping) {
        await contributionAPI.updateMapping(form.mapping_name, form);
      } else {
        await contributionAPI.createMapping(form);
      }

      toast.success(editingMapping ? 'Mapping updated' : 'Mapping created');
      setShowModal(false);
      onRefresh && onRefresh();
    } catch (error) {
      if (error instanceof SyntaxError) {
        toast.error('Invalid JSON in mapping data');
      } else {
        toast.error('Failed to save mapping');
      }
    }
  };

  const handleDelete = async (mappingName) => {
    if (!window.confirm(`Delete mapping "${mappingName}"?`)) return;

    try {
      await contributionAPI.deleteMapping(mappingName);
      toast.success('Mapping deleted');
      onRefresh && onRefresh();
    } catch (error) {
      toast.error('Failed to delete mapping');
    }
  };

  return (
    <div className="p-6 space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-semibold">Suffix Mappings ({mappings.length})</h3>
        <button onClick={handleAdd} className="btn-primary btn-sm">
          <Plus size={14} /> New Mapping
        </button>
      </div>

      <div className="space-y-3">
        {mappings.length === 0 ? (
          <p className="text-gray-400 text-center py-8">No mappings created yet</p>
        ) : (
          mappings.map((mapping) => (
            <div key={mapping.mapping_name} className="border rounded-lg p-4 hover:bg-gray-50">
              <div className="flex justify-between items-start">
                <div className="flex-1">
                  <h4 className="font-semibold text-gray-900">{mapping.mapping_name}</h4>
                  <p className="text-sm text-gray-600 mt-1">{mapping.description}</p>
                  <div className="mt-2 grid grid-cols-2 gap-4 text-xs">
                    <div>
                      <span className="font-semibold text-gray-700">Mappings:</span>
                      <pre className="bg-gray-50 p-2 mt-1 rounded overflow-x-auto text-xs">
                        {JSON.stringify(JSON.parse(mapping.mapping_json), null, 2)}
                      </pre>
                    </div>
                    <div>
                      <span className="font-semibold text-gray-700">Fallback:</span>
                      <pre className="bg-gray-50 p-2 mt-1 rounded overflow-x-auto text-xs">
                        {JSON.stringify(JSON.parse(mapping.fallback_json), null, 2)}
                      </pre>
                    </div>
                  </div>
                </div>
                <div className="flex gap-2 ml-4">
                  <button onClick={() => handleEdit(mapping)} className="p-1 hover:bg-gray-200 rounded">
                    <Edit2 size={14} />
                  </button>
                  <button onClick={() => handleDelete(mapping.mapping_name)} className="p-1 hover:bg-red-100 text-red-600 rounded">
                    <Trash2 size={14} />
                  </button>
                </div>
              </div>
            </div>
          ))
        )}
      </div>

      {/* Modal */}
      {showModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white rounded-xl shadow-xl w-full max-w-2xl m-4">
            <div className="flex items-center justify-between px-6 py-4 border-b">
              <h2 className="text-lg font-semibold">
                {editingMapping ? 'Edit Mapping' : 'Create Mapping'}
              </h2>
              <button onClick={() => setShowModal(false)} className="p-1 hover:bg-gray-100 rounded">
                <X size={18} />
              </button>
            </div>

            <div className="p-6 space-y-4 max-h-96 overflow-y-auto">
              <div>
                <label className="label">Mapping Name*</label>
                <input
                  type="text"
                  value={form.mapping_name}
                  onChange={(e) => setForm({ ...form, mapping_name: e.target.value })}
                  className="input"
                  placeholder="e.g., SKU_Mapping"
                  disabled={!!editingMapping}
                />
              </div>

              <div>
                <label className="label">Description</label>
                <textarea
                  value={form.description}
                  onChange={(e) => setForm({ ...form, description: e.target.value })}
                  className="input"
                  rows="2"
                  placeholder="Describe this mapping..."
                />
              </div>

              <div>
                <label className="label">Suffix Mappings (JSON)*</label>
                <textarea
                  value={form.mapping_json}
                  onChange={(e) => setForm({ ...form, mapping_json: e.target.value })}
                  className="font-mono text-xs input"
                  rows="5"
                  placeholder='{"suffix_mapping": {"SKU_001": "PROD_A"}}'
                />
              </div>

              <div>
                <label className="label">Fallback Values (JSON)</label>
                <textarea
                  value={form.fallback_json}
                  onChange={(e) => setForm({ ...form, fallback_json: e.target.value })}
                  className="font-mono text-xs input"
                  rows="3"
                  placeholder='{"default": "DEFAULT_VALUE"}'
                />
              </div>
            </div>

            <div className="flex gap-3 px-6 py-4 border-t bg-gray-50">
              <button onClick={() => setShowModal(false)} className="btn-secondary flex-1">
                Cancel
              </button>
              <button onClick={handleSave} className="btn-primary flex-1">
                <Save size={14} /> Save Mapping
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
