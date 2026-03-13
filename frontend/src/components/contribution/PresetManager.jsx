import React, { useState, useEffect } from 'react';
import { Plus, Edit2, Trash2, X, Save } from 'lucide-react';
import toast from 'react-hot-toast';
import { contributionAPI } from '../../services/api';

export default function PresetManager({ presets, setPresets, onRefresh }) {
  const [showModal, setShowModal] = useState(false);
  const [editingPreset, setEditingPreset] = useState(null);
  const [form, setForm] = useState({
    preset_name: '',
    preset_type: 'standard',
    description: '',
    config_json: '{}',
    sequence_order: 9999,
  });

  const handleAdd = () => {
    setEditingPreset(null);
    setForm({
      preset_name: '',
      preset_type: 'standard',
      description: '',
      config_json: '{}',
      sequence_order: 9999,
    });
    setShowModal(true);
  };

  const handleEdit = (preset) => {
    setEditingPreset(preset);
    setForm(preset);
    setShowModal(true);
  };

  const handleSave = async () => {
    try {
      // Validate JSON
      JSON.parse(form.config_json);
      
      if (!form.preset_name.trim()) {
        toast.error('Preset name is required');
        return;
      }

      if (editingPreset) {
        await contributionAPI.updatePreset(form.preset_name, form);
      } else {
        await contributionAPI.createPreset(form);
      }
      
      toast.success(editingPreset ? 'Preset updated' : 'Preset created');
      setShowModal(false);
      onRefresh && onRefresh();
    } catch (error) {
      if (error instanceof SyntaxError) {
        toast.error('Invalid JSON in configuration');
      } else {
        toast.error('Failed to save preset');
      }
    }
  };

  const handleDelete = async (presetName) => {
    if (!window.confirm(`Delete preset "${presetName}"?`)) return;
    
    try {
      await contributionAPI.deletePreset(presetName);
      toast.success('Preset deleted');
      onRefresh && onRefresh();
    } catch (error) {
      toast.error('Failed to delete preset');
    }
  };

  return (
    <div className="p-6 space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-semibold">Presets ({presets.length})</h3>
        <button onClick={handleAdd} className="btn-primary btn-sm">
          <Plus size={14} /> New Preset
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {presets.length === 0 ? (
          <p className="text-gray-400 col-span-2 text-center py-8">No presets created yet</p>
        ) : (
          presets.map((preset) => (
            <div key={preset.preset_name} className="border rounded-lg p-4 hover:bg-gray-50">
              <div className="flex justify-between items-start mb-2">
                <div>
                  <h4 className="font-semibold text-gray-900">{preset.preset_name}</h4>
                  <span className={`text-xs px-2 py-1 rounded mt-1 inline-block ${
                    preset.preset_type === 'formula' 
                      ? 'bg-purple-100 text-purple-700' 
                      : 'bg-blue-100 text-blue-700'
                  }`}>
                    {preset.preset_type}
                  </span>
                </div>
                <div className="flex gap-2">
                  <button onClick={() => handleEdit(preset)} className="p-1 hover:bg-gray-200 rounded">
                    <Edit2 size={14} />
                  </button>
                  <button onClick={() => handleDelete(preset.preset_name)} className="p-1 hover:bg-red-100 text-red-600 rounded">
                    <Trash2 size={14} />
                  </button>
                </div>
              </div>
              <p className="text-sm text-gray-600">{preset.description}</p>
              <p className="text-xs text-gray-400 mt-2">Seq: {preset.sequence_order}</p>
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
                {editingPreset ? 'Edit Preset' : 'Create Preset'}
              </h2>
              <button onClick={() => setShowModal(false)} className="p-1 hover:bg-gray-100 rounded">
                <X size={18} />
              </button>
            </div>

            <div className="p-6 space-y-4 max-h-96 overflow-y-auto">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="label">Preset Name*</label>
                  <input
                    type="text"
                    value={form.preset_name}
                    onChange={(e) => setForm({ ...form, preset_name: e.target.value })}
                    className="input"
                    placeholder="e.g., Q4_Analysis"
                    disabled={!!editingPreset}
                  />
                </div>
                <div>
                  <label className="label">Type*</label>
                  <select
                    value={form.preset_type}
                    onChange={(e) => setForm({ ...form, preset_type: e.target.value })}
                    className="input"
                  >
                    <option value="standard">Standard</option>
                    <option value="formula">Formula</option>
                  </select>
                </div>
              </div>

              <div>
                <label className="label">Description</label>
                <textarea
                  value={form.description}
                  onChange={(e) => setForm({ ...form, description: e.target.value })}
                  className="input"
                  rows="2"
                  placeholder="Describe this preset..."
                />
              </div>

              <div>
                <label className="label">Sequence Order</label>
                <input
                  type="number"
                  value={form.sequence_order}
                  onChange={(e) => setForm({ ...form, sequence_order: parseInt(e.target.value) })}
                  className="input"
                  min="0"
                />
              </div>

              <div>
                <label className="label">Configuration (JSON)*</label>
                <textarea
                  value={form.config_json}
                  onChange={(e) => setForm({ ...form, config_json: e.target.value })}
                  className="font-mono text-xs input"
                  rows="6"
                  placeholder='{"filters": {}, "groupby": "ST_CD"}'
                />
                <p className="text-xs text-gray-500 mt-1">Enter valid JSON configuration</p>
              </div>
            </div>

            <div className="flex gap-3 px-6 py-4 border-t bg-gray-50">
              <button onClick={() => setShowModal(false)} className="btn-secondary flex-1">
                Cancel
              </button>
              <button onClick={handleSave} className="btn-primary flex-1">
                <Save size={14} /> Save Preset
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
