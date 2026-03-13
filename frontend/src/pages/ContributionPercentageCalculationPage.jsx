import React, { useState, useEffect } from 'react';
import { Settings, Zap, FileUp, FileDown, BarChart3, ChevronDown } from 'lucide-react';
import toast from 'react-hot-toast';
import { contributionAPI } from '../services/api';
import PresetManager from '../components/contribution/PresetManager';
import MappingManager from '../components/contribution/MappingManager';
import AssignmentManager from '../components/contribution/AssignmentManager';
import ExecutionPanel from '../components/contribution/ExecutionPanel';
import ReviewExport from '../components/contribution/ReviewExport';


export default function ContributionPercentageCalculationPage() {
  const [activeTab, setActiveTab] = useState(0);
  const [presets, setPresets] = useState([]);
  const [mappings, setMappings] = useState([]);
  const [assignments, setAssignments] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    setLoading(true);
    try {
      const [presetsRes, mappingsRes, assignmentsRes] = await Promise.all([
        contributionAPI.listPresets().catch(e => {
          console.error('Presets error:', e);
          return { data: [] };
        }),
        contributionAPI.listMappings().catch(e => {
          console.error('Mappings error:', e);
          return { data: [] };
        }),
        contributionAPI.listAssignments().catch(e => {
          console.error('Assignments error:', e);
          return { data: [] };
        })
      ]);
      
      setPresets(presetsRes.data || []);
      setMappings(mappingsRes.data || []);
      setAssignments(assignmentsRes.data || []);
    } catch (error) {
      console.error('Failed to load configuration:', error);
      toast.error(
        'Failed to load configuration. ' +
        'Please ensure the database tables are created. ' +
        'See CONTRIBUTION_API_SETUP.md for help.'
      );
    } finally {
      setLoading(false);
    }
  };

  const tabs = [
    { label: '⚙️ Presets', icon: Settings, id: 0 },
    { label: '🔗 Mappings', icon: FileUp, id: 1 },
    { label: '📋 Assignments', icon: BarChart3, id: 2 },
    { label: '🚀 Execute', icon: Zap, id: 3 },
    { label: '📊 Review & Export', icon: FileDown, id: 4 },
  ];

  return (
    <div className="space-y-5">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Contribution Percentage Analysis</h1>
        <p className="text-gray-500 text-sm mt-0.5">Analyze stock contribution percentages with KPI calculations (v1.3.0)</p>
      </div>

      {/* Tab Navigation */}
      <div className="card border-b-0 rounded-b-none">
        <div className="flex gap-1 px-6 overflow-x-auto">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
                activeTab === tab.id
                  ? 'border-primary-600 text-primary-600'
                  : 'border-transparent text-gray-600 hover:text-gray-900'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Tab Content */}
      <div className="card rounded-t-none">
        {activeTab === 0 && <PresetManager presets={presets} setPresets={setPresets} onRefresh={loadInitialData} />}
        {activeTab === 1 && <MappingManager mappings={mappings} setMappings={setMappings} onRefresh={loadInitialData} />}
        {activeTab === 2 && <AssignmentManager mappings={mappings} assignments={assignments} setAssignments={setAssignments} onRefresh={loadInitialData} />}
        {activeTab === 3 && <ExecutionPanel presets={presets} />}
        {activeTab === 4 && <ReviewExport />}
      </div>
    </div>
  );
}
