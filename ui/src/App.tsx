import { useState, useEffect } from 'react';
import { Activity, Plus, Settings2, Trash2, X } from 'lucide-react';

interface Endpoint {
  id: string;
  name: string;
  target_url: string;
  transformer_script: string;
  created_at: string;
}

function App() {
  const [apiKey, setApiKey] = useState(localStorage.getItem('vahak_api_key') || '');
  const [isAuthed, setIsAuthed] = useState(!!localStorage.getItem('vahak_api_key'));
  const [endpoints, setEndpoints] = useState<Endpoint[]>([]);
  
  // Modal State
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [formData, setFormData] = useState({ name: '', target_url: '', transformer_script: '' });

  const fetchEndpoints = async () => {
    try {
      const res = await fetch('/api/endpoints', {
        headers: { 'X-Api-Key': apiKey }
      });
      if (res.status === 401) {
        handleLogout();
        return;
      }
      const data = await res.json();
      setEndpoints(data || []);
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    if (isAuthed) {
      fetchEndpoints();
    }
  }, [isAuthed]);

  const handleLogin = (e: React.FormEvent) => {
    e.preventDefault();
    if (apiKey.trim()) {
      localStorage.setItem('vahak_api_key', apiKey.trim());
      setIsAuthed(true);
    }
  };

  const handleLogout = () => {
    localStorage.removeItem('vahak_api_key');
    setApiKey('');
    setIsAuthed(false);
  };

  const openNewModal = () => {
    setEditingId(null);
    setFormData({ name: '', target_url: '', transformer_script: '' });
    setIsModalOpen(true);
  };

  const openEditModal = (ep: Endpoint) => {
    setEditingId(ep.id);
    setFormData({ name: ep.name, target_url: ep.target_url, transformer_script: ep.transformer_script });
    setIsModalOpen(true);
  };

  const handleDelete = async (id: string) => {
    if (!confirm('Are you sure you want to delete this endpoint?')) return;
    try {
      await fetch(`/api/endpoints/${id}`, {
        method: 'DELETE',
        headers: { 'X-Api-Key': apiKey }
      });
      fetchEndpoints();
    } catch (err) {
      console.error(err);
    }
  };

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      if (editingId) {
        await fetch(`/api/endpoints/${editingId}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json', 'X-Api-Key': apiKey },
          body: JSON.stringify(formData)
        });
      } else {
        await fetch('/api/endpoints', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Api-Key': apiKey },
          body: JSON.stringify(formData)
        });
      }
      setIsModalOpen(false);
      fetchEndpoints();
    } catch (err) {
      console.error(err);
    }
  };

  if (!isAuthed) {
    return (
      <div className="app-container auth-container">
        <div className="auth-card">
          <Activity size={32} style={{ marginBottom: 16 }} />
          <h2>Vahak Engine</h2>
          <p>High-performance webhook delivery gateway</p>
          <form onSubmit={handleLogin}>
            <input 
              type="password" 
              placeholder="API Key" 
              className="input-field" 
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              autoFocus
            />
            <button type="submit" className="btn-primary">Authenticate</button>
          </form>
        </div>
      </div>
    );
  }

  return (
    <div className="app-container">
      <header className="header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <Activity size={24} />
          <h1>Vahak</h1>
        </div>
        <div style={{ display: 'flex', gap: 16, alignItems: 'center' }}>
          <button onClick={openNewModal} className="btn-primary" style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 16px', background: 'transparent', border: '1px solid var(--border-color)', color: 'var(--text-primary)' }}>
            <Plus size={16} /> New Endpoint
          </button>
          <button onClick={handleLogout} style={{ background: 'transparent', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 14 }}>
            Sign Out
          </button>
        </div>
      </header>

      <div className="endpoint-grid">
        {endpoints.map((ep) => (
          <div key={ep.id} className="endpoint-card">
            <h3><span className="status-indicator"></span> {ep.name}</h3>
            <p style={{ marginTop: 8 }}>{ep.target_url}</p>
            
            <div style={{ marginTop: 24, display: 'flex', gap: 16, justifyContent: 'flex-end', borderTop: '1px solid var(--border-color)', paddingTop: 16 }}>
              <button onClick={() => openEditModal(ep)} style={{ background: 'transparent', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', padding: 4 }}>
                <Settings2 size={16} />
              </button>
              <button onClick={() => handleDelete(ep.id)} style={{ background: 'transparent', border: 'none', color: 'var(--danger)', cursor: 'pointer', padding: 4 }}>
                <Trash2 size={16} />
              </button>
            </div>
          </div>
        ))}
      </div>

      {isModalOpen && (
        <div className="modal-overlay">
          <div className="modal-content">
            <div className="modal-header">
              <h2>{editingId ? 'Edit Endpoint' : 'New Endpoint'}</h2>
              <button onClick={() => setIsModalOpen(false)} className="modal-close"><X size={20} /></button>
            </div>
            <form onSubmit={handleSave}>
              <div className="form-group">
                <label>Endpoint Name</label>
                <input required type="text" className="input-field" value={formData.name} onChange={e => setFormData({...formData, name: e.target.value})} placeholder="e.g. Stripe Webhooks" />
              </div>
              <div className="form-group">
                <label>Target URL</label>
                <input required type="url" className="input-field" value={formData.target_url} onChange={e => setFormData({...formData, target_url: e.target.value})} placeholder="https://api.yourdomain.com/webhook" />
              </div>
              <div className="form-group">
                <label>Transformer Script (JS)</label>
                <textarea className="textarea-field" value={formData.transformer_script} onChange={e => setFormData({...formData, transformer_script: e.target.value})} placeholder="// Optional: Transform the payload before delivery"></textarea>
              </div>
              <div className="modal-footer">
                <button type="button" onClick={() => setIsModalOpen(false)} className="btn-secondary">Cancel</button>
                <button type="submit" className="btn-primary" style={{ width: 'auto' }}>Save Endpoint</button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
