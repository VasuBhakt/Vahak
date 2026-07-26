import { useState, useEffect } from 'react';
import { Activity, Plus, Settings2, Trash2, X, Copy, ArrowLeft, RefreshCw, Play } from 'lucide-react';

interface Endpoint {
  id: string;
  name: string;
  target_url: string;
  transformer_script: string;
  created_at: string;
}

interface RequestLog {
  id: string;
  endpoint_id: string;
  method: string;
  source_ip: string;
  body: string;
  received_at: string;
}

function App() {
  const [apiKey, setApiKey] = useState(localStorage.getItem('vahak_api_key') || '');
  const [isAuthed, setIsAuthed] = useState(!!localStorage.getItem('vahak_api_key'));
  const [endpoints, setEndpoints] = useState<Endpoint[]>([]);
  
  // Modal State
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [formData, setFormData] = useState({ name: '', target_url: '', transformer_script: '' });

  // Detail View State
  const [selectedEndpoint, setSelectedEndpoint] = useState<Endpoint | null>(null);
  const [requests, setRequests] = useState<RequestLog[]>([]);
  const [expandedRequestId, setExpandedRequestId] = useState<string | null>(null);

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

  const fetchRequests = async (id: string) => {
    try {
      const res = await fetch(`/api/endpoints/${id}/requests`, {
        headers: { 'X-Api-Key': apiKey }
      });
      const data = await res.json();
      setRequests(data || []);
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    if (isAuthed) {
      fetchEndpoints();
    }
  }, [isAuthed]);

  useEffect(() => {
    if (selectedEndpoint) {
      fetchRequests(selectedEndpoint.id);
    }
  }, [selectedEndpoint]);

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

  const openEditModal = (ep: Endpoint, e: React.MouseEvent) => {
    e.stopPropagation();
    setEditingId(ep.id);
    setFormData({ name: ep.name, target_url: ep.target_url, transformer_script: ep.transformer_script });
    setIsModalOpen(true);
  };

  const handleDelete = async (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!confirm('Are you sure you want to delete this endpoint?')) return;
    try {
      await fetch(`/api/endpoints/${id}`, {
        method: 'DELETE',
        headers: { 'X-Api-Key': apiKey }
      });
      if (selectedEndpoint?.id === id) setSelectedEndpoint(null);
      fetchEndpoints();
    } catch (err) {
      console.error(err);
    }
  };

  const copyWebhookUrl = (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    const url = `http://localhost:8080/hooks/${id}`;
    navigator.clipboard.writeText(url);
    const btn = document.getElementById(`copy-btn-${id}`);
    if (btn) {
      const originalColor = btn.style.color;
      btn.style.color = 'var(--success)';
      setTimeout(() => btn.style.color = originalColor, 1000);
    }
  };

  const handleReplay = async (requestId: string, endpointId: string) => {
    try {
      await fetch(`/api/endpoints/${endpointId}/replay/${requestId}`, {
        method: 'POST',
        headers: { 'X-Api-Key': apiKey }
      });
      alert('Replay queued successfully!');
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
          {!selectedEndpoint && (
            <button onClick={openNewModal} className="btn-primary" style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 16px', background: 'transparent', border: '1px solid var(--border-color)', color: 'var(--text-primary)' }}>
              <Plus size={16} /> New Endpoint
            </button>
          )}
          <button onClick={handleLogout} style={{ background: 'transparent', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 14 }}>
            Sign Out
          </button>
        </div>
      </header>

      {selectedEndpoint ? (
        <div className="detail-view" style={{ animation: 'fadeIn 0.2s ease-in' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 24 }}>
            <button onClick={() => setSelectedEndpoint(null)} style={{ background: 'transparent', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', padding: 4 }}>
              <ArrowLeft size={20} />
            </button>
            <h2 style={{ fontSize: 20, margin: 0 }}>{selectedEndpoint.name} <span style={{ fontSize: 12, color: 'var(--text-secondary)', marginLeft: 8, fontFamily: 'monospace' }}>{selectedEndpoint.id}</span></h2>
            <button onClick={() => fetchRequests(selectedEndpoint.id)} style={{ marginLeft: 'auto', background: 'transparent', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6, fontSize: 13 }}>
              <RefreshCw size={14} /> Refresh Logs
            </button>
          </div>

          <div style={{ background: 'var(--bg-surface)', border: '1px solid var(--border-color)', borderRadius: 8, overflow: 'hidden' }}>
            {requests.length === 0 ? (
              <div style={{ padding: 32, textAlign: 'center', color: 'var(--text-secondary)' }}>No webhooks received yet.</div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column' }}>
                <div style={{ display: 'grid', gridTemplateColumns: '150px 100px 150px 1fr 100px', padding: '12px 16px', borderBottom: '1px solid var(--border-color)', fontSize: 12, color: 'var(--text-secondary)', fontWeight: 600 }}>
                  <span>Time</span>
                  <span>Method</span>
                  <span>Source IP</span>
                  <span>Payload Preview</span>
                  <span style={{ textAlign: 'right' }}>Actions</span>
                </div>
                {requests.map(req => (
                  <div key={req.id} style={{ display: 'flex', flexDirection: 'column', borderBottom: '1px solid var(--border-color)' }}>
                    <div 
                      style={{ display: 'grid', gridTemplateColumns: '150px 100px 150px 1fr 100px', padding: '12px 16px', alignItems: 'center', fontSize: 13, cursor: 'pointer' }}
                      onClick={() => setExpandedRequestId(expandedRequestId === req.id ? null : req.id)}
                    >
                      <span style={{ color: 'var(--text-secondary)' }}>{new Date(req.received_at).toLocaleTimeString()}</span>
                      <span style={{ fontWeight: 600, color: 'var(--accent)' }}>{req.method}</span>
                      <span style={{ fontFamily: 'monospace', color: 'var(--text-secondary)' }}>{req.source_ip}</span>
                      <span style={{ fontFamily: 'monospace', color: 'var(--text-secondary)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', paddingRight: 16 }}>
                        {req.body}
                      </span>
                      <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                        <button 
                          onClick={(e) => { e.stopPropagation(); handleReplay(req.id, selectedEndpoint.id); }} 
                          style={{ background: 'var(--bg-base)', border: '1px solid var(--border-color)', color: 'var(--text-primary)', padding: '4px 8px', borderRadius: 4, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4, fontSize: 11 }}
                          title="Replay Webhook"
                        >
                          <Play size={10} /> Replay
                        </button>
                      </div>
                    </div>
                    {expandedRequestId === req.id && (
                      <div style={{ padding: '16px', background: 'var(--bg-base)', borderTop: '1px dashed var(--border-color)' }}>
                        <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginBottom: 8, textTransform: 'uppercase', letterSpacing: 1 }}>Raw JSON Payload</div>
                        <pre style={{ margin: 0, padding: 12, background: 'rgba(0,0,0,0.3)', borderRadius: 4, fontSize: 12, color: 'var(--text-primary)', overflowX: 'auto', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                          {req.body}
                        </pre>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="endpoint-grid">
          {endpoints.map((ep) => (
            <div key={ep.id} className="endpoint-card" onClick={() => setSelectedEndpoint(ep)} style={{ cursor: 'pointer', transition: 'border-color 0.2s' }} onMouseEnter={(e) => e.currentTarget.style.borderColor = 'var(--text-secondary)'} onMouseLeave={(e) => e.currentTarget.style.borderColor = 'var(--border-color)'}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <h3><span className="status-indicator"></span> {ep.name}</h3>
              </div>
              <p style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-secondary)', marginTop: 4 }}>ID: {ep.id}</p>
              <p style={{ marginTop: 12 }}>{ep.target_url}</p>
              
              <div style={{ marginTop: 24, display: 'flex', gap: 16, justifyContent: 'flex-end', borderTop: '1px solid var(--border-color)', paddingTop: 16 }}>
                <button id={`copy-btn-${ep.id}`} onClick={(e) => copyWebhookUrl(ep.id, e)} style={{ background: 'transparent', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', padding: 4, marginRight: 'auto', transition: 'color 0.2s' }} title="Copy Webhook URL">
                  <Copy size={16} />
                </button>
                <button onClick={(e) => openEditModal(ep, e)} style={{ background: 'transparent', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', padding: 4 }} title="Settings">
                  <Settings2 size={16} />
                </button>
                <button onClick={(e) => handleDelete(ep.id, e)} style={{ background: 'transparent', border: 'none', color: 'var(--danger)', cursor: 'pointer', padding: 4 }} title="Delete">
                  <Trash2 size={16} />
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

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
