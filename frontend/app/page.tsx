'use client';

import React, { useState, useEffect } from 'react';

interface IngestionJob {
  id: string; 
  callId: string;
  geminiStatus: 'QUEUED' | 'PROCESSING' | 'COMPLETED' | 'FAULTED';
  outboundState: 'PENDING' | 'TRIGGERED' | 'SIMULATED' | 'BLOCKED' | 'SKIPPED';
  timestamp: string;
  rawTranscript?: string;
  aiAnalysisPass?: string;
}

interface JobDetail {
  id: string;
  callId: string;
  status: string;
  aiAnalysisPass: string; 
  createdAt: string;
}

interface DBJobPayload {
  id: string;
  callId: string;
  status: 'QUEUED' | 'PROCESSING' | 'COMPLETED' | 'FAULTED';
  outboundTriggered: boolean;
  createdAt: string;
  rawTranscript?: string;
  aiAnalysisPass?: string;
}

export default function LiveIngestionStream() {
  const [jobs, setJobs] = useState<IngestionJob[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const [searchQuery, setSearchQuery] = useState<string>('');
  const [statusFilter, setStatusFilter] = useState<'ALL' | 'PROCESSING' | 'COMPLETED' | 'FAULTED'>('ALL');

  const [simCallId, setSimCallId] = useState('');
  const [simTranscript, setSimTranscript] = useState('');
  const [simLoading, setSimLoading] = useState(false);
  const [simStatus, setSimStatus] = useState<{ type: 'idle' | 'success' | 'error'; message: string }>({ type: 'idle', message: '' });

  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [jobDetail, setJobDetail] = useState<JobDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState<boolean>(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchPipelineData() {
      try {
        const response = await fetch('http://localhost:3000/api/v1/jobs');
        if (!response.ok) {
          throw new Error(`Server returned status: ${response.status}`);
        }
        const data = await response.json();
        
        const formattedJobs = data.map((item: DBJobPayload) => ({
          id: String(item.id), 
          callId: item.callId || 'N/A',
          geminiStatus: (item.status || 'COMPLETED') as IngestionJob['geminiStatus'],
          outboundState: (item.outboundTriggered ? 'TRIGGERED' : 'SIMULATED') as IngestionJob['outboundState'],
          timestamp: item.createdAt ? new Date(item.createdAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : 'N/A',
          rawTranscript: item.rawTranscript || '',
          aiAnalysisPass: item.aiAnalysisPass || ''
        }));
        
        setJobs(formattedJobs);
        setError(null);
      } catch (err: unknown) {
        setError(err instanceof Error ? err.message : 'Failed to establish connection to backend API node.');
      } finally {
        setLoading(false);
      }
    }

    fetchPipelineData();

    // Establish Server-Sent Events (SSE) connection for real-time updates
    const eventSource = new EventSource('http://localhost:3000/api/v1/jobs/stream');

    eventSource.onopen = () => {
      console.log('SSE connection successfully opened');
      setError(null);
    };

    eventSource.onmessage = (event) => {
      try {
        const updatedJob = JSON.parse(event.data);
        const formattedJob = {
          id: String(updatedJob.id),
          callId: updatedJob.callId || 'N/A',
          geminiStatus: (updatedJob.status || 'COMPLETED') as IngestionJob['geminiStatus'],
          outboundState: (updatedJob.outboundTriggered ? 'TRIGGERED' : 'SIMULATED') as IngestionJob['outboundState'],
          timestamp: updatedJob.createdAt ? new Date(updatedJob.createdAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : 'N/A',
          rawTranscript: updatedJob.rawTranscript || '',
          aiAnalysisPass: updatedJob.aiAnalysisPass || ''
        };

        setJobs((prevJobs) => {
          const index = prevJobs.findIndex((j) => j.id === formattedJob.id);
          if (index !== -1) {
            // Update existing job in-place
            const newJobs = [...prevJobs];
            newJobs[index] = formattedJob;
            return newJobs;
          } else {
            // Prepend new job to the stream
            return [formattedJob, ...prevJobs];
          }
        });
      } catch (err) {
        console.error('Error parsing SSE event data:', err);
      }
    };

    eventSource.onerror = (err) => {
      console.error('SSE connection error:', err);
      setError('Live updates stream connection lost. Attempting to reconnect...');
    };

    return () => {
      eventSource.close();
    };
  }, []);

  const handleOpenDetails = async (id: string) => {
    setSelectedJobId(id);
    setDetailLoading(true);
    setDetailError(null);
    setJobDetail(null);

    try {
      const response = await fetch(`http://localhost:3000/api/v1/jobs/${id}`);
      if (!response.ok) {
        throw new Error(`Failed to load details. Server code: ${response.status}`);
      }
      const data = await response.json();
      setJobDetail(data);
    } catch (err: unknown) {
      setDetailError(err instanceof Error ? err.message : 'Could not reach backend lookup gateway.');
    } finally {
      setDetailLoading(false);
    }
  };

  const handleCloseDetails = () => {
    setSelectedJobId(null);
    setJobDetail(null);
  };

  const handleRetryJob = async (id: string) => {
    try {
      const response = await fetch(`http://localhost:3000/api/v1/jobs/${id}/retry`, {
        method: 'POST',
      });
      if (response.ok) {
        handleCloseDetails();
      } else {
        const data = await response.json();
        alert(`Retry failed: ${data.message || 'Unknown error'}`);
      }
    } catch (err: unknown) {
      alert(`Network error: ${err instanceof Error ? err.message : 'Failed to connect to backend'}`);
    }
  };

  const filteredJobs = jobs.filter((job) => {
    if (statusFilter !== 'ALL' && job.geminiStatus !== statusFilter) {
      return false;
    }
    if (searchQuery.trim() !== '') {
      const query = searchQuery.toLowerCase();
      const matchCallId = job.callId.toLowerCase().includes(query);
      const matchTranscript = (job.rawTranscript || '').toLowerCase().includes(query);
      const matchAnalysis = (job.aiAnalysisPass || '').toLowerCase().includes(query);
      return matchCallId || matchTranscript || matchAnalysis;
    }
    return true;
  });

  const handlePipelineInjection = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!simCallId || !simTranscript) {
      setSimStatus({ type: 'error', message: 'INSUFFICIENT DATA FIELDS: Please specify both Core Key and Transcript.' });
      return;
    }

    setSimLoading(true);
    setSimStatus({ type: 'idle', message: '' });

    try {
      const response = await fetch('http://localhost:3000/api/v1/webhooks/gong', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callId: simCallId, rawTranscript: simTranscript }),
      });

      const data = await response.json();

      if (response.ok) {
        setSimStatus({ type: 'success', message: `TRANSACTION ACCEPTED // Job Enqueued: #${String(data.jobId).padStart(4, '0')}` });
        setSimCallId('');
        setSimTranscript('');
      } else {
        setSimStatus({ type: 'error', message: data.message || 'Pipeline ingestion handoff failure.' });
      }
    } catch {
      setSimStatus({ type: 'error', message: 'NETWORK REFUSAL: Unable to patch payload into port 3000.' });
    } finally {
      setSimLoading(false);
    }
  };

  const StatusBadge = ({ text }: { text: string }) => {
    const styles: Record<string, string> = {
      COMPLETED: 'bg-emerald-950/40 text-emerald-400 border-emerald-800/60',
      PROCESSING: 'bg-amber-950/40 text-amber-400 border-amber-800/60',
      FAULTED: 'bg-rose-950/40 text-rose-400 border-rose-800/60',
      QUEUED: 'bg-slate-800/40 text-slate-400 border-slate-700/60',
      TRIGGERED: 'bg-indigo-950/40 text-indigo-400 border-indigo-800/60',
      SIMULATED: 'bg-cyan-950/40 text-cyan-400 border-cyan-800/60',
      SKIPPED: 'bg-zinc-800/60 text-zinc-400 border-zinc-700/60',
      BLOCKED: 'bg-orange-950/40 text-orange-400 border-orange-800/60',
    };

    return (
      <span className={`px-2.5 py-0.5 text-xs font-mono tracking-wider font-semibold border rounded-sm ${styles[text] || styles.QUEUED}`}>
        {text}
      </span>
    );
  };

  // Dynamic stats calculation for Analytics & Aggregates Bar
  const totalIngested = jobs.length;
  const activeProcessingCount = jobs.filter((j) => j.geminiStatus === 'PROCESSING').length;
  const failureFaultedCount = jobs.filter((j) => j.geminiStatus === 'FAULTED').length;
  const successCompletedCount = jobs.filter((j) => j.geminiStatus === 'COMPLETED').length;
  const successRatePercentage = totalIngested > 0 ? Math.round((successCompletedCount / totalIngested) * 100) : 100;

  return (
    <div className="min-h-screen bg-[#0A0B0D] text-[#E4E6EB] p-8 antialiased selection:bg-indigo-500/30 relative overflow-x-hidden">
      
      {/* Top Header Section */}
      <header className="flex justify-between items-center border-b border-[#1F2229] pb-6 mb-8">
        <div>
          <div className="text-xs font-mono tracking-[0.3em] text-indigo-400 uppercase font-bold">Intelligence Layer</div>
          <h1 className="text-2xl font-light tracking-tight text-white mt-1">GTM CONTEXT ENGINE // Pipeline Node</h1>
        </div>
        <div className="flex items-center gap-2 px-3 py-1 bg-emerald-950/30 border border-emerald-900/50 rounded-sm">
          <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
          <span className="text-xs font-mono uppercase tracking-widest text-emerald-400 font-semibold">System Operational</span>
        </div>
      </header>

      {/* Analytics & Aggregates Bar */}
      <section className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        {[
          { 
            label: 'Total Transcripts Ingested', 
            value: loading ? '...' : String(totalIngested), 
            sub: 'PostgreSQL Commits' 
          },
          { 
            label: 'Success Rate', 
            value: loading ? '...' : `${successRatePercentage}%`, 
            sub: 'Completed Pipelines' 
          },
          { 
            label: 'Active Queue Count', 
            value: loading ? '...' : String(activeProcessingCount), 
            sub: 'PROCESSING State' 
          },
          { 
            label: 'Failure Count', 
            value: loading ? '...' : String(failureFaultedCount), 
            sub: 'FAULTED Objections' 
          }
        ].map((metric, idx) => (
          <div key={idx} className="bg-[#12141A] border border-[#1F2229] p-5 rounded-sm">
            <div className="text-xs font-mono tracking-wider text-[#737885] uppercase">{metric.label}</div>
            <div className="text-3xl font-light tracking-tight text-white mt-2 font-mono">{metric.value}</div>
            <div className="text-[10px] font-mono text-[#4F535E] uppercase tracking-widest mt-1">{metric.sub}</div>
          </div>
        ))}
      </section>

      {/* Pipeline Ingestion Simulator Panel Block */}
      <section className="bg-[#12141A] border border-[#1F2229] rounded-sm p-6 mb-8">
        <h2 className="text-xs font-mono tracking-widest uppercase text-white font-bold mb-4">
          Pipeline Ingestion Simulator
        </h2>
        <form onSubmit={handlePipelineInjection} className="space-y-4 font-mono text-xs">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
            <div className="md:col-span-1">
              <label className="block text-[#737885] uppercase tracking-wider mb-1.5 font-bold">Source Core Key</label>
              <input
                type="text"
                placeholder="e.g., call_test_700"
                value={simCallId}
                onChange={(e) => setSimCallId(e.target.value)}
                className="w-full bg-[#0E1015] border border-[#1F2229] rounded-sm px-3 py-2 text-[#E4E6EB] focus:outline-none focus:border-indigo-500/60 transition-colors placeholder-[#4F535E]"
              />
            </div>
            <div className="md:col-span-3">
              <label className="block text-[#737885] uppercase tracking-wider mb-1.5 font-bold">Raw Transcript Stream Text</label>
              <textarea
                placeholder="Paste structural client meeting logs or conversations directly into buffer..."
                value={simTranscript}
                onChange={(e) => setSimTranscript(e.target.value)}
                rows={2}
                className="w-full bg-[#0E1015] border border-[#1F2229] rounded-sm px-3 py-2 text-[#E4E6EB] focus:outline-none focus:border-indigo-500/60 transition-colors resize-none placeholder-[#4F535E]"
              />
            </div>
          </div>

          <div className="flex items-center justify-between pt-2 border-t border-[#1F2229]/50">
            <div className="h-4">
              {simStatus.type === 'success' && <p className="text-emerald-400 font-semibold">{simStatus.message}</p>}
              {simStatus.type === 'error' && <p className="text-rose-400 font-semibold">{simStatus.message}</p>}
            </div>
            <button
              type="submit"
              disabled={simLoading}
              className="bg-indigo-600 hover:bg-indigo-500 text-white font-semibold py-1.5 px-4 rounded-sm tracking-widest uppercase transition-colors disabled:opacity-30 disabled:hover:bg-indigo-600"
            >
              {simLoading ? 'Injecting Context...' : 'Inject into Pipeline'}
            </button>
          </div>
        </form>
      </section>

      {/* Primary Data Matrix Table */}
      <main className="bg-[#12141A] border border-[#1F2229] rounded-sm overflow-hidden">
        <div className="px-6 py-4 border-b border-[#1F2229] flex justify-between items-center bg-[#161920]">
          <div className="flex items-center gap-4">
            <h2 className="text-xs font-mono tracking-widest uppercase text-white font-bold">Live Message Log Stream Matrix</h2>
            {!loading && (
              <span className="text-[10px] font-mono px-2 py-0.5 bg-[#1F2229] text-[#737885] rounded-sm">
                Showing {filteredJobs.length} of {jobs.length}
              </span>
            )}
          </div>
          {error ? (
            <span className="text-[11px] font-mono text-rose-400 animate-pulse">⚠️ Connection Error: {error}</span>
          ) : (
            <span className="text-[11px] font-mono text-emerald-400">● Streaming live database records</span>
          )}
        </div>

        {/* Search & Filter Controls Bar */}
        <div className="p-4 border-b border-[#1F2229] bg-[#12141A] flex flex-col md:flex-row gap-4 justify-between items-center">
          {/* Status Tabs */}
          <div className="flex gap-2">
            {(['ALL', 'PROCESSING', 'COMPLETED', 'FAULTED'] as const).map((status) => {
              const isActive = statusFilter === status;
              return (
                <button
                  key={status}
                  type="button"
                  onClick={() => setStatusFilter(status)}
                  className={`px-3 py-1.5 text-xs font-mono tracking-wider font-semibold border rounded-sm transition-colors uppercase cursor-pointer ${
                    isActive
                      ? 'bg-indigo-600 text-white border-indigo-500'
                      : 'bg-[#0E1015] text-[#737885] border-[#1F2229] hover:bg-[#161920] hover:text-white'
                  }`}
                >
                  {status}
                </button>
              );
            })}
          </div>

          {/* Search Input */}
          <div className="w-full md:w-80 relative">
            <input
              type="text"
              placeholder="Search callId, transcripts, summaries..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full bg-[#0E1015] border border-[#1F2229] rounded-sm px-3 py-1.5 text-xs font-mono text-[#E4E6EB] focus:outline-none focus:border-indigo-500/60 transition-colors placeholder-[#4F535E]"
            />
            {searchQuery && (
              <button
                type="button"
                onClick={() => setSearchQuery('')}
                className="absolute right-2.5 top-1/2 -translate-y-1/2 text-[#737885] hover:text-white font-mono text-xs cursor-pointer bg-transparent border-0"
              >
                ×
              </button>
            )}
          </div>
        </div>
        
        <div className="overflow-x-auto">
          {loading && jobs.length === 0 ? (
            <div className="p-12 text-center text-xs font-mono text-[#737885] uppercase tracking-widest">Establishing Pipeline Buffer Context...</div>
          ) : jobs.length === 0 ? (
            <div className="p-12 text-center text-xs font-mono text-[#737885] uppercase tracking-widest">No transaction summaries synced to database yet.</div>
          ) : filteredJobs.length === 0 ? (
            <div className="p-12 text-center text-xs font-mono text-[#737885] uppercase tracking-widest">No matching summaries found.</div>
          ) : (
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="border-b border-[#1F2229] bg-[#0E1015] text-[11px] font-mono tracking-wider text-[#737885] uppercase">
                  <th className="py-3 px-6">Job ID</th>
                  <th className="py-3 px-6">Source Core Key</th>
                  <th className="py-3 px-6">AI Core Generation (Gemini)</th>
                  <th className="py-3 px-6">Outbound Orchestrator (Clay)</th>
                  <th className="py-3 px-6 text-right">Processed Time</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[#1F2229]/60 font-mono text-xs">
                {filteredJobs.map((job) => (
                  <tr key={job.id} className="hover:bg-[#161920]/40 transition-colors group">
                    <td className="py-4 px-6">
                      <button 
                        onClick={() => handleOpenDetails(job.id)} 
                        className="text-indigo-400 hover:text-indigo-300 font-semibold text-left underline focus:outline-none"
                      >
                        #{job.id.slice(0, 8)}...
                      </button>
                    </td>
                    <td className="py-4 px-6 font-semibold text-white tracking-wide">{job.callId}</td>
                    <td className="py-4 px-6"><StatusBadge text={job.geminiStatus} /></td>
                    <td className="py-4 px-6"><StatusBadge text={job.outboundState} /></td>
                    <td className="py-4 px-6 text-right text-[#5E6370] group-hover:text-slate-300 transition-colors">{job.timestamp}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </main>

      {/* Slide-out Insights Overlay Drawer Component */}
      <div className={`fixed top-0 right-0 h-full w-full sm:w-[550px] bg-[#0F1115] border-l border-[#1F2229] shadow-2xl transform transition-transform duration-300 ease-in-out z-50 p-6 flex flex-col ${selectedJobId ? 'translate-x-0' : 'translate-x-full'}`}>
        <div className="flex justify-between items-center pb-4 border-b border-[#1F2229] mb-4">
          <div>
            <span className="text-[10px] font-mono tracking-widest text-indigo-400 uppercase font-bold">Pipeline Insight Summary</span>
            <h3 className="text-md text-white font-mono mt-0.5">Job: #{selectedJobId?.slice(0, 12)}...</h3>
          </div>
          <button 
            onClick={handleCloseDetails}
            className="text-[#737885] hover:text-white font-mono text-sm border border-[#1F2229] px-2.5 py-1 rounded-sm bg-[#12141A] transition-colors"
          >
            ESC // CLOSE
          </button>
        </div>

        <div className="flex-1 overflow-y-auto font-mono text-xs text-[#E4E6EB] space-y-4 pr-1">
          {detailLoading && (
            <div className="h-full flex flex-col items-center justify-center text-[#737885] uppercase tracking-widest animate-pulse">
              <span>Querying PostgreSQL Node...</span>
            </div>
          )}
          
          {detailError && (
            <div className="p-4 border border-rose-900/40 bg-rose-950/20 text-rose-400 rounded-sm">
              <span className="font-bold">CRITICAL EXCEPTION REJECTION</span>
              <p className="mt-1 opacity-80">{detailError}</p>
            </div>
          )}

          {jobDetail && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-2 bg-[#12141A] border border-[#1F2229] p-3 rounded-sm text-[11px]">
                <div>
                  <span className="text-[#4F535E] block">SOURCE CORE KEY:</span>
                  <span className="text-white font-bold">{jobDetail.callId}</span>
                </div>
                <div>
                  <span className="text-[#4F535E] block">TIMESTAMP COMMITTED:</span>
                  <span className="text-zinc-400">{new Date(jobDetail.createdAt).toLocaleString()}</span>
                </div>
              </div>

              {jobDetail.status === 'FAULTED' && (
                <div className="pt-2">
                  <button
                    onClick={() => handleRetryJob(jobDetail.id)}
                    className="w-full bg-rose-950/40 hover:bg-rose-900/60 text-rose-400 border border-rose-800/60 font-semibold py-2 px-4 rounded-sm font-mono tracking-widest uppercase transition-colors text-center cursor-pointer"
                  >
                    🔄 Retry AI Analysis
                  </button>
                </div>
              )}

              <div className="pt-2">
                <span className="text-[#737885] block mb-2 font-bold uppercase tracking-wider">Structured AI Report:</span>
                <pre className="w-full bg-[#060709] border border-[#1F2229] rounded-sm p-4 text-[#E4E6EB] whitespace-pre-wrap font-sans text-sm leading-relaxed overflow-x-auto">
                  {jobDetail.aiAnalysisPass || "No markdown payload generated for this job instance."}
                </pre>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}