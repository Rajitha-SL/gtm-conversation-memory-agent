'use client';

import React, { useState, useEffect } from 'react';

interface IngestionJob {
  id: string;
  callId: string;
  geminiStatus: 'QUEUED' | 'PROCESSING' | 'COMPLETED' | 'FAULTED';
  outboundState: 'PENDING' | 'TRIGGERED' | 'SIMULATED' | 'BLOCKED' | 'SKIPPED';
  timestamp: string;
}

export default function LiveIngestionStream() {
  const [jobs, setJobs] = useState<IngestionJob[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // Poll the backend API server on port 3000 for data
  useEffect(() => {
    async function fetchPipelineData() {
      try {
        const response = await fetch('http://localhost:3000/api/v1/jobs');
        if (!response.ok) {
          throw new Error(`Server returned status: ${response.status}`);
        }
        const data = await response.json();
        
        // Map the backend fields to our clean UI data model
        const formattedJobs = data.map((item: any) => ({
          id: String(item.id),
          callId: item.callId || 'N/A',
          geminiStatus: item.status || 'COMPLETED', // Maps fallback status
          outboundState: item.outboundTriggered ? 'TRIGGERED' : 'SIMULATED',
          timestamp: item.createdAt ? new Date(item.createdAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : 'N/A'
        }));
        
        setJobs(formattedJobs);
        setError(null);
      } catch (err: any) {
        setError(err.message || 'Failed to establish connection to backend API node.');
      } finally {
        setLoading(false);
      }
    }

    // Run fetch instantly, then poll every 3000ms
    fetchPipelineData();
    const interval = setInterval(fetchPipelineData, 3000);
    return () => clearInterval(interval);
  }, []);

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

  return (
    <div className="min-h-screen bg-[#0A0B0D] text-[#E4E6EB] p-8 antialiased selection:bg-indigo-500/30">
      
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

      {/* Metrics Row Grid */}
      <section className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        {[
          { label: 'Active Queue Consumers', value: '01', sub: 'BullMQ Active Thread' },
          { label: 'Current Queue Backlog', value: '00', sub: 'Redis Memory Stack' },
          { label: 'Total Synced Passages', value: loading ? '...' : String(jobs.length), sub: 'PostgreSQL Commits' },
          { label: 'Outbound Pipelines Tripped', value: loading ? '...' : String(jobs.filter(j => j.outboundState === 'TRIGGERED').length), sub: 'Clay Webhook Injections' }
        ].map((metric, idx) => (
          <div key={idx} className="bg-[#12141A] border border-[#1F2229] p-5 rounded-sm">
            <div className="text-xs font-mono tracking-wider text-[#737885] uppercase">{metric.label}</div>
            <div className="text-3xl font-light tracking-tight text-white mt-2 font-mono">{metric.value}</div>
            <div className="text-[10px] font-mono text-[#4F535E] uppercase tracking-widest mt-1">{metric.sub}</div>
          </div>
        ))}
      </section>

      {/* Primary Data Matrix Table */}
      <main className="bg-[#12141A] border border-[#1F2229] rounded-sm overflow-hidden">
        <div className="px-6 py-4 border-b border-[#1F2229] flex justify-between items-center bg-[#161920]">
          <h2 className="text-xs font-mono tracking-widest uppercase text-white font-bold">Live Message Log Stream Matrix</h2>
          {error ? (
            <span className="text-[11px] font-mono text-rose-400 animate-pulse">⚠️ Connection Error: {error}</span>
          ) : (
            <span className="text-[11px] font-mono text-emerald-400">● Streaming live database records</span>
          )}
        </div>
        
        <div className="overflow-x-auto">
          {loading && jobs.length === 0 ? (
            <div className="p-12 text-center text-xs font-mono text-[#737885] uppercase tracking-widest">Establishing Pipeline Buffer Context...</div>
          ) : jobs.length === 0 ? (
            <div className="p-12 text-center text-xs font-mono text-[#737885] uppercase tracking-widest">No transaction summaries synced to database yet.</div>
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
                {jobs.map((job) => (
                  <tr key={job.id} className="hover:bg-[#161920]/40 transition-colors group">
                    <td className="py-4 px-6 text-indigo-400 font-semibold">#{job.id.padStart(4, '0')}</td>
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
    </div>
  );
}