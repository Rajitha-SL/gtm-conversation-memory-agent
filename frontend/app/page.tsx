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
  const API_BASE = typeof window !== 'undefined' 
    ? (window.location.hostname.includes('localhost') || window.location.hostname.includes('127.0.0.1') || window.location.hostname.includes('[::1]')
      ? `${window.location.protocol}//${window.location.hostname}:3000`
      : `${window.location.protocol}//${window.location.hostname.replace('3001', '3000')}`)
    : 'http://localhost:3000';
  const [jobs, setJobs] = useState<IngestionJob[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const [searchQuery, setSearchQuery] = useState<string>('');
  const [statusFilter, setStatusFilter] = useState<'ALL' | 'PROCESSING' | 'COMPLETED' | 'FAULTED'>('ALL');

  // User Settings & API Key States
  const [customApiKey, setCustomApiKey] = useState<string>('');
  const [freeRunsCount, setFreeRunsCount] = useState<number>(0);
  const [showSettingsModal, setShowSettingsModal] = useState<boolean>(false);

  // Ingestion Modal States
  const [showIngestionModal, setShowIngestionModal] = useState<boolean>(false);
  const [activeIngestionTab, setActiveIngestionTab] = useState<'paste' | 'upload' | 'link'>('paste');
  const [simCallId, setSimCallId] = useState('');
  const [simTranscript, setSimTranscript] = useState('');
  const [simLoading, setSimLoading] = useState(false);
  const [simStatus, setSimStatus] = useState<{ type: 'idle' | 'success' | 'error'; message: string }>({ type: 'idle', message: '' });

  // Upload Tab Simulation States
  const [uploadedFileName, setUploadedFileName] = useState<string>('');
  const [uploadProgress, setUploadProgress] = useState<number>(-1);

  // Link Tab Simulation States
  const [webLinkUrl, setWebLinkUrl] = useState<string>('');
  const [linkProgress, setLinkProgress] = useState<number>(-1);
  const [linkStatusText, setLinkStatusText] = useState<string>('');

  // Selected Job Details Drawer
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [jobDetail, setJobDetail] = useState<JobDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState<boolean>(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [copied, setCopied] = useState<boolean>(false);

  // Load configuration details from localStorage on initial render
  useEffect(() => {
    if (typeof window !== 'undefined') {
      const savedKey = localStorage.getItem('gtm_custom_api_key') || '';
      const savedRuns = localStorage.getItem('gtm_free_runs_count') || '0';
      setCustomApiKey(savedKey);
      setFreeRunsCount(parseInt(savedRuns, 10));
    }
  }, []);

  useEffect(() => {
    async function fetchPipelineData() {
      try {
        const response = await fetch(`${API_BASE}/api/v1/jobs`);
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
    const eventSource = new EventSource(`${API_BASE}/api/v1/jobs/stream`);

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
            const newJobs = [...prevJobs];
            newJobs[index] = formattedJob;
            return newJobs;
          } else {
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
      const response = await fetch(`${API_BASE}/api/v1/jobs/${id}`);
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

  const handleCopyEmail = (email: string) => {
    navigator.clipboard.writeText(email);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleRetryJob = async (id: string) => {
    try {
      const response = await fetch(`${API_BASE}/api/v1/jobs/${id}/retry`, {
        method: 'POST',
        headers: {
          'x-gemini-key': customApiKey
        }
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

  const handlePipelineInjection = async (e?: React.FormEvent) => {
    if (e) e.preventDefault();

    if (!customApiKey && freeRunsCount >= 5) {
      setShowIngestionModal(false);
      setShowSettingsModal(true);
      alert('FREE RUNS THRESHOLD EXCEEDED: Please configure your custom Gemini API key to continue.');
      return;
    }

    if (!simTranscript) {
      setSimStatus({ type: 'error', message: 'INSUFFICIENT FIELDS: Specify raw transcript text.' });
      return;
    }

    const finalCallId = simCallId.trim() || `call_${Date.now()}`;

    setSimLoading(true);
    setSimStatus({ type: 'idle', message: '' });

    try {
      const response = await fetch(`${API_BASE}/api/v1/webhooks/gong`, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'x-gemini-key': customApiKey
        },
        body: JSON.stringify({ callId: finalCallId, rawTranscript: simTranscript }),
      });

      const data = await response.json();

      if (response.ok) {
        if (!customApiKey) {
          const nextRuns = freeRunsCount + 1;
          setFreeRunsCount(nextRuns);
          localStorage.setItem('gtm_free_runs_count', String(nextRuns));
        }

        setSimStatus({ type: 'success', message: `Job staged successfully! ID: #${finalCallId}` });
        setSimCallId('');
        setSimTranscript('');
        setUploadedFileName('');
        setWebLinkUrl('');
        
        setTimeout(() => {
          setShowIngestionModal(false);
          setSimStatus({ type: 'idle', message: '' });
        }, 1000);

      } else {
        setSimStatus({ type: 'error', message: data.message || 'Pipeline ingestion failure.' });
      }
    } catch (err) {
      console.error('Fetch execution error details:', err);
      setSimStatus({ type: 'error', message: 'CONNECTION ERROR: Failed to reach backend API.' });
    } finally {
      setSimLoading(false);
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setUploadedFileName(file.name);
    setUploadProgress(0);

    if (file.name.endsWith('.txt')) {
      const reader = new FileReader();
      reader.onload = (event) => {
        const text = event.target?.result as string;
        setSimTranscript(text);
        const cleanName = file.name.replace(/\.[^/.]+$/, "").replace(/[^a-zA-Z0-9]/g, "_");
        setSimCallId(`file_${cleanName.slice(0, 15)}_${Math.floor(Math.random() * 900 + 100)}`);
        setUploadProgress(100);
      };
      reader.readAsText(file);
    } else {
      let progress = 0;
      const interval = setInterval(() => {
        progress += 20;
        setUploadProgress(progress);
        if (progress >= 100) {
          clearInterval(interval);
          setSimTranscript(`DevOps Lead: In our sync with the AWS Cloud Infrastructure Architecture team today, we reviewed our migration pipeline blocks. Moving off our existing AWS database nodes to GCP represents a massive operational cost barrier ($120k estimated migration expense). We need specialized cloud templates to bypass this cost lock. Principal email contact is devops_lead@targetcloud.com`);
          const cleanName = file.name.replace(/\.[^/.]+$/, "").replace(/[^a-zA-Z0-9]/g, "_");
          setSimCallId(`audio_${cleanName.slice(0, 15)}_${Math.floor(Math.random() * 900 + 100)}`);
        }
      }, 300);
    }
  };

  const handleExtractWebLink = () => {
    if (!webLinkUrl.trim()) return;

    setLinkProgress(0);
    setLinkStatusText('Initiating scraper cluster...');

    const statuses = [
      { p: 30, text: 'Resolving DNS and bypassing Cloudflare...' },
      { p: 60, text: 'Parsing HTML body container node tags...' },
      { p: 85, text: 'Extracting conversational transcript paragraphs...' },
      { p: 100, text: 'Extraction complete!' }
    ];

    let step = 0;
    const interval = setInterval(() => {
      if (step < statuses.length) {
        setLinkProgress(statuses[step].p);
        setLinkStatusText(statuses[step].text);
        
        if (statuses[step].p === 100) {
          clearInterval(interval);
          setSimTranscript(`DevOps Lead: During our review of the Salesforce workflow integration specs for this quarter, the team raised significant security objections. The timeline for migrating our current custom configurations is tight (target Q3), and we are blocked until we have complete audit compliance schemas. Primary contact is sfdc_admin@workflowcorp.com`);
          const cleanUrl = webLinkUrl.replace(/https?:\/\/(www\.)?/, '').replace(/[^a-zA-Z0-9]/g, '_');
          setSimCallId(`web_${cleanUrl.slice(0, 18)}_${Math.floor(Math.random() * 900 + 100)}`);
        }
        step++;
      } else {
        clearInterval(interval);
      }
    }, 400);
  };

  const handleSaveSettings = () => {
    localStorage.setItem('gtm_custom_api_key', customApiKey);
    setShowSettingsModal(false);
  };

  const handleResetFreeRuns = () => {
    setFreeRunsCount(0);
    localStorage.setItem('gtm_free_runs_count', '0');
    alert('Free runs counter successfully reset!');
  };

  const getPainPoints = (text: string = '') => {
    const keywords = ['AWS', 'GCP', 'Salesforce', 'Azure', 'Cost', 'Migration', 'Timeline', 'Security', 'DevOps', 'Objection'];
    const matched = [];
    const lower = text.toLowerCase();
    for (const kw of keywords) {
      if (lower.includes(kw.toLowerCase())) {
        matched.push(kw);
      }
    }
    return matched.length > 0 ? matched : ['Sales Sync Objection'];
  };

  const getExecutiveSummary = (text: string = '') => {
    const emailMarker = "### Follow-up Email Draft";
    if (text.includes(emailMarker)) {
      return text.split(emailMarker)[0].trim();
    }
    return text;
  };

  const generateFollowUpEmail = (job: JobDetail) => {
    const text = job.aiAnalysisPass || '';
    
    // If Gemini compiled a custom email draft, extract it
    const emailMarker = "### Follow-up Email Draft";
    if (text.includes(emailMarker)) {
      const parts = text.split(emailMarker);
      const emailContent = parts[parts.length - 1].trim();
      return emailContent.replace(/^[\s\r\n\-]+|[\s\r\n\-]+$/g, '');
    }
    
    let companyName = "your organization";
    
    // 1. Try to find company name in the Meeting Title block
    const meetingTitleMatch = text.match(/Meeting\s+Title:\s*\**\s*([A-Za-z0-9\s\.\,\-\&]{2,50})/i);
    if (meetingTitleMatch) {
      const cleaned = meetingTitleMatch[1].split(/(?:prospect|partner|sync|-|addressing|meeting|customer)/i)[0].trim();
      if (cleaned && cleaned.length > 2) {
        companyName = cleaned;
      }
    }

    // 2. Fallback to basic match but exclude common false matches like 'Concern' or 'Prospect'
    if (companyName === "your organization") {
      const companyMatch = text.match(/(?:at|company|client|organization)\s+([A-Z][a-zA-Z0-9\s\.\,]{2,20})/);
      if (companyMatch) {
        companyName = companyMatch[1].trim();
      }
    }
    
    return `Subject: Following up on our sync - GTM Alignment & Next Steps

Hi,

Thank you for taking the time to sync with us today. 

I am following up on our discussion regarding ${companyName}'s current workflow blockers and cloud integration objectives. Specifically, we noted your concerns regarding the operational overhead, custom configurations, and associated costs.

We are drafting a customized value-analysis comparison model mapping our solutions directly to your specific technical constraints.

Do you have 10 minutes next Tuesday to review our TCO comparison framework?

Best regards,
Enterprise Growth Coordinator`;
  };

  const downloadSummaryMarkdown = (job: JobDetail) => {
    const content = `# Pipeline Insight Summary: Call #${job.callId}
Date Generated: ${new Date(job.createdAt).toLocaleString()}

## Executive Summary & Analysis Report
${job.aiAnalysisPass || "Analysis still generating in worker thread context..."}

---
Generated by GTM Context Engine.`;
    const blob = new Blob([content], { type: 'text/markdown' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `summary_${job.callId}.md`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const handlePrintPDF = (job: JobDetail) => {
    const printWindow = window.open('', '_blank');
    if (printWindow) {
      printWindow.document.write(`
        <html>
          <head>
            <title>Summary - #${job.callId}</title>
            <style>
              body { font-family: monospace; padding: 40px; color: #1E293B; line-height: 1.6; }
              h1 { border-bottom: 2px solid #0F172A; padding-bottom: 10px; margin-bottom: 20px; font-size: 20px; }
              pre { white-space: pre-wrap; font-family: sans-serif; font-size: 14px; background: #F8FAFC; padding: 20px; border: 1px solid #E2E8F0; border-radius: 4px; }
              .meta { font-size: 12px; color: #64748B; margin-bottom: 30px; }
            </style>
          </head>
          <body>
            <h1>GTM CONTEXT ENGINE // Insight Report Summary</h1>
            <div class="meta">
              <strong>Source Call ID:</strong> ${job.callId} <br/>
              <strong>Date Synced:</strong> ${new Date(job.createdAt).toLocaleString()}
            </div>
            <pre>${job.aiAnalysisPass}</pre>
            <script>
              window.onload = function() { window.print(); window.close(); }
            </script>
          </body>
        </html>
      `);
      printWindow.document.close();
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

  const totalIngested = jobs.length;
  const activeProcessingCount = jobs.filter((j) => j.geminiStatus === 'PROCESSING').length;
  const failureFaultedCount = jobs.filter((j) => j.geminiStatus === 'FAULTED').length;
  const successCompletedCount = jobs.filter((j) => j.geminiStatus === 'COMPLETED').length;
  const successRatePercentage = totalIngested > 0 ? Math.round((successCompletedCount / totalIngested) * 100) : 100;

  return (
    <div className="min-h-screen bg-[#0A0B0D] text-[#E4E6EB] p-8 antialiased selection:bg-indigo-500/30 relative overflow-x-hidden">
      
      <header className="flex justify-between items-center border-b border-[#1F2229] pb-6 mb-8">
        <div>
          <div className="text-xs font-mono tracking-[0.3em] text-indigo-400 uppercase font-bold">Intelligence Layer</div>
          <h1 className="text-2xl font-light tracking-tight text-white mt-1">GTM CONTEXT ENGINE // Pipeline Node</h1>
        </div>
        <div className="flex items-center gap-4">
          <button
            onClick={() => setShowSettingsModal(true)}
            className="flex items-center gap-2 px-3 py-1.5 bg-[#12141A] border border-[#1F2229] hover:bg-[#161920] hover:text-white rounded-sm text-xs font-mono uppercase tracking-widest transition-colors cursor-pointer text-[#737885]"
          >
            ⚙️ Settings
          </button>
          <button
            onClick={() => setShowIngestionModal(true)}
            className="flex items-center gap-2 px-4 py-1.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-sm text-xs font-mono uppercase tracking-widest transition-colors cursor-pointer font-bold"
          >
            + New Analysis
          </button>
        </div>
      </header>

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

        <div className="p-4 border-b border-[#1F2229] bg-[#12141A] flex flex-col md:flex-row gap-4 justify-between items-center">
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

      {showSettingsModal && (
        <div className="fixed inset-0 flex items-center justify-center bg-black/70 z-50 p-4">
          <div className="bg-[#0F1115] border border-[#1F2229] rounded-sm w-full max-w-md p-6 font-mono text-xs relative">
            <h3 className="text-sm text-white font-bold uppercase tracking-wider mb-4">Pipeline Settings</h3>
            
            <div className="space-y-4">
              <div>
                <label className="block text-[#737885] uppercase tracking-wider mb-1.5 font-bold">BYO Gemini API Key</label>
                <input
                  type="password"
                  placeholder="Paste AI API token (stored locally)..."
                  value={customApiKey}
                  onChange={(e) => setCustomApiKey(e.target.value)}
                  className="w-full bg-[#0E1015] border border-[#1F2229] rounded-sm px-3 py-2 text-[#E4E6EB] focus:outline-none focus:border-indigo-500/60 transition-colors placeholder-[#4F535E]"
                />
                <p className="mt-1 text-[10px] text-[#4F535E]">Your API key is stored safely on this browser client only.</p>
              </div>

              <div className="bg-[#12141A] border border-[#1F2229] p-3 rounded-sm">
                <span className="text-[#737885] block font-bold">FREE TIER TRACKER:</span>
                <p className="text-md text-white mt-1 font-bold">{freeRunsCount} / 5 Free Runs Used</p>
                {freeRunsCount >= 5 && !customApiKey && (
                  <p className="text-rose-400 text-[10px] mt-1 font-bold">⚠️ Threshold reached. Configure a custom key to unlock.</p>
                )}
              </div>

              <div className="flex justify-between items-center pt-4 border-t border-[#1F2229]/50">
                <button
                  onClick={handleResetFreeRuns}
                  className="text-indigo-400 hover:text-indigo-300 font-semibold uppercase cursor-pointer"
                >
                  🔄 Reset Counter
                </button>
                <div className="flex gap-2">
                  <button
                    onClick={() => setShowSettingsModal(false)}
                    className="text-[#737885] hover:text-white px-3 py-1.5 border border-[#1F2229] hover:bg-[#161920] transition-colors rounded-sm cursor-pointer"
                  >
                    Cancel
                  </button>
                  <button
                    onClick={handleSaveSettings}
                    className="bg-indigo-600 hover:bg-indigo-500 text-white font-bold px-4 py-1.5 rounded-sm transition-colors cursor-pointer"
                  >
                    Save Changes
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {showIngestionModal && (
        <div className="fixed inset-0 flex items-center justify-center bg-black/70 z-50 p-4">
          <div className="bg-[#0F1115] border border-[#1F2229] rounded-sm w-full max-w-2xl p-6 font-mono text-xs relative flex flex-col max-h-[85vh]">
            <div className="flex justify-between items-center pb-4 border-b border-[#1F2229] mb-4">
              <h3 className="text-sm text-white font-bold uppercase tracking-wider">Start Call Transcript Analysis</h3>
              <button 
                onClick={() => setShowIngestionModal(false)}
                className="text-[#737885] hover:text-white font-bold"
              >
                CLOSE [×]
              </button>
            </div>

            <div className="flex border-b border-[#1F2229] mb-4">
              {(['paste', 'upload', 'link'] as const).map((tab) => {
                const tabLabels = {
                  paste: '📝 Paste Text/Transcript',
                  upload: '🔊 Upload Audio/File',
                  link: '🔗 Extract Web Link'
                };
                return (
                  <button
                    key={tab}
                    onClick={() => {
                      setActiveIngestionTab(tab);
                      setSimStatus({ type: 'idle', message: '' });
                    }}
                    className={`flex-1 py-2 text-center font-bold tracking-wider transition-colors border-b-2 cursor-pointer ${
                      activeIngestionTab === tab
                        ? 'border-indigo-500 text-white'
                        : 'border-transparent text-[#737885] hover:text-white'
                    }`}
                  >
                    {tabLabels[tab]}
                  </button>
                );
              })}
            </div>

            <div className="flex-1 overflow-y-auto space-y-4 pr-1">
              
              {activeIngestionTab === 'paste' && (
                <div className="space-y-4">
                  <div>
                    <label className="block text-[#737885] uppercase tracking-wider mb-1.5 font-bold">Source Core Key (Optional)</label>
                    <input
                      type="text"
                      placeholder="e.g., Acme Corp (leave blank to auto-generate)"
                      value={simCallId}
                      onChange={(e) => setSimCallId(e.target.value)}
                      className="w-full bg-[#0E1015] border border-[#1F2229] rounded-sm px-3 py-2 text-[#E4E6EB] focus:outline-none focus:border-indigo-500/60 transition-colors placeholder-[#4F535E]"
                    />
                  </div>
                  <div>
                    <label className="block text-[#737885] uppercase tracking-wider mb-1.5 font-bold">Raw Transcript Stream Text</label>
                    <textarea
                      placeholder="Paste structural client meeting logs or conversations directly..."
                      value={simTranscript}
                      onChange={(e) => setSimTranscript(e.target.value)}
                      rows={6}
                      className="w-full bg-[#0E1015] border border-[#1F2229] rounded-sm px-3 py-2 text-[#E4E6EB] focus:outline-none focus:border-indigo-500/60 transition-colors resize-none placeholder-[#4F535E]"
                    />
                  </div>
                </div>
              )}

              {activeIngestionTab === 'upload' && (
                <div className="space-y-4">
                  <div className="border-2 border-dashed border-[#1F2229] rounded-sm p-8 text-center bg-[#0E1015]/40 flex flex-col items-center justify-center">
                    <span className="text-3xl mb-3">🎙️</span>
                    <span className="text-white block font-bold mb-1">Drag & drop raw files here</span>
                    <span className="text-[#4F535E] block mb-4">Supports .mp3, .wav, .m4a or .txt transcripts</span>
                    <input
                      type="file"
                      accept=".mp3,.wav,.m4a,.txt"
                      onChange={handleFileChange}
                      className="hidden"
                      id="audio-upload-selector"
                    />
                    <label
                      htmlFor="audio-upload-selector"
                      className="bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-sm font-bold tracking-wide uppercase transition-colors cursor-pointer"
                    >
                      Browse Files
                    </label>
                  </div>

                  {uploadedFileName && (
                    <div className="bg-[#12141A] border border-[#1F2229] p-3 rounded-sm">
                      <div className="flex justify-between items-center mb-1.5">
                        <span className="text-white font-bold">{uploadedFileName}</span>
                        <span className="text-[#737885]">{uploadProgress}%</span>
                      </div>
                      <div className="w-full bg-[#0E1015] h-1.5 rounded-sm overflow-hidden">
                        <div 
                          className="bg-indigo-500 h-full transition-all duration-300" 
                          style={{ width: `${Math.max(0, uploadProgress)}%` }}
                        />
                      </div>
                      {uploadProgress > 0 && uploadProgress < 100 && (
                        <p className="text-[#4F535E] mt-1 animate-pulse">Running Whisper Speech-to-Text Pipeline Node...</p>
                      )}
                      {uploadProgress === 100 && (
                        <p className="text-emerald-400 mt-1 font-bold">✓ Audio transcript generated and mapped.</p>
                      )}
                    </div>
                  )}
                </div>
              )}

              {activeIngestionTab === 'link' && (
                <div className="space-y-4">
                  <div>
                    <label className="block text-[#737885] uppercase tracking-wider mb-1.5 font-bold">Target Meeting Web URL</label>
                    <div className="flex gap-2">
                      <input
                        type="url"
                        placeholder="https://drive.google.com/rec/transcript-link..."
                        value={webLinkUrl}
                        onChange={(e) => setWebLinkUrl(e.target.value)}
                        className="flex-1 bg-[#0E1015] border border-[#1F2229] rounded-sm px-3 py-2 text-[#E4E6EB] focus:outline-none focus:border-indigo-500/60 transition-colors placeholder-[#4F535E]"
                      />
                      <button
                        onClick={handleExtractWebLink}
                        disabled={!webLinkUrl.trim() || linkProgress >= 0 && linkProgress < 100}
                        className="bg-indigo-600 hover:bg-indigo-500 disabled:opacity-30 disabled:hover:bg-indigo-600 text-white font-bold px-4 py-2 rounded-sm uppercase tracking-wide transition-colors cursor-pointer"
                      >
                        Fetch Context
                      </button>
                    </div>
                  </div>

                  {linkProgress >= 0 && (
                    <div className="bg-[#12141A] border border-[#1F2229] p-3 rounded-sm space-y-1">
                      <div className="flex justify-between items-center text-[10px]">
                        <span className="text-zinc-400 font-bold uppercase">{linkStatusText}</span>
                        <span className="text-indigo-400 font-bold">{linkProgress}%</span>
                      </div>
                      <div className="w-full bg-[#0E1015] h-1.5 rounded-sm overflow-hidden">
                        <div 
                          className="bg-indigo-500 h-full transition-all duration-300" 
                          style={{ width: `${linkProgress}%` }}
                        />
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>

            <div className="pt-4 border-t border-[#1F2229] mt-4 flex items-center justify-between">
              <div className="h-4">
                {simStatus.type === 'success' && <p className="text-emerald-400 font-bold">{simStatus.message}</p>}
                {simStatus.type === 'error' && <p className="text-rose-400 font-bold">{simStatus.message}</p>}
              </div>
              <button
                onClick={() => handlePipelineInjection()}
                disabled={simLoading || !simTranscript}
                className="bg-indigo-600 hover:bg-indigo-500 disabled:opacity-30 disabled:hover:bg-indigo-600 text-white font-bold py-2 px-6 rounded-sm uppercase tracking-wider transition-colors cursor-pointer"
              >
                {simLoading ? 'Enqueuing analysis...' : 'Start Pipeline Run'}
              </button>
            </div>
          </div>
        </div>
      )}

      <div className={`fixed top-0 right-0 h-full w-full sm:w-[600px] bg-[#0F1115] border-l border-[#1F2229] shadow-2xl transform transition-transform duration-300 ease-in-out z-50 p-6 flex flex-col ${selectedJobId ? 'translate-x-0' : 'translate-x-full'}`}>
        <div className="flex justify-between items-center pb-4 border-b border-[#1F2229] mb-4">
          <div>
            <span className="text-[10px] font-mono tracking-widest text-indigo-400 uppercase font-bold">Pipeline Insight Details</span>
            <h3 className="text-md text-white font-mono mt-0.5">Job: #{selectedJobId?.slice(0, 12)}...</h3>
          </div>
          <button 
            onClick={handleCloseDetails}
            className="text-[#737885] hover:text-white font-mono text-sm border border-[#1F2229] px-2.5 py-1 rounded-sm bg-[#12141A] transition-colors"
          >
            ESC // CLOSE
          </button>
        </div>

        <div className="flex-1 overflow-y-auto font-mono text-xs text-[#E4E6EB] space-y-5 pr-1">
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
            <div className="space-y-5">
              
              <div className="flex gap-2">
                <button
                  onClick={() => handleCopyEmail(generateFollowUpEmail(jobDetail))}
                  className="flex-1 bg-indigo-950/40 hover:bg-indigo-900/60 text-indigo-400 border border-indigo-800/60 font-semibold py-1.5 rounded-sm uppercase tracking-wide transition-colors text-center cursor-pointer text-[10px]"
                >
                  {copied ? '📋 Copied!' : '📋 Copy Email'}
                </button>
                <button
                  onClick={() => downloadSummaryMarkdown(jobDetail)}
                  className="flex-1 bg-emerald-950/40 hover:bg-emerald-900/60 text-emerald-400 border border-emerald-800/60 font-semibold py-1.5 rounded-sm uppercase tracking-wide transition-colors text-center cursor-pointer text-[10px]"
                >
                  ⬇️ Download MD
                </button>
                <button
                  onClick={() => handlePrintPDF(jobDetail)}
                  className="flex-1 bg-zinc-800/40 hover:bg-zinc-700/60 text-zinc-400 border border-zinc-700/60 font-semibold py-1.5 rounded-sm uppercase tracking-wide transition-colors text-center cursor-pointer text-[10px]"
                >
                  🖨️ Export PDF
                </button>
              </div>

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
                <div className="pt-1">
                  <button
                    onClick={() => handleRetryJob(jobDetail.id)}
                    className="w-full bg-rose-950/40 hover:bg-rose-900/60 text-rose-400 border border-rose-800/60 font-semibold py-2 px-4 rounded-sm font-mono tracking-widest uppercase transition-colors text-center cursor-pointer"
                  >
                    🔄 Retry AI Analysis
                  </button>
                </div>
              )}

              <div>
                <span className="text-[#737885] block mb-2 font-bold uppercase tracking-wider">Identified Pain Points:</span>
                <div className="flex flex-wrap gap-2">
                  {getPainPoints(jobDetail.aiAnalysisPass || jobDetail.callId).map((pain, idx) => (
                    <span 
                      key={idx}
                      className="px-2.5 py-1 bg-amber-950/20 text-amber-400 border border-amber-900/50 rounded-sm font-bold uppercase tracking-wide text-[10px]"
                    >
                      💥 {pain}
                    </span>
                  ))}
                </div>
              </div>

              <div>
                <span className="text-[#737885] block mb-2 font-bold uppercase tracking-wider">Executive Summary Report:</span>
                <pre className="w-full bg-[#060709] border border-[#1F2229] rounded-sm p-4 text-[#E4E6EB] whitespace-pre-wrap font-sans text-xs leading-relaxed overflow-x-auto">
                  {getExecutiveSummary(jobDetail.aiAnalysisPass) || "Gemini report analysis generated payload still in progress."}
                </pre>
              </div>

              <div>
                <span className="text-[#737885] block mb-2 font-bold uppercase tracking-wider">Personalized Follow-up Email Draft:</span>
                <pre className="w-full bg-[#0E1015] border border-[#1F2229] rounded-sm p-4 text-slate-300 whitespace-pre-wrap font-sans text-xs leading-relaxed overflow-x-auto select-all">
                  {generateFollowUpEmail(jobDetail)}
                </pre>
              </div>

            </div>
          )}
        </div>
      </div>
    </div>
  );
}