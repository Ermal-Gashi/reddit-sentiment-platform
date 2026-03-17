import React, { useState } from 'react';
import { useRouter } from 'next/router';
import { Play, Search, HelpCircle, X, Info, Activity } from 'lucide-react';
import Button from '../common/Button';

const VIEW_CONFIG = {
  '/': { title: "Pipeline Overview", description: "High-level command center..." },
  '/topics': { title: "Topic Modeling (LDA)", description: "Visualizes the Latent Dirichlet Allocation (LDA) results..." },
  '/market': { title: "Market Correlation", description: "Overlays sentiment data against real-time stock tickers..." },
  '/etl': { title: "ETL Status Logs", description: "Technical telemetry for the backend pipeline..." }
};

export default function Header() {
  const router = useRouter();
  const [showModal, setShowModal] = useState(false);
  const currentView = VIEW_CONFIG[router.pathname] || VIEW_CONFIG['/'];

  const handleRunPipeline = () => {
    console.log("Triggering FastAPI Pipeline...");
  };

  return (
    <>
      {/* HEADER: bg-navy-800 (Seamless with Layout) */}
      <header className="h-24 sticky top-0 z-40 bg-navy-800 flex flex-col justify-between px-8 transition-all">

        <div className="flex-1 flex items-center justify-between">

            {/* LEFT: Title Area */}
            <div className="flex items-center gap-6">

                {/* Help Button */}
                <button
                    onClick={() => setShowModal(true)}
                    className="h-10 w-10 bg-navy-700 border border-white/10 rounded-full flex items-center justify-center
                               text-slate-400 hover:text-white hover:border-emerald-500 hover:shadow-[0_0_15px_rgba(249,115,22,0.4)]
                               transition-all duration-200 group"
                    title="View Page Context"
                >
                    <HelpCircle size={20} className="group-hover:scale-110 transition-transform" />
                </button>

                <div className="flex flex-col">
                    <h2 className="text-2xl font-black text-white tracking-tight leading-none">
                        {currentView.title}
                    </h2>
                    <span className="text-[10px] text-slate-500 font-bold uppercase tracking-widest mt-1 ml-0.5">
                        Thesis.io / {router.pathname === '/' ? 'Dashboard' : router.pathname.replace('/', '')}
                    </span>
                </div>

                {/* Vertical Divider */}
                <div className="h-10 w-px bg-white/10 mx-2"></div>

                {/* Status Indicator */}
                <div className="flex items-center gap-3 bg-navy-900 px-4 py-2 rounded-lg border border-white/5 shadow-inner">
                    <div className="relative flex h-2.5 w-2.5">
                        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-500 opacity-75"></span>
                        <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-emerald-500"></span>
                    </div>
                    <span className="text-xs font-bold text-slate-400 tracking-wide">
                        API: <span className="text-emerald-400">ONLINE</span>
                    </span>
                </div>
            </div>

            {/* RIGHT: Tools Area */}
            <div className="flex items-center gap-4">

                {/* Search */}
                <div className="relative group">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500 group-focus-within:text-emerald-400 transition-colors" />
                    <input
                        type="text"
                        placeholder="Search data..."
                        className="pl-10 pr-12 py-2.5 w-72 bg-navy-900 border border-white/10 rounded-xl text-sm text-slate-200 font-medium
                                   placeholder:text-slate-600 focus:outline-none focus:border-emerald-500/50 focus:ring-1 focus:ring-emerald-500/20 transition-all shadow-inner"
                    />
                    <div className="absolute right-3 top-1/2 -translate-y-1/2 text-[10px] text-slate-500 font-bold border border-white/10 px-1.5 py-0.5 rounded bg-navy-800">
                        ⌘K
                    </div>
                </div>

                <div className="pl-2">
                    <Button
                        variant="primary"
                        size="md"
                        icon={Play}
                        onClick={handleRunPipeline}
                        className="shadow-lg shadow-emerald-500/20 hover:shadow-emerald-500/40 border border-emerald-400/20"
                    >
                        Run Pipeline
                    </Button>
                </div>
            </div>
        </div>

        {/* LASER LINE: Matches Sidebar Border (Orange/Emerald-500) */}
        <div className="h-[2px] w-full bg-gradient-to-r from-emerald-500 via-emerald-400 to-transparent opacity-100 shadow-[0_0_15px_rgba(249,115,22,0.5)]"></div>

      </header>

      {/* --- MODAL (Dark Mode) --- */}
      {showModal && (
        <div className="fixed inset-0 z-[60] flex items-center justify-center p-4">
          <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={() => setShowModal(false)}></div>
          <div className="relative bg-navy-800 rounded-xl shadow-2xl w-full max-w-lg overflow-hidden animate-in zoom-in-95 duration-200 border border-white/10">
            <div className="bg-navy-900/50 p-6 border-b border-white/5 flex justify-between items-start">
                 <div>
                    <h3 className="text-xl font-bold text-white">View Context</h3>
                    <p className="text-xs text-emerald-400 font-bold font-mono mt-1">{currentView.title}</p>
                 </div>
                 <button onClick={() => setShowModal(false)}><X size={24} className="text-slate-500 hover:text-white" /></button>
            </div>
            <div className="p-8 text-slate-300 text-sm leading-relaxed font-medium">
               {currentView.description}
            </div>
            <div className="p-4 bg-navy-900/50 border-t border-white/5 flex justify-end">
               <Button variant="secondary" size="sm" onClick={() => setShowModal(false)}>Close</Button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
