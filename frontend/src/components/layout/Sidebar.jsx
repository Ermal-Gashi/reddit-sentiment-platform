// components/layout/Sidebar.jsx
import React, { useState, useEffect } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/router';
import {
  LayoutDashboard, TrendingUp, MessageSquare,
  Calendar, Filter, ChevronLeft, Menu, Activity, Settings2, Search
} from 'lucide-react';
import clsx from 'clsx';

const MENU_ITEMS = [
  { name: 'Overview', path: '/', icon: LayoutDashboard },
  { name: 'Topics', path: '/topics', icon: MessageSquare },
  { name: 'Market', path: '/market', icon: TrendingUp },
];

// Simple fetcher for the sidebar list
const fetchTickerList = async (start, end) => {
  try {
    const res = await fetch(
      `http://127.0.0.1:8000/api/market/landscape?start=${start}&end=${end}&sort=price`
    );
    if (!res.ok) return [];
    const json = await res.json();
    return json.landscape || [];
  } catch (e) {
    console.error("Sidebar Fetch Error", e);
    return [];
  }
};

export default function Sidebar({
  isCollapsed,
  toggleSidebar,

  marketState = {},
  updateMarketState = () => {},

  // ✅ TOPIC STATE (Single Day)
  topicState = {},
  updateTopicState = () => {}
}) {
  const router = useRouter();
  const [searchTerm, setSearchTerm] = useState("");
  const [sidebarTickerList, setSidebarTickerList] = useState([]);

  // --- FETCH TICKER LIST (Market only) ---
  useEffect(() => {
    if (
      router.pathname === '/market' &&
      marketState.startDate &&
      marketState.endDate
    ) {
      fetchTickerList(marketState.startDate, marketState.endDate)
        .then(setSidebarTickerList);
    }
  }, [router.pathname, marketState.startDate, marketState.endDate]);

  // Filter logic
  const filteredTickers = sidebarTickerList
    .filter(t => (t.ticker || "").toLowerCase().includes(searchTerm.toLowerCase()))
    .sort((a, b) => (b.mentions || 0) - (a.mentions || 0))
    .slice(0, 100);

  // --- RENDERER ---
  const renderViewSpecificFilters = () => {
    switch (router.pathname) {

      // ✅ TOPICS: SINGLE "FOCUS DAY" PICKER
      case '/topics':
        return (
          <div className="flex flex-col h-full min-h-0">

            {/* 1. FOCUS DAY (Fixed Height - No Scroll) */}
            <div className="shrink-0 mb-6">
              <div className="flex items-center gap-2 text-[10px] font-bold text-emerald-400 uppercase tracking-widest mb-4">
                <Calendar size={12} /> Focus Day
              </div>

              <div className="space-y-4">
                <div className="relative group">
                  <label className="absolute -top-2 left-3 bg-navy-800 px-1 text-[9px] font-bold text-slate-500 group-focus-within:text-emerald-500 transition-colors z-10">
                    DATE
                  </label>
                  <input
                    type="date"
                    value={topicState.focusDate || ''}
                    onChange={(e) => updateTopicState({ focusDate: e.target.value })}
                    onClick={(e) => {
                      try { e.target.showPicker(); }
                      catch (err) { console.warn("Picker not supported", err); }
                    }}
                    style={{ colorScheme: 'dark' }}
                    className="w-full bg-navy-900 border border-white/10 rounded-xl px-4 py-3 text-xs text-white font-mono shadow-sm focus:border-emerald-500/50 focus:ring-1 focus:ring-emerald-500/20 outline-none transition-all uppercase accent-emerald-500 cursor-emerald"
                  />
                </div>
              </div>
            </div>

            {/* 2. TOPIC CONTEXT (Placeholder) */}
            <div className="flex-1 flex flex-col min-h-0">
              <div className="flex items-center gap-2 text-[10px] font-bold text-emerald-400 uppercase tracking-widest mb-3 shrink-0">
                <Filter size={12} /> Topic Context
              </div>

              <div className="bg-navy-900 rounded-xl border border-white/10 flex flex-col flex-1 min-h-0 overflow-hidden shadow-inner relative">
                <div className="p-4 text-[11px] text-slate-500 leading-relaxed">
                  Topic controls will live here (keyword search, junk toggle, min doc count, window size).
                </div>

                <div className="absolute bottom-0 left-0 right-0 h-4 bg-gradient-to-t from-navy-900 to-transparent pointer-events-none"></div>
              </div>
            </div>

          </div>
        );

      case '/market':
        return (
          <div className="flex flex-col h-full min-h-0">

            {/* 1. DATE PICKER SECTION (Fixed Height - No Scroll) */}
            <div className="shrink-0 mb-6">
              <div className="flex items-center gap-2 text-[10px] font-bold text-emerald-400 uppercase tracking-widest mb-4">
                <Calendar size={12} /> Time Horizon
              </div>

              <div className="space-y-4">
                {/* START DATE */}
                <div className="relative group">
                  <label className="absolute -top-2 left-3 bg-navy-800 px-1 text-[9px] font-bold text-slate-500 group-focus-within:text-emerald-500 transition-colors z-10">
                    START DATE
                  </label>
                  <input
                    type="date"
                    value={marketState.startDate || ''}
                    onChange={(e) => updateMarketState({ startDate: e.target.value })}
                    onClick={(e) => {
                      try { e.target.showPicker(); }
                      catch (err) { console.warn("Picker not supported", err); }
                    }}
                    style={{ colorScheme: 'dark' }}
                    className="w-full bg-navy-900 border border-white/10 rounded-xl px-4 py-3 text-xs text-white font-mono shadow-sm focus:border-emerald-500/50 focus:ring-1 focus:ring-emerald-500/20 outline-none transition-all uppercase accent-emerald-500 cursor-emerald"
                  />
                </div>

                {/* END DATE */}
                <div className="relative group">
                  <label className="absolute -top-2 left-3 bg-navy-800 px-1 text-[9px] font-bold text-slate-500 group-focus-within:text-emerald-500 transition-colors z-10">
                    END DATE
                  </label>
                  <input
                    type="date"
                    value={marketState.endDate || ''}
                    onChange={(e) => updateMarketState({ endDate: e.target.value })}
                    onClick={(e) => {
                      try { e.target.showPicker(); }
                      catch (err) { console.warn("Picker not supported", err); }
                    }}
                    style={{ colorScheme: 'dark' }}
                    className="w-full bg-navy-900 border border-white/10 rounded-xl px-4 py-3 text-xs text-white font-mono shadow-sm focus:border-emerald-500/50 focus:ring-1 focus:ring-emerald-500/20 outline-none transition-all uppercase accent-emerald-500 cursor-emerald"
                  />
                </div>
              </div>
            </div>

            {/* 2. TICKER LIST SECTION (Flex Grow - This one scrolls) */}
            <div className="flex-1 flex flex-col min-h-0">
              <div className="flex items-center gap-2 text-[10px] font-bold text-emerald-400 uppercase tracking-widest mb-3 shrink-0">
                <Filter size={12} /> Asset Context
              </div>

              <div className="bg-navy-900 rounded-xl border border-white/10 flex flex-col flex-1 min-h-0 overflow-hidden shadow-inner relative">
                <div className="p-3 border-b border-white/5 relative shrink-0">
                  <Search size={14} className="absolute left-5 top-5 text-slate-500" />
                  <input
                    type="text"
                    placeholder="Search Assets..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="w-full bg-navy-800 rounded-lg pl-9 pr-3 py-2 text-xs text-white border border-transparent focus:border-emerald-500/50 outline-none placeholder:text-slate-600 transition-all cursor-emerald"
                  />
                </div>

                <div className="overflow-y-auto flex-1 p-2 custom-scrollbar cursor-emerald">
                  {filteredTickers.length === 0 ? (
                    <div className="text-center py-8 text-[10px] text-slate-600">
                      No assets found for this range.
                    </div>
                  ) : (
                    filteredTickers.map((item) => (
                      <button
                        key={item.ticker}
                        onClick={() => updateMarketState({ selectedTicker: item.ticker })}
                        className={clsx(
                          "w-full flex justify-between items-center px-3 py-2.5 rounded-lg mb-1 transition-all text-left group cursor-emerald",
                          marketState.selectedTicker === item.ticker
                            ? "bg-emerald-500/10 border border-emerald-500/30 shadow-sm"
                            : "hover:bg-white/5 border border-transparent"
                        )}
                      >
                        <div>
                          <span className={clsx(
                            "text-xs font-bold block",
                            marketState.selectedTicker === item.ticker
                              ? "text-emerald-400"
                              : "text-slate-300 group-hover:text-white"
                          )}>
                            {item.ticker}
                          </span>
                          <span className="text-[9px] text-slate-500 font-mono">
                            {(item.mentions || 0).toLocaleString()} vol
                          </span>
                        </div>

                        <span className={clsx(
                          "text-[9px] font-mono font-bold px-1.5 py-0.5 rounded",
                          (item.price_change || 0) >= 0
                            ? "text-brown-400 bg-brown-500/10"
                            : "text-rose-400 bg-rose-500/10"
                        )}>
                          {(item.price_change || 0) > 0 ? "+" : ""}
                          {((item.price_change || 0) * 100).toFixed(1)}%
                        </span>
                      </button>
                    ))
                  )}
                </div>

                <div className="absolute bottom-0 left-0 right-0 h-4 bg-gradient-to-t from-navy-900 to-transparent pointer-events-none"></div>
              </div>
            </div>

          </div>
        );

      default:
        return <div className="text-slate-500 text-xs">Global filters...</div>;
    }
  };

  return (
    <>
      <style jsx global>{`
        ::-webkit-calendar-picker-indicator {
          filter: invert(1) opacity(0.5);
          cursor: pointer;
        }
      `}</style>

      <aside className={clsx(
        "h-screen flex flex-col fixed left-0 top-0 z-50 transition-all duration-300 bg-navy-900 border-r border-emerald-500/30 shadow-[4px_0_24px_-2px_rgba(249,115,22,0.15)] cursor-emerald",
        isCollapsed ? "w-20" : "w-72"
      )}>

        {/* BRANDING */}
        <div className="h-24 flex items-center px-6 border-b border-white/5 shrink-0">
          <div className="flex items-center gap-3 text-emerald-500">
            <div className="p-2 bg-emerald-500/10 rounded-lg border border-emerald-500/20">
              <Activity size={20} />
            </div>
            {!isCollapsed && (
              <span className="font-bold text-lg text-slate-100 tracking-tight">
                Thesis<span className="text-emerald-500">.io</span>
              </span>
            )}
          </div>
        </div>

        {/* NAVIGATION */}
        <div className="pt-6 px-3 flex flex-col gap-1 shrink-0">
          <nav className="space-y-1">
            {MENU_ITEMS.map((item) => {
              const isActive = router.pathname === item.path;
              return (
                <Link
                  key={item.path}
                  href={item.path}
                  className={clsx(
                    "flex items-center gap-3 px-3 py-3 rounded-xl transition-all duration-200 group relative overflow-hidden cursor-emerald",
                    isActive
                      ? "bg-white/5 text-white shadow-lg shadow-emerald-500/10 ring-1 ring-emerald-500/50"
                      : "text-slate-400 hover:text-slate-100 hover:bg-white/5"
                  )}
                >
                  {isActive && (
                    <div className="absolute left-0 top-0 bottom-0 w-1 bg-emerald-500 shadow-[0_0_10px_rgba(249,115,22,0.6)]" />
                  )}
                  <item.icon
                    size={20}
                    className={clsx(
                      "shrink-0 transition-colors",
                      isActive ? "text-emerald-500" : "group-hover:text-emerald-400"
                    )}
                  />
                  {!isCollapsed && (
                    <span className={clsx("text-sm font-medium", isActive && "font-semibold")}>
                      {item.name}
                    </span>
                  )}
                </Link>
              );
            })}
          </nav>
        </div>

        {/* CONTROL DECK */}
        {!isCollapsed && (
          <div className="mx-3 mt-8 bg-navy-800 rounded-2xl border border-white/5 p-4 relative overflow-hidden flex-1 min-h-0 flex flex-col mb-4">
            <div className="flex items-center gap-2 mb-4 text-slate-400 shrink-0">
              <Settings2 size={14} />
              <span className="text-xs font-bold uppercase tracking-wider">Control Deck</span>
            </div>

            <div className="animate-in slide-in-from-bottom-2 fade-in duration-300 flex-1 min-h-0">
              {renderViewSpecificFilters()}
            </div>
          </div>
        )}

        {/* COLLAPSE BUTTON */}
        <button
          onClick={toggleSidebar}
          className="h-12 border-t border-white/5 flex items-center justify-center text-slate-500 hover:text-white hover:bg-white/5 transition-colors shrink-0 cursor-emerald"
        >
          {isCollapsed ? <Menu size={18} /> : <ChevronLeft size={18} />}
        </button>
      </aside>
    </>
  );
}
