import React, { useMemo, useState } from 'react';
import clsx from 'clsx';
import Sidebar from './Sidebar';
import Header from './Header';

// ---- helpers ----
function toISODate(d) {
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function getDefaultDateRange(daysBack = 7) {
  const end = new Date();
  const start = new Date();
  start.setDate(end.getDate() - (daysBack - 1));
  return { startDate: toISODate(start), endDate: toISODate(end) };
}

export default function Layout({ children }) {
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);

  // ✅ Default range for range-based views (Market / Overview)
  const defaultRange = useMemo(() => getDefaultDateRange(7), []);

  // --------------------------------------------------
  // 1. MARKET STATE (UNCHANGED)
  // --------------------------------------------------
  const [marketState, setMarketState] = useState({
    startDate: '2025-10-05',
    endDate: '2025-12-04',
    selectedTicker: 'NVDA',
  });

  const updateMarketState = (updates) => {
    setMarketState((prev) => ({ ...prev, ...updates }));
  };

  // --------------------------------------------------
  // 2. ✅ TOPIC STATE (SINGLE DAY — FIXED DEFAULT)
  // --------------------------------------------------
  const [topicState, setTopicState] = useState({
    // 🔒 FIX: Always initialize Topic View at Oct 1, 2025
    focusDate: '2025-10-01',
  });

  const updateTopicState = (updates) => {
    setTopicState((prev) => ({ ...prev, ...updates }));
  };

  return (
    <div className="min-h-screen bg-navy-800 cursor-emerald text-slate-200 font-sans selection:bg-emerald-500/30 flex">
      <Sidebar
        isCollapsed={isSidebarCollapsed}
        toggleSidebar={() => setIsSidebarCollapsed(!isSidebarCollapsed)}
        // Market
        marketState={marketState}
        updateMarketState={updateMarketState}
        // Topics
        topicState={topicState}
        updateTopicState={updateTopicState}
      />

      <div
        className={clsx(
          'flex-1 flex flex-col min-h-screen transition-all duration-300 ease-in-out',
          isSidebarCollapsed ? 'ml-20' : 'ml-72'
        )}
      >
        <Header />

        <main className="flex-1 p-6 overflow-y-auto bg-navy-800">
          <div className="w-full space-y-6">
            {React.Children.map(children, (child) => {
              if (React.isValidElement(child)) {
                return React.cloneElement(child, {
                  // Market
                  marketState,
                  updateMarketState,
                  // Topics
                  topicState,
                  updateTopicState,
                });
              }
              return child;
            })}
          </div>
        </main>
      </div>
    </div>
  );
}
