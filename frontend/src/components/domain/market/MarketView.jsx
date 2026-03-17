import React, { useState } from 'react';
import { useMarketViewData } from '../../../hooks/useMarketViewData';
import ChartContainer from '../../common/ChartContainer';

// Modules
import MarketHeroChart from './modules/MarketHeroChart';
import MarketKPIs from './modules/MarketKPIs';
import MarketCorrelation from './modules/MarketCorrelation';
import MarketDivergence from './modules/MarketDivergence';

// --- HEADER TOGGLE BUTTON ---
const HeaderToggle = ({ label, active, onClick }) => (
    <button
        onClick={onClick}
        className={`
            px-3 py-1 text-[9px] font-bold uppercase tracking-wider transition-all rounded-[4px] border
            ${active 
                ? 'bg-emerald-500/10 border-emerald-500/20 text-emerald-400 shadow-[0_0_10px_-3px_rgba(16,185,129,0.3)]' 
                : 'bg-transparent border-transparent text-slate-500 hover:text-slate-300 hover:bg-white/5'
            }
        `}
    >
        {label}
    </button>
);

export default function MarketView(props) {
  const {
    loading,
    heroData,
    tickerSelection,
    error,
    startDate,
    endDate
  } = useMarketViewData(props);

  // STATE FOR CHART MODES
  const [heroMode, setHeroMode] = useState('price'); // 'price' | 'sentiment' | 'interaction'
  const [correlationMode, setCorrelationMode] = useState('correlation'); // 'correlation' | 'zscore'

  return (
    <div className="space-y-6 animate-in fade-in duration-500">

      {/* 1. ERROR BANNER */}
      {error && (
        <div className="bg-rose-500/10 border border-rose-500/50 text-rose-200 px-4 py-3 rounded-lg text-sm font-mono flex items-center gap-3">
          <div className="w-2 h-2 rounded-full bg-rose-500 animate-pulse"></div>
          API ERROR: {error}
        </div>
      )}

      {/* 2. HERO CHART */}
      <ChartContainer
          title={`HISTORICAL PERFORMANCE: ${tickerSelection || "SELECT TICKER"} • ${startDate} — ${endDate}`}
          subtitle="Daily Intervals"
          infoText="This chart visualizes the correlation between daily closing price and aggregated sentiment scores extracted from Reddit discussions."
          height="h-[500px]"
          // --- FIX: BUTTONS RESTORED HERE ---
          controls={
            <div className="flex bg-navy-900/50 p-0.5 rounded-md border border-white/5 gap-0.5">
                <HeaderToggle label="Price" active={heroMode === 'price'} onClick={() => setHeroMode('price')} />
                <HeaderToggle label="Sentiment" active={heroMode === 'sentiment'} onClick={() => setHeroMode('sentiment')} />
                <HeaderToggle label="Volume" active={heroMode === 'interaction'} onClick={() => setHeroMode('interaction')} />
            </div>
          }
      >
        <MarketHeroChart
            data={heroData}
            loading={loading}
            viewMode={heroMode} // Pass state down to chart
        />
      </ChartContainer>

      {/* 3. KPIS */}
      <MarketKPIs data={heroData} loading={loading} />

      {/* 4. SECONDARY CHARTS */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 pt-2">

        {/* LEFT: CORRELATION */}
        <ChartContainer
            title={`STATISTICAL REGIME: ${tickerSelection || "NONE"} • ${startDate} — ${endDate}`}
            subtitle={correlationMode === 'correlation' ? "7-Day Rolling Pearson Correlation" : "Z-Score Anomaly Detection"}
            infoText="Measures statistical relationship."
            height="h-80"
            controls={
                <div className="flex bg-navy-900/50 p-0.5 rounded-md border border-white/5 gap-0.5">
                    <HeaderToggle label="Rolling Corr" active={correlationMode === 'correlation'} onClick={() => setCorrelationMode('correlation')} />
                    <HeaderToggle label="Z-Score" active={correlationMode === 'zscore'} onClick={() => setCorrelationMode('zscore')} />
                </div>
            }
        >
            <MarketCorrelation
                data={heroData}
                loading={loading}
                mode={correlationMode}
            />
        </ChartContainer>

        {/* RIGHT: DIVERGENCE */}
        <ChartContainer
            title={`MOMENTUM DIVERGENCE: ${tickerSelection || "NONE"} • ${startDate} — ${endDate}`}
            subtitle="Daily Velocity Delta (Price - Sentiment)"
            infoText="Measures daily speed difference. \n• Teal: Price moved faster than sentiment. \n• Pink: Sentiment moved faster than price."
            height="h-80"
        >
            <MarketDivergence
                data={heroData}
                loading={loading}
                tickerName={tickerSelection}
            />
        </ChartContainer>
      </div>

    </div>
  );
}
