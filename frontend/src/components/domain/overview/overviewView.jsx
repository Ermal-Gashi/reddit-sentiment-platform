import React from 'react';
import Button from '../../common/Button';
import { useOverviewData } from '../../../hooks/useOverviewData';

// Modules
import OverviewKPIs from './modules/OverviewKPIs';
import OverviewCumulativeGrowth from './modules/OverviewCumulativeGrowth';
import OverviewSystemHealth from './modules/OverviewSystemHealth';
// NEW MODULES
import OverviewHourlyActivity from './modules/OverviewHourlyActivity';
import OverviewTopTickers from './modules/OverviewTopTickers';

export default function OverviewView() {
  const {
    loading,
    kpiItems,
    cumulativeData,
    systemStatus,
    hourlyData,  // New Data
    topicData    // Used for Top Tickers
  } = useOverviewData();

  return (
    <div className="space-y-6">

      <div className="flex justify-end">
         <Button variant="secondary" size="xs">Download Report</Button>
      </div>

      <OverviewKPIs kpiItems={kpiItems} loading={loading} />

      {/* Row 1: Growth + Health */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
         <div className="lg:col-span-2">
            {/* The updated OverviewCumulativeGrowth handles its own internal mode switching */}
            <OverviewCumulativeGrowth data={cumulativeData} loading={loading} />
         </div>
         <div className="lg:col-span-1">
            <OverviewSystemHealth data={systemStatus} loading={loading} />
         </div>
      </div>

      {/* Row 2: The Two Bar Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

         {/* Left: Hourly Rhythm (Orange) */}
         <OverviewHourlyActivity
            data={hourlyData}
            loading={loading}
         />

         {/* Right: Top 10 Tickers (Cyan) */}
         <OverviewTopTickers
            data={topicData}
            loading={loading}
         />

      </div>

    </div>
  );
}
