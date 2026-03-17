import { useState, useEffect, useMemo } from 'react';
import {
  MessageSquareText,
  GitBranch,
  Activity,
  TrendingUp
} from 'lucide-react';

export function useOverviewData() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // 1. CLEAN FETCH
  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch("http://127.0.0.1:8000/api/overview");
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        const jsonData = await response.json();
        setData(jsonData);
        setLoading(false);
      } catch (err) {
        console.error("API Fetch Error:", err);
        setError(err);
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  // 2. MAP KPI ITEMS
  const kpiItems = useMemo(() => {
    if (!data || !data.kpis) return [];

    const kpis = data.kpis;
    const avgDaily = kpis.avg_daily_comments_global ?? 0;
    const topTickerObj = kpis.top_ticker_all_time || {};

    return [
      {
        label: "Total Comments",
        icon: MessageSquareText,
        value: kpis.total_comments?.toLocaleString() ?? "--",
        trend: 12,
        trendLabel: "All Time"
      },
      {
        label: "Total Threads",
        icon: GitBranch,
        value: kpis.total_threads?.toLocaleString() ?? "--",
        trend: 0,
        trendLabel: "Active Discussions"
      },
      {
        label: "Avg Daily Volume",
        icon: Activity,
        value: Math.round(avgDaily).toLocaleString(),
        trend: 4.2,
        trendLabel: "Global Avg"
      },
      {
        label: "Top Ticker",
        icon: TrendingUp,
        value: topTickerObj.ticker ?? "--",
        trend: 0,
        trendLabel: topTickerObj.count ? `${topTickerObj.count.toLocaleString()} mentions` : "All Time"
      },
    ];
  }, [data]);

  // 3. MAP CUMULATIVE DATA (Split Growth)
  const cumulativeData = useMemo(() => {
    if (!data || !data.cumulative_comments) return [];
    return data.cumulative_comments.map(item => ({
      date: new Date(item.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      company: item.ticker,
      market: item.market
    }));
  }, [data]);

  // 4. MAP SYSTEM HEALTH (Multi-Color)
  const systemStatus = useMemo(() => {
    if (!data || !data.ingest_health) return [];
    const h = data.ingest_health;
    const isHealthy = h.dataset_health_score > 80;

    const formatTime = (isoString) => {
        if (!isoString) return "Never";
        return new Date(isoString).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    };

    return [
      { id: 'ingest', name: "Ingestion Engine", status: h.last_ingest_ts ? "ONLINE" : "OFFLINE", detail: `Last activity: ${formatTime(h.last_ingest_ts)}`, color: h.last_ingest_ts ? "orange" : "rose" },
      { id: 'volume', name: "Daily Throughput", status: h.comments_today > 0 ? "ACTIVE" : "IDLE", detail: `${h.comments_today.toLocaleString()} comments processed`, color: h.comments_today > 0 ? "cyan" : "slate" },
      { id: 'continuity', name: "Data Continuity", status: h.missing_days === 0 ? "OPTIMAL" : "DEGRADED", detail: h.missing_days === 0 ? "No data gaps detected" : `${h.missing_days} missing days`, color: h.missing_days === 0 ? "purple" : "rose" },
      { id: 'score', name: "Pipeline Health", status: `${h.dataset_health_score}%`, detail: `System Status: ${h.status.toUpperCase()}`, color: isHealthy ? "teal" : "rose" }
    ];
  }, [data]);

  // 5. MAP HOURLY ACTIVITY (New)
  const hourlyData = useMemo(() => {
    if (!data || !data.hourly_activity) return [];

    return data.hourly_activity.map(item => {
      const hour = item.hour;
      // Convert 0-23 to readable "12 AM", "2 PM" etc.
      const label = hour === 0 ? '12 AM' : hour < 12 ? `${hour} AM` : hour === 12 ? '12 PM' : `${hour - 12} PM`;

      return {
        time: label,
        avg: item.avg
      };
    });
  }, [data]);

  // 6. MAP TOP 10 TICKERS (New - Sliced)
  const topicData = useMemo(() => {
    const rawList = data?.top_20_tickers || data?.top_10_tickers || [];
    // Slice to top 10 for the leaderboard chart
    return rawList.slice(0, 10);
  }, [data]);

  return {
    loading,
    error,
    kpiItems,
    cumulativeData,
    systemStatus,
    hourlyData, // For the Bottom-Left Bar Chart
    topicData,  // For the Bottom-Right Bar Chart
  };
}