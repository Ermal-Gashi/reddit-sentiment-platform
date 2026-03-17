import { useState, useEffect, useMemo } from 'react';

const API_ROOT = "http://127.0.0.1:8000/api";

const API_ENDPOINTS = {

    candles: (ticker, start, end) =>
        `${API_ROOT}/marketview/candles/${ticker}?start=${start}&end=${end}`,


    marketKpis: (start, end) =>
        `${API_ROOT}/marketview/kpis?start=${start}&end=${end}`,
};

async function fetchJsonOrDie(url) {
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`HTTP Error! Status: ${response.status}`);
    }
    return response.json();
}

// 1. Accept 'props' to receive state from Layout/Sidebar
export function useMarketViewData(props = {}) {


    const incomingState = props.marketState || {};
    const startDate = incomingState.startDate || "2025-10-05";
    const endDate = incomingState.endDate || "2025-12-04";
    const tickerSelection = incomingState.selectedTicker || "NVDA";


    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [heroData, setHeroData] = useState([]); // <--- Stores the Line Chart Data


    useEffect(() => {
        const fetchHero = async () => {
            if (!tickerSelection) return;

            setLoading(true);
            setError(null);

            try {
                // Call the Backend Candle Endpoint
                const url = API_ENDPOINTS.candles(tickerSelection, startDate, endDate);
                const json = await fetchJsonOrDie(url);

                // Handle the response (Backend returns { candles: [...] })
                const candles = json.candles || [];

                if (candles.length === 0) {
                    console.warn("API returned empty candles for", tickerSelection);
                }

                setHeroData(candles);

            } catch (err) {
                console.error("HERO CHART ERROR:", err);
                setHeroData([]);
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        fetchHero();
    }, [tickerSelection, startDate, endDate]);

    return {
        loading,
        error,
        heroData,          // <--- The Array needed for the Chart
        tickerSelection,   // <--- The Name needed for the Header
        startDate,
        endDate
    };
}
