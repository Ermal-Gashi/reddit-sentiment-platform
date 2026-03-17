// src/hooks/useTopicViewData.js
import { useEffect, useMemo, useRef, useState } from "react";


const API_BASE = "http://127.0.0.1:8000/api";

// --------------------------------------------------
// Helpers
// --------------------------------------------------
function toISODate(d) {
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function normalizeTopicRow(row) {
  const topicId = row?.topic_id ?? row?.topicId ?? row?.id;
  return {
    date_utc: row?.date_utc ?? row?.date ?? null,
    topic_id: Number(topicId),
    topic_title: row?.topic_title ?? row?.title ?? "",
    topic_keywords: row?.topic_keywords ?? row?.keywords ?? [],
    doc_count: row?.doc_count ?? row?.docCount ?? 0,
    is_junk: row?.is_junk ?? row?.isJunk ?? false,
    metrics: row?.metrics ?? {},
    model_version: row?.model_version ?? row?.modelVersion ?? null,
  };
}

function extractDailyTopicsPayload(json) {
  if (Array.isArray(json)) {
    return { focus_date: null, topics: json };
  }

  const focus_date =
    json?.focus_date ??
    json?.focusDate ??
    json?.date ??
    json?.day ??
    null;

  const topics =
    json?.topics ??
    json?.daily_topics ??
    json?.rows ??
    json?.data ??
    json?.items ??
    [];

  return {
    focus_date,
    topics: Array.isArray(topics) ? topics : [],
  };
}

async function safeFetchJson(url, { signal } = {}) {
  const res = await fetch(url, { signal });
  if (!res.ok) {
    const text = await res.text();
    const err = new Error(text || res.statusText);
    err.status = res.status;
    throw err;
  }
  return res.json();
}

// --------------------------------------------------
// Hook
// --------------------------------------------------
export function useTopicViewData(props = {}) {
  const topicState = props.topicState || {};
  const updateTopicState = props.updateTopicState || (() => {});

  const focusDate = topicState.focusDate || "";
  const limit = topicState.limit ?? 15;

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const [topics, setTopics] = useState([]);
  const [selectedTopicId, _setSelectedTopicId] = useState(null);

  // Detail payloads
  const [topicSummary, setTopicSummary] = useState(null);
  const [topicEvolution, setTopicEvolution] = useState(null);
  const [topicHeatmap, setTopicHeatmap] = useState(null);
  const [topicSankey, setTopicSankey] = useState(null);

  //  NEW — Representative Sentences
  const [topicRepresentatives, setTopicRepresentatives] = useState(null);

  const reqIdRef = useRef(0);

  // --------------------------------------------------
  // 0) Initialize focus date
  // --------------------------------------------------
  useEffect(() => {
    if (!topicState.focusDate) {
      updateTopicState({ focusDate: toISODate(new Date()) });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // --------------------------------------------------
  // 1) Fetch DAILY TOPICS
  // --------------------------------------------------
  useEffect(() => {
    if (!focusDate) return;

    const reqId = ++reqIdRef.current;
    const controller = new AbortController();

    const run = async () => {
      setLoading(true);
      setError(null);

      try {
        const qs = new URLSearchParams({
          start: focusDate,
          end: focusDate,
          limit: String(limit),
          focus_date: focusDate,
        });

        const url = `${API_BASE}/topics/daily?${qs.toString()}`;
        const json = await safeFetchJson(url, {
          signal: controller.signal,
        });

        if (reqId !== reqIdRef.current) return;

        const { focus_date: apiFocusDate, topics: rawTopics } =
          extractDailyTopicsPayload(json);

        const normalized = rawTopics.map(normalizeTopicRow);
        setTopics(normalized);

        if (apiFocusDate && apiFocusDate !== focusDate) {
          updateTopicState({ focusDate: apiFocusDate });
        }

        const exists = normalized.some(
          (t) => Number(t.topic_id) === Number(selectedTopicId)
        );

        if (!exists) {
          const first = normalized[0]?.topic_id ?? null;
          _setSelectedTopicId(first != null ? Number(first) : null);
        }

        setLoading(false);
      } catch (e) {
        if (e?.name === "AbortError") return;
        if (reqId !== reqIdRef.current) return;

        console.error("Topic daily fetch error:", e);
        setError(
          `Daily topics failed: ${e?.status ?? ""} ${String(
            e?.message || e
          )}`
        );
        setTopics([]);
        setLoading(false);
      }
    };

    run();
    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focusDate, limit]);

  // --------------------------------------------------
  // 2) Fetch TOPIC SUMMARY
  // --------------------------------------------------
  useEffect(() => {
    if (!focusDate || !selectedTopicId) return;

    const reqId = ++reqIdRef.current;
    const controller = new AbortController();

    const run = async () => {
      try {
        const qs = new URLSearchParams({
          start: focusDate,
          end: focusDate,
          focus_date: focusDate,
        });

        const url = `${API_BASE}/topics/${encodeURIComponent(
          selectedTopicId
        )}/summary?${qs.toString()}`;

        const json = await safeFetchJson(url, {
          signal: controller.signal,
        });

        if (reqId !== reqIdRef.current) return;
        setTopicSummary(json);
      } catch (e) {
        if (e?.name === "AbortError") return;
        if (reqId !== reqIdRef.current) return;

        console.warn(
          "Topic summary failed (non-fatal):",
          e?.status,
          e?.message
        );
        setTopicSummary(null);
      }
    };

    run();
    return () => controller.abort();
  }, [focusDate, selectedTopicId]);

  // --------------------------------------------------
  // 3) Optional + Representatives endpoints
  // --------------------------------------------------
  useEffect(() => {
    if (!focusDate || !selectedTopicId) return;

    const controller = new AbortController();

    const qs = new URLSearchParams({
      start: focusDate,
      end: focusDate,
      focus_date: focusDate,
    });

    const tryFetch = async (path, setter) => {
      try {
        const url = `${API_BASE}${path}?${qs.toString()}`;
        const json = await safeFetchJson(url, {
          signal: controller.signal,
        });
        setter(json);
      } catch (e) {
        if (e?.name === "AbortError") return;
        setter(null);
      }
    };

    //  Representative Sentences
    tryFetch(
      `/topics/${encodeURIComponent(selectedTopicId)}/representatives`,
      setTopicRepresentatives
    );

    // Optional / legacy modules
    tryFetch(
      `/topics/${encodeURIComponent(selectedTopicId)}/evolution`,
      setTopicEvolution
    );
    tryFetch(
      `/topics/${encodeURIComponent(selectedTopicId)}/heatmap`,
      setTopicHeatmap
    );
    tryFetch(
      `/topics/${encodeURIComponent(selectedTopicId)}/sankey`,
      setTopicSankey
    );

    return () => controller.abort();
  }, [focusDate, selectedTopicId]);

  // --------------------------------------------------
  // 4) Selected Topic (merged view)
  // --------------------------------------------------
  const selectedTopic = useMemo(() => {
    const base =
      topics.find((t) => Number(t.topic_id) === Number(selectedTopicId)) ||
      null;

    if (!base && topicSummary) return normalizeTopicRow(topicSummary);

    if (base && topicSummary) {
      return {
        ...base,
        ...topicSummary,
      };
    }

    return base;
  }, [topics, selectedTopicId, topicSummary]);

  // --------------------------------------------------
  // 5) Date range contract
  // --------------------------------------------------
  const dateRange = useMemo(() => {
    return {
      focusDate,
      start: focusDate,
      end: focusDate,
      days: [focusDate],
    };
  }, [focusDate]);

  const setSelectedTopicId = (id) => _setSelectedTopicId(Number(id));

  // --------------------------------------------------
  // Public API
  // --------------------------------------------------
  return {
    loading,
    error,

    dateRange,
    focusDate,

    topics,
    selectedTopic,

    topicSummary,
    topicEvolution,
    topicHeatmap,
    topicSankey,

    //  NEW
    topicRepresentatives,

    setSelectedTopicId,
  };
}
