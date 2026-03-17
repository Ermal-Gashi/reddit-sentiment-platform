import api from "./api";

// Sentiment data (from your Gold stage)
export const fetchSentimentVsPrice = async (params) => {
  const res = await api.get("/api/sentiment-vs-price", { params });
  return res.data;
};

// Sector summary (aggregates)
export const fetchSectorSummary = async () => {
  const res = await api.get("/api/sector-summary");
  return res.data;
};

// Trigger ETL manually
export const runETL = async () => {
  const res = await api.post("/api/run-etl");
  return res.data;
};
