"use client";

import { useState, useEffect } from "react";

const STATUS_URL =
  "https://raw.githubusercontent.com/kurmudgen/Kalbot/master/dashboard/status.json";

export interface Status {
  updated_at: string;
  bot_status: string;
  accounts: {
    kalshi: { balance: number; paper_mode: boolean };
    alpaca: { cash: number; portfolio_value: number; paper_mode: boolean };
  };
  pnl: {
    today: number;
    all_time: number;
    history: { date: string; trades: number; deployed: number }[];
  };
  trades: {
    total: number;
    executed: number;
    skipped: number;
    recent: {
      ticker: string;
      title: string;
      side: string;
      amount: number;
      probability: number;
      confidence: number;
      market_price: number;
      price_gap: number;
      mode: string;
      executed: boolean;
      reasoning: string;
      category: string;
      time: string;
    }[];
  };
  strategies: Record<
    string,
    { trades: number; executed: number; deployed: number }
  >;
  ensemble: {
    models: Record<string, { analyses: number; avg_probability: number }>;
    consensus_rate: number;
    recent_votes: {
      perplexity: number | null;
      claude: number | null;
      deepseek: number | null;
      consensus: boolean;
      time: string;
    }[];
  };
  scanner: {
    total_markets: number;
    with_prices: number;
    passed_filter: number;
    total_scored: number;
    categories: Record<string, number>;
  };
  sniper_markets: {
    ticker: string;
    title: string;
    price: number;
    close_time: string;
    category: string;
  }[];
  news: {
    text: string;
    category: string;
    source: string;
    time: string;
  }[];
  alerts: { level: string; message: string }[];
  system: {
    ollama: string;
    errors_24h: number;
    cycles: number;
    uptime_hours: number;
  };
}

export function useStatus(refreshInterval = 60000) {
  const [status, setStatus] = useState<Status | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchStatus = async () => {
    try {
      const res = await fetch(STATUS_URL + "?t=" + Date.now(), {
        cache: "no-store",
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setStatus(data);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to fetch");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchStatus();
    const interval = setInterval(fetchStatus, refreshInterval);
    return () => clearInterval(interval);
  }, [refreshInterval]);

  return { status, loading, error };
}
