"use client";

import { useStatus } from "../components/useStatus";

export default function NewsPage() {
  const { status, loading } = useStatus();
  if (loading || !status) return <div className="text-gray-500 p-8">Loading...</div>;

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">News Pool</h1>
      <p className="text-sm text-gray-500">Perplexity research findings shared across all strategies</p>

      {status.news.length > 0 ? (
        <div className="space-y-3">
          {status.news.map((n, i) => (
            <div key={i} className="card">
              <div className="flex items-center gap-2 mb-2">
                <span className="badge bg-blue-500/20 text-blue-300">{n.category}</span>
                <span className="text-xs text-gray-500">{n.time}</span>
                <span className="text-xs text-gray-600">via {n.source}</span>
              </div>
              <p className="text-sm">{n.text}</p>
            </div>
          ))}
        </div>
      ) : (
        <div className="card text-center py-8">
          <p className="text-gray-500">No research findings yet</p>
          <p className="text-xs text-gray-600 mt-1">Perplexity stores findings as it analyzes markets</p>
        </div>
      )}
    </div>
  );
}
