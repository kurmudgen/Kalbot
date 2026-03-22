"use client";

import { useStatus } from "../components/useStatus";

export default function MarketsPage() {
  const { status, loading } = useStatus();
  if (loading || !status) return <div className="text-gray-500 p-8">Loading...</div>;

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Market Scanner</h1>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="card"><p className="text-2xl font-bold">{status.scanner.total_markets}</p><p className="text-xs text-gray-500">Total</p></div>
        <div className="card"><p className="text-2xl font-bold">{status.scanner.with_prices}</p><p className="text-xs text-gray-500">With prices</p></div>
        <div className="card"><p className="text-2xl font-bold">{status.scanner.total_scored}</p><p className="text-xs text-gray-500">Scored</p></div>
        <div className="card"><p className="text-2xl font-bold text-green-400">{status.scanner.passed_filter}</p><p className="text-xs text-gray-500">Passed</p></div>
      </div>

      {status.sniper_markets.length > 0 && (
        <div className="card">
          <h3 className="text-sm font-semibold text-gray-400 mb-3">Sniper Targets (Expiring Soon)</h3>
          <div className="space-y-2">
            {status.sniper_markets.map((m, i) => {
              const expires = new Date(m.close_time);
              const now = new Date();
              const hoursLeft = Math.max(0, (expires.getTime() - now.getTime()) / 3600000);
              return (
                <div key={i} className="flex items-center justify-between py-2 border-b border-[#2a2e3f]/50">
                  <div>
                    <p className="text-sm">{m.title}</p>
                    <span className="badge bg-blue-500/20 text-blue-300">{m.category}</span>
                  </div>
                  <div className="text-right">
                    <p className="font-mono text-lg">{m.price}c</p>
                    <p className={`text-xs ${hoursLeft < 2 ? "text-red-400" : hoursLeft < 6 ? "text-yellow-400" : "text-gray-500"}`}>
                      {hoursLeft.toFixed(1)}hr left
                    </p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div className="card">
        <h3 className="text-sm font-semibold text-gray-400 mb-3">Categories</h3>
        <div className="flex flex-wrap gap-3">
          {Object.entries(status.scanner.categories || {}).map(([cat, n]) => (
            <div key={cat} className="bg-[#22263a] rounded-lg px-4 py-3 text-center">
              <p className="text-lg font-bold">{n}</p>
              <p className="text-xs text-gray-500 capitalize">{cat}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
