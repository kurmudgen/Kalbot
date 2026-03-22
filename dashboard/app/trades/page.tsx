"use client";

import { useStatus } from "../components/useStatus";
import { useState } from "react";

export default function TradesPage() {
  const { status, loading } = useStatus();
  const [filter, setFilter] = useState<"all" | "executed" | "skipped">("executed");

  if (loading || !status) return <div className="text-gray-500 p-8">Loading...</div>;

  const trades = status.trades.recent.filter((t) => {
    if (filter === "executed") return t.executed;
    if (filter === "skipped") return !t.executed;
    return true;
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Trade Log</h1>
        <div className="flex gap-2">
          {(["all", "executed", "skipped"] as const).map((f) => (
            <button key={f} onClick={() => setFilter(f)}
              className={`px-3 py-1 rounded-lg text-sm transition-colors ${
                filter === f ? "bg-blue-500 text-white" : "bg-[#1a1d27] text-gray-400 hover:text-white"
              }`}>{f}</button>
          ))}
        </div>
      </div>

      <div className="card">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-500 text-xs border-b border-[#2a2e3f]">
                <th className="text-left py-2">Time</th>
                <th className="text-left">Market</th>
                <th className="text-left">Cat</th>
                <th className="text-left">Side</th>
                <th className="text-right">Amount</th>
                <th className="text-right">Prob</th>
                <th className="text-right">Conf</th>
                <th className="text-right">Price</th>
                <th className="text-right">Edge</th>
                <th className="text-left">Status</th>
              </tr>
            </thead>
            <tbody>
              {trades.map((t, i) => (
                <tr key={i} className="border-b border-[#2a2e3f]/50 hover:bg-[#22263a] transition-colors group">
                  <td className="py-3 text-gray-500 text-xs whitespace-nowrap">{t.time?.slice(5, 16)}</td>
                  <td className="max-w-[200px]">
                    <p className="truncate">{t.title}</p>
                    <p className="text-xs text-gray-600 truncate group-hover:whitespace-normal">{t.reasoning}</p>
                  </td>
                  <td><span className="badge bg-blue-500/20 text-blue-300">{t.category}</span></td>
                  <td><span className={`badge ${t.side === "YES" ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400"}`}>{t.side}</span></td>
                  <td className="text-right">${t.amount.toFixed(2)}</td>
                  <td className="text-right">{(t.probability * 100).toFixed(0)}%</td>
                  <td className="text-right">{(t.confidence * 100).toFixed(0)}%</td>
                  <td className="text-right">{(t.market_price * 100).toFixed(0)}c</td>
                  <td className="text-right">{(t.price_gap * 100).toFixed(1)}%</td>
                  <td>{t.executed
                    ? <span className="badge bg-green-500/20 text-green-400">Executed</span>
                    : <span className="badge bg-gray-500/20 text-gray-400">Skipped</span>
                  }</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {trades.length === 0 && <p className="text-gray-600 text-sm py-4 text-center">No trades match filter</p>}
      </div>
    </div>
  );
}
