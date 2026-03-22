"use client";

import { useStatus } from "../components/useStatus";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";

export default function AnalyticsPage() {
  const { status, loading } = useStatus();
  if (loading || !status) return <div className="text-gray-500 p-8">Loading...</div>;

  // Confidence distribution
  const confBuckets = [
    { range: "0-30%", count: 0 },
    { range: "30-50%", count: 0 },
    { range: "50-70%", count: 0 },
    { range: "70-90%", count: 0 },
    { range: "90%+", count: 0 },
  ];
  for (const t of status.trades.recent) {
    const c = t.confidence * 100;
    if (c < 30) confBuckets[0].count++;
    else if (c < 50) confBuckets[1].count++;
    else if (c < 70) confBuckets[2].count++;
    else if (c < 90) confBuckets[3].count++;
    else confBuckets[4].count++;
  }

  // Edge distribution
  const edgeBuckets = [
    { range: "0-5%", count: 0 },
    { range: "5-10%", count: 0 },
    { range: "10-15%", count: 0 },
    { range: "15-20%", count: 0 },
    { range: "20%+", count: 0 },
  ];
  for (const t of status.trades.recent) {
    const e = t.price_gap * 100;
    if (e < 5) edgeBuckets[0].count++;
    else if (e < 10) edgeBuckets[1].count++;
    else if (e < 15) edgeBuckets[2].count++;
    else if (e < 20) edgeBuckets[3].count++;
    else edgeBuckets[4].count++;
  }

  // Category breakdown
  const catCounts: Record<string, { total: number; executed: number }> = {};
  for (const t of status.trades.recent) {
    const cat = t.category || "unknown";
    if (!catCounts[cat]) catCounts[cat] = { total: 0, executed: 0 };
    catCounts[cat].total++;
    if (t.executed) catCounts[cat].executed++;
  }
  const catData = Object.entries(catCounts).map(([name, data]) => ({ name, ...data }));

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Analytics</h1>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="card">
          <p className="text-xs text-gray-500">Total Decisions</p>
          <p className="text-3xl font-bold">{status.trades.total}</p>
        </div>
        <div className="card">
          <p className="text-xs text-gray-500">Execution Rate</p>
          <p className="text-3xl font-bold text-blue-400">
            {status.trades.total > 0 ? ((status.trades.executed / status.trades.total) * 100).toFixed(0) : 0}%
          </p>
        </div>
        <div className="card">
          <p className="text-xs text-gray-500">Consensus Rate</p>
          <p className="text-3xl font-bold text-green-400">
            {(status.ensemble.consensus_rate * 100).toFixed(0)}%
          </p>
        </div>
        <div className="card">
          <p className="text-xs text-gray-500">Avg Confidence</p>
          <p className="text-3xl font-bold">
            {status.trades.recent.length > 0
              ? (status.trades.recent.reduce((s, t) => s + t.confidence, 0) / status.trades.recent.length * 100).toFixed(0)
              : 0}%
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="card">
          <h3 className="text-sm font-semibold text-gray-400 mb-4">Confidence Distribution</h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={confBuckets}>
              <XAxis dataKey="range" tick={{ fontSize: 10, fill: "#6b7280" }} />
              <YAxis tick={{ fontSize: 10, fill: "#6b7280" }} />
              <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3f", borderRadius: 8 }} />
              <Bar dataKey="count" fill="#3b82f6" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="card">
          <h3 className="text-sm font-semibold text-gray-400 mb-4">Edge Distribution</h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={edgeBuckets}>
              <XAxis dataKey="range" tick={{ fontSize: 10, fill: "#6b7280" }} />
              <YAxis tick={{ fontSize: 10, fill: "#6b7280" }} />
              <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3f", borderRadius: 8 }} />
              <Bar dataKey="count" fill="#22c55e" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {catData.length > 0 && (
        <div className="card">
          <h3 className="text-sm font-semibold text-gray-400 mb-4">By Category</h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={catData}>
              <XAxis dataKey="name" tick={{ fontSize: 10, fill: "#6b7280" }} />
              <YAxis tick={{ fontSize: 10, fill: "#6b7280" }} />
              <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3f", borderRadius: 8 }} />
              <Bar dataKey="total" fill="#3b82f6" radius={[4, 4, 0, 0]} name="Decisions" />
              <Bar dataKey="executed" fill="#22c55e" radius={[4, 4, 0, 0]} name="Executed" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
