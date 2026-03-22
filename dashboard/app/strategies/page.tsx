"use client";

import { useStatus } from "../components/useStatus";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";

const COLORS = ["#3b82f6", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6",
                "#ec4899", "#06b6d4", "#f97316", "#14b8a6"];

export default function StrategiesPage() {
  const { status, loading } = useStatus();
  if (loading || !status) return <div className="text-gray-500 p-8">Loading...</div>;

  const strategies = Object.entries(status.strategies).map(([name, data], i) => ({
    name, ...data, fill: COLORS[i % COLORS.length],
  }));

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Strategy Performance</h1>

      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
        {strategies.map((s) => (
          <div key={s.name} className="card border-l-4" style={{ borderLeftColor: s.fill }}>
            <h3 className="font-semibold text-sm">{s.name}</h3>
            <p className="text-2xl font-bold mt-2">{s.executed}</p>
            <p className="text-xs text-gray-500">executed trades</p>
            <div className="mt-2 text-xs text-gray-400">
              <span>{s.trades} decisions</span>
              <span className="mx-2">|</span>
              <span>${(s.deployed || 0).toFixed(2)} deployed</span>
            </div>
          </div>
        ))}
      </div>

      {strategies.length > 0 && (
        <div className="card">
          <h3 className="text-sm font-semibold text-gray-400 mb-4">Trades by Strategy</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={strategies}>
              <XAxis dataKey="name" tick={{ fontSize: 11, fill: "#6b7280" }} />
              <YAxis tick={{ fontSize: 11, fill: "#6b7280" }} />
              <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3f", borderRadius: 8 }} />
              <Bar dataKey="trades" radius={[6, 6, 0, 0]}>
                {strategies.map((s, i) => <Cell key={i} fill={s.fill} />)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
