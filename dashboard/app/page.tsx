"use client";

import { useStatus } from "./components/useStatus";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell,
} from "recharts";

function StatCard({ label, value, sub, color }: {
  label: string; value: string; sub?: string; color?: string;
}) {
  return (
    <div className="card">
      <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
      <p className={`text-2xl font-bold mt-1 ${color || ""}`}>{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  );
}

const COLORS = ["#3b82f6", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6",
                "#ec4899", "#06b6d4", "#f97316", "#14b8a6"];

export default function Dashboard() {
  const { status, loading, error } = useStatus();

  if (loading) return (
    <div className="flex items-center justify-center h-screen">
      <div className="text-gray-500 animate-pulse">Loading dashboard...</div>
    </div>
  );

  if (error || !status) return (
    <div className="flex items-center justify-center h-screen">
      <div className="card text-center max-w-md">
        <p className="text-xl mb-2">No data yet</p>
        <p className="text-gray-500 text-sm">
          {error || "Waiting for bot to publish first status update."}
        </p>
        <p className="text-gray-600 text-xs mt-4">
          The bot pushes status.json to GitHub every cycle.
        </p>
      </div>
    </div>
  );

  const pnlColor = status.pnl.all_time >= 0 ? "profit" : "loss";
  const botRunning = status.bot_status === "running";

  const stratData = Object.entries(status.strategies).map(([name, data], i) => ({
    name, value: data.trades || 1, fill: COLORS[i % COLORS.length],
  }));

  const recentExecuted = status.trades.recent.filter((t) => t.executed).slice(0, 5);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div>
          <h1 className="text-2xl font-bold">Dashboard</h1>
          <p className="text-sm text-gray-500">
            Updated {new Date(status.updated_at).toLocaleString()}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <span className={`pulse-dot ${botRunning ? "green" : "red"}`} />
          <span className="text-sm">{botRunning ? "Running" : "Stopped"}</span>
          {status.accounts.kalshi.paper_mode && (
            <span className="badge bg-yellow-500/20 text-yellow-400 ml-2">PAPER</span>
          )}
        </div>
      </div>

      {/* Alerts */}
      {status.alerts.length > 0 && (
        <div className="space-y-2">
          {status.alerts.map((a, i) => (
            <div key={i} className={`card border-l-4 ${
              a.level === "critical" ? "border-red-500 bg-red-500/5" :
              a.level === "warning" ? "border-yellow-500 bg-yellow-500/5" :
              "border-blue-500"
            }`}>
              <p className="text-sm">{a.message}</p>
            </div>
          ))}
        </div>
      )}

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard label="All-Time P&L" value={`$${status.pnl.all_time.toFixed(2)}`} color={pnlColor} />
        <StatCard label="Trades Executed" value={String(status.trades.executed)} sub={`${status.trades.skipped} skipped`} />
        <StatCard label="Kalshi Balance" value={`$${status.accounts.kalshi.balance.toFixed(2)}`} sub={status.accounts.kalshi.paper_mode ? "Paper" : "Live"} />
        <StatCard label="Alpaca Value" value={`$${status.accounts.alpaca.portfolio_value.toLocaleString()}`} sub={status.accounts.alpaca.paper_mode ? "Paper" : "Live"} />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="card md:col-span-2">
          <h3 className="text-sm font-semibold text-gray-400 mb-4">Activity</h3>
          {status.pnl.history.length > 0 ? (
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={status.pnl.history}>
                <XAxis dataKey="date" tick={{ fontSize: 10, fill: "#6b7280" }} />
                <YAxis tick={{ fontSize: 10, fill: "#6b7280" }} />
                <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3f", borderRadius: 8 }} />
                <Line type="monotone" dataKey="deployed" stroke="#3b82f6" strokeWidth={2} dot={false} />
                <Line type="monotone" dataKey="trades" stroke="#22c55e" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-[200px] flex items-center justify-center text-gray-600">No data yet</div>
          )}
        </div>

        <div className="card">
          <h3 className="text-sm font-semibold text-gray-400 mb-4">Strategy Mix</h3>
          {stratData.length > 0 ? (
            <>
              <ResponsiveContainer width="100%" height={160}>
                <PieChart>
                  <Pie data={stratData} cx="50%" cy="50%" innerRadius={40} outerRadius={70} paddingAngle={2} dataKey="value">
                    {stratData.map((e, i) => <Cell key={i} fill={e.fill} />)}
                  </Pie>
                  <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3f", borderRadius: 8 }} />
                </PieChart>
              </ResponsiveContainer>
              <div className="flex flex-wrap gap-2 mt-2">
                {stratData.map((s, i) => (
                  <span key={i} className="text-xs flex items-center gap-1">
                    <span className="w-2 h-2 rounded-full" style={{ background: s.fill }} />
                    {s.name}
                  </span>
                ))}
              </div>
            </>
          ) : (
            <div className="h-[160px] flex items-center justify-center text-gray-600">No data</div>
          )}
        </div>
      </div>

      {/* Ensemble + Scanner */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="card">
          <h3 className="text-sm font-semibold text-gray-400 mb-3">Ensemble</h3>
          <div className="flex items-center gap-6 mb-3">
            <div>
              <p className="text-3xl font-bold text-blue-400">{(status.ensemble.consensus_rate * 100).toFixed(0)}%</p>
              <p className="text-xs text-gray-500">Consensus</p>
            </div>
            {Object.entries(status.ensemble.models).map(([name, data]) => (
              <div key={name} className="text-center">
                <p className="text-lg font-semibold">{data.analyses}</p>
                <p className="text-xs text-gray-500 capitalize">{name}</p>
              </div>
            ))}
          </div>
          <div className="space-y-1">
            {status.ensemble.recent_votes.slice(0, 5).map((v, i) => (
              <div key={i} className="flex items-center gap-2 text-xs">
                <span className={`w-2 h-2 rounded-full ${v.consensus ? "bg-green-500" : "bg-red-500"}`} />
                <span className="text-gray-500 w-12">{v.time?.slice(11, 16)}</span>
                <span>P:{v.perplexity?.toFixed(2) ?? "--"}</span>
                <span>C:{v.claude?.toFixed(2) ?? "--"}</span>
                <span>D:{v.deepseek?.toFixed(2) ?? "--"}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="card">
          <h3 className="text-sm font-semibold text-gray-400 mb-3">Scanner</h3>
          <div className="grid grid-cols-2 gap-3">
            <div><p className="text-2xl font-bold">{status.scanner.total_markets}</p><p className="text-xs text-gray-500">Markets</p></div>
            <div><p className="text-2xl font-bold">{status.scanner.with_prices}</p><p className="text-xs text-gray-500">With prices</p></div>
            <div><p className="text-2xl font-bold">{status.scanner.total_scored}</p><p className="text-xs text-gray-500">Scored</p></div>
            <div><p className="text-2xl font-bold text-green-400">{status.scanner.passed_filter}</p><p className="text-xs text-gray-500">Passed</p></div>
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            {Object.entries(status.scanner.categories || {}).map(([cat, n]) => (
              <span key={cat} className="badge bg-blue-500/20 text-blue-300">{cat}: {n}</span>
            ))}
          </div>
        </div>
      </div>

      {/* Recent Trades */}
      <div className="card">
        <h3 className="text-sm font-semibold text-gray-400 mb-3">Recent Trades</h3>
        {recentExecuted.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-500 text-xs border-b border-[#2a2e3f]">
                  <th className="text-left py-2">Time</th>
                  <th className="text-left">Market</th>
                  <th className="text-left">Side</th>
                  <th className="text-right">$</th>
                  <th className="text-right">Conf</th>
                  <th className="text-right">Edge</th>
                </tr>
              </thead>
              <tbody>
                {recentExecuted.map((t, i) => (
                  <tr key={i} className="border-b border-[#2a2e3f]/50 hover:bg-[#22263a] transition-colors">
                    <td className="py-2 text-gray-500 text-xs">{t.time?.slice(11, 16)}</td>
                    <td className="max-w-[250px] truncate">{t.title}</td>
                    <td><span className={`badge ${t.side === "YES" ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400"}`}>{t.side}</span></td>
                    <td className="text-right">${t.amount.toFixed(2)}</td>
                    <td className="text-right">{(t.confidence * 100).toFixed(0)}%</td>
                    <td className="text-right">{(t.price_gap * 100).toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-gray-600 text-sm">No executed trades yet</p>
        )}
      </div>

      {/* System */}
      <div className="card">
        <div className="flex flex-wrap gap-6 text-sm">
          <div className="flex items-center gap-2">
            <span className={`w-2 h-2 rounded-full ${status.system.ollama === "ok" ? "bg-green-500" : "bg-red-500"}`} />
            Ollama
          </div>
          <div>Cycles: {status.system.cycles}</div>
          <div>Uptime: {status.system.uptime_hours}hr</div>
          <div>Errors: {status.system.errors_24h}</div>
        </div>
      </div>
    </div>
  );
}
