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
    <div className="card min-w-0">
      <p className="text-[10px] sm:text-xs text-gray-500 uppercase tracking-wide truncate">{label}</p>
      <p className={`text-lg sm:text-2xl font-bold mt-1 ${color || ""}`}>{value}</p>
      {sub && <p className="text-[10px] sm:text-xs text-gray-500 mt-1 line-clamp-2">{sub}</p>}
    </div>
  );
}

const COLORS = ["#3b82f6", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6",
                "#ec4899", "#06b6d4", "#f97316", "#14b8a6"];

export default function Dashboard() {
  const { status, loading, error } = useStatus();

  if (loading) return (
    <div className="flex items-center justify-center h-[80vh]">
      <div className="text-gray-500 animate-pulse text-center">
        <p className="text-2xl mb-2">Loading...</p>
        <p className="text-xs">Fetching latest data from bot</p>
      </div>
    </div>
  );

  if (error || !status) return (
    <div className="flex items-center justify-center h-[80vh]">
      <div className="card text-center max-w-sm mx-4">
        <p className="text-xl mb-2">No data yet</p>
        <p className="text-gray-500 text-sm">{error || "Waiting for the bot to start publishing data."}</p>
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
    <div className="space-y-4 sm:space-y-6 pb-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl sm:text-2xl font-bold">Dashboard</h1>
          <p className="text-[10px] sm:text-sm text-gray-500">
            {new Date(status.updated_at).toLocaleTimeString()}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <span className={`pulse-dot ${botRunning ? "green" : "red"}`} />
          <span className="text-xs sm:text-sm">{botRunning ? "Running" : "Stopped"}</span>
          {status.accounts.kalshi.paper_mode && (
            <span className="badge bg-yellow-500/20 text-yellow-400">PAPER</span>
          )}
        </div>
      </div>

      {/* Alerts */}
      {status.alerts.length > 0 && (
        <div className="space-y-2">
          {status.alerts.map((a, i) => (
            <div key={i} className={`card border-l-4 py-2 ${
              a.level === "critical" ? "border-red-500 bg-red-500/5" :
              a.level === "warning" ? "border-yellow-500 bg-yellow-500/5" :
              "border-blue-500"
            }`}>
              <p className="text-xs sm:text-sm">{a.message}</p>
            </div>
          ))}
        </div>
      )}

      {/* Stats — 2 cols on mobile, 5 on desktop */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2 sm:gap-4">
        <StatCard label="Money Made" value={`$${status.pnl.all_time.toFixed(2)}`} color={pnlColor} sub="Verified P&L" />
        <StatCard label="Win Rate" value={status.pnl.resolved?.total_trades ? `${((status.pnl.resolved?.win_rate ?? 0) * 100).toFixed(0)}%` : "—"} color={(status.pnl.resolved?.win_rate ?? 0) > 0.55 ? "profit" : ""} sub={status.pnl.resolved?.total_trades ? `${status.pnl.resolved?.wins ?? 0}W / ${status.pnl.resolved?.losses ?? 0}L` : "Waiting for results"} />
        <StatCard label="Trades" value={String(status.trades.executed)} sub={`${status.trades.skipped} skipped`} />
        <StatCard label="Kalshi" value={`$${status.accounts.kalshi.balance.toFixed(0)}`} sub={status.accounts.kalshi.paper_mode ? "Practice" : "Live"} />
        <StatCard label="Stocks" value={`$${(status.accounts.alpaca.portfolio_value / 1000).toFixed(0)}K`} sub={status.accounts.alpaca.paper_mode ? "Practice" : "Live"} />
      </div>

      {/* Treasury */}
      {status.treasury?.ok && (
        <div className="card">
          <h3 className="text-xs sm:text-sm font-semibold text-gray-400 mb-2">Treasury</h3>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div>
              <p className="text-[10px] text-gray-500">Checking (...3906)</p>
              <p className="text-lg font-bold">${status.treasury.checking_balance.toFixed(2)}</p>
              <p className="text-[10px] text-gray-500">operational</p>
            </div>
            <div>
              <p className="text-[10px] text-gray-500">Savings (...4242)</p>
              <p className="text-lg font-bold">${status.treasury.savings_balance.toFixed(2)}</p>
              <p className="text-[10px] text-gray-500">reserve</p>
            </div>
            <div>
              <p className="text-[10px] text-gray-500">Total</p>
              <p className="text-lg font-bold text-blue-400">${status.treasury.total.toFixed(2)}</p>
            </div>
            <div>
              <p className="text-[10px] text-gray-500">Runway</p>
              <p className="text-lg font-bold">{status.treasury.runway_days > 9000 ? "\u221E" : `${status.treasury.runway_days}d`}</p>
              <p className="text-[10px] text-gray-500">${status.treasury.daily_burn.toFixed(2)}/day burn</p>
            </div>
          </div>
        </div>
      )}

      {/* Charts — stacked on mobile */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="card lg:col-span-2">
          <h3 className="text-xs sm:text-sm font-semibold text-gray-400 mb-3">Activity</h3>
          {status.pnl.history.length > 0 ? (
            <ResponsiveContainer width="100%" height={160}>
              <LineChart data={status.pnl.history}>
                <XAxis dataKey="date" tick={{ fontSize: 9, fill: "#6b7280" }} />
                <YAxis tick={{ fontSize: 9, fill: "#6b7280" }} width={30} />
                <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3f", borderRadius: 8, fontSize: 12 }} />
                <Line type="monotone" dataKey="deployed" stroke="#3b82f6" strokeWidth={2} dot={false} />
                <Line type="monotone" dataKey="trades" stroke="#22c55e" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-[160px] flex items-center justify-center text-gray-600 text-sm">No data yet</div>
          )}
        </div>

        <div className="card">
          <h3 className="text-xs sm:text-sm font-semibold text-gray-400 mb-3">Strategy Mix</h3>
          {stratData.length > 0 ? (
            <>
              <ResponsiveContainer width="100%" height={130}>
                <PieChart>
                  <Pie data={stratData} cx="50%" cy="50%" innerRadius={35} outerRadius={55} paddingAngle={2} dataKey="value">
                    {stratData.map((e, i) => <Cell key={i} fill={e.fill} />)}
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
              <div className="flex flex-wrap gap-1.5 mt-1">
                {stratData.map((s, i) => (
                  <span key={i} className="text-[10px] flex items-center gap-1">
                    <span className="w-1.5 h-1.5 rounded-full" style={{ background: s.fill }} />
                    {s.name}
                  </span>
                ))}
              </div>
            </>
          ) : (
            <div className="h-[130px] flex items-center justify-center text-gray-600 text-sm">No data</div>
          )}
        </div>
      </div>

      {/* AI + Scanner — stacked on mobile */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <div className="card">
          <h3 className="text-xs sm:text-sm font-semibold text-gray-400 mb-2">AI Agreement</h3>
          <div className="flex items-center gap-4 mb-2">
            <div>
              <p className="text-2xl sm:text-3xl font-bold text-blue-400">{(status.ensemble.consensus_rate * 100).toFixed(0)}%</p>
              <p className="text-[10px] text-gray-500">3 AIs agree</p>
            </div>
            <div className="flex gap-3">
              {Object.entries(status.ensemble.models).map(([name, data]) => (
                <div key={name} className="text-center">
                  <p className="text-sm font-semibold">{data.analyses}</p>
                  <p className="text-[10px] text-gray-500 capitalize">{name.slice(0, 4)}</p>
                </div>
              ))}
            </div>
          </div>
          <div className="space-y-0.5">
            {status.ensemble.recent_votes.slice(0, 4).map((v, i) => (
              <div key={i} className="flex items-center gap-1.5 text-[10px]">
                <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${v.consensus ? "bg-green-500" : "bg-red-500"}`} />
                <span className="text-gray-500">{v.time?.slice(11, 16)}</span>
                <span className="font-mono">P:{v.perplexity?.toFixed(2) ?? "--"}</span>
                <span className="font-mono">C:{v.claude?.toFixed(2) ?? "--"}</span>
                <span className="font-mono">D:{v.deepseek?.toFixed(2) ?? "--"}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="card">
          <h3 className="text-xs sm:text-sm font-semibold text-gray-400 mb-2">Scanner</h3>
          <div className="grid grid-cols-2 gap-2">
            <div><p className="text-xl font-bold">{status.scanner.total_markets}</p><p className="text-[10px] text-gray-500">Markets</p></div>
            <div><p className="text-xl font-bold">{status.scanner.with_prices}</p><p className="text-[10px] text-gray-500">Priced</p></div>
            <div><p className="text-xl font-bold">{status.scanner.total_scored}</p><p className="text-[10px] text-gray-500">Scored</p></div>
            <div><p className="text-xl font-bold text-green-400">{status.scanner.passed_filter}</p><p className="text-[10px] text-gray-500">Tradeable</p></div>
          </div>
          <div className="mt-2 flex flex-wrap gap-1">
            {Object.entries(status.scanner.categories || {}).map(([cat, n]) => (
              <span key={cat} className="text-[10px] px-1.5 py-0.5 rounded bg-blue-500/20 text-blue-300">{cat}: {n}</span>
            ))}
          </div>
        </div>
      </div>

      {/* Recent Trades — card layout on mobile */}
      <div className="card">
        <h3 className="text-xs sm:text-sm font-semibold text-gray-400 mb-2">Recent Bets</h3>
        {recentExecuted.length > 0 ? (
          <div className="space-y-2 sm:space-y-0">
            {/* Mobile: card layout */}
            <div className="sm:hidden space-y-2">
              {recentExecuted.map((t, i) => (
                <div key={i} className="bg-[#22263a] rounded-lg p-3">
                  <div className="flex items-center justify-between mb-1">
                    <span className={`badge ${t.side === "YES" ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400"}`}>{t.side}</span>
                    <span className="text-sm font-bold">${t.amount.toFixed(2)}</span>
                  </div>
                  <p className="text-xs truncate">{t.title}</p>
                  <div className="flex gap-3 mt-1 text-[10px] text-gray-500">
                    <span>Conf: {(t.confidence * 100).toFixed(0)}%</span>
                    <span>Edge: {(t.price_gap * 100).toFixed(1)}%</span>
                    <span>{t.time?.slice(11, 16)}</span>
                  </div>
                </div>
              ))}
            </div>
            {/* Desktop: table */}
            <div className="hidden sm:block overflow-x-auto">
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
          </div>
        ) : (
          <p className="text-gray-600 text-xs sm:text-sm">No bets placed yet. The bot only trades when the AIs agree.</p>
        )}
      </div>

      {/* System bar */}
      <div className="card py-2 sm:py-3">
        <div className="flex flex-wrap gap-3 sm:gap-6 text-xs sm:text-sm">
          <div className="flex items-center gap-1.5">
            <span className={`w-2 h-2 rounded-full ${status.system.ollama === "ok" ? "bg-green-500" : "bg-red-500"}`} />
            Ollama
          </div>
          <div>Cycles: {status.system.cycles}</div>
          <div>Up: {status.system.uptime_hours}hr</div>
          <div>Errors: {status.system.errors_24h}</div>
        </div>
      </div>
    </div>
  );
}
