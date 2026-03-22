"use client";

import { useStatus } from "../components/useStatus";

function HealthDot({ ok }: { ok: boolean }) {
  return <span className={`w-3 h-3 rounded-full inline-block ${ok ? "bg-green-500" : "bg-red-500"}`} />;
}

export default function SystemPage() {
  const { status, loading } = useStatus();
  if (loading || !status) return <div className="text-gray-500 p-8">Loading...</div>;

  const sys = status.system;

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">System Health</h1>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="card">
          <p className="text-xs text-gray-500">Uptime</p>
          <p className="text-3xl font-bold">{sys.uptime_hours}hr</p>
        </div>
        <div className="card">
          <p className="text-xs text-gray-500">Cycles</p>
          <p className="text-3xl font-bold">{sys.cycles}</p>
        </div>
        <div className="card">
          <p className="text-xs text-gray-500">Errors (24hr)</p>
          <p className={`text-3xl font-bold ${sys.errors_24h > 0 ? "text-red-400" : "text-green-400"}`}>
            {sys.errors_24h}
          </p>
        </div>
        <div className="card">
          <p className="text-xs text-gray-500">Bot Status</p>
          <p className="text-3xl font-bold">{status.bot_status === "running" ? "ON" : "OFF"}</p>
        </div>
      </div>

      <div className="card">
        <h3 className="text-sm font-semibold text-gray-400 mb-4">Service Health</h3>
        <div className="space-y-3">
          <div className="flex items-center justify-between py-2 border-b border-[#2a2e3f]/50">
            <div className="flex items-center gap-3">
              <HealthDot ok={sys.ollama === "ok"} />
              <span>Ollama (Local LLM)</span>
            </div>
            <span className="text-sm text-gray-400">{sys.ollama}</span>
          </div>
          <div className="flex items-center justify-between py-2 border-b border-[#2a2e3f]/50">
            <div className="flex items-center gap-3">
              <HealthDot ok={status.bot_status === "running"} />
              <span>Kalshi Bot</span>
            </div>
            <span className="text-sm text-gray-400">{status.bot_status}</span>
          </div>
          <div className="flex items-center justify-between py-2 border-b border-[#2a2e3f]/50">
            <div className="flex items-center gap-3">
              <HealthDot ok={(status.ensemble.models.perplexity?.analyses || 0) > 0} />
              <span>Perplexity API</span>
            </div>
            <span className="text-sm text-gray-400">{status.ensemble.models.perplexity?.analyses || 0} calls</span>
          </div>
          <div className="flex items-center justify-between py-2 border-b border-[#2a2e3f]/50">
            <div className="flex items-center gap-3">
              <HealthDot ok={(status.ensemble.models.claude?.analyses || 0) > 0} />
              <span>Claude API</span>
            </div>
            <span className="text-sm text-gray-400">{status.ensemble.models.claude?.analyses || 0} calls</span>
          </div>
          <div className="flex items-center justify-between py-2">
            <div className="flex items-center gap-3">
              <HealthDot ok={(status.ensemble.models.deepseek?.analyses || 0) > 0} />
              <span>DeepSeek API</span>
            </div>
            <span className="text-sm text-gray-400">{status.ensemble.models.deepseek?.analyses || 0} calls</span>
          </div>
        </div>
      </div>

      <div className="card">
        <h3 className="text-sm font-semibold text-gray-400 mb-4">Accounts</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="bg-[#22263a] rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <span className="font-semibold">Kalshi</span>
              <span className={`badge ${status.accounts.kalshi.paper_mode ? "bg-yellow-500/20 text-yellow-400" : "bg-green-500/20 text-green-400"}`}>
                {status.accounts.kalshi.paper_mode ? "Paper" : "Live"}
              </span>
            </div>
            <p className="text-2xl font-bold">${status.accounts.kalshi.balance.toFixed(2)}</p>
          </div>
          <div className="bg-[#22263a] rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <span className="font-semibold">Alpaca</span>
              <span className={`badge ${status.accounts.alpaca.paper_mode ? "bg-yellow-500/20 text-yellow-400" : "bg-green-500/20 text-green-400"}`}>
                {status.accounts.alpaca.paper_mode ? "Paper" : "Live"}
              </span>
            </div>
            <p className="text-2xl font-bold">${status.accounts.alpaca.portfolio_value.toLocaleString()}</p>
            <p className="text-xs text-gray-500">Cash: ${status.accounts.alpaca.cash.toLocaleString()}</p>
          </div>
        </div>
      </div>

      {status.alerts.length > 0 && (
        <div className="card">
          <h3 className="text-sm font-semibold text-gray-400 mb-3">Active Alerts</h3>
          {status.alerts.map((a, i) => (
            <div key={i} className={`p-3 rounded-lg mb-2 ${
              a.level === "critical" ? "bg-red-500/10 text-red-400" :
              a.level === "warning" ? "bg-yellow-500/10 text-yellow-400" :
              "bg-blue-500/10 text-blue-400"
            }`}>
              {a.message}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
