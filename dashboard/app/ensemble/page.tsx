"use client";

import { useStatus } from "../components/useStatus";

export default function EnsemblePage() {
  const { status, loading } = useStatus();
  if (loading || !status) return <div className="text-gray-500 p-8">Loading...</div>;

  const models = Object.entries(status.ensemble.models);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Ensemble View</h1>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="card">
          <p className="text-xs text-gray-500">Consensus Rate</p>
          <p className="text-4xl font-bold text-blue-400 mt-1">
            {(status.ensemble.consensus_rate * 100).toFixed(0)}%
          </p>
        </div>
        {models.map(([name, data]) => (
          <div key={name} className="card">
            <p className="text-xs text-gray-500 capitalize">{name}</p>
            <p className="text-3xl font-bold mt-1">{data.analyses}</p>
            <p className="text-xs text-gray-500">analyses</p>
          </div>
        ))}
      </div>

      <div className="card">
        <h3 className="text-sm font-semibold text-gray-400 mb-3">Recent Votes</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-500 text-xs border-b border-[#2a2e3f]">
                <th className="text-left py-2">Time</th>
                <th className="text-right">Perplexity</th>
                <th className="text-right">Claude</th>
                <th className="text-right">DeepSeek</th>
                <th className="text-left">Result</th>
              </tr>
            </thead>
            <tbody>
              {status.ensemble.recent_votes.map((v, i) => (
                <tr key={i} className="border-b border-[#2a2e3f]/50 hover:bg-[#22263a]">
                  <td className="py-2 text-gray-500">{v.time?.slice(11, 19)}</td>
                  <td className="text-right font-mono">{v.perplexity?.toFixed(2) ?? "--"}</td>
                  <td className="text-right font-mono">{v.claude?.toFixed(2) ?? "--"}</td>
                  <td className="text-right font-mono">{v.deepseek?.toFixed(2) ?? "--"}</td>
                  <td>
                    <span className={`badge ${v.consensus ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400"}`}>
                      {v.consensus ? "Consensus" : "Split"}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
