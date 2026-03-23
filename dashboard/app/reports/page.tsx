"use client";

import { useState, useEffect } from "react";

interface Report {
  date: string;
  text: string;
  html: string;
}

const REPORTS_URL =
  "https://raw.githubusercontent.com/kurmudgen/Kalbot/master/dashboard/reports.json";

export default function ReportsPage() {
  const [reports, setReports] = useState<Report[]>([]);
  const [selected, setSelected] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(REPORTS_URL + "?t=" + Date.now())
      .then((r) => r.json())
      .then((data) => { setReports(data); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  if (loading) return <div className="text-gray-500 p-8">Loading reports...</div>;

  if (reports.length === 0) return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Daily Reports</h1>
      <div className="card text-center py-8">
        <p className="text-gray-500">No reports yet. First report generates at 8am tomorrow.</p>
      </div>
    </div>
  );

  const current = reports[selected];

  const downloadReport = (report: Report) => {
    const blob = new Blob([report.text], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `kalbot-report-${report.date}.txt`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl sm:text-2xl font-bold">Daily Reports</h1>
        <button
          onClick={() => downloadReport(current)}
          className="px-3 py-1.5 bg-blue-500/20 text-blue-400 rounded-lg text-sm hover:bg-blue-500/30 transition-colors"
        >
          Download
        </button>
      </div>

      {/* Report selector */}
      <div className="flex gap-2 overflow-x-auto pb-2">
        {reports.map((r, i) => (
          <button
            key={i}
            onClick={() => setSelected(i)}
            className={`px-3 py-1.5 rounded-lg text-xs whitespace-nowrap transition-colors ${
              selected === i
                ? "bg-blue-500 text-white"
                : "bg-[#1a1d27] text-gray-400 hover:text-white"
            }`}
          >
            {r.date}
          </button>
        ))}
      </div>

      {/* Current report */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold">Report — {current.date}</h2>
          <span className="text-xs text-gray-500">
            {selected === 0 ? "Latest" : `${selected} days ago`}
          </span>
        </div>
        <pre className="text-sm text-gray-300 whitespace-pre-wrap font-mono leading-relaxed">
          {current.text}
        </pre>
      </div>
    </div>
  );
}
