"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const nav = [
  { href: "/", label: "Dashboard", icon: "📊" },
  { href: "/trades", label: "Trades", icon: "💰" },
  { href: "/strategies", label: "Strategies", icon: "🎯" },
  { href: "/ensemble", label: "Ensemble", icon: "🤖" },
  { href: "/markets", label: "Markets", icon: "📈" },
  { href: "/news", label: "News", icon: "📰" },
  { href: "/analytics", label: "Analytics", icon: "📉" },
  { href: "/system", label: "System", icon: "⚙️" },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed left-0 top-0 h-full w-16 md:w-56 bg-[#1a1d27] border-r border-[#2a2e3f] flex flex-col z-50">
      <div className="p-4 border-b border-[#2a2e3f]">
        <h1 className="text-xl font-bold text-blue-400 hidden md:block">KalBot</h1>
        <span className="text-xl md:hidden block text-center">⚡</span>
      </div>
      <nav className="flex-1 py-2">
        {nav.map((item) => {
          const active = pathname === item.href;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`flex items-center gap-3 px-4 py-3 text-sm transition-colors ${
                active
                  ? "bg-blue-500/10 text-blue-400 border-r-2 border-blue-400"
                  : "text-gray-400 hover:text-gray-200 hover:bg-[#22263a]"
              }`}
            >
              <span className="text-lg">{item.icon}</span>
              <span className="hidden md:inline">{item.label}</span>
            </Link>
          );
        })}
      </nav>
      <div className="p-4 border-t border-[#2a2e3f] text-xs text-gray-500 hidden md:block">
        v0.1 | Paper Mode
      </div>
    </aside>
  );
}
