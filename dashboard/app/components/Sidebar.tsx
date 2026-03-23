"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const nav = [
  { href: "/", label: "Home", icon: "📊" },
  { href: "/trades", label: "Trades", icon: "💰" },
  { href: "/strategies", label: "Strategy", icon: "🎯" },
  { href: "/ensemble", label: "AI", icon: "🤖" },
  { href: "/markets", label: "Markets", icon: "📈" },
  { href: "/news", label: "News", icon: "📰" },
  { href: "/analytics", label: "Stats", icon: "📉" },
  { href: "/system", label: "System", icon: "⚙️" },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <>
      {/* Desktop sidebar */}
      <aside className="hidden md:flex fixed left-0 top-0 h-full w-56 bg-[#1a1d27] border-r border-[#2a2e3f] flex-col z-50">
        <div className="p-4 border-b border-[#2a2e3f]">
          <h1 className="text-xl font-bold text-blue-400">KalBot</h1>
        </div>
        <nav className="flex-1 py-2">
          {nav.map((item) => {
            const active = pathname === item.href;
            return (
              <Link key={item.href} href={item.href}
                className={`flex items-center gap-3 px-4 py-3 text-sm transition-colors ${
                  active
                    ? "bg-blue-500/10 text-blue-400 border-r-2 border-blue-400"
                    : "text-gray-400 hover:text-gray-200 hover:bg-[#22263a]"
                }`}>
                <span className="text-lg">{item.icon}</span>
                <span>{item.label}</span>
              </Link>
            );
          })}
        </nav>
        <div className="p-4 border-t border-[#2a2e3f] text-xs text-gray-500">v0.1 | Paper Mode</div>
      </aside>

      {/* Mobile bottom nav */}
      <nav className="md:hidden fixed bottom-0 left-0 right-0 bg-[#1a1d27] border-t border-[#2a2e3f] z-50 flex justify-around py-1 safe-bottom">
        {nav.slice(0, 5).map((item) => {
          const active = pathname === item.href;
          return (
            <Link key={item.href} href={item.href}
              className={`flex flex-col items-center py-1.5 px-2 text-[10px] transition-colors ${
                active ? "text-blue-400" : "text-gray-500"
              }`}>
              <span className="text-lg">{item.icon}</span>
              <span>{item.label}</span>
            </Link>
          );
        })}
      </nav>
    </>
  );
}
