import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { Sidebar } from "./components/Sidebar";

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  title: "KalBot Dashboard",
  description: "Multi-strategy prediction market & stock trading bot",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${geistSans.variable} ${geistMono.variable} dark`}>
      <body className="min-h-screen bg-[#0f1117] text-gray-200 flex">
        <Sidebar />
        {/* Desktop: sidebar left margin. Mobile: bottom nav padding */}
        <main className="flex-1 md:ml-56 p-3 sm:p-4 md:p-6 overflow-auto pb-20 md:pb-6">
          {children}
        </main>
      </body>
    </html>
  );
}
