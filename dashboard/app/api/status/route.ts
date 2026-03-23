import { NextResponse } from "next/server";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";

const STATUS_FILE = join(process.cwd(), "public", "status.json");

// GET: return the latest status
export async function GET() {
  try {
    if (existsSync(STATUS_FILE)) {
      const data = readFileSync(STATUS_FILE, "utf-8");
      return NextResponse.json(JSON.parse(data));
    }
    return NextResponse.json({ error: "No status data yet" }, { status: 404 });
  } catch {
    return NextResponse.json({ error: "Failed to read status" }, { status: 500 });
  }
}

// POST: update the status (called by the bot)
export async function POST(request: Request) {
  try {
    const body = await request.json();
    writeFileSync(STATUS_FILE, JSON.stringify(body, null, 2));
    return NextResponse.json({ ok: true });
  } catch {
    return NextResponse.json({ error: "Failed to write status" }, { status: 500 });
  }
}
