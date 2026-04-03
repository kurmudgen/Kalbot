"""
Reliable process checker for KalBot.
Works on Windows with pythonw.exe and python.exe processes.
"""

import subprocess
import os


def get_bot_status() -> dict:
    """Check which bot processes are running.

    Returns dict with keys: dual_strategy, stock_bot, watchdog, ollama
    Each value is True/False.
    """
    status = {
        "dual_strategy": False,
        "stock_bot": False,
        "watchdog": False,
        "ollama": False,
    }

    try:
        # wmic with single quotes works on Windows
        result = subprocess.run(
            ["wmic", "process", "where",
             "name='pythonw.exe' or name='python.exe'",
             "get", "commandline"],
            capture_output=True, text=True, timeout=10,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        output = result.stdout
        status["dual_strategy"] = "dual_strategy" in output
        status["stock_bot"] = "run_stock_bot" in output
        status["watchdog"] = "watchdog" in output
    except Exception:
        pass

    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq ollama.exe", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=5,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        status["ollama"] = "ollama" in result.stdout.lower()
    except Exception:
        pass

    return status


def status_line() -> str:
    """One-line status string for display."""
    s = get_bot_status()
    parts = []
    for name, running in s.items():
        parts.append(f"{name}={'ON' if running else 'OFF'}")
    kill = "ACTIVE" if os.path.exists(os.path.join(os.path.dirname(__file__), "..", "STOP")) else "off"
    return " | ".join(parts) + f" | kill_switch={kill}"


if __name__ == "__main__":
    print(status_line())
