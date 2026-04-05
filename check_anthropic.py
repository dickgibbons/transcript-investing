#!/usr/bin/env python3
"""One-shot check: does your Anthropic API key work from this machine? Run on the Droplet."""

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env", override=True, encoding="utf-8-sig")

from src.secrets import anthropic_api_key


def _diagnose_key_env() -> None:
    raw = os.environ.get("ANTHROPIC_API_KEY") or ""
    if raw != raw.strip():
        print("WARNING: key had leading/trailing whitespace in the environment (strip applied).")
    if "\n" in raw or "\r" in raw:
        print("WARNING: key contains a newline — use a single line in .env (no line break in the key).")
    if raw and not raw.strip().startswith("sk-ant-"):
        print(
            "WARNING: value does not start with sk-ant- — "
            "this may be an OpenAI or other vendor key, not an Anthropic API key."
        )


def main() -> None:
    _diagnose_key_env()
    key = anthropic_api_key()
    print(f"Key length: {len(key)} (typical full key ~95–110 chars, starts with sk-ant-api03-...)")

    import anthropic

    client = anthropic.Anthropic(api_key=key)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=20,
        messages=[{"role": "user", "content": "Reply with exactly: OK"}],
    )
    text = ""
    for b in msg.content:
        if hasattr(b, "text"):
            text += b.text
    print("API response:", text.strip() or "(empty)")
    print("SUCCESS — key is valid. If main.py still fails, say what error you see.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("FAILED:", type(e).__name__, e)
        print()
        print("Fix: open https://console.anthropic.com → API keys → create new key")
        print("Paste into .env as ONE line (no quotes needed):")
        print("  ANTHROPIC_API_KEY=sk-ant-api03-...")
        print("Then: unset ANTHROPIC_API_KEY   # if you ever exported a wrong key in this shell")
        print("If 401 persists with a brand-new key, confirm Console → Workspaces shows API access enabled.")
        raise SystemExit(1)
