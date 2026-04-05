#!/usr/bin/env python3
"""One-shot check: does your Anthropic API key work from this machine? Run on the Droplet."""

from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env", override=True, encoding="utf-8-sig")

from src.secrets import anthropic_api_key


def main() -> None:
    key = anthropic_api_key()
    print(f"Key length: {len(key)} (expect ~90+ for a full sk-ant-... key)")

    import anthropic

    client = anthropic.Anthropic(api_key=key)
    msg = client.messages.create(
        model="claude-3-5-haiku-20241022",
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
        print("Paste into .env as: ANTHROPIC_API_KEY=sk-ant-...")
        raise SystemExit(1)
