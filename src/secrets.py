"""Read API keys from the environment (.env loaded by main / pipeline)."""

import os


def anthropic_api_key() -> str:
    k = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not k:
        raise RuntimeError(
            "ANTHROPIC_API_KEY is missing or empty. Check /opt/transcript-invest/.env "
            "(no spaces around =, one line, save as plain UTF-8)."
        )
    return k
