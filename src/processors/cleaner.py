"""Transcript text normalization and chunking utilities."""

import re
from typing import Iterator


MAX_CHUNK_CHARS = 80_000


def normalize(text: str) -> str:
    """Remove boilerplate, excessive whitespace, and noise from transcript text."""
    # Strip common article boilerplate patterns
    boilerplate_patterns = [
        r"(?i)subscribe (now|today|to).*",
        r"(?i)sign up for.*newsletter.*",
        r"(?i)advertisement\s*",
        r"(?i)related articles?:.*",
        r"(?i)read more:.*",
        r"(?i)click here.*",
        r"(?i)\[music\]|\[applause\]|\[laughter\]|\[inaudible\]",
        r"https?://\S+",         # bare URLs
        r"\s{3,}",               # excessive whitespace → double newline
    ]
    for pat in boilerplate_patterns[:-1]:
        text = re.sub(pat, " ", text)
    text = re.sub(r"\s{3,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def chunk_transcript(text: str, max_chars: int = MAX_CHUNK_CHARS) -> list[str]:
    """
    Split a long transcript into overlapping chunks that fit within max_chars.
    Splits prefer paragraph boundaries to avoid cutting mid-sentence.
    """
    if len(text) <= max_chars:
        return [text]

    paragraphs = re.split(r"\n\n+", text)
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    overlap_buffer: list[str] = []

    for para in paragraphs:
        para_len = len(para) + 2  # +2 for "\n\n"
        if current_len + para_len > max_chars and current:
            chunks.append("\n\n".join(current))
            # Keep last ~10% of current chunk as overlap for context continuity
            overlap_target = max_chars // 10
            overlap_buffer = []
            overlap_len = 0
            for p in reversed(current):
                if overlap_len + len(p) > overlap_target:
                    break
                overlap_buffer.insert(0, p)
                overlap_len += len(p) + 2
            current = overlap_buffer[:]
            current_len = overlap_len

        current.append(para)
        current_len += para_len

    if current:
        chunks.append("\n\n".join(current))

    return chunks
