"""Market data fetching and ticker validation using yfinance."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def enrich_ticker(ticker: str) -> dict[str, Any]:
    """
    Fetch basic market data for a ticker symbol.
    Returns an enriched dict; returns minimal stub if ticker is invalid.
    """
    try:
        import yfinance as yf

        t = yf.Ticker(ticker)
        info = t.info

        # Validate that the ticker exists and returned meaningful data
        name = info.get("longName") or info.get("shortName") or ""
        if not name:
            return _unknown_ticker(ticker)

        # 1-month price performance
        hist = t.history(period="1mo")
        if not hist.empty:
            start_price = float(hist["Close"].iloc[0])
            end_price = float(hist["Close"].iloc[-1])
            perf_1m = round((end_price - start_price) / start_price * 100, 2)
        else:
            perf_1m = None

        # 52-week range
        week52_low = info.get("fiftyTwoWeekLow")
        week52_high = info.get("fiftyTwoWeekHigh")
        current_price = info.get("currentPrice") or info.get("regularMarketPrice")

        return {
            "ticker": ticker.upper(),
            "name": name,
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "market_cap": info.get("marketCap"),
            "current_price": current_price,
            "currency": info.get("currency", "USD"),
            "perf_1m_pct": perf_1m,
            "week52_low": week52_low,
            "week52_high": week52_high,
            "pe_ratio": info.get("trailingPE"),
            "description": (info.get("longBusinessSummary") or "")[:400],
            "valid": True,
        }
    except Exception as exc:
        logger.warning("Could not fetch market data for %s: %s", ticker, exc)
        return _unknown_ticker(ticker)


def _unknown_ticker(ticker: str) -> dict[str, Any]:
    return {
        "ticker": ticker.upper(),
        "name": ticker,
        "sector": "",
        "industry": "",
        "market_cap": None,
        "current_price": None,
        "currency": "USD",
        "perf_1m_pct": None,
        "week52_low": None,
        "week52_high": None,
        "pe_ratio": None,
        "description": "",
        "valid": False,
    }


def enrich_tickers(tickers: list[str]) -> dict[str, dict[str, Any]]:
    """Enrich a list of ticker symbols, returning a dict keyed by ticker."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    result: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(enrich_ticker, t): t for t in tickers}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result[ticker.upper()] = future.result()
            except Exception as exc:
                logger.warning("Ticker enrichment failed for %s: %s", ticker, exc)
                result[ticker.upper()] = _unknown_ticker(ticker)
    return result
