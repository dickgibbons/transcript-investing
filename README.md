# TranscriptInvest

AI-powered investment intelligence extracted from public speech — earnings calls, YouTube keynotes, news interviews, and podcasts.

## How It Works

1. **Scrape** — For each entity in your watchlist, the agent searches all public speech from the last 30 days across four sources: Seeking Alpha (earnings calls), YouTube, news/conference interviews, and podcasts.
2. **Analyze** — Each transcript is sent to Claude Opus 4.6 (with adaptive thinking) to extract forward-looking signals: specific bets the speaker is making about AI, technology, and the economy.
3. **Map** — A second Claude pass synthesizes signals across all entities into ranked investment opportunities (macro theme → sector → ETFs → individual stocks → private/crypto).
4. **Report** — An interactive HTML dashboard and a downloadable PDF are written to `output/<date>/`.

---

## Quick Start

### 1. Clone and set up

```bash
cd "AI Projects/Transcript Investing"
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and fill in your API keys
```

Required:
- `ANTHROPIC_API_KEY` — your Claude API key

Strongly recommended (enables more sources):
- `TAVILY_API_KEY` — web/news/podcast search ([tavily.com](https://tavily.com), free tier available)
- `YOUTUBE_API_KEY` — YouTube Data API v3 ([console.cloud.google.com](https://console.cloud.google.com))
- `SEEKING_ALPHA_EMAIL` + `SEEKING_ALPHA_PASSWORD` — your existing premium account

Optional:
- `OPENAI_API_KEY` — enables Whisper transcription for podcasts without text transcripts
- `POLYGON_API_KEY` — additional market data source

### 3. Edit your watchlist

```bash
# Open config/watchlist.yaml and add your entities, or use the CLI:
python main.py watchlist add "Elon Musk" --type person --alias "Tesla CEO" --alias "SpaceX"
python main.py watchlist add "Amazon" --ticker AMZN --alias "Andy Jassy"
```

### 4. Run the pipeline

```bash
python main.py run
```

### 5. Open the dashboard

```bash
python main.py open
# Or: open output/latest/index.html
```

---

## CLI Reference

```
python main.py run                   # Full pipeline run
python main.py run --dry-run         # Scrape only, skip Claude API calls
python main.py watchlist list        # Show watchlist
python main.py watchlist add NAME    # Add entity (--ticker, --type, --alias)
python main.py watchlist remove NAME # Remove entity
python main.py history               # Show run history
python main.py open                  # Open latest dashboard in browser
```

---

## VPS Deployment

### Prerequisites

- Ubuntu/Debian VPS (or similar)
- Python 3.11+
- Git

### 1. Upload the project

```bash
# On your local machine:
scp -r "AI Projects/Transcript Investing" user@your-vps:/opt/transcript-invest

# Or use git:
ssh user@your-vps
git clone https://your-repo.git /opt/transcript-invest
```

### 2. Set up Python environment on the VPS

```bash
ssh user@your-vps
cd /opt/transcript-invest

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
playwright install-deps chromium
```

### 3. Create the .env file

```bash
cp .env.example .env
nano .env   # Fill in your API keys
```

### 4. Test a manual run

```bash
source .venv/bin/activate
python main.py run
```

### 5. Deploy as a systemd service (recommended)

Create `/etc/systemd/system/transcript-invest.service`:

```ini
[Unit]
Description=TranscriptInvest Scheduler
After=network.target

[Service]
Type=simple
User=YOUR_USERNAME
WorkingDirectory=/opt/transcript-invest
ExecStart=/opt/transcript-invest/.venv/bin/python scheduler.py
Restart=always
RestartSec=60
Environment=PYTHONUNBUFFERED=1

# Load environment variables from .env
EnvironmentFile=/opt/transcript-invest/.env

[Install]
WantedBy=multi-user.target
```

Then enable and start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable transcript-invest
sudo systemctl start transcript-invest
sudo systemctl status transcript-invest

# View logs:
sudo journalctl -u transcript-invest -f
```

### 6. Alternative: run in a screen session

If you prefer not to use systemd:

```bash
screen -S transcript-invest
cd /opt/transcript-invest
source .venv/bin/activate
python scheduler.py
# Detach with Ctrl+A, D
# Reattach with: screen -r transcript-invest
```

### 7. Access output files

Output is written to `/opt/transcript-invest/output/<date>/`:

```
output/
├── latest -> 2026-04-05/        # Symlink to most recent run
└── 2026-04-05/
    ├── index.html               # Interactive dashboard
    └── report_2026-04-05.pdf    # Formatted PDF
```

To copy the latest report to your local machine:

```bash
# On your local machine:
scp -r user@your-vps:/opt/transcript-invest/output/latest ./latest-report
open ./latest-report/index.html
```

Or serve it over HTTP with nginx:

```nginx
# /etc/nginx/sites-available/transcript-invest
server {
    listen 80;
    server_name your-domain.com;

    root /opt/transcript-invest/output/latest;
    index index.html;

    location / {
        try_files $uri $uri/ =404;
    }
}
```

---

## Configuration

### `config/watchlist.yaml`

```yaml
watchlist:
  - name: "Jensen Huang"
    type: person          # "person" or "company"
    ticker: NVDA          # Optional — enables market data enrichment
    aliases:
      - "NVIDIA CEO"
      - "NVDA earnings"
    seeking_alpha_slug: NVDA   # seekingalpha.com/symbol/<slug>
```

### `config/settings.yaml`

| Setting | Default | Description |
|---|---|---|
| `scraping.lookback_days` | 30 | How many days back to search |
| `scraping.max_results_per_source` | 10 | Max results per entity per source |
| `analysis.model` | `claude-opus-4-6` | Claude model for transcript analysis |
| `investment.top_opportunities` | 10 | How many opportunities to generate |
| `scheduler.day_of_week` | `sun` | Day of weekly run |
| `scheduler.hour` | 20 | Hour of weekly run (24h) |
| `scheduler.timezone` | `America/New_York` | Timezone for scheduling |
| `output.keep_runs` | 10 | Number of historical runs to retain |

---

## Project Structure

```
.
├── config/
│   ├── watchlist.yaml       # Who to track
│   └── settings.yaml        # Pipeline configuration
├── src/
│   ├── scrapers/            # Four scrapers (SA, YouTube, News, Podcast)
│   ├── processors/          # Text cleaning + Claude analyzer
│   ├── investment/          # Signal mapper + market data
│   ├── output/              # Dashboard + PDF generators
│   ├── db.py                # SQLite interface
│   └── pipeline.py          # End-to-end orchestration
├── main.py                  # CLI entry point
├── scheduler.py             # APScheduler cron runner
├── data/                    # SQLite database (auto-created)
├── output/                  # Generated reports (auto-created)
└── requirements.txt
```

---

## API Keys Needed

| Service | Purpose | Cost | Signup |
|---|---|---|---|
| Anthropic | Core analysis (Claude Opus 4.6) | ~$5/1M input tokens | [console.anthropic.com](https://console.anthropic.com) |
| Tavily | Web/news/podcast search | Free tier: 1k/mo | [tavily.com](https://tavily.com) |
| YouTube Data API v3 | YouTube transcript search | Free (10k/day quota) | [console.cloud.google.com](https://console.cloud.google.com) |
| Seeking Alpha | Earnings call transcripts | Existing premium login | — |
| OpenAI | Whisper podcast transcription | ~$0.006/min | [platform.openai.com](https://platform.openai.com) |
| Polygon.io | Stock/ETF market data | Free tier | [polygon.io](https://polygon.io) |

---

## Disclaimer

This tool is for informational and research purposes only. It does not constitute financial advice. Always conduct your own due diligence before making investment decisions.
