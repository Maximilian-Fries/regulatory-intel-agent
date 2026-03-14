# Regulatory Intelligence Agent

A daily automated agent that monitors 20+ regulatory sources across Switzerland, EU, US, and UK — evaluates each update through a startup opportunity lens using Claude — and delivers a prioritized digest.

## What it does

1. **Fetches** RSS feeds from regulators (FINMA, SEC, FCA, EBA, ESMA...), law firm blogs, and vertical-specific sources across finance, agriculture, logistics, and construction
2. **Deduplicates** against a local SQLite database so you never see the same item twice
3. **Evaluates** each new item via Claude API with a customizable "startup lens" prompt — scoring relevance 1-5 and explaining the startup angle
4. **Generates** an HTML digest with high-signal items (score 4-5) and worth-watching items (score 3)
5. **Optionally emails** the digest to you via SMTP

## Quick start

### Option A: GitHub Actions (recommended)

1. Fork or push this repo to GitHub
2. Go to **Settings → Secrets and variables → Actions**
3. Add secret: `ANTHROPIC_API_KEY` = your Anthropic API key
4. (Optional) Add email secrets: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `EMAIL_TO`
5. The agent runs daily at 08:00 Zurich time. You can also trigger it manually from the **Actions** tab

Digests are saved as GitHub Actions artifacts (downloadable HTML files) and optionally emailed.

### Option B: Run locally

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY="sk-ant-..."
python agent.py
# Digest saved to data/latest_digest.html
```

## Configuration

### Startup Lens

Edit the `STARTUP_LENS` variable in `agent.py` to change what the agent looks for. This is the core prompt that shapes all evaluations. As your focus shifts (e.g., from broad exploration to a specific vertical), update this lens.

### Sources

Add or remove feeds in the `FEEDS` list in `agent.py`. Each feed needs:
- `name`: display name
- `url`: RSS/Atom feed URL
- `category`: one of `finance`, `agriculture`, `logistics`, `construction`, `technology`, `general`, `legal_analysis`, `data_protection`
- `jurisdiction`: `CH`, `EU`, `US`, `UK`, or `INTL`

### Thresholds

- `RELEVANCE_THRESHOLD`: minimum score (1-5) to include in digest. Default: 3
- `MAX_AGE_DAYS`: ignore feed items older than this. Default: 3
- `MAX_ITEMS_PER_FEED`: cap per feed per run. Default: 15

## Cost estimate

Each run evaluates ~20-80 new items (varies by day). At ~500 tokens per evaluation using Claude Sonnet, that's roughly $0.05-0.20 per daily run, or **~$3-6/month**.

## File structure

```
regulatory-intel-agent/
├── agent.py                     # Main script
├── requirements.txt
├── .github/workflows/
│   └── daily-run.yml            # GitHub Actions schedule
├── data/                        # Created at runtime (gitignored)
│   ├── regulatory_intel.db      # SQLite: seen items + evaluated items
│   └── latest_digest.html       # Most recent digest
└── README.md
```

## Extending

**Add web scraping**: For sources without RSS (some regulator sites), add a scraping function using `requests` + `BeautifulSoup` and feed the results into the same pipeline.

**Weekly summary**: Add a second script that queries the SQLite database for the past 7 days and generates a weekly trend report via Claude.

**Slack/Telegram delivery**: Replace or supplement the email function with a webhook-based notification.
