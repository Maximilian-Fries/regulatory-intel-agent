#!/usr/bin/env python3
"""
Regulatory Intelligence Agent
Fetches regulatory updates daily, evaluates them through a startup lens,
and sends a digest email.
"""

import os
import json
import sqlite3
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, asdict
from pathlib import Path

import feedparser
import requests
from anthropic import Anthropic

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent / "data" / "regulatory_intel.db"
DIGEST_OUTPUT_PATH = Path(__file__).parent / "data" / "latest_digest.html"
MAX_ITEMS_PER_FEED = 15          # per fetch cycle
MAX_AGE_DAYS = 3                 # ignore items older than this
RELEVANCE_THRESHOLD = 2          # 1-5 scale, keep items >= this
ANTHROPIC_MODEL = "claude-sonnet-4-20250514"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("reg-intel")


# ---------------------------------------------------------------------------
# Startup Lens – edit this to change what the agent looks for
# ---------------------------------------------------------------------------

STARTUP_LENS = """
You are a regulatory intelligence analyst working for someone exploring
startup opportunities across multiple industries. Your job is to evaluate
whether a regulatory update is relevant for someone looking to BUILD a
business around regulatory change.

The person is particularly interested in:
1. New compliance requirements that incumbents will struggle to meet
   (= opportunity to build compliance tooling / automation)
2. New licensing categories, sandboxes, or experimental frameworks that
   lower barriers to entry for startups
3. Deregulation that opens previously closed markets
4. Liability gaps or definitional ambiguities where a startup could position
   as the solution (insurance, certification, audit tooling)
5. Cross-border regulatory divergence that creates arbitrage opportunities
6. Reporting / disclosure requirements that could be productized as SaaS
7. AI-specific regulation that changes what's permitted or required

Industries of interest: Finance / Banking / Insurance, Construction,
Logistics / Supply Chain, Agriculture / Food, Healthcare (peripherally),
and any cross-cutting AI or data regulation.

Geographic focus (priority order): Switzerland, EU, US, UK.
"""


# ---------------------------------------------------------------------------
# Feed sources – add or remove as needed
# ---------------------------------------------------------------------------

FEEDS: list[dict] = [
    # --- Switzerland ---
    {
        "name": "FINMA News",
        "url": "https://www.finma.ch/en/news/rss/",
        "category": "finance",
        "jurisdiction": "CH",
    },
    {
        "name": "Swiss Federal Gazette",
        "url": "https://www.fedlex.admin.ch/eli/oc/rss/en",
        "category": "general",
        "jurisdiction": "CH",
    },
    {
        "name": "FDPIC News",
        "url": "https://www.edoeb.admin.ch/edoeb/en/home.rss",
        "category": "data_protection",
        "jurisdiction": "CH",
    },

    # --- EU ---
    {
        "name": "EUR-Lex Recent Acts",
        "url": "https://eur-lex.europa.eu/EN/display-feed.rss?myRssId=Vqc6cHJbEq2JLMfJvfyclw%3D%3D",
        "category": "general",
        "jurisdiction": "EU",
    },
    {
        "name": "EBA Publications",
        "url": "https://www.eba.europa.eu/rss-feeds",
        "category": "finance",
        "jurisdiction": "EU",
    },
    {
        "name": "ESMA News",
        "url": "https://www.esma.europa.eu/rss.xml",
        "category": "finance",
        "jurisdiction": "EU",
    },
    {
        "name": "European Commission – Digital Economy",
        "url": "https://digital-strategy.ec.europa.eu/en/rss.xml",
        "category": "technology",
        "jurisdiction": "EU",
    },

    # --- US ---
    {
        "name": "Federal Register – Proposed Rules",
        "url": "https://www.federalregister.gov/documents/search.atom?conditions%5Btype%5D=PRORULE",
        "category": "general",
        "jurisdiction": "US",
    },
    {
        "name": "Federal Register – Final Rules",
        "url": "https://www.federalregister.gov/documents/search.atom?conditions%5Btype%5D=RULE",
        "category": "general",
        "jurisdiction": "US",
    },
    {
        "name": "SEC Press Releases",
        "url": "https://www.sec.gov/news/pressreleases.rss",
        "category": "finance",
        "jurisdiction": "US",
    },
    {
        "name": "CFTC Press Room",
        "url": "https://www.cftc.gov/Newsroom/PressRoom/RSS",
        "category": "finance",
        "jurisdiction": "US",
    },

    # --- UK ---
    {
        "name": "FCA News",
        "url": "https://www.fca.org.uk/news/rss.xml",
        "category": "finance",
        "jurisdiction": "UK",
    },
    {
        "name": "Bank of England Publications",
        "url": "https://www.bankofengland.co.uk/rss/publications",
        "category": "finance",
        "jurisdiction": "UK",
    },

    # --- Law Firm Alerts (high-signal, pre-interpreted) ---
    {
        "name": "Freshfields Digital",
        "url": "https://technologyquotient.freshfields.com/feed/",
        "category": "legal_analysis",
        "jurisdiction": "INTL",
    },
    {
        "name": "Linklaters Fintech",
        "url": "https://www.linklaters.com/en/insights/blogs/fintechlinks/rss",
        "category": "legal_analysis",
        "jurisdiction": "INTL",
    },

    # --- Vertical: Agriculture ---
    {
        "name": "EFSA News",
        "url": "https://www.efsa.europa.eu/en/rss",
        "category": "agriculture",
        "jurisdiction": "EU",
    },
    {
        "name": "USDA APHIS Stakeholder Announcements",
        "url": "https://www.aphis.usda.gov/newsroom/stakeholder-info/rss/stakeholder-announcements.xml",
        "category": "agriculture",
        "jurisdiction": "US",
    },

    # --- Vertical: Logistics ---
    {
        "name": "EU Transport Policy",
        "url": "https://transport.ec.europa.eu/rss_en",
        "category": "logistics",
        "jurisdiction": "EU",
    },

    # --- Vertical: Construction ---
    {
        "name": "EU Energy & Buildings",
        "url": "https://energy.ec.europa.eu/rss_en",
        "category": "construction",
        "jurisdiction": "EU",
    },
]


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class RawItem:
    source: str
    title: str
    url: str
    summary: str
    published: str          # ISO date string
    category: str
    jurisdiction: str
    content_hash: str       # SHA-256 of title+url for dedup


@dataclass
class EvaluatedItem:
    raw: RawItem
    relevance_score: int    # 1-5
    verticals: list[str]
    startup_angle: str      # 2-3 sentence explanation
    headline: str           # one-line rewritten headline


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def init_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_items (
            content_hash TEXT PRIMARY KEY,
            first_seen   TEXT NOT NULL,
            source       TEXT,
            title        TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS digest_items (
            content_hash    TEXT PRIMARY KEY,
            digest_date     TEXT NOT NULL,
            relevance_score INTEGER,
            verticals       TEXT,
            startup_angle   TEXT,
            headline        TEXT,
            source          TEXT,
            title           TEXT,
            url             TEXT,
            jurisdiction    TEXT,
            published       TEXT
        )
    """)
    conn.commit()
    return conn


def is_seen(conn: sqlite3.Connection, content_hash: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM seen_items WHERE content_hash = ?", (content_hash,)
    ).fetchone()
    return row is not None


def mark_seen(conn: sqlite3.Connection, item: RawItem):
    conn.execute(
        "INSERT OR IGNORE INTO seen_items (content_hash, first_seen, source, title) VALUES (?, ?, ?, ?)",
        (item.content_hash, datetime.now(timezone.utc).isoformat(), item.source, item.title),
    )
    conn.commit()


def save_evaluated(conn: sqlite3.Connection, item: EvaluatedItem, digest_date: str):
    conn.execute(
        """INSERT OR REPLACE INTO digest_items
           (content_hash, digest_date, relevance_score, verticals, startup_angle,
            headline, source, title, url, jurisdiction, published)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            item.raw.content_hash, digest_date, item.relevance_score,
            json.dumps(item.verticals), item.startup_angle, item.headline,
            item.raw.source, item.raw.title, item.raw.url,
            item.raw.jurisdiction, item.raw.published,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Step 1: Fetch feeds
# ---------------------------------------------------------------------------

def fetch_all_feeds() -> list[RawItem]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    items: list[RawItem] = []

    for feed_cfg in FEEDS:
        try:
            log.info(f"Fetching: {feed_cfg['name']}")
            parsed = feedparser.parse(feed_cfg["url"])

            for entry in parsed.entries[:MAX_ITEMS_PER_FEED]:
                title = entry.get("title", "").strip()
                link = entry.get("link", "").strip()
                if not title or not link:
                    continue

                # Parse published date
                published_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                if published_parsed:
                    pub_dt = datetime(*published_parsed[:6], tzinfo=timezone.utc)
                    if pub_dt < cutoff:
                        continue
                    pub_str = pub_dt.isoformat()
                else:
                    pub_str = datetime.now(timezone.utc).isoformat()

                # Build summary from available fields
                summary = (
                    entry.get("summary", "")
                    or entry.get("description", "")
                    or ""
                )[:1500]  # truncate long HTML summaries

                content_hash = hashlib.sha256(f"{title}|{link}".encode()).hexdigest()

                items.append(RawItem(
                    source=feed_cfg["name"],
                    title=title,
                    url=link,
                    summary=summary,
                    published=pub_str,
                    category=feed_cfg["category"],
                    jurisdiction=feed_cfg["jurisdiction"],
                    content_hash=content_hash,
                ))
        except Exception as e:
            log.warning(f"Failed to fetch {feed_cfg['name']}: {e}")

    log.info(f"Fetched {len(items)} total items from {len(FEEDS)} feeds")
    return items


# ---------------------------------------------------------------------------
# Step 2: Evaluate via LLM
# ---------------------------------------------------------------------------

EVAL_PROMPT = """You are evaluating a regulatory/legal update for startup relevance.

<startup_lens>
{lens}
</startup_lens>

<item>
Source: {source} ({jurisdiction})
Title: {title}
Published: {published}
Category: {category}
Summary: {summary}
</item>

Respond with ONLY valid JSON (no markdown, no backticks):
{{
  "relevance_score": <1-5 integer, where 1=irrelevant noise, 3=potentially interesting, 5=high-impact opportunity or threat>,
  "verticals": [<list of affected verticals from: "finance", "insurance", "construction", "logistics", "agriculture", "healthcare", "technology", "cross_cutting">],
  "startup_angle": "<2-3 sentences: what changed, why it matters for a startup founder>",
  "headline": "<one punchy line summarising the opportunity or implication>"
}}
"""


def evaluate_item(client: Anthropic, item: RawItem) -> EvaluatedItem | None:
    try:
        response = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=500,
            messages=[{
                "role": "user",
                "content": EVAL_PROMPT.format(
                    lens=STARTUP_LENS,
                    source=item.source,
                    jurisdiction=item.jurisdiction,
                    title=item.title,
                    published=item.published,
                    category=item.category,
                    summary=item.summary[:1000],
                ),
            }],
        )

        text = response.content[0].text.strip()
        # Handle possible markdown wrapping
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        data = json.loads(text)

        return EvaluatedItem(
            raw=item,
            relevance_score=int(data["relevance_score"]),
            verticals=data["verticals"],
            startup_angle=data["startup_angle"],
            headline=data["headline"],
        )
    except Exception as e:
        log.warning(f"Evaluation failed for '{item.title}': {e}")
        return None


# ---------------------------------------------------------------------------
# Step 3: Generate digest
# ---------------------------------------------------------------------------

def generate_digest_html(items: list[EvaluatedItem], digest_date: str) -> str:
    """Generate an HTML digest email/page."""

    # Sort: highest relevance first, then by published date
    items.sort(key=lambda x: (-x.relevance_score, x.raw.published), reverse=False)

    top_items = [i for i in items if i.relevance_score >= 4]
    other_items = [i for i in items if i.relevance_score == RELEVANCE_THRESHOLD]

    jurisdiction_emoji = {
        "CH": "🇨🇭", "EU": "🇪🇺", "US": "🇺🇸", "UK": "🇬🇧", "INTL": "🌐",
    }

    def score_bar(score: int) -> str:
        return "🟢" * score + "⚪" * (5 - score)

    def render_item(item: EvaluatedItem) -> str:
        flag = jurisdiction_emoji.get(item.raw.jurisdiction, "🌐")
        verticals_str = ", ".join(v.replace("_", " ").title() for v in item.verticals)
        return f"""
        <div style="border-left: 4px solid {'#22c55e' if item.relevance_score >= 4 else '#f59e0b'};
                    padding: 12px 16px; margin-bottom: 16px;
                    background: #fafafa; border-radius: 0 8px 8px 0;">
            <div style="font-size: 13px; color: #6b7280; margin-bottom: 4px;">
                {flag} {item.raw.source} &middot; {verticals_str} &middot; {score_bar(item.relevance_score)}
            </div>
            <div style="font-size: 16px; font-weight: 600; margin-bottom: 6px;">
                <a href="{item.raw.url}" style="color: #111827; text-decoration: none;">
                    {item.headline}
                </a>
            </div>
            <div style="font-size: 14px; color: #374151; line-height: 1.5;">
                {item.startup_angle}
            </div>
            <div style="font-size: 12px; color: #9ca3af; margin-top: 6px;">
                Original: {item.raw.title} &middot; {item.raw.published[:10]}
            </div>
        </div>
        """

    top_section = ""
    if top_items:
        top_section = f"""
        <h2 style="color: #111827; border-bottom: 2px solid #22c55e; padding-bottom: 8px;">
            🎯 High-Signal Updates ({len(top_items)})
        </h2>
        {"".join(render_item(i) for i in top_items)}
        """

    other_section = ""
    if other_items:
        other_section = f"""
        <h2 style="color: #111827; border-bottom: 2px solid #f59e0b; padding-bottom: 8px;">
            📋 Worth Watching ({len(other_items)})
        </h2>
        {"".join(render_item(i) for i in other_items)}
        """

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
             max-width: 680px; margin: 0 auto; padding: 20px; background: #ffffff;">

    <div style="text-align: center; padding: 20px 0; border-bottom: 1px solid #e5e7eb;">
        <h1 style="font-size: 22px; color: #111827; margin: 0;">
            Regulatory Intel Digest
        </h1>
        <p style="color: #6b7280; font-size: 14px; margin: 4px 0 0 0;">
            {digest_date} &middot; {len(items)} relevant updates found
        </p>
    </div>

    <div style="padding: 20px 0;">
        {top_section}
        {other_section}
    </div>

    {f'<p style="color:#9ca3af; text-align:center; font-size:13px;">No relevant updates today.</p>' if not items else ""}

    <div style="border-top: 1px solid #e5e7eb; padding-top: 16px; margin-top: 20px;
                font-size: 12px; color: #9ca3af; text-align: center;">
        Regulatory Intel Agent &middot; Filtered from {len(FEEDS)} sources &middot;
        Relevance threshold: {RELEVANCE_THRESHOLD}/5
    </div>
</body>
</html>"""

    return html


# ---------------------------------------------------------------------------
# Step 4: Email delivery (optional, via SMTP)
# ---------------------------------------------------------------------------

def send_email(html: str, subject: str):
    """Send digest via email. Configure SMTP in environment variables."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    email_to = os.environ.get("EMAIL_TO", "")

    if not all([smtp_host, smtp_user, smtp_pass, email_to]):
        log.info("SMTP not configured — skipping email delivery")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = email_to
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, email_to, msg.as_string())

    log.info(f"Digest email sent to {email_to}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=== Regulatory Intel Agent — starting run ===")

    # Init
    conn = init_db()
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set. Exiting.")
        return
    client = Anthropic(api_key=api_key)

    # Step 1: Fetch
    raw_items = fetch_all_feeds()

    # Deduplicate against previously seen items
    new_items = []
    for item in raw_items:
        if not is_seen(conn, item.content_hash):
            new_items.append(item)
            mark_seen(conn, item)
    log.info(f"{len(new_items)} new items (after dedup)")

    if not new_items:
        log.info("No new items. Generating empty digest.")
        digest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        html = generate_digest_html([], digest_date)
        DIGEST_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        DIGEST_OUTPUT_PATH.write_text(html)
        log.info(f"Empty digest saved to {DIGEST_OUTPUT_PATH}")
        send_email(html, f"🔍 Regulatory Intel Digest — {digest_date}")
        return

    # Step 2: Evaluate
    evaluated: list[EvaluatedItem] = []
    for i, item in enumerate(new_items):
        log.info(f"Evaluating [{i+1}/{len(new_items)}]: {item.title[:80]}")
        result = evaluate_item(client, item)
        if result and result.relevance_score >= RELEVANCE_THRESHOLD:
            evaluated.append(result)
            log.info(f"  → Score {result.relevance_score}/5: {result.headline}")
        elif result:
            log.info(f"  → Score {result.relevance_score}/5: below threshold, skipping")

    log.info(f"{len(evaluated)} items passed relevance threshold")

    # Step 3: Generate digest
    digest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for item in evaluated:
        save_evaluated(conn, item, digest_date)

    html = generate_digest_html(evaluated, digest_date)
    DIGEST_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    DIGEST_OUTPUT_PATH.write_text(html)
    log.info(f"Digest saved to {DIGEST_OUTPUT_PATH}")

    # Step 4: Email (if configured)
    send_email(html, f"🔍 Regulatory Intel Digest — {digest_date}")

    # Summary
    log.info("=== Run complete ===")
    log.info(f"  Sources checked:   {len(FEEDS)}")
    log.info(f"  New items found:   {len(new_items)}")
    log.info(f"  Relevant items:    {len(evaluated)}")

    conn.close()


if __name__ == "__main__":
    main()
