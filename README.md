# AI Hugh — AI-Powered SDR Execution Platform

> AI Hugh is a full-stack AI execution platform that automates the entire SDR workflow — from lead prioritization and personalized outreach to meeting processing, product intelligence extraction, and market intelligence. Built as a solo project by an SDR who wanted to work smarter, it now runs autonomously with **4 scheduled agents, 10 integrated databases, and 3 AI APIs.**

---

## What this is

This repo is a sanitized, public-portfolio version of a production system I run every day in my SDR role. The architecture, scoring logic, pipelines, and agent design are all preserved. Real customer data, credentials, and Notion database IDs have been replaced with placeholders.

It is an honest record of how an SDR with engineering chops can compound their output by treating their pipeline as a software problem.

---

## What's Not in This Public Version

This repo is a sanitized portfolio version of a production system. The following modules exist in the private version but are excluded here because they contain employer-specific configurations, credentials infrastructure, or proprietary workflow logic:

**Automation & Scheduling:** `overnight_run.sh`, `morning_notify.sh`, `overnight_prompt.md` — launchd-scheduled agents for autonomous overnight execution and morning briefing delivery.

**Pipeline Scripts:** `check_claims.py`, `build_today_send_apac.py`, `bootstrap_notion_product_feedback_db.py`, `crm_import/run.py` — CRM data import, APAC localization pipeline, and Notion database bootstrapping.

**Core Modules:** `translations.py` (OpenAI-powered KR/JP/CN localization), `triage.py` (lead urgency scoring queue), `product_intel.py` (product intelligence extraction from sales conversations), `templates.py` (email template engine).

**Market Intelligence Pipeline:** `market_intel/` — RSS ingestion, relevance scoring, daily/weekly briefs, and podcast generation across 8 product-specific feeds.

**Knowledge-Base Pipelines:** `extract_product_intel.py`, `process_product_knowledge.py`, `summarize_extracted_intel.py`, `run_signal_matching.py` — product intelligence extraction, knowledge compilation, and signal matching against the private knowledge base.

**Tests:** `core/tests/test_notes.py`, `test_transcripts.py` — unit tests for transcript processing and note normalization.

**Experimental:** `podcast_generator.py`, `generate_podcast_series.py` — AI-generated audio briefing pipeline (prototype).

The architecture diagrams and system documentation describe the full production system. Everything in this repo runs independently — the excluded modules extend functionality but aren't required dependencies.

---

## Architecture

```
                    ┌────────────────────────────┐
                    │   Meeting Notes            │
                    │   (structured input)       │
                    └─────────────┬──────────────┘
                                  ▼
                    ┌────────────────────────────┐
                    │   process_meeting.py /     │  ← Claude API (Opus)
                    │   process_call.py          │     extracts tasks,
                    └─────────────┬──────────────┘     objections, deals
                                  ▼
   ┌──────────────────────────────────────────────────────────┐
   │                       Notion                             │
   │  Companies · Contacts · Tasks · Meetings · Objections    │
   │  Proof Points · Product Feedback · Deals · Comp Intel    │
   │  Sprints (10 databases — sync via core/notion.py)        │
   └──────────────────────────────────────────────────────────┘
                                  │
                                  ▼
   ┌──────────────────────────────────────────────────────────┐
   │  scripts/build_today_send.py                             │
   │  scripts/build_today_calls.py                            │
   │  scripts/build_call_sheet.py    → persona-tailored copy  │
   │  scripts/build_today_batch.py   → APAC localization      │
   └────────────────────────────┬─────────────────────────────┘
                                ▼
   ┌──────────────────────────────────────────────────────────┐
   │  send_emails.py        SMTP (Office 365) + Salesforce    │
   │                        BCC for CRM activity logging      │
   └──────────────────────────────────────────────────────────┘

   ┌──────────────────────────────────────────────────────────┐
   │  Market Intelligence pipeline (market_intel/)            │
   │  RSS feeds → fetch → score (Claude) → daily/weekly briefs│
   └──────────────────────────────────────────────────────────┘

   ┌──────────────────────────────────────────────────────────┐
   │  Overnight agent (Claude Code, scheduled via launchd)    │
   │  Builds APAC batch, prunes memory, runs claims scanner,  │
   │  drafts morning briefing — all without supervision.      │
   └──────────────────────────────────────────────────────────┘
```

Four scheduled agents run via macOS `launchd`:

| Schedule | Job | What it does |
|---|---|---|
| Daily 02:00 | `overnight_run.sh` (Claude Code) | APAC batch, memory pruning, claims scan, briefing draft |
| Weekdays 06:30 | `morning_notify.sh` | Push notification with top action |

(Two additional weekly market-intelligence jobs run in the production version — see "What's Not in This Public Version" above.)

---

## Key features

### 1. Automated lead prioritization & outreach
- `core/task_engine.py` — urgency (0–100) and confidence (0–100) scoring with deterministic, explainable rules.
- Priority order: revenue actions → confirmed meetings → external dependency next steps → internal projects → long-term work.
- Channel routing with work-hour and channel-hour enforcement.
- "Live confirmation rule": if you're actively talking to the lead, prefer live confirmation over creating a follow-up task.

### 2. Product intelligence extraction pipeline
- `extract_product_intel.py` — parses every PDF / DOCX / PPTX / CSV in `intel/<campaign>/raw/`.
- `process_product_knowledge.py` — routes extracted text into product-specific knowledge briefs.
- `summarize_extracted_intel.py` — rolls briefs up into a learning-ready brain document.
- The same knowledge briefs feed sales talk tracks **and** the training podcast generator.

### 3. Market intelligence pipeline
- `market_intel/fetch_sources.py` — pulls 8 curated RSS feeds (sustainability, regulatory, compliance).
- `market_intel/score_relevance.py` — batch-scores items 0–10 with Claude using product-specific buyer-pain context.
- `market_intel/generate_daily_brief.py` — top items above threshold + "talk track" hooks for each item.
- `market_intel/generate_weekly_*.py` — three per-product weekly briefs + a weekly intelligence brief + a 25-min podcast script.

### 4. Overnight autonomous agent
- The system runs Claude Code at 02:00 against `overnight_prompt.md`.
- It builds the APAC send batch (timezone-respecting), prunes decision memory older than 90 days, runs the claims-verification scanner, syncs Notion deltas, and drafts the morning briefing.
- Anything ambiguous gets logged to `overnight_log/` for the morning review.

### 5. APAC localization engine
- `scripts/build_today_send_apac.py` (in production) localizes templates for KR / JP / CN / TW with timezone-aware send windows.
- Translation registry catches name-romanization edge cases overnight, before sends.

### 6. Claims verification scanner
- `scripts/check_claims.py` (in production) scans every generated email and template for claims that aren't backed by a citation in the proof-points DB.
- Catches LLM-hallucinated stats before they ship.

### 7. CRM integration (Notion + Salesforce)
- `core/notion.py` — 67 functions covering all 10 databases.
- Salesforce activity logging via SMTP BCC (`SALESFORCE_BCC`) — every send appears as activity in SF without an API integration.
- Bidirectional drift detection between Notion and `config/tasks.json`.

---

## Tech stack

- **Languages:** Python 3, Bash
- **AI:** Anthropic Claude API (Opus for extraction, Sonnet for scoring), OpenAI API (translations + synthesis)
- **Data:** Notion API (10 databases), CSV, JSON
- **Email:** Office 365 SMTP
- **Scheduling:** macOS `launchd` (4 plist jobs)
- **Agent orchestration:** Claude Code CLI (overnight automation)

---

## Database schema (Notion)

| Database | Key fields |
|---|---|
| **Companies** | name, domain, segment, ICP score, region, parent, owner |
| **Contacts** | first/last, email, title, persona, company → Companies |
| **Tasks** | due_at, urgency, confidence, channel, status, lead → Contacts |
| **Meetings** | date, attendees, summary, next_steps, source link |
| **Objections** | objection text, handling script, source meeting |
| **Proof Points** | claim, citation, vertical, persona, format (link/quote/case) |
| **Product Feedback** | request, source meeting, severity, status, owner |
| **Deals** | stage, ARR, close_date, decision_makers, blockers |
| **Competitive Intel** | competitor, scenario, response script, source |
| **Sprints** | start, end, theme, target accounts, KPIs |

---

## How a typical day works

1. **06:30 — Morning briefing.** `morning_notify.sh` reads the overnight summary and sends a push notification: *top action, overdue, due today, upcoming, multi-thread gaps, accounts going stale, new outreach signals.*
2. **08:00 — Build batch.** `scripts/build_today_send.py` dedupes against `sent_log`, Salesforce activity, and exclusion segments; emits `today_send.csv` sorted east → west by inferred timezone.
3. **08:30 — Send.** `send_emails.py` runs schema guards, sends through O365 SMTP with Salesforce BCC, logs every attempt to `sent_log.csv`.
4. **Throughout the day — Process calls.** Meeting notes drop into `notes/inbox/`. `process_call.py` and `process_meeting.py` use Claude to extract tasks, objections, competitive intel, and product feedback — all written back to Notion.
5. **17:00 — Wrap.** `today.py` shows what got done vs. what slipped. Anything that slipped is rescored and re-routed for tomorrow.
6. **02:00 — Overnight agent.** Claude Code runs the APAC batch, prunes memory, sweeps claims, and drafts the next morning's briefing.
7. **05:30 — Market intel.** Daily RSS sweep + scoring + brief generation. Hot items get surfaced in the morning briefing's "new outreach signals" section.

---

## Stats

- **10** Notion databases, **10,400+** total entries across them
- **4** scheduled agents (daily, weekly, morning, overnight)
- **3** AI API integrations (Anthropic, OpenAI, Notion)
- **8** RSS feeds monitored
- **4** APAC markets supported (KR / JP / CN / TW)
- **67** Notion API helper functions in `core/notion.py`

---

## Running the system

```bash
# Run main demo / orchestration
python3 core/main.py

# Sales playbook ingestion
python3 process_sales_playbook.py         # Convert sales playbook CSVs to JSON

# Daily send loop
python3 scripts/build_today_send.py
python3 send_emails.py
```

The intelligence and market-intel pipelines (`extract_product_intel.py`, `process_product_knowledge.py`, `summarize_extracted_intel.py`, `market_intel/run_*`) ship in the production version only — see "What's Not in This Public Version" above.

No package manager required for the v1 core — pure Python 3 standard library plus `anthropic`, `openai`, `notion-client`, `python-dotenv`. Install with:

```bash
pip install -r requirements.txt
```

---

## Setup

1. **Clone and install.**
   ```bash
   git clone <your-fork>
   cd ai-hugh
   pip install -r requirements.txt
   ```
2. **Copy `.env.example` → `.env`** and fill in your API keys + Notion DB IDs.
3. **Bootstrap your Notion databases.**
   ```bash
   python3 scripts/bootstrap_notion_tasks_db.py
   python3 scripts/bootstrap_notion_product_feedback_db.py
   ```
4. **Drop your campaign source data** into `intel/<your-campaign>/raw/` (PDFs, decks, CSVs).
5. **Run the extraction pipeline** (see "Running the system" above).
6. **Configure your work hours** in `config/preferences.json`.
7. **Optional — install launchd jobs** by copying files in `launchd/` to `~/Library/LaunchAgents/` and running `launchctl load <plist>`.

---

## Operating rules (encoded in the system)

- **Priority order:** revenue → confirmed meetings → external next steps → internal → long-term.
- **Urgency scoring:** 90–100 act now · 70–89 today · 40–69 soon · <40 backlog.
- **Confidence is separate** from urgency. A 95-urgency / 42-confidence task gets routed to a call, not an email.
- **Live confirmation rule:** If you're talking to them, get confirmation live; don't create a follow-up task unless the next step is genuinely ambiguous and the conversation is over.
- **Safety:** Never auto-send messages or auto-book meetings. Recommend first, execute only with approval. The default `integration_mode` is `dry_run`.

See `docs/OPERATING_RULES.md` for the full set.

---

## Why I built this

I'm a quota-carrying SDR. Most SDR tooling tries to make you faster at the same low-leverage motions — write more emails, make more dials, hit more activity targets. The real bottleneck is *judgment*: which 5 leads matter today, what's the actual next step on each one, and what's the one thing this prospect needs to hear before they'll book a meeting.

AI Hugh is the system I wished existed. It treats prioritization as a scoring problem, treats meeting notes as structured data, treats market intelligence as a daily input rather than something you Google when prep time runs out. It runs while I sleep so the first 30 minutes of my day is execution, not planning.

If I were a PM I'd want to see how an operator close to the problem builds tooling for the problem. This repo is that, exposed.

---

## Repository layout

```
ai-hugh/
├── core/                          # task engine, notion sync, note processing
│   ├── task_engine.py             # urgency/confidence scoring, channel routing
│   ├── notion.py                  # 67 helpers across 10 databases
│   ├── note_processor.py          # routing + normalization
│   └── main.py                    # orchestration
├── scripts/                       # daily build + send pipeline
│   ├── build_today_send.py        # dedupe + sort + persona-tailor
│   ├── build_today_calls.py       # call sheet generator
│   ├── build_call_sheet.py        # persona-tailored scripts per call
│   └── sync_tasks_to_notion.py
├── config/                        # preferences, tasks, decision memory, segments
├── templates/                     # campaign templates per persona
│   └── example_campaign/
├── docs/                          # operating rules + security architecture
├── launchd/                       # example plist jobs
├── notes/examples/                # sample meeting notes
├── send_emails.py                 # SMTP + Salesforce BCC + schema guard
├── process_call.py                # Claude API → task extraction
├── process_meeting.py             # Claude API → meeting object → Notion
├── process_sales_playbook.py      # CSV → structured sales playbook JSON
├── today.py                       # morning briefing (stdlib only)
└── README.md
```

---

## License

MIT — see `LICENSE` for full terms.

---

*Built and maintained by Hugh Robertson. Contact via the LinkedIn link in any AI-Hugh-rendered email signature.*
