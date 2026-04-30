#!/usr/bin/env python3
"""Process structured meeting notes into Notion entries.

One Opus call per meeting → structured body + Notion properties. For demos,
also drafts a follow-up email in the same call.

Usage:
    python3 process_meeting.py --type team --input notes.md
    python3 process_meeting.py --type demo --input notes.md
    python3 process_meeting.py --type demo --no-notion --input notes.md

Flags:
    --type {team,demo}   required
    --input PATH         path to a meeting-notes file (markdown or text)
    --no-notion          process only, skip the Notion write
    --yes                auto-accept the "Write to Notion?" prompt
    --backup-only        write output to output/meeting_backups/ without Notion

Never loses your processed output: if the Notion write fails, a backup file
is dumped to output/meeting_backups/ so the Opus call isn't wasted.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

try:
    import anthropic
except ImportError:
    print("ERROR: anthropic package not installed.", file=sys.stderr)
    sys.exit(1)

sys.path.insert(0, str(PROJECT_ROOT))
try:
    from core.notion import (
        _get_client,
        _resolve_data_source_id,
        _body_blocks,
        _rich_text,
    )
    _NOTION_AVAILABLE = True
except Exception as _e:
    print(f"[notion] module unavailable: {_e}", file=sys.stderr)
    _NOTION_AVAILABLE = False


MODEL = "claude-opus-4-6"
BACKUP_DIR = PROJECT_ROOT / "output" / "meeting_backups"
TASKS_FILE = PROJECT_ROOT / "config" / "tasks.json"
NAME_ALIASES_FILE = PROJECT_ROOT / "config" / "name_aliases.json"

_SLUG_STOPWORDS = {
    "a", "an", "the", "of", "to", "and", "or", "for", "on", "in", "with",
    "by", "from", "at", "as", "be", "is", "this", "that", "via", "up", "not",
}

_WEEKDAYS = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_COMMON_RULES = """You are Hugh Robertson's execution copilot. Hugh is an SDR.
Output format — STRICT. Three sections separated by fenced delimiters. Nothing
outside the fences except optional whitespace.

===META===
<JSON object only>
===NOTES===
<markdown body only>
===EMAIL===
<plain text; only for demos>

Never output the delimiter lines inside any section's body. Never add prose
before, between, or after the fenced sections."""

_TEAM_SYSTEM_PROMPT = _COMMON_RULES + """

TEAM / 1:1 MEETING TEMPLATE
===META===
{
  "title": "YYYY-MM-DD · <short topic>",
  "date": "YYYY-MM-DD",
  "meeting_type": "1:1" | "Team meeting" | "Skip-level" | "Pipeline Review" | "Ad-hoc" | "Performance",
  "attendees": ["Hugh Robertson", "..."],
  "has_open_actions": true | false,
  "tags": ["optional", "..."]
}

===NOTES===
## Discussion
- <factual bullets of what was discussed>

## Decisions
- <explicit decisions; "None" if no decisions>

## Action items — Mine
- [ ] <due date if stated>

## Action items — Others'
- [ ] <who owes what>

## Questions for next time
- <open threads>

## Relevant context
- <anything that shapes how to interpret the above>

(Do not include ===EMAIL=== section for team meetings — leave it empty.)
"""

_DEMO_SYSTEM_PROMPT = _COMMON_RULES + """

DEMO TEMPLATE
Hugh runs demos for the company's enterprise sustainability and compliance
software portfolio. Outcome reflects the state of the opportunity after the demo:
Advancing = clear next step agreed; Stalled = no forward motion but alive;
Dead = explicit no; TBD = too early to tell.

===META===
{
  "title": "YYYY-MM-DD · <Company> <context>",
  "date": "YYYY-MM-DD",
  "company": "<Company>",
  "primary_contact": "<full name of main buyer contact>",
  "other_attendees": "<comma-separated names, or empty>",
  "stage": "Demo" | "Follow-up" | "Proposal" | "Negotiation" | "Closed Won" | "Closed Lost",
  "outcome": "TBD" | "Advancing" | "Stalled" | "Dead",
  "next_steps": "<one-line summary of what's next; empty if none>"
}

===NOTES===
## Pre-demo prep
- <what Hugh knew going in: pain, stakeholders, trigger>

## Demo flow
- <what was shown, in order; 4-8 bullets>

## Questions they asked
- <their questions — exact quotes where possible>

## Objections / concerns
- <pushbacks, hesitations, things they flagged>

## Buying signals
- <anything positive: timeline mentions, "when we do this", budget probes, CFO references>

## Budget conversation
- <what was said about budget — "None" if not raised>

## Next steps
- <what was agreed verbally>

## My action items
- [ ] <what Hugh owes>

## Their action items
- [ ] <what they owe>

## Debrief
- <Hugh's honest read: what went well, what to fix next time>

## Relevant context
- <anything that didn't fit above but shapes the follow-up>

===EMAIL===
Subject: <max 8 words, tied to a specific point raised in the demo>

Body:
<Under 120 words. 3 paragraphs max.
P1: thank + one specific thing they said.
P2: recap next step with a proposed time or deliverable.
P3: conversational close, no hard CTA.
No meeting link.>
"""


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------

def _load_name_aliases() -> dict:
    """Load config/name_aliases.json. Returns {} on any failure so a missing or
    malformed file never blocks a meeting write.

    Entries whose key starts with underscore (e.g. "_description") are filtered
    out so documentation fields in the JSON don't get treated as aliases.
    """
    try:
        with open(NAME_ALIASES_FILE) as f:
            data = json.load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"[warn] could not load {NAME_ALIASES_FILE}: {e}", file=sys.stderr)
        return {}
    return {k: v for k, v in data.items() if not k.startswith("_") and isinstance(v, str)}


def _canonicalize_name(name: str, aliases: dict) -> str:
    """Case-insensitive whole-name match. 'Sam' → 'Sam Jordan', but 'Sam Lee'
    is not touched.
    """
    if not name:
        return name
    key = name.strip()
    lookup = {k.lower(): v for k, v in aliases.items()}
    return lookup.get(key.lower(), name)


def tty_input(prompt: str) -> str:
    """Interactive input via /dev/tty (stdin was consumed reading the meeting notes)."""
    try:
        with open("/dev/tty", "r") as tty:
            sys.stderr.write(prompt)
            sys.stderr.flush()
            return tty.readline().strip()
    except OSError:
        # Non-interactive environment — caller should decide defaults
        return ""


def read_meeting_notes(input_path: str | None) -> str:
    if input_path:
        with open(input_path, "r") as f:
            return f.read()
    if sys.stdin.isatty():
        print(
            "ERROR: no meeting notes provided. Pass --input PATH or pipe notes on stdin:\n"
            "  python3 process_meeting.py --type demo --input notes.md",
            file=sys.stderr,
        )
        sys.exit(2)
    return sys.stdin.read()


# ---------------------------------------------------------------------------
# Opus call + parsing
# ---------------------------------------------------------------------------

def call_opus(system_prompt: str, meeting_notes: str) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set", file=sys.stderr)
        sys.exit(1)
    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=3000,
        system=system_prompt,
        messages=[
            {
                "role": "user",
                "content": (
                    "Process these meeting notes into the fenced format "
                    "described in the system prompt. Today's date is "
                    f"{datetime.now().date().isoformat()}.\n\n"
                    "MEETING NOTES:\n"
                    f"{meeting_notes}"
                ),
            }
        ],
        temperature=0.2,
    )
    return resp.content[0].text


_SECTION_RE = re.compile(r"^===(META|NOTES|EMAIL)===\s*$", re.MULTILINE)


def parse_output(raw: str) -> dict:
    """Split the Opus output into {meta: dict, notes: str, email: str}.
    Raises ValueError if META block is missing or not parseable JSON.
    """
    # Find section boundaries
    spans = [(m.group(1), m.start(), m.end()) for m in _SECTION_RE.finditer(raw)]
    if not spans:
        raise ValueError("no ===META===/===NOTES=== fences in model output")

    sections = {}
    for i, (name, _, end) in enumerate(spans):
        next_start = spans[i + 1][1] if i + 1 < len(spans) else len(raw)
        sections[name] = raw[end:next_start].strip()

    meta_raw = sections.get("META", "")
    if not meta_raw:
        raise ValueError("empty ===META=== block")
    # Strip ```json fences if the model added them
    m = re.search(r"\{.*\}", meta_raw, re.DOTALL)
    if not m:
        raise ValueError(f"no JSON object in META block: {meta_raw[:200]!r}")
    meta = json.loads(m.group(0))

    return {
        "meta": meta,
        "notes": sections.get("NOTES", "").strip(),
        "email": sections.get("EMAIL", "").strip(),
    }


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

def backup(parsed: dict, kind: str, reason: str = "") -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    title = parsed["meta"].get("title") or f"{kind}_meeting"
    slug = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")[:60] or "meeting"
    path = BACKUP_DIR / f"{ts}_{kind}_{slug}.md"
    parts = []
    if reason:
        parts.append(f"<!-- backup reason: {reason} -->\n")
    parts.append(f"# {title}\n")
    parts.append("\n## Properties\n```json\n" + json.dumps(parsed["meta"], indent=2) + "\n```\n")
    parts.append("\n## Notes\n" + parsed["notes"] + "\n")
    if parsed.get("email"):
        parts.append("\n## Follow-up email\n" + parsed["email"] + "\n")
    path.write_text("\n".join(parts))
    return path


# ---------------------------------------------------------------------------
# Notion writers
# ---------------------------------------------------------------------------

def _ensure_demo_identity(meta: dict, use_tty: bool) -> dict:
    """For demos, if company or primary_contact are missing, prompt via tty."""
    missing = []
    if not (meta.get("company") or "").strip():
        missing.append("company")
    if not (meta.get("primary_contact") or "").strip():
        missing.append("primary_contact")
    if not missing:
        return meta
    if not use_tty:
        print(
            f"[warn] demo meta missing: {', '.join(missing)}. Use --yes to auto-skip prompts.",
            file=sys.stderr,
        )
        return meta
    if "company" in missing:
        v = tty_input("Company (required for Deals entry): ")
        if v:
            meta["company"] = v
    if "primary_contact" in missing:
        v = tty_input("Primary contact (required for Deals entry): ")
        if v:
            meta["primary_contact"] = v
    return meta


def write_team_meeting(parsed: dict) -> str:
    meta = parsed["meta"]
    client = _get_client()
    db_id = os.environ["NOTION_MEETINGS_DB_ID"]
    data_source_id = _resolve_data_source_id(db_id)

    title = meta.get("title") or f"{datetime.now().date().isoformat()} · Meeting"
    props = {"Name": {"title": _rich_text(title)}}

    # Map meeting_type to the DB's Type select
    mtype = meta.get("meeting_type")
    valid_types = {"1:1", "Team meeting", "Skip-level", "Pipeline Review", "Ad-hoc", "Performance"}
    if mtype and mtype in valid_types:
        props["Type"] = {"select": {"name": mtype}}

    if meta.get("date"):
        props["Date"] = {"date": {"start": meta["date"]}}

    if meta.get("attendees"):
        aliases = _load_name_aliases()
        canonical = [_canonicalize_name(a, aliases) for a in meta["attendees"] if a]
        props["Attendees"] = {"multi_select": [{"name": a} for a in canonical]}

    if meta.get("tags"):
        props["Tags"] = {"multi_select": [{"name": t} for t in meta["tags"] if t]}

    if meta.get("has_open_actions") is not None:
        props["Open Actions?"] = {"checkbox": bool(meta["has_open_actions"])}

    page = client.pages.create(
        parent={"type": "data_source_id", "data_source_id": data_source_id},
        properties=props,
        children=_body_blocks(parsed["notes"]),
    )
    return page["url"]


def write_deal(parsed: dict) -> str:
    meta = parsed["meta"]
    client = _get_client()
    db_id = os.environ["NOTION_DEALS_DB_ID"]
    data_source_id = _resolve_data_source_id(db_id)

    title = meta.get("title") or f"{datetime.now().date().isoformat()} · Demo"
    props = {"Name": {"title": _rich_text(title)}}

    if meta.get("date"):
        props["Date"] = {"date": {"start": meta["date"]}}

    if (meta.get("company") or "").strip():
        props["Company"] = {"rich_text": _rich_text(meta["company"])}

    if (meta.get("primary_contact") or "").strip():
        props["Primary Contact"] = {"rich_text": _rich_text(meta["primary_contact"])}

    other = meta.get("other_attendees")
    if isinstance(other, list):
        names = other
    elif isinstance(other, str) and other:
        names = [n.strip() for n in other.split(",")]
    else:
        names = []
    if names:
        aliases = _load_name_aliases()
        canonical = [_canonicalize_name(n, aliases) for n in names if n]
        props["Other Attendees"] = {"rich_text": _rich_text(", ".join(canonical))}

    valid_stages = {"Demo", "Follow-up", "Proposal", "Negotiation", "Closed Won", "Closed Lost"}
    if meta.get("stage") in valid_stages:
        props["Stage"] = {"select": {"name": meta["stage"]}}

    valid_outcomes = {"TBD", "Advancing", "Stalled", "Dead"}
    if meta.get("outcome") in valid_outcomes:
        props["Outcome"] = {"select": {"name": meta["outcome"]}}

    if (meta.get("next_steps") or "").strip():
        props["Next Steps"] = {"rich_text": _rich_text(meta["next_steps"])}

    page = client.pages.create(
        parent={"type": "data_source_id", "data_source_id": data_source_id},
        properties=props,
        children=_body_blocks(parsed["notes"]),
    )
    return page["url"]


# ---------------------------------------------------------------------------
# Task sync — pulls action items from the "Mine" section into tasks.json
# ---------------------------------------------------------------------------

_MINE_SECTION_HEADERS = (
    re.compile(r"^##\s+Action items\s*[—–-]\s*Mine\s*$", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^##\s+My action items\s*$", re.MULTILINE | re.IGNORECASE),
)

_BULLET_RE = re.compile(r"^\s*[-*]\s*\[\s*[ xX]?\s*\]\s*(.+?)\s*$")


def _extract_mine_action_items(notes: str) -> list[str]:
    """Pull every '- [ ] text' bullet under a 'Mine' / 'My action items' heading."""
    items: list[str] = []
    for pattern in _MINE_SECTION_HEADERS:
        m = pattern.search(notes)
        if not m:
            continue
        start = m.end()
        next_header = re.search(r"^##\s", notes[start:], re.MULTILINE)
        section = notes[start:start + next_header.start()] if next_header else notes[start:]
        for line in section.splitlines():
            bm = _BULLET_RE.match(line)
            if bm:
                text = bm.group(1).strip()
                if text and not text.lower().startswith("none"):
                    items.append(text)
    return items


def _slugify_action(text: str, max_tokens: int = 3) -> str:
    """Turn an action-item sentence into a short deterministic lead_id slug.

    Hyphenated compounds stay intact: 'win-back' → 'win_back', 'follow-up'
    → 'follow_up'. ISO dates are stripped before tokenizing so 2026-04-27
    doesn't leak into the slug as a 3-part hyphen compound.
    """
    cleaned = re.sub(r"\([^)]*\)", "", text)
    cleaned = re.sub(r"\d{4}-\d{2}-\d{2}", "", cleaned)
    cleaned = re.sub(r"\[[^\]]*\]", "", cleaned)
    # Match alphanumeric runs that may contain internal ASCII hyphens
    raw = re.findall(r"[a-zA-Z0-9][a-zA-Z0-9\-]*", cleaned.lower())
    tokens: list[str] = []
    for t in raw:
        t = t.replace("-", "_").strip("_")
        if not t or t in _SLUG_STOPWORDS or len(t) < 2 or t.isdigit():
            continue
        tokens.append(t)
    return "_".join(tokens[:max_tokens]) or "action"


def _extract_due_date(text: str) -> Optional[str]:
    """Return an ISO date parsed from the action-item text, or None.

    Recognizes: explicit YYYY-MM-DD, weekday names (→ next occurrence),
    'tomorrow', 'today' / 'EOD' / 'end of day'.
    """
    from datetime import date as _date, timedelta as _td

    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
    if m:
        return m.group(1)

    lower = text.lower()
    today = _date.today()

    if "tomorrow" in lower:
        return (today + _td(days=1)).isoformat()
    if re.search(r"\btoday\b|\beod\b|end of day", lower):
        return today.isoformat()

    for name, idx in _WEEKDAYS.items():
        if re.search(rf"\b{name}\b", lower):
            diff = (idx - today.weekday()) % 7
            if diff == 0:
                diff = 7
            return (today + _td(days=diff)).isoformat()

    return None


def _derive_meeting_source(meta: dict) -> str:
    """Build a compact source identifier like 'alex_1on1_2026-04-23' or
    'demo_acme_2026-04-23' from the meeting meta block.
    """
    date_str = (meta.get("date") or "").strip() or datetime.now().date().isoformat()

    company = (meta.get("company") or "").strip()
    if company:
        slug = re.sub(r"[^a-z0-9]+", "_", company.lower()).strip("_") or "meeting"
        return f"demo_{slug}_{date_str}"

    mtype = (meta.get("meeting_type") or "").strip()
    mtype_slug = {"1:1": "1on1"}.get(
        mtype, re.sub(r"[^a-z0-9]+", "_", mtype.lower()).strip("_")
    )
    others = [a for a in (meta.get("attendees") or []) if "hugh" not in (a or "").lower()]
    if others and mtype_slug:
        first_name = re.sub(r"[^a-z0-9]+", "", others[0].split()[0].lower())
        if first_name:
            return f"{first_name}_{mtype_slug}_{date_str}"
    if mtype_slug:
        return f"{mtype_slug}_{date_str}"
    tokens = re.findall(r"[a-zA-Z0-9]+", (meta.get("title") or "meeting").lower())[:3]
    return "_".join(tokens or ["meeting"]) + f"_{date_str}"


def _append_action_items(items: list[str], meeting_source: str) -> list[dict]:
    """Append each action-item text to config/tasks.json and return the list of
    records actually added. Idempotent per (source + title): re-running on the
    same meeting won't duplicate.

    Never raises. Returns [] on any failure with a stderr log so the caller
    can surface the problem without the Notion write being rolled back.
    """
    if not items:
        return []
    try:
        TASKS_FILE.parent.mkdir(parents=True, exist_ok=True)
        existing = json.loads(TASKS_FILE.read_text()) if TASKS_FILE.exists() else {}
        if not isinstance(existing, dict):
            print(f"[tasks] {TASKS_FILE} is not a dict — skipping task sync", file=sys.stderr)
            return []
    except Exception as e:
        print(f"[tasks] could not read {TASKS_FILE} — {type(e).__name__}: {e}", file=sys.stderr)
        return []

    from datetime import date as _date, timedelta as _td
    now_iso = datetime.now().isoformat(timespec="seconds")
    # Default due = tomorrow, but if tomorrow lands on Sat/Sun roll forward to Monday
    _default = _date.today() + _td(days=1)
    while _default.weekday() >= 5:  # 5 = Sat, 6 = Sun
        _default += _td(days=1)
    tomorrow = _default.isoformat()

    # Idempotency: skip items whose (source + title) pair already exists.
    seen_pairs = {
        (t.get("source"), t.get("title"))
        for t in existing.values()
        if isinstance(t, dict)
    }

    added: list[dict] = []
    for text in items:
        if (meeting_source, text) in seen_pairs:
            continue
        base_slug = _slugify_action(text)
        lead_id = base_slug
        counter = 2
        while lead_id in existing:
            lead_id = f"{base_slug}_{counter}"
            counter += 1

        due = _extract_due_date(text)
        record = {
            "lead_id": lead_id,
            "title": text,
            "status": "pending",
            "due_at": due or tomorrow,
            "source": meeting_source,
            "priority": "high" if due else "medium",
            "created_at": now_iso,
        }
        existing[lead_id] = record
        added.append(record)

    if not added:
        return []

    try:
        tmp = TASKS_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(existing, indent=2))
        tmp.replace(TASKS_FILE)
    except Exception as e:
        print(f"[tasks] failed to write {TASKS_FILE} — {type(e).__name__}: {e}", file=sys.stderr)
        return []
    return added


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Process structured meeting notes into Notion.")
    parser.add_argument("--type", required=True, choices=["team", "demo"],
                        help="Meeting type — routes to Meetings or Deals DB.")
    parser.add_argument("--input", default=None,
                        help="Path to a meeting-notes file (markdown or text). If omitted, read stdin.")
    parser.add_argument("--no-notion", action="store_true",
                        help="Process only; skip the Notion write and the y/n prompt.")
    parser.add_argument("--yes", action="store_true",
                        help="Auto-accept the 'Write to Notion?' prompt (for piped / scripted use).")
    parser.add_argument("--backup-only", action="store_true",
                        help="Skip Notion entirely and write a backup file. Implies --no-notion.")
    parser.add_argument("--no-tasks", action="store_true",
                        help="Skip appending 'Mine' action items to config/tasks.json.")
    args = parser.parse_args()

    meeting_notes = read_meeting_notes(args.input)
    if not meeting_notes.strip():
        print("ERROR: empty meeting notes", file=sys.stderr)
        return 2

    # Opus call
    system_prompt = _DEMO_SYSTEM_PROMPT if args.type == "demo" else _TEAM_SYSTEM_PROMPT
    print(f"[opus] processing {len(meeting_notes)} chars of meeting notes as type={args.type}…", file=sys.stderr)
    raw = call_opus(system_prompt, meeting_notes)

    # Parse
    try:
        parsed = parse_output(raw)
    except (ValueError, json.JSONDecodeError) as e:
        print(f"ERROR parsing model output: {e}", file=sys.stderr)
        # Still back up the raw text so the Opus call isn't wasted
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        backup_path = BACKUP_DIR / f"{ts}_{args.type}_PARSE_FAILED.md"
        backup_path.write_text(
            f"<!-- parse failure: {e} -->\n\n## Raw model output\n\n{raw}\n"
        )
        print(f"Raw output backed up to: {backup_path}", file=sys.stderr)
        return 3

    # Print for review
    print("\n" + "=" * 60)
    print(f"STRUCTURED MEETING NOTES — {parsed['meta'].get('title', '(no title)')}")
    print("=" * 60)
    print(parsed["notes"])
    if parsed.get("email"):
        print("\n" + "=" * 60)
        print("FOLLOW-UP EMAIL DRAFT")
        print("=" * 60)
        print(parsed["email"])
    print("\n" + "-" * 60)
    print("META (properties to be written):")
    print(json.dumps(parsed["meta"], indent=2))
    print("-" * 60 + "\n")

    # --backup-only short-circuits before the Notion prompt
    if args.backup_only or args.no_notion:
        if args.backup_only:
            path = backup(parsed, args.type, reason="backup-only mode")
            print(f"Backup written: {path}")
        else:
            print("Skipped Notion write (--no-notion). Copy the output above into Notion manually.")
        return 0

    # Guard: demos need Company + Primary Contact
    use_tty = not args.yes
    if args.type == "demo":
        parsed["meta"] = _ensure_demo_identity(parsed["meta"], use_tty=use_tty)

    # y/n prompt
    if args.yes:
        do_write = True
    else:
        ans = tty_input("Write to Notion? (y/n): ").lower()
        do_write = ans == "y"

    if not do_write:
        print("Skipped. Copy the output above into Notion manually.")
        return 0

    if not _NOTION_AVAILABLE:
        path = backup(parsed, args.type, reason="core.notion module unavailable")
        print(f"Notion module unavailable — backup written to {path}")
        return 4

    try:
        if args.type == "team":
            url = write_team_meeting(parsed)
        else:
            url = write_deal(parsed)
        print(f"✓ Written to Notion: {url}")
    except Exception as e:
        print(f"✗ Notion write failed: {type(e).__name__}: {e}", file=sys.stderr)
        path = backup(parsed, args.type, reason=f"notion write failed: {type(e).__name__}: {e}")
        print(f"Backup written: {path}", file=sys.stderr)
        return 5

    # Task sync — only on successful Notion write
    if args.no_tasks:
        return 0

    items = _extract_mine_action_items(parsed["notes"])
    if not items:
        print("[tasks] no 'Mine' action items detected — nothing to sync")
        return 0

    meeting_source = _derive_meeting_source(parsed["meta"])
    added = _append_action_items(items, meeting_source)

    if not added:
        print(f"[tasks] no new tasks appended (all {len(items)} were already logged against {meeting_source!r})")
        return 0

    print(f"\n[tasks] Added {len(added)} action item(s) to {TASKS_FILE.relative_to(PROJECT_ROOT)}:")
    for t in added:
        due_tag = t["due_at"]
        prio = t["priority"]
        print(f"  • [{prio:6}] {t['lead_id']}  (due {due_tag})")
        print(f"             {t['title']}")
    print(f"  source: {meeting_source}")

    # Push new tasks to Notion Tasks DB. No-op if NOTION_TASKS_DB_ID is unset.
    if os.getenv("NOTION_TASKS_DB_ID"):
        try:
            from core.notion import add_task
            pushed = 0
            for t in added:
                try:
                    add_task(t)
                    pushed += 1
                except Exception as e:
                    print(f"[tasks] Notion push failed for {t['lead_id']}: "
                          f"{type(e).__name__}: {e}", file=sys.stderr)
            if pushed:
                print(f"[tasks] pushed {pushed}/{len(added)} to Notion Tasks DB")
        except Exception as e:
            print(f"[tasks] Notion import failed — {type(e).__name__}: {e}",
                  file=sys.stderr)
    else:
        print("[tasks] NOTION_TASKS_DB_ID not set — skipping Notion push "
              "(run scripts/bootstrap_notion_tasks_db.py to set up)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
