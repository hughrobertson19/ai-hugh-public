#!/usr/bin/env python3
"""today.py -- AI Hugh morning briefing. Reads config/tasks.json, prints the top action, overdue, due today, upcoming windows, multi-threading gaps, accounts going stale, and new-outreach signals. Stdlib only."""

import json, re
from datetime import datetime, date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TASKS_FILE = ROOT / "config" / "tasks.json"
INTEL_DIR = ROOT / "market_intel" / "data" / "processed"
TODAY = date.today()

# Statuses that count as actionable. `open` is the legacy core/main.py flow,
# `pending` is the process_meeting.py flow. Anything else (completed, archived,
# cancelled) is ignored.
OPEN_STATUSES = {"open", "pending"}

FALLBACKS = {"call": "SMS + email bump.", "email": "LinkedIn DM + call.",
             "linkedin": "Email + call.", "sms": "Email + call.",
             "crm": "Close out from laptop today."}

NAME_STOPS = set((
    "Apr May Jun Jul Aug Sep Oct Nov Dec Jan Feb Mar "
    "Mon Tue Wed Thu Fri Sat Sun Monday Tuesday Wednesday Thursday Friday Saturday Sunday "
    "Call Email SMS Text LinkedIn Ping Reply Follow "
    "Today Tomorrow Yesterday Morning Afternoon Evening "
    "UL Product Stewardship Enterprise Sustainability Scope Prospector Formulator "
    "Knowledge Summit Regulatory Affairs Account Manager Director Senior Global Solutions "
    "Tech Free New North South East West Europe Americas "
    "Italy Germany Norway France China India Canada Netherlands Spain Poland "
    "VP EHS SDS CSRD ESG SAP Teams "
    "Final Cold Called Emailed Promised Bumped "
    "Sent Left Missed Spoke Mgr Compliance Category Studio "
    "President Schedule Update Audit Confirm Check Win Lock Ping "
    "Will Not Close Open Owned Dashboards SF CRM PM AM "
    # Status/disposition tokens that pair up as two capitalized words in reasons
    "Closed Lost Won Stage Status Active Paused Pending Rejected Approved "
    # Generic noise tokens that leak through as false contacts
    "International Limited Corporation Industries Incorporated"
    # NOTE: production version also includes common company tokens specific to your
    # lead set (e.g. "Acme Corp", "Globex Industries") to keep them from getting
    # mistaken for contact names. Populate from your own data.
).split())

# Title phrases that precede a contact name. Prefix is case-insensitive via (?i:),
# the captured name keeps Upper/lower sensitivity so "if no reply" is not swept in.
TITLE_NAME_PATTERNS = [
    re.compile(r"(?i:\bfollow up with)\s+([A-Z][\w.\-]+(?:\s+[A-Z][\w.\-]+)+)"),
    re.compile(r"(?i:\breach out to)\s+([A-Z][\w.\-]+(?:\s+[A-Z][\w.\-]+)+)"),
    re.compile(r"(?i:\bclose out)\s+([A-Z][\w.\-]+(?:\s+[A-Z][\w.\-]+)+)"),
    re.compile(r"(?i:\bre-?engage)\s+([A-Z][\w.\-]+(?:\s+[A-Z][\w.\-]+)+)"),
    re.compile(r"(?i:\bconfirm meeting time with)\s+([A-Z][\w.\-]+(?:\s+[A-Z][\w.\-]+)+)"),
    re.compile(r"(?i:\bcheck if)\s+([A-Z][\w.\-]+(?:\s+[A-Z][\w.\-]+)+)"),
    re.compile(r"(?i:\b(?:LinkedIn|Call|Email|SMS|Text))\s+([A-Z][\w.\-]+(?:\s+(?:van|de|la|von|den))?(?:\s+[A-Z][\w.\-]+)+)"),
]


def load_tasks():
    if not TASKS_FILE.exists(): return {}
    try: return json.loads(TASKS_FILE.read_text())
    except json.JSONDecodeError: return {}


def parse_due(s):
    try: return datetime.fromisoformat(s) if s else None
    except ValueError: return None


def filter_and_sort(raw):
    out = []
    for lead_id, t in raw.items():
        if not isinstance(t, dict) or t.get("status", "open") not in OPEN_STATUSES:
            continue
        due = parse_due(t.get("due_at"))
        if due is None or due.date() > TODAY:
            continue
        out.append(dict(t, _lead_id=lead_id, _due=due, _overdue=due.date() < TODAY))
    out.sort(key=lambda t: (not t["_overdue"], t["_due"], -float(t.get("confidence") or 0)))
    return out


def filter_upcoming(raw, days=3):
    out = []
    cutoff = TODAY + timedelta(days=days)
    for lead_id, t in raw.items():
        if not isinstance(t, dict) or t.get("status", "open") not in OPEN_STATUSES:
            continue
        due = parse_due(t.get("due_at"))
        if due is None or due.date() <= TODAY or due.date() > cutoff:
            continue
        out.append(dict(t, _lead_id=lead_id, _due=due, _overdue=False))
    out.sort(key=lambda t: (t["_due"], -float(t.get("confidence") or 0)))
    return out


def find_single_thread_accounts(raw):
    account_contacts = {}
    for lead_id, t in raw.items():
        if not isinstance(t, dict) or t.get("status", "open") not in OPEN_STATUSES:
            continue
        company = lead_id.split("_")[0]
        names = extract_names(t.get("reason") or "")
        if company not in account_contacts:
            account_contacts[company] = {"leads": [], "names": set(), "value": ""}
        account_contacts[company]["leads"].append(lead_id)
        account_contacts[company]["names"].update(names)
        val_match = re.search(r'\$[\d,.]+[KkMm]?', t.get("reason") or "")
        if val_match:
            account_contacts[company]["value"] = val_match.group()
    gaps = []
    for company, info in account_contacts.items():
        if len(info["leads"]) <= 1 and len(info["names"]) <= 1:
            gaps.append((company, info))
    return gaps


def first_sentence(text, limit=180):
    if not text:
        return ""
    m = re.search(r'^(.+?[.!?])(?:\s|$)', text)
    s = m.group(1) if m else text
    return s[:limit].rsplit(" ", 1)[0] + "..." if len(s) > limit else s


def extract_names(text):
    seen = []
    for first, last in re.findall(r'\b([A-Z][a-z]+)\s+([A-Z][a-z]+)\b', text or ""):
        if first in NAME_STOPS or last in NAME_STOPS:
            continue
        full = f"{first} {last}"
        if full not in seen:
            seen.append(full)
    return seen


def load_today_signals():
    path = INTEL_DIR / f"{TODAY.isoformat()}_scored.json"
    if not path.exists():
        return None
    try:
        return (json.loads(path.read_text()) or {}).get("items") or []
    except json.JSONDecodeError:
        return []


def pick_new_outreach(signals, haystack, n=3):
    stop = {"announced","launches","million","billion","signs","company","acquires","invests","portfolio","market","global","fund"}
    picks = []
    for item in sorted(signals, key=lambda s: -(s.get("relevance_score") or 0)):
        title = (item.get("title") or "").strip()
        if not title:
            continue
        words = [w for w in re.findall(r'[A-Za-z]{5,}', title) if w.lower() not in stop]
        if any(w.lower() in haystack for w in words[:3]):
            continue
        picks.append(item)
        if len(picks) >= n:
            break
    return picks


def extract_value(text):
    m = re.search(r'\$[\d,.]+[KkMm]?', text or "")
    if m:
        return m.group()
    m = re.search(r'EUR\s*[\d,.]+[KkMm]?', text or "", re.IGNORECASE)
    if m:
        return m.group()
    return ""


def value_numeric(text):
    """Parse the first $X or EUR X figure in text to an integer.
    "$340K" -> 340000, "$2,993" -> 2993, "EUR 290K" -> 290000.
    Returns 0 if nothing found.
    """
    if not text:
        return 0
    m = re.search(r'(?:\$|EUR\s*)([\d,.]+)([KkMm]?)', text, re.IGNORECASE)
    if not m:
        return 0
    num_s, unit = m.group(1), m.group(2).lower()
    try:
        n = float(num_s.replace(',', ''))
    except ValueError:
        return 0
    if unit == 'k':
        n *= 1_000
    elif unit == 'm':
        n *= 1_000_000
    return int(n)


def task_value(task):
    """Dollar value inferred from the task reason. 0 if none."""
    return value_numeric(task.get("reason") or "")


def extract_title_contact(title):
    if not title:
        return None
    for pat in TITLE_NAME_PATTERNS:
        m = pat.search(title)
        if not m:
            continue
        cand = re.sub(r"\s+", " ", m.group(1).strip())
        parts = cand.split(" ")
        if len(parts) < 2:
            continue
        if parts[0] in NAME_STOPS or not parts[0][0].isupper() or not parts[-1][0].isupper():
            continue
        return cand
    return None


def contact_for(task):
    """Prefer title-extracted name, then reason-sweep, else lead_id."""
    name = extract_title_contact(task.get("title") or "")
    if name:
        return name
    names = extract_names(task.get("reason") or "")
    if names:
        return names[0]
    return task["_lead_id"]


def parse_iso_date(s):
    try:
        return datetime.fromisoformat(s).date() if s else None
    except ValueError:
        return None


def business_days_ago(d):
    if not d or d >= TODAY:
        return 0
    cur, n = d, 0
    while cur < TODAY:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            n += 1
    return n


def stale_accounts(raw, min_bd=7):
    out = []
    for lead_id, t in raw.items():
        if not isinstance(t, dict) or t.get("status", "open") not in OPEN_STATUSES:
            continue
        created = parse_iso_date(t.get("created_at"))
        if not created or business_days_ago(created) < min_bd:
            continue
        out.append(dict(t, _lead_id=lead_id, _created=created))
    out.sort(key=lambda t: t["_created"])
    return out




def is_meeting_item(task):
    """A meeting action item has a `source` field (e.g. alex_1on1_2026-04-23)
    and typically no `channel`/`reason`. Deal tasks (legacy format) have
    channel and reason but no source.
    """
    return bool(task.get("source")) and not task.get("reason")


def split_by_kind(tasks):
    """Return (deal_tasks, meeting_items)."""
    deal, meeting = [], []
    for t in tasks:
        (meeting if is_meeting_item(t) else deal).append(t)
    return deal, meeting


def group_meeting_items(items):
    """Group meeting items by source. Returns ordered list of (source, [items]).
    Sources are ordered by their earliest due date (most overdue first).
    """
    buckets = {}
    for t in items:
        src = t.get("source") or "uncategorized"
        buckets.setdefault(src, []).append(t)
    def _source_key(entry):
        _, lst = entry
        return min(t["_due"] for t in lst)
    return sorted(buckets.items(), key=_source_key)


def _source_label(source):
    """Human-readable label for a meeting source slug like `alex_1on1_2026-04-23`."""
    parts = source.split("_")
    date_idx = next(
        (i for i, p in enumerate(parts) if len(p) == 10 and p[4] == "-" and p[7] == "-"),
        -1,
    )
    if date_idx >= 0:
        label = " ".join(parts[:date_idx]).title() or source
        return f"{label} ({parts[date_idx]})"
    return source.replace("_", " ").title()


def main():
    raw = load_tasks()
    tasks = filter_and_sort(raw)
    upcoming = filter_upcoming(raw, days=3)
    deal_tasks, meeting_items = split_by_kind(tasks)

    print(f"\n{'='*56}")
    print(f"  AI HUGH -- MORNING BRIEFING ({TODAY.strftime('%a %b %d %Y')})")
    print(f"{'='*56}\n")

    if deal_tasks:
        def _score(t):
            val = task_value(t)
            conf = float(t.get("confidence") or 0.5)
            days_late = (TODAY - t["_due"].date()).days if t["_overdue"] else 0
            return (val + 1) * (conf + 0.1) + days_late
        top = max(deal_tasks, key=_score)
        channel = (top.get("channel") or "action").upper()
        contact = contact_for(top)
        value = extract_value(top.get("reason") or "")
        value_str = f" ({value})" if value else ""

        print("YOUR #1 ACTION RIGHT NOW")
        tag = " [OVERDUE]" if top["_overdue"] else ""
        print(f"  {channel}: {top.get('title')}{tag}")
        print(f"  Contact: {contact}{value_str}")
        print(f"  Why: {first_sentence(top.get('reason') or '')}")
        print()
    elif meeting_items:
        top = next(
            (t for t in meeting_items if t.get("priority") == "high"),
            meeting_items[0],
        )
        prio = (top.get("priority") or "medium").upper()
        print("YOUR #1 ACTION RIGHT NOW")
        print(f"  [{prio}] {top.get('title')}")
        print(f"  From: {_source_label(top.get('source') or '')}")
        print()

    overdue_deal = [t for t in deal_tasks if t["_overdue"]]
    due_today_deal = [t for t in deal_tasks if not t["_overdue"]]

    if overdue_deal:
        print(f"OVERDUE DEAL TASKS ({len(overdue_deal)})")
        for i, t in enumerate(overdue_deal, 1):
            contact = contact_for(t)
            channel = (t.get("channel") or "?").upper()
            days_late = (TODAY - t["_due"].date()).days
            print(f"  {i}. {channel} {contact} -- {t.get('title')} ({days_late}d late)")
            print(f"     {first_sentence(t.get('reason') or '')}")
        print()

    if due_today_deal:
        print(f"DUE TODAY ({len(due_today_deal)})")
        for i, t in enumerate(due_today_deal, 1):
            contact = contact_for(t)
            channel = (t.get("channel") or "?").upper()
            value = extract_value(t.get("reason") or "")
            value_str = f" {value}" if value else ""
            print(f"  {i}. {t['_due'].strftime('%H:%M')} CT -- {channel} {contact}{value_str}")
            print(f"     {first_sentence(t.get('reason') or '')}")
        print()

    if meeting_items:
        fresh, stale_items = [], []
        for t in meeting_items:
            days_late = (TODAY - t["_due"].date()).days
            (stale_items if days_late > 14 else fresh).append(t)

        if fresh:
            print(f"ACTION ITEMS FROM MEETINGS ({len(fresh)})")
            for source, items in group_meeting_items(fresh):
                print(f"  -- {_source_label(source)} --")
                for t in items:
                    prio = (t.get("priority") or "medium")
                    prio_tag = "!" if prio == "high" else " "
                    days_late = (TODAY - t["_due"].date()).days
                    age = f" ({days_late}d late)" if t["_overdue"] else ""
                    print(f"   {prio_tag} {t.get('title')}{age}")
            print()

        if stale_items:
            print(f"STALE FOR REVIEW ({len(stale_items)} meeting items >14 days past due)")
            for t in sorted(stale_items, key=lambda x: x["_due"])[:10]:
                days_late = (TODAY - t["_due"].date()).days
                print(f"  - [{days_late}d late] {t.get('title')}")
                print(f"    source: {t.get('source')}")
            print("  -> close these out in tasks.json or recommit with a new due date\n")

    if not tasks:
        print("No tasks due today or overdue.\n")

    if upcoming:
        up_deal = [t for t in upcoming if not is_meeting_item(t)]
        up_meet = [t for t in upcoming if is_meeting_item(t)]
        print(f"UPCOMING ({len(upcoming)} in next 3 days)")
        for t in up_deal[:5]:
            contact = contact_for(t)
            channel = (t.get("channel") or "?").upper()
            print(f"  {t['_due'].strftime('%a %b %d %H:%M')} -- {channel} {contact}: {t.get('title')}")
        for t in up_meet[:3]:
            print(f"  {t['_due'].strftime('%a %b %d')} -- MTG: {t.get('title')}")
        extra = len(upcoming) - min(5, len(up_deal)) - min(3, len(up_meet))
        if extra > 0:
            print(f"  ... and {extra} more")
        print()

    valued = [t for t in deal_tasks if task_value(t) > 0]
    if valued:
        valued.sort(key=lambda t: -task_value(t))
        print(f"TOP BY VALUE (top {min(5, len(valued))} of {len(valued)} open $ tasks due/overdue)")
        for t in valued[:5]:
            contact = contact_for(t)
            channel = (t.get("channel") or "?").upper()
            age = "" if not t["_overdue"] else f" ({(TODAY - t['_due'].date()).days}d late)"
            print(f"  ${task_value(t)//1000:>4}k -- {channel} {contact}{age}")
            print(f"         {first_sentence(t.get('reason') or '', limit=140)}")
        print()

    print("MULTI-THREADING FLAGS")
    flagged = False
    for t in deal_tasks:
        names = extract_names(t.get("reason") or "")
        if len(names) >= 2:
            flagged = True
            print(f"  + {t['_lead_id']} -- {len(names)} contacts: {', '.join(names[:4])}")
    gaps = find_single_thread_accounts(raw)
    high_value_gaps = [(c, info) for c, info in gaps if info["value"]]
    if high_value_gaps:
        for company, info in high_value_gaps[:3]:
            print(f"  ! {company} {info['value']} -- single-threaded, needs second contact")
    if not flagged and not high_value_gaps:
        print("  (none)")
    print()


    stale = stale_accounts(raw, min_bd=7)
    stale = [t for t in stale if not is_meeting_item(t)]
    if stale:
        print(f"GOING STALE ({len(stale)} accounts >7 business days since created)")
        for t in stale[:5]:
            contact = extract_title_contact(t.get("title") or "") or t["_lead_id"]
            days = business_days_ago(t["_created"])
            value = extract_value(t.get("reason") or "")
            value_str = f" {value}" if value else ""
            print(f"  - {t['_lead_id']}{value_str} ({days}bd old) -- {contact}")
        print()

    print("NEW OUTREACH")
    signals = load_today_signals()
    if signals is None:
        print("  No market intel for today.")
    else:
        picks = pick_new_outreach(signals, json.dumps(raw).lower())
        if not picks:
            print("  (no new signals above threshold)")
        for p in picks:
            angle = p.get("outreach_angle") or p.get("why_it_matters") or ""
            if len(angle) > 140:
                angle = angle[:140] + "..."
            print(f"  -> {(p.get('title') or '').strip()}")
            if angle:
                print(f"      {angle}")
    print()

    all_active = [
        t for t in raw.values()
        if isinstance(t, dict) and t.get("status", "open") in OPEN_STATUSES
    ]
    total_value = sum(value_numeric(t.get("reason") or "") for t in all_active)
    print(
        f"PIPELINE: {len(all_active)} active tasks "
        f"({len(deal_tasks)} deal + {len(meeting_items)} meeting due/overdue) "
        f"| {len(tasks)} due today/overdue | {len(upcoming)} upcoming"
    )
    if total_value:
        print(f"          ${total_value/1000:,.0f}k total value across open tasks with a $ figure")
    print()


if __name__ == "__main__":
    main()
