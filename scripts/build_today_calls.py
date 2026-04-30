#!/usr/bin/env python3
"""
Build today_calls.csv — call sheet combining:
  - Uncalled leads emailed Apr 21, Apr 22 (from sent_log.csv)
  - Today's target batch (today_send.csv)

No call-outcome log exists yet, so every Apr 21 / Apr 22 email
is treated as uncalled per Hugh's fallback rule.

Output: today_calls.csv (and a printed terminal view)
  Name | Company | Title | Phone | Mobile | Timezone | Emailed
Sorted ET → CT → MT → PT → unknown, warmest (Apr 21) first within each TZ.
"""

import argparse
import csv
import json
import os
import sys
from collections import OrderedDict
from datetime import date, timedelta

BASE = os.path.join(os.path.dirname(__file__), "..", "intel", "example_campaign")
BASE = os.path.abspath(BASE)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

SENT_LOG = os.path.join(BASE, "sent_log.csv")
TODAY_SEND = os.path.join(BASE, "today_send.csv")
LEADS = os.path.join(BASE, "leads.csv")
OUT_CSV = os.path.join(BASE, "today_calls.csv")
CALL_ONLY_CONFIG = os.path.join(REPO_ROOT, "config", "segments", "call_only.json")

TEST_ADDRS = {"your-email@company.com", "your-email@company.com"}
TZ_ORDER = {"ET": 1, "AT": 1, "CT": 2, "MT": 3, "PT": 4}


def _prev_business_days(anchor, n):
    """Return the n most-recent business days strictly before `anchor`, newest first."""
    out = []
    d = anchor
    while len(out) < n:
        d = d - timedelta(days=1)
        if d.weekday() < 5:  # Mon-Fri
            out.append(d)
    return out


def resolve_snapshot_paths(snapshot_dates):
    """Given a list of ISO date strings (newest first), return {date_str: path}.

    Today's snapshot (if present in the list) is looked up as
    `today_send_audited_with_copy.csv` (the un-suffixed current file).
    Prior-day snapshots are looked up as `today_send_audited_with_copy_<date>.csv`.
    Missing files are silently skipped — the rest of the pipeline handles absence.
    """
    today_str = date.today().isoformat()
    paths = {}
    for d in snapshot_dates:
        if d == today_str:
            p = os.path.join(BASE, "today_send_audited_with_copy.csv")
        else:
            p = os.path.join(BASE, f"today_send_audited_with_copy_{d}.csv")
        if os.path.exists(p):
            paths[d] = p
    return paths


# Leads in call_only.json are mirrored into this dict keyed by lowercase email so
# the existing overlay logic can look them up without a second JSON read.
def load_call_only_overrides():
    overrides = {}
    try:
        with open(CALL_ONLY_CONFIG) as f:
            data = json.load(f)
    except FileNotFoundError:
        return overrides
    except Exception as e:
        print(f"Warning: could not load {CALL_ONLY_CONFIG} — {e}", file=sys.stderr)
        return overrides
    for seg in (data.get("segments") or {}).values():
        for lead in (seg.get("leads") or []):
            em = (lead.get("email") or "").strip().lower()
            if not em:
                continue
            overrides[em] = {
                "first_name": lead.get("first_name", ""),
                "last_name": lead.get("last_name", ""),
                "company": lead.get("company", ""),
                "timezone": lead.get("timezone", ""),
            }
    return overrides


CALL_ONLY_OVERRIDES = load_call_only_overrides()

US_STATE_TZ = {
    "CT": "ET", "DC": "ET", "DE": "ET", "FL": "ET", "GA": "ET", "MA": "ET",
    "MD": "ET", "ME": "ET", "MI": "ET", "NC": "ET", "NH": "ET", "NJ": "ET",
    "NY": "ET", "OH": "ET", "PA": "ET", "RI": "ET", "SC": "ET", "VA": "ET",
    "VT": "ET", "WV": "ET", "IN": "ET", "KY": "ET",
    "AL": "CT", "AR": "CT", "IA": "CT", "IL": "CT", "KS": "CT", "LA": "CT",
    "MN": "CT", "MO": "CT", "MS": "CT", "ND": "CT", "NE": "CT", "OK": "CT",
    "SD": "CT", "TN": "CT", "TX": "CT", "WI": "CT",
    "AZ": "MT", "CO": "MT", "ID": "MT", "MT": "MT", "NM": "MT", "UT": "MT",
    "WY": "MT",
    "CA": "PT", "NV": "PT", "OR": "PT", "WA": "PT",
    "PR": "AT", "VI": "AT",
}


def norm_email(s):
    return (s or "").strip().lower()


def build_lead_index(snapshot_paths):
    """Compose a single email -> lead-record index from given snapshot paths +
    today_send + leads_dataset.

    `snapshot_paths` is a list of CSV paths in oldest-to-newest order.
    Later sources override earlier ones only when they add missing fields.
    leads.csv is loaded last so its phone/mobile pair wins for those columns.
    """
    idx = {}

    def merge(email, fields):
        email = norm_email(email)
        if not email:
            return
        rec = idx.setdefault(email, {
            "first_name": "", "last_name": "", "company": "", "title": "",
            "state": "", "timezone": "", "phone": "", "mobile": "",
        })
        for k, v in fields.items():
            v = (v or "").strip()
            if v and not rec.get(k):
                rec[k] = v

    for path in list(snapshot_paths) + [TODAY_SEND]:
        if not os.path.exists(path):
            continue
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                merge(row.get("email"), {
                    "first_name": row.get("first_name", ""),
                    "last_name": row.get("last_name", ""),
                    "company": row.get("company", ""),
                    "title": row.get("title", ""),
                    "state": row.get("state", ""),
                    "timezone": row.get("timezone", ""),
                    "phone": row.get("phone", ""),
                })

    # leads.csv is the authoritative source of mobile + canonical phone
    if os.path.exists(LEADS):
        with open(LEADS, newline="") as f:
            for row in csv.DictReader(f):
                email = norm_email(row.get("email"))
                if not email:
                    continue
                # mobile only lives here
                mobile = (row.get("mobile") or "").strip()
                phone = (row.get("phone") or "").strip()
                name = (row.get("name") or "").strip()
                company = (row.get("company") or "").strip()
                rec = idx.setdefault(email, {
                    "first_name": "", "last_name": "", "company": "", "title": "",
                    "state": "", "timezone": "", "phone": "", "mobile": "",
                })
                if mobile and not rec.get("mobile"):
                    rec["mobile"] = mobile
                if phone and not rec.get("phone"):
                    rec["phone"] = phone
                if company and not rec.get("company"):
                    rec["company"] = company
                # Fill in last_name if the snapshot had only first_name (common in
                # the Apr 22 audited-with-copy file). leads_dataset has `Full Name`.
                if name and not rec.get("last_name"):
                    parts = name.split(None, 1)
                    if not rec.get("first_name"):
                        rec["first_name"] = parts[0] if parts else ""
                    rec["last_name"] = parts[1] if len(parts) > 1 else ""

    return idx


def pull_sent_by_date():
    """email -> MOST RECENT send date (YYYY-MM-DD) for status=sent rows, excluding tests.

    Latest-date matters because a re-send on Apr 22 makes the lead warm today,
    regardless of an earlier Apr 16 touch. Using earliest date would have
    hidden two Apr 22 resends behind their stale Apr 16 entry.
    """
    last_sent = {}
    with open(SENT_LOG, newline="") as f:
        rdr = csv.reader(f)
        header = next(rdr, None)
        if not header:
            return last_sent
        for row in rdr:
            if not row or len(row) < 6:
                continue
            ts = row[0].strip()
            email = norm_email(row[1])
            status = row[5].strip().lower()
            if status != "sent":
                continue
            if email in TEST_ADDRS:
                continue
            date = ts[:10]
            if email not in last_sent or date > last_sent[email]:
                last_sent[email] = date
    return last_sent


def lead_timezone(rec):
    tz = (rec.get("timezone") or "").strip()
    if tz:
        return tz
    state = (rec.get("state") or "").strip().upper()
    return US_STATE_TZ.get(state, "")


def _fmt_short(d):
    """2026-04-21 → 'Apr 21'"""
    from datetime import date as _date
    try:
        return _date.fromisoformat(d).strftime("%b %d")
    except Exception:
        return d


def main():
    parser = argparse.ArgumentParser(
        description="Build today's call sheet — today's emails + uncalled leads from prior snapshot dates."
    )
    parser.add_argument(
        "--lookback", type=int, default=2,
        help="Number of prior business days to include as warm/uncalled leads (default: 2)",
    )
    parser.add_argument(
        "--snapshot-dates", default="",
        help="Comma-separated ISO dates to use as snapshot sources (overrides --lookback). "
             "Example: --snapshot-dates 2026-04-21,2026-04-22",
    )
    args = parser.parse_args()

    today = date.today()
    today_str = today.isoformat()

    # Determine snapshot dates: explicit override wins, else derive from lookback
    if args.snapshot_dates.strip():
        snapshot_dates = [d.strip() for d in args.snapshot_dates.split(",") if d.strip()]
    else:
        snapshot_dates = [d.isoformat() for d in _prev_business_days(today, args.lookback)]

    # Oldest first (warmest first within each TZ when sorted)
    snapshot_dates_asc = sorted(snapshot_dates)
    warm_date_set = set(snapshot_dates_asc)

    # Dynamic warmth ranking: oldest = lowest rank = warmest (sorted earliest)
    date_order = {d: i for i, d in enumerate(snapshot_dates_asc)}
    date_order[today_str] = len(snapshot_dates_asc)  # today goes after all warm dates

    snapshot_paths_map = resolve_snapshot_paths(snapshot_dates_asc)
    snapshot_paths_asc = [snapshot_paths_map[d] for d in snapshot_dates_asc if d in snapshot_paths_map]

    idx = build_lead_index(snapshot_paths_asc)
    sent_by_date = pull_sent_by_date()

    # Step 1: uncalled from prior snapshot days (assumption: no call log ⇒ all uncalled)
    warm_emails = {e: d for e, d in sent_by_date.items() if d in warm_date_set}

    # Step 2: merge with today's batch
    todays_emails = set()
    with open(TODAY_SEND, newline="") as f:
        for row in csv.DictReader(f):
            em = norm_email(row.get("email"))
            if em and em not in TEST_ADDRS:
                todays_emails.add(em)

    # Dedupe by email. Emailed-date: warm map wins; today's-only rows get today_str.
    combined = OrderedDict()
    for em, d in warm_emails.items():
        combined[em] = d
    for em in todays_emails:
        if em not in combined:
            combined[em] = today_str

    # Build display rows
    rows = []
    for em, emailed_date in combined.items():
        rec = dict(idx.get(em, {}))
        override = CALL_ONLY_OVERRIDES.get(em)
        if override:
            for k, v in override.items():
                if v:
                    rec[k] = v
        first = rec.get("first_name", "")
        last = rec.get("last_name", "")
        name = (first + " " + last).strip() or em
        tz = lead_timezone(rec) or "??"
        rows.append({
            "name": name,
            "company": rec.get("company", ""),
            "title": rec.get("title", ""),
            "email": em,
            "phone": rec.get("phone", ""),
            "mobile": rec.get("mobile", ""),
            "state": rec.get("state", ""),
            "timezone": tz,
            "emailed": emailed_date,
        })

    # Sort: timezone, then date-order (warmest first), then name
    rows.sort(key=lambda r: (
        TZ_ORDER.get(r["timezone"], 99),
        date_order.get(r["emailed"], 99),
        r["name"].lower(),
    ))

    # Write CSV
    fieldnames = ["name", "company", "title", "email", "phone", "mobile", "state", "timezone", "emailed"]
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # Terminal print
    missing_phone = [r for r in rows if not r["phone"] and not r["mobile"]]
    by_tz = {}
    by_date = {d: 0 for d in snapshot_dates_asc + [today_str]}
    for r in rows:
        by_tz.setdefault(r["timezone"], 0)
        by_tz[r["timezone"]] += 1
        by_date[r["emailed"]] = by_date.get(r["emailed"], 0) + 1

    print(f"\n=== TODAY'S CALL SHEET — {len(rows)} leads ===")
    print(f"By timezone: " + ", ".join(f"{tz}={n}" for tz, n in sorted(by_tz.items(), key=lambda kv: TZ_ORDER.get(kv[0], 99))))
    date_breakdown = ", ".join(
        f"{_fmt_short(d) if d != today_str else 'Today'}={by_date.get(d, 0)}"
        for d in snapshot_dates_asc + [today_str]
    )
    print(f"By emailed:  {date_breakdown}")
    print(f"Missing phone (no office & no mobile): {len(missing_phone)}")
    print(f"Output: {OUT_CSV}\n")

    current_tz = None
    current_date = None
    warmest = snapshot_dates_asc[0] if snapshot_dates_asc else None
    for r in rows:
        if r["timezone"] != current_tz:
            current_tz = r["timezone"]
            current_date = None
            print(f"\n--- {current_tz} ---")
        if r["emailed"] != current_date:
            current_date = r["emailed"]
            if current_date == today_str:
                label = "today's batch"
            elif current_date == warmest:
                label = f"emailed {_fmt_short(current_date)} (warmest)"
            else:
                label = f"emailed {_fmt_short(current_date)}"
            print(f"  [{label}]")
        phone = r["phone"] or "—"
        mobile = r["mobile"] or "—"
        flag = "  ⚠ NO PHONE" if not r["phone"] and not r["mobile"] else ""
        title = r["title"][:40] if r["title"] else ""
        print(f"    {r['name']:<28} {r['company'][:24]:<25} {phone:<18} mob:{mobile:<18} {title}{flag}")


if __name__ == "__main__":
    main()
