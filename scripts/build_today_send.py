#!/usr/bin/env python3
"""Build today_send.csv for the 2026-04-23 batch.

Dedupe sources (STEP 1):
  - sent_log.csv                (status in sent|bounced)
  - batch_1/2/3.csv             (earlier sent batches)
  - today_send_audited*.csv     (prior audited send lists)
  - today_send_86.csv           (Apr 14 send)
  - today_mail_merge*.csv       (Apr 14 merge files)
  - crm_activity_export.csv      (anything touched in SF with Last Activity)

Exclusions:
  - excluded accounts (excluded_leads.csv: emails + company match)
  - Partner Co batch (@partnerco.example.com or company norm contains 'partnerco')
  - Bounced addresses (status=bounced in sent_log)

Source: leads_with_titles.csv
Target: 55 leads
Subject: {Subject line for the current campaign}
Body: 3-paragraph product hook (see render_body)
Sort: ET, CT, MT, PT, unknown last (TZ inferred from area code)
Output: intel/example_campaign/today_send.csv
"""

import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SI = ROOT / "intel" / "example_campaign"

SOURCE = SI / "leads_with_titles.csv"
PHONE_MASTER = SI / "leads.csv"
SENT_LOG = SI / "sent_log.csv"
EXCLUDED = SI / "excluded_leads.csv"
SF_ACTIVITY = SI / "crm_activity_export.csv"
OUT = SI / "today_send.csv"

DEDUPE_CSVS = [
    SI / "batch_1.csv",
    SI / "batch_2.csv",
    SI / "batch_3.csv",
    SI / "today_send_audited_with_copy.csv",
    SI / "today_send_audited_with_copy_2026-04-21.csv",
    SI / "today_send_audited.csv",
    SI / "today_send_86.csv",
    SI / "today_mail_merge.csv",
    SI / "today_mail_merge_v2.csv",
    SI / "today_mail_merge_test.csv",
]

TARGET_N = 55
TEST_EMAIL = "your-email@company.com"
BOOKING_URL = "https://example.com/book-meeting"

SUBJECT = "{Subject line for the current campaign}"

AREA_TZ = {
    'ET': {'202','203','207','212','215','216','217','301','302','304','305','321','401','404','407','410','412','413','434','440','443','475','478','484','508','516','518','540','561','571','585','607','610','614','617','631','646','678','703','704','716','717','718','724','727','732','734','740','754','757','770','772','774','781','786','803','804','813','814','828','843','845','848','850','856','857','860','862','863','864','865','878','901','904','908','910','912','914','917','919','929','937','941','947','954','959','973','978','980','984'},
    'CT': {'205','214','218','224','225','228','251','254','256','262','270','281','309','312','314','316','318','319','320','334','337','346','361','381','405','409','414','419','423','430','432','438','469','479','501','504','507','512','515','531','539','563','573','601','605','608','612','618','620','630','636','641','651','662','682','701','708','712','713','715','731','737','762','763','769','773','779','785','806','812','815','816','817','830','832','847','870','872','903','913','915','918','920','936','940','952','956','972','979','985','989'},
    'MT': {'208','303','307','385','406','435','480','505','520','575','602','623','719','720','801','928','970','986'},
    'PT': {'209','213','253','310','323','341','360','408','415','424','425','442','458','503','509','510','530','541','559','562','619','626','650','657','661','669','702','707','714','725','747','760','775','805','818','831','858','909','916','925','949','951','971'},
}

TZ_RANK = {"ET": 1, "CT": 2, "MT": 3, "PT": 4}

_GOOD_FIRST_NAME = re.compile(r"^[A-Z][A-Za-z'\-]{1,}$")


def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def phone_tz(phone):
    if not phone:
        return ""
    m = re.search(r"\((\d{3})\)", phone) or re.search(r"^\s*(\d{3})[-.\s]", phone) or re.search(r"^\+?1[-.\s]?(\d{3})", phone)
    if not m:
        return ""
    ac = m.group(1)
    for tz, acs in AREA_TZ.items():
        if ac in acs:
            return tz
    return ""


def load_emails_from_csv(path, email_field="email"):
    out = set()
    if not path.exists():
        return out
    try:
        with open(path, newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                em = (row.get(email_field) or "").strip().lower()
                if em:
                    out.add(em)
    except Exception:
        pass
    return out


def load_sent_log_emails():
    """Return (sent_or_bounced_emails, bounced_emails). Defensive against col drift
    (same pattern send_emails.py uses after the April sent_log bug)."""
    blocking = set()
    bounced = set()
    if not SENT_LOG.exists():
        return blocking, bounced
    with open(SENT_LOG, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return blocking, bounced
        for row in reader:
            if not row or len(row) < 2:
                continue
            statuses = {c.strip().lower() for c in row}
            em = row[1].strip().lower()
            if not em:
                continue
            if "sent" in statuses or "bounced" in statuses:
                blocking.add(em)
            if "bounced" in statuses:
                bounced.add(em)
    return blocking, bounced


def load_sf_touched_emails():
    out = set()
    if not SF_ACTIVITY.exists():
        return out
    with open(SF_ACTIVITY, newline="") as f:
        for row in csv.DictReader(f):
            em = (row.get("email") or "").strip().lower()
            last_act = (row.get("last_activity") or "").strip()
            if em and last_act:
                out.add(em)
    return out


def load_excluded():
    emails, companies = set(), set()
    if not EXCLUDED.exists():
        return emails, companies
    with open(EXCLUDED, newline="") as f:
        for row in csv.DictReader(f):
            em = (row.get("email") or "").strip().lower()
            co = norm(row.get("excluded_account") or "")
            if em:
                emails.add(em)
            if co:
                companies.add(co)
    return emails, companies


def load_phone_lookup():
    lookup = {}
    if not PHONE_MASTER.exists():
        return lookup
    with open(PHONE_MASTER, newline="") as f:
        for row in csv.DictReader(f):
            em = (row.get("email") or "").strip().lower()
            if em:
                lookup[em] = (
                    (row.get("phone") or "").strip(),
                    (row.get("mobile") or "").strip(),
                )
    return lookup


def clean_first_name(full_name):
    if not full_name:
        return ""
    parts = full_name.strip().split()
    if len(parts) < 2:
        return ""
    candidate = parts[0].strip(".,")
    return candidate if _GOOD_FIRST_NAME.match(candidate) else ""


def render_body(first_name):
    return (
        f"Hi {first_name},\n\n"
        "{Opening paragraph: pain hook tied to the trigger event for this campaign — "
        "e.g., the upcoming reporting cycle, deadline, or regulatory change.}\n\n"
        "{Middle paragraph: one-sentence product hook describing the differentiator. "
        "Avoid feature lists; lead with what's unique.}\n\n"
        f"If this lines up with what your team is looking at this cycle, worth 15 minutes "
        f"to walk through it. Grab any slot here: {BOOKING_URL}"
    )


def main():
    # --- STEP 1: dedupe universe ---
    sent_blocking, bounced = load_sent_log_emails()
    sf_touched = load_sf_touched_emails()

    per_file_counts = {}
    batch_emails = set()
    for path in DEDUPE_CSVS:
        emails = load_emails_from_csv(path)
        per_file_counts[path.name] = len(emails)
        batch_emails |= emails

    dedupe_universe = set()
    dedupe_universe |= sent_blocking
    dedupe_universe |= sf_touched
    dedupe_universe |= batch_emails
    dedupe_universe.discard(TEST_EMAIL.lower())

    print("=" * 60)
    print("STEP 1 — DEDUPE UNIVERSE")
    print("=" * 60)
    print(f"  sent_log.csv (sent+bounced):     {len(sent_blocking):5}")
    print(f"  crm_activity_export.csv (touched): {len(sf_touched):5}")
    for name, n in per_file_counts.items():
        print(f"  {name:40} {n:5}")
    print(f"  {'-' * 50}")
    print(f"  TOTAL UNIQUE ALREADY CONTACTED:   {len(dedupe_universe):5}")
    print(f"  (bounced, excluded separately):   {len(bounced):5}")
    print()

    # --- STEP 2: build today_send.csv ---
    excluded_emails, excluded_companies = load_excluded()
    phone_lookup = load_phone_lookup()

    stats = {
        "total_source": 0, "no_email": 0, "bad_name": 0,
        "already_contacted": 0, "bounced": 0,
        "excluded": 0, "partner_excluded": 0, "kept": 0,
    }

    pool = []
    seen_in_source = set()
    with open(SOURCE, newline="") as f:
        for row in csv.DictReader(f):
            stats["total_source"] += 1
            em = (row.get("email") or "").strip().lower()
            if not em:
                stats["no_email"] += 1
                continue
            if em in seen_in_source:
                continue
            seen_in_source.add(em)

            fn = clean_first_name(row.get("name") or "")
            if not fn:
                stats["bad_name"] += 1
                continue

            if em in bounced:
                stats["bounced"] += 1
                continue
            if em in dedupe_universe:
                stats["already_contacted"] += 1
                continue

            company = (row.get("company") or "").strip()
            nco = norm(company)
            # partner-account batch exclusion
            if em.endswith("@partnerco.example.com") or em.endswith("@its.partnerco.example.com") or "partnerco" in nco:
                stats["partner_excluded"] += 1
                continue
            # excluded-account exclusion
            if em in excluded_emails or nco in excluded_companies:
                stats["excluded"] += 1
                continue

            phone = (row.get("phone") or "").strip()
            mobile = (row.get("mobile") or "").strip()
            if not phone and em in phone_lookup:
                phone, mobile = phone_lookup[em]
            tz = phone_tz(phone) or phone_tz(mobile)
            title = (row.get("title") or "").strip()

            pool.append({
                "first_name": fn, "last_name": "", "email": em,
                "company": company, "title": title, "state": "",
                "timezone": tz, "phone": phone,
            })
            stats["kept"] += 1

    pool.sort(key=lambda r: (
        TZ_RANK.get(r["timezone"], 99),
        r["company"].lower(),
        r["first_name"].lower(),
    ))
    selected = pool[:TARGET_N]

    rows = []
    for r in selected:
        body = render_body(r["first_name"])
        rows.append({
            **r,
            "variant": "A-NetNew",
            "subject": SUBJECT,
            "body": body,
        })

    fields = ["first_name", "last_name", "email", "company", "title", "state",
              "timezone", "phone", "variant", "subject", "body"]
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print("=" * 60)
    print("STEP 2 — BUILD today_send.csv")
    print("=" * 60)
    print(f"  Source rows:          {stats['total_source']}")
    print(f"  Bad/missing name:     {stats['bad_name']}")
    print(f"  No email:             {stats['no_email']}")
    print(f"  Already contacted:    {stats['already_contacted']}")
    print(f"  Bounced excluded:     {stats['bounced']}")
    print(f"  excluded:                 {stats['excluded']}")
    print(f"  partner excluded:         {stats['partner_excluded']}")
    print(f"  Clean pool:           {stats['kept']}")
    print(f"  Selected:             {len(selected)} (target {TARGET_N})")
    print()

    from collections import Counter
    tz_counts = Counter(r["timezone"] or "??" for r in selected)
    print("  TZ breakdown:")
    for tz in ("ET", "CT", "MT", "PT", "??"):
        if tz in tz_counts:
            print(f"    {tz}: {tz_counts[tz]}")
    print()

    print("  First 5 rows:")
    print(f"  {'TZ':<4} {'FIRST':<12} {'COMPANY':<25} {'EMAIL'}")
    for r in selected[:5]:
        print(f"  {(r['timezone'] or '??'):<4} {r['first_name']:<12} {r['company'][:24]:<25} {r['email']}")
    print()

    selected_emails = {r["email"] for r in selected}
    overlap = selected_emails & dedupe_universe
    print(f"  Overlap with dedupe universe: {len(overlap)}")
    print()

    # Sync sent_log stats to the active sprint in Notion. Never blocks —
    # Notion outage or missing sprint logs to stderr and returns {}.
    try:
        import sys as _sys
        from pathlib import Path as _Path
        _repo = _Path(__file__).resolve().parent.parent
        _sys.path.insert(0, str(_repo))
        from core.notion import sync_sprint_stats_from_log
        sync_sprint_stats_from_log(str(SENT_LOG))
    except Exception as _sync_e:
        print(f"[notion] sprint sync skipped — {type(_sync_e).__name__}: {_sync_e}")

    return len(selected), len(overlap)


if __name__ == "__main__":
    n, overlap = main()
    sys.exit(1 if overlap else 0)
