#!/usr/bin/env python3
"""Build today_send_audited_with_copy.csv.

Sources (in priority order):
  Name source:  batch_1.csv + batch_2.csv + batch_3.csv  (clean first_name column)
  Phone lookup: leads.csv  (by email)
  Tiers/excluded:    customer_tiers.csv + excluded_leads.csv
  Dedupe:       sent_log.csv (status=sent)

Selection:
  1. Exclude anyone already emailed (sent_log).
  2. Exclude excluded strategic accounts.
  3. Classify: Variant B if company is TIC or Software customer in customer_tiers.csv;
     else Variant A.
  4. Infer timezone from phone area code.
  5. Sort ET first; within each TZ, partner before net-new; then by company.
  6. Take top N (default 60). Prepend Variant A + Variant B test rows to Hugh.
"""

import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SI = ROOT / "intel" / "example_campaign"

SENT_LOG = SI / "sent_log.csv"
TIERS = SI / "customer_tiers.csv"
EXCLUDED = SI / "excluded_leads.csv"
PHONE_MASTER = SI / "leads.csv"
BATCH_FILES = [SI / "batch_1.csv", SI / "batch_2.csv", SI / "batch_3.csv"]
TITLE_MASTER = SI / "leads_with_titles.csv"
OUT = SI / "today_send_audited_with_copy.csv"

TEST_EMAIL = "your-email@company.com"
BOOKING_URL = "https://example.com/book-meeting"

DEFAULT_N = 60

COMPANY_DISPLAY_OVERRIDES = {
    # Map normalized (lowercased, alphanumeric) company names → preferred display form.
    # Populate this from your own lead data — these are placeholders.
    "examplecorp": "Example Corp",
    "acmechemicals": "Acme Chemicals",
    "globaltech": "GlobalTech Industries",
}

AREA_TZ = {
    'ET': {'202','203','207','212','215','216','217','301','302','304','305','321','401','404','407','410','412','413','434','440','443','475','478','484','508','516','518','540','561','571','585','607','610','614','617','631','646','678','703','704','716','717','718','724','727','732','734','740','754','757','770','772','774','781','786','803','804','813','814','828','843','845','848','850','856','857','860','862','863','864','865','878','901','904','908','910','912','914','917','919','929','937','941','947','954','959','973','978','980','984'},
    'CT': {'205','214','218','224','225','228','251','254','256','262','270','281','309','312','314','316','318','319','320','334','337','346','361','381','405','409','414','419','423','430','432','438','469','479','501','504','507','512','515','531','539','563','573','601','605','608','612','618','620','630','636','641','651','662','682','701','708','712','713','715','731','737','762','763','769','773','779','785','806','812','815','816','817','830','832','847','870','872','903','913','915','918','920','936','940','952','956','972','979','985','989'},
    'MT': {'208','303','307','385','406','435','480','505','520','575','602','623','719','720','801','928','970','986'},
    'PT': {'209','213','253','310','323','341','360','408','415','424','425','442','458','503','509','510','530','541','559','562','619','626','650','657','661','669','702','707','714','725','747','760','775','805','818','831','858','909','916','925','949','951','971'},
}


def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def phone_tz(phone):
    if not phone:
        return ""
    m = re.search(r"\((\d{3})\)", phone) or re.search(r"^\s*(\d{3})[-.\s]", phone)
    if not m:
        return ""
    ac = m.group(1)
    for tz, acs in AREA_TZ.items():
        if ac in acs:
            return tz
    return ""


def load_sent_emails():
    emails = set()
    with open(SENT_LOG) as f:
        for r in csv.DictReader(f):
            if r.get("status", "").strip() == "sent":
                em = r.get("email", "").strip().lower()
                if em:
                    emails.add(em)
    return emails


def load_tiers():
    tiers = {}
    with open(TIERS, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            co = norm(r.get("Account Names", ""))
            if not co:
                continue
            tiers[co] = {
                "excluded": bool(r.get("Excluded Account?", "").strip()),
                "tic": r.get("Are they TIC?", "").strip().upper() == "TIC",
                "sw": r.get("Are they Software?", "").strip().lower().startswith("software"),
            }
    return tiers


def load_excluded_sets():
    emails, companies = set(), set()
    try:
        with open(EXCLUDED) as f:
            for r in csv.DictReader(f):
                em = r.get("email", "").strip().lower()
                co = norm(r.get("excluded_account", ""))
                if em:
                    emails.add(em)
                if co:
                    companies.add(co)
    except FileNotFoundError:
        pass
    return emails, companies


def load_phone_lookup():
    """email -> phone from leads.csv."""
    lookup = {}
    with open(PHONE_MASTER) as f:
        for r in csv.DictReader(f):
            em = r.get("email", "").strip().lower()
            if em:
                lookup[em] = r.get("phone", "").strip()
    return lookup


def load_title_lookup():
    """email -> title from leads_with_titles.csv."""
    lookup = {}
    try:
        with open(TITLE_MASTER) as f:
            for r in csv.DictReader(f):
                em = r.get("email", "").strip().lower()
                if em:
                    lookup[em] = r.get("title", "").strip()
    except FileNotFoundError:
        pass
    return lookup


_GOOD_FIRST_NAME = re.compile(r"^[A-Z][A-Za-z'\-]{1,}$")


def _clean_first_name(full_name):
    """Extract a clean first name from a full-name string, or '' if junk."""
    if not full_name:
        return ""
    parts = full_name.strip().split()
    if not parts:
        return ""
    candidate = parts[0].strip(".,")
    if not _GOOD_FIRST_NAME.match(candidate):
        return ""
    # Need at least one more token (last name) OR the single token must be <= 15 chars and alphabetic
    if len(parts) < 2:
        return ""
    return candidate


def load_batch_leads():
    """Merge every available lead source into email -> {first_name, company, email}.
    Source priority (first wins):
      1. batch_1/2/3.csv (pre-curated with clean first_name)
      2. leads_with_titles.csv / leads.csv (name field parsed with _clean_first_name)
    Drop any row whose first name fails the clean-name heuristic."""
    merged = {}
    # Priority 1: curated batches
    for path in BATCH_FILES:
        if not path.exists():
            continue
        with open(path) as f:
            for r in csv.DictReader(f):
                em = r.get("email", "").strip().lower()
                fn = (r.get("first_name", "") or "").strip()
                co = r.get("company", "").strip()
                if not em or em in merged:
                    continue
                if not _GOOD_FIRST_NAME.match(fn):
                    continue
                merged[em] = {"first_name": fn, "company": co, "email": em}

    # Priority 2: master lead list (parse `name` field)
    for master_path in (TITLE_MASTER, PHONE_MASTER):
        if not master_path.exists():
            continue
        with open(master_path) as f:
            for r in csv.DictReader(f):
                em = r.get("email", "").strip().lower()
                co = r.get("company", "").strip()
                if not em or em in merged:
                    continue
                fn = _clean_first_name(r.get("name", ""))
                if not fn:
                    continue
                merged[em] = {"first_name": fn, "company": co, "email": em}

    return merged


def display_company(raw_company):
    nco = norm(raw_company)
    if nco in COMPANY_DISPLAY_OVERRIDES:
        return COMPANY_DISPLAY_OVERRIDES[nco]
    return raw_company.strip()


def render_variant_a(first_name):
    subject = "{Subject line for the cold/net-new variant}"
    body = (
        f"Hi {first_name},\n\n"
        "{Opening paragraph: pain hook tied to the trigger event for this campaign.}\n\n"
        "{Middle paragraph: one-sentence product hook describing the differentiator.}\n\n"
        f"Worth 15 minutes this week to walk through it? Grab any slot here: {BOOKING_URL}"
    )
    return subject, body


def render_variant_b(first_name, partner_company):
    subject = f"{partner_company} + {{Your Company}} — {{add-on description}}"
    body = (
        f"Hi {first_name},\n\n"
        f"Appreciate the ongoing partnership — some teams at {partner_company} already "
        "work with us, and I wanted to flag something relevant as {the relevant cycle} approaches.\n\n"
        "{One-sentence product hook framing this as an extension of what they already use.}\n\n"
        f"Worth 15 minutes to see how it connects? Grab any slot here: {BOOKING_URL}"
    )
    return subject, body


def build_row(lead, is_partner, tz, phone, title):
    fn = lead["first_name"]
    company = display_company(lead["company"])
    if is_partner:
        variant = "B-Partner"
        subject, body = render_variant_b(fn, company)
    else:
        variant = "A-NetNew"
        subject, body = render_variant_a(fn)
    return {
        "first_name": fn,
        "last_name": "",
        "email": lead["email"],
        "company": company,
        "title": title,
        "state": "",
        "timezone": tz,
        "phone": phone,
        "variant": variant,
        "subject": subject,
        "body": body,
    }


def build_test_rows():
    a_subj, a_body = render_variant_a("Hugh")
    b_subj, b_body = render_variant_b("Hugh", "Partner Co")
    return [
        {"first_name": "Hugh", "last_name": "Robertson", "email": "your-email@company.com",
         "company": "TEST — Variant A (cold/net-new)", "title": "SDR", "state": "IL",
         "timezone": "CT", "phone": "(000) 000-0000", "variant": "A-NetNew",
         "subject": f"[TEST A] {a_subj}", "body": a_body},
        {"first_name": "Hugh", "last_name": "Robertson", "email": "your-email@company.com",
         "company": "TEST — Variant B (partner/Partner Co)", "title": "SDR", "state": "IL",
         "timezone": "CT", "phone": "(000) 000-0000", "variant": "B-Partner",
         "subject": f"[TEST B] {b_subj}", "body": b_body},
    ]


def main(n=DEFAULT_N):
    sent = load_sent_emails()
    tiers = load_tiers()
    excluded_emails, excluded_companies = load_excluded_sets()
    phone_lookup = load_phone_lookup()
    title_lookup = load_title_lookup()
    batch_leads = load_batch_leads()

    stats = {"sent": 0, "excluded": 0, "no_phone": 0, "kept": 0}
    pool = []
    for em, lead in batch_leads.items():
        if em in sent:
            stats["sent"] += 1
            continue
        nco = norm(lead["company"])
        tier = tiers.get(nco, {})
        if em in excluded_emails or nco in excluded_companies or tier.get("excluded"):
            stats["excluded"] += 1
            continue
        phone = phone_lookup.get(em, "")
        tz = phone_tz(phone)
        is_partner = bool(tier.get("tic") or tier.get("sw"))
        pool.append({
            "lead": lead,
            "is_partner": is_partner,
            "tz": tz,
            "phone": phone,
            "title": title_lookup.get(em, ""),
        })
        stats["kept"] += 1

    et_partner = [x for x in pool if x["tz"] == "ET" and x["is_partner"]]
    et_netnew = [x for x in pool if x["tz"] == "ET" and not x["is_partner"]]
    other_partner = [x for x in pool if x["tz"] != "ET" and x["is_partner"]]
    other_netnew = [x for x in pool if x["tz"] != "ET" and not x["is_partner"]]

    for bucket in (et_partner, et_netnew, other_partner, other_netnew):
        bucket.sort(key=lambda x: (x["lead"]["company"].lower(), x["lead"]["first_name"].lower()))

    ordered = et_partner + et_netnew + other_partner + other_netnew
    selected = ordered[:n]

    rows = build_test_rows() + [
        build_row(x["lead"], x["is_partner"], x["tz"], x["phone"], x["title"])
        for x in selected
    ]

    fields = ["first_name", "last_name", "email", "company", "title", "state",
              "timezone", "phone", "variant", "subject", "body"]
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    from collections import Counter
    print(f"Wrote {OUT}")
    print(f"  Batch-merge pool:   {len(batch_leads)}")
    print(f"  Already sent:       {stats['sent']}")
    print(f"  excluded accounts:       {stats['excluded']}")
    print(f"  Clean pool:         {stats['kept']}")
    print()
    print(f"  ET partner:    {len(et_partner):3}  (selected: {sum(1 for x in selected if x['tz']=='ET' and x['is_partner'])})")
    print(f"  ET net-new:    {len(et_netnew):3}  (selected: {sum(1 for x in selected if x['tz']=='ET' and not x['is_partner'])})")
    print(f"  Other partner: {len(other_partner):3}  (selected: {sum(1 for x in selected if x['tz']!='ET' and x['is_partner'])})")
    print(f"  Other net-new: {len(other_netnew):3}  (selected: {sum(1 for x in selected if x['tz']!='ET' and not x['is_partner'])})")
    print()
    print(f"Final CSV: {len(rows)} rows ({len(rows)-2} real + 2 test)")
    print("\nBy company × variant:")
    real = [r for r in rows if not r["company"].startswith("TEST")]
    for (co, v), count in Counter((r["company"], r["variant"]) for r in real).most_common():
        print(f"  {count:3}  [{v}]  {co}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_N
    main(n)
