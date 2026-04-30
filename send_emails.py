#!/usr/bin/env python3
"""
CDP Email Sender
Reads today_mail_merge_test.csv and sends via Office 365 SMTP.
BCC Salesforce on every send. 45-second delay between emails.
Password prompted at runtime — never stored.
"""

import csv
import os
import re
import smtplib
import time
import getpass
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# --- Config ---
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.example.com")
SMTP_PORT = 587
FROM_EMAIL = "your-email@company.com"
TEST_EMAIL = "your-email@company.com"
BCC_SALESFORCE = "salesforce-bcc@example.com"
INPUT_FILE = "intel/example_campaign/today_send.csv"
PHONE_FILE = "intel/example_campaign/leads.csv"
LOG_FILE = "intel/example_campaign/outreach_log.csv"
SF_ACTIVITY_FILE = "intel/example_campaign/crm_activity_export.csv"
CALL_SHEET_DIR = "intel/example_campaign"
DELAY_SECONDS = 45

# East-to-west call ordering so western leads still have workday left by the time Hugh gets to them
TZ_ORDER = {"AT": 0, "ET": 1, "CT": 2, "MT": 3, "PT": 4}

# Fallback when `timezone` column is blank: infer from `state`. Mixed-TZ states
# resolve to their more populous zone (TX/FL → CT/ET, KS/NE/ND/SD → CT).
US_STATE_TZ = {
    # ET
    "CT": "ET", "DC": "ET", "DE": "ET", "FL": "ET", "GA": "ET", "MA": "ET",
    "MD": "ET", "ME": "ET", "MI": "ET", "NC": "ET", "NH": "ET", "NJ": "ET",
    "NY": "ET", "OH": "ET", "PA": "ET", "RI": "ET", "SC": "ET", "VA": "ET",
    "VT": "ET", "WV": "ET", "IN": "ET", "KY": "ET",
    # CT
    "AL": "CT", "AR": "CT", "IA": "CT", "IL": "CT", "KS": "CT", "LA": "CT",
    "MN": "CT", "MO": "CT", "MS": "CT", "ND": "CT", "NE": "CT", "OK": "CT",
    "SD": "CT", "TN": "CT", "TX": "CT", "WI": "CT",
    # MT
    "AZ": "MT", "CO": "MT", "ID": "MT", "MT": "MT", "NM": "MT", "UT": "MT",
    "WY": "MT",
    # PT
    "CA": "PT", "NV": "PT", "OR": "PT", "WA": "PT",
    # Puerto Rico / USVI → AT
    "PR": "AT", "VI": "AT",
}


def lead_tz_rank(lead):
    """TZ sort key. Use explicit `timezone`; else infer from `state`; else end-of-list."""
    tz = (lead.get("timezone") or "").strip()
    if not tz:
        state = (lead.get("state") or "").strip().upper()
        tz = US_STATE_TZ.get(state, "")
    return TZ_ORDER.get(tz, 99)

# Leads to call but never cold-email. Loaded from config/segments/call_only.json so
# warm-angle segments can be added without editing this file. Missing or malformed
# JSON falls back to an empty list — script still runs, call sheet just has no overrides.
_CALL_ONLY_CONFIG = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config", "segments", "call_only.json"
)


def _load_call_only_leads():
    import json
    try:
        with open(_CALL_ONLY_CONFIG) as _f:
            data = json.load(_f)
    except FileNotFoundError:
        return []
    except Exception as _e:
        print(f"Warning: could not load {_CALL_ONLY_CONFIG} — {_e}")
        return []
    out = []
    for _seg in (data.get("segments") or {}).values():
        for lead in (_seg.get("leads") or []):
            out.append({
                "first_name": lead.get("first_name", ""),
                "last_name": lead.get("last_name", ""),
                "email": lead.get("email", ""),
                "company": lead.get("company", ""),
                "timezone": lead.get("timezone", ""),
            })
    return out


CALL_ONLY_LEADS = _load_call_only_leads()


def load_phone_lookup():
    """Build email -> (phone, mobile) lookup from leads.csv."""
    lookup = {}
    try:
        with open(PHONE_FILE, "r") as f:
            for row in csv.DictReader(f):
                email = row.get("email", "").strip().lower()
                if email:
                    lookup[email] = (row.get("phone", "").strip(), row.get("mobile", "").strip())
    except FileNotFoundError:
        print(f"Warning: {PHONE_FILE} not found — call sheet will have no phone numbers.")
    return lookup


def load_already_sent():
    """Emails with a delivery-outcome row in outreach_log.csv — never re-email them.

    Schema (9 cols): timestamp, email, first_name, company, title,
                     channel, action, status, notes.
    Blocking statuses: {'delivered', 'bounce'} — both mean the mailbox has
    already been contacted for this outreach. 'error' is non-blocking because
    transient SMTP failures (timeouts, connection resets) should retry.

    assert_sent_log_schema_ok() runs before this and aborts on row-width drift,
    so DictReader alignment is safe.
    """
    sent = set()
    blocking_statuses = {"delivered", "bounce"}
    try:
        with open(LOG_FILE, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (row.get("status") or "").strip().lower() in blocking_statuses:
                    em = (row.get("email") or "").strip().lower()
                    if em:
                        sent.add(em)
    except FileNotFoundError:
        pass
    return sent


def load_sf_contacted():
    """Emails that have a 'Last Activity' timestamp in the Salesforce export.

    Catches leads Hugh touched via Outlook, SF Tasks, or manual calls — channels
    that never write to sent_log.csv. Without this, a lead emailed via Outlook
    on Apr 14 could be re-emailed by the script days later. This is the second
    dedupe source; sent_log.csv is the first.

    Returns {email: (last_activity, lead_status, last_touch_campaign)}.
    Refresh the source file by re-exporting CDP leads from SF.
    """
    contacted = {}
    try:
        with open(SF_ACTIVITY_FILE, "r", newline="") as f:
            for row in csv.DictReader(f):
                em = (row.get("email") or "").strip().lower()
                last_act = (row.get("last_activity") or "").strip()
                if em and last_act:
                    contacted[em] = (
                        last_act,
                        (row.get("lead_status") or "").strip(),
                        (row.get("last_touch_campaign") or "").strip(),
                    )
    except FileNotFoundError:
        print(f"Warning: {SF_ACTIVITY_FILE} not found — SF activity dedupe disabled.")
    return contacted


def dedupe_against_sent_log(leads, already_sent, sf_contacted=None):
    """Drop any lead already contacted via our script (sent_log) OR Salesforce activity.
    TEST_EMAIL is always allowed. Returns (kept, dropped) where each dropped entry is
    (first_name, company, email, reason)."""
    sf_contacted = sf_contacted or {}
    test_lc = TEST_EMAIL.strip().lower()
    kept, dropped = [], []
    for lead in leads:
        em = lead.get("email", "").strip().lower()
        if em and em != test_lc:
            if em in already_sent:
                dropped.append((
                    lead.get("first_name", "").strip(),
                    lead.get("company", "").strip(),
                    em,
                    "sent_log",
                ))
                continue
            if em in sf_contacted:
                last_act, status, camp = sf_contacted[em]
                reason = f"sf_activity:{last_act}"
                if camp:
                    reason += f" ({camp})"
                dropped.append((
                    lead.get("first_name", "").strip(),
                    lead.get("company", "").strip(),
                    em,
                    reason,
                ))
                continue
        kept.append(lead)
    return kept, dropped


def generate_call_sheet(sent_leads, phone_lookup, batch_name, call_only_leads=None):
    """Print and save a call sheet. Call-only leads go first; emailed leads follow, sorted east→west by TZ."""
    call_only_leads = call_only_leads or []
    sorted_sent = sorted(sent_leads, key=lead_tz_rank)
    ordered = [(l, True) for l in call_only_leads] + [(l, False) for l in sorted_sent]

    lines = []
    for lead, is_call_only in ordered:
        name = lead.get("first_name", "").strip() or "there"
        company = lead.get("company", "").strip()
        email = lead.get("email", "").strip().lower()
        tz = lead.get("timezone", "").strip() or "??"
        phone, mobile = phone_lookup.get(email, ("", ""))
        tag = "CALL ONLY — NOT EMAILED" if is_call_only else f"TZ: {tz}"

        lines.append("=" * 44)
        lines.append(f"{name} — {company}  [{tag}]")
        lines.append(f"Phone: {phone or 'N/A'}  |  Mobile: {mobile or 'N/A'}")
        lines.append("=" * 44)
        lines.append("")
        lines.append("LIVE CALL SCRIPT:")
        lines.append(f'"Hey {name}, it\'s {{Sender Name}} from {{Your Company}}, how\'s it going?')
        lines.append("")
        lines.append("[pause]")
        lines.append("")
        lines.append("Reason I\'m calling — {trigger event for the campaign}")
        lines.append("and we just launched {one-sentence product hook}.")
        lines.append("")
        lines.append(f'Was curious how {company} is handling {{the relevant workflow}}')
        lines.append('this year?"')
        lines.append("")
        lines.append('[If interested] → "Worth a 15-minute call this week')
        lines.append('to walk through it?"')
        lines.append("")
        lines.append('[If not the right person] → "No worries — who owns')
        lines.append('{the relevant workflow} on your end?"')
        lines.append("")
        if is_call_only:
            lines.append('[If busy] → "Totally get it — mind if I send over a quick')
            lines.append('email with the details so you can look when you get a sec?"')
        else:
            lines.append('[If busy] → "Totally get it — I just sent you an email')
            lines.append('with the details. Worth a look when you get a sec."')
        lines.append("")
        lines.append("-" * 44)
        lines.append("")
        lines.append("VOICEMAIL SCRIPT:")
        if is_call_only:
            lines.append(f'"Hey {name}, it\'s {{Sender Name}} from {{Your Company}} — calling about')
            lines.append('{topic}. I\'ll send a quick email with details.')
            lines.append('Cheers."')
        else:
            lines.append(f'"Hey {name}, it\'s {{Sender Name}} from {{Your Company}}. Just sent you')
            lines.append('an email about {topic} — worth a quick look.')
            lines.append('Cheers."')
        lines.append("")
        lines.append("=" * 44)
        lines.append("")
        lines.append("")

    sheet_text = "\n".join(lines)
    total_count = len(ordered)
    call_only_count = len(call_only_leads)

    # Print to console
    print(f"\n{'=' * 50}")
    print(f"CALL SHEET — {total_count} leads ({call_only_count} call-only, {total_count - call_only_count} emailed)")
    print(f"{'=' * 50}\n")
    print(sheet_text)

    # Save to file
    filename = os.path.join(CALL_SHEET_DIR, f"call_sheet_{batch_name}.txt")
    with open(filename, "w") as f:
        f.write(sheet_text)
    print(f"Call sheet saved to {filename}")


BOOKING_URL = "https://example.com/book-meeting"

COLD_SUBJECT = "{Subject line for the current campaign}"
COLD_BODY = f"""Hi {{Recipient first name}},

{{Opening paragraph: pain hook tied to the trigger event for this campaign — e.g., the upcoming reporting cycle, deadline, or regulatory change.}}

{{Middle paragraph: one-sentence product hook describing the differentiator. Avoid feature lists.}}

Worth 15 minutes this week to walk through it? Grab any slot here: {BOOKING_URL}"""

PARTNER_SUBJECT_TEMPLATE = "{partner} + {{Your Company}} — {{add-on description}}"
PARTNER_BODY_TEMPLATE = """Hi {first_name},

Appreciate the ongoing partnership — some teams at {partner} already work with us, and I wanted to flag something relevant as {{the relevant cycle}} approaches.

{{One-sentence product hook framing this as an extension of what they already use.}}

Worth 15 minutes to see how it connects? Grab any slot here: """ + BOOKING_URL


def render_partner_email(partner_company, first_name="Hugh"):
    """Render the partner-framed variant for any partner-account warm send."""
    return (
        PARTNER_SUBJECT_TEMPLATE.format(partner=partner_company),
        PARTNER_BODY_TEMPLATE.format(partner=partner_company, first_name=first_name),
    )


def build_test_leads(partner_company="Partner Co"):
    """Generate two test emails to Hugh's work address — one cold, one partner."""
    partner_subject, partner_body = render_partner_email(partner_company, first_name="Hugh")
    return [
        {"first_name": "Hugh", "company": "TEST-Cold", "email": TEST_EMAIL,
         "subject": COLD_SUBJECT, "body": COLD_BODY},
        {"first_name": "Hugh", "company": f"TEST-Partner ({partner_company})", "email": TEST_EMAIL,
         "subject": partner_subject, "body": partner_body},
    ]


def assert_sent_log_schema_ok():
    """Fail loudly BEFORE any SMTP connect if sent_log.csv has row-width drift.

    Multiple code paths have historically written to sent_log with different
    schemas. If that recurs, dedupe can silently miss already-sent leads.
    This check aborts the run so Hugh never has to manually verify.
    """
    try:
        with open(LOG_FILE, "r", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return
            expected = len(header)
            offenders = []
            for lineno, row in enumerate(reader, start=2):
                if not row:
                    continue
                if len(row) != expected:
                    offenders.append((lineno, len(row), row[1] if len(row) > 1 else "?"))
            if offenders:
                print(f"\nABORT: sent_log.csv has {len(offenders)} row(s) with column-width drift "
                      f"(expected {expected}). Dedupe would be unreliable. First 5:")
                for lineno, n, em in offenders[:5]:
                    print(f"  line {lineno}: cols={n}  email={em}")
                print("Fix the log (normalize to header width) before sending.")
                raise SystemExit(2)
    except FileNotFoundError:
        pass


def main():
    # Schema guard: prevent silent dedupe misses from a malformed sent_log.
    assert_sent_log_schema_ok()

    # Load leads (test rows are now included at the top of the audited CSV)
    with open(INPUT_FILE, "r") as f:
        leads = list(csv.DictReader(f))

    raw_total = len(leads)

    # Hard gate: drop anyone already touched via our script (sent_log) OR Salesforce (Last Activity).
    already_sent = load_already_sent()
    sf_contacted = load_sf_contacted()
    leads, dropped = dedupe_against_sent_log(leads, already_sent, sf_contacted)

    if dropped:
        print(f"\nDedupe gate: {len(dropped)} lead(s) already contacted — skipping:")
        for name, company, em, reason in dropped:
            print(f"  SKIP  {name or '(no name)'} — {company} — {em}  [{reason}]")

    # Load phone lookup for call sheet
    phone_lookup = load_phone_lookup()

    total = len(leads)
    print(f"\nCDP Email Sender")
    print(f"{'=' * 50}")
    print(f"CSV rows:        {raw_total}")
    print(f"Already sent:    {len(dropped)} (skipped)")
    print(f"Leads to send:   {total}")
    print(f"From:            {FROM_EMAIL}")
    print(f"BCC:             {BCC_SALESFORCE[:40]}...")
    print(f"Delay:           {DELAY_SECONDS}s between sends")
    print(f"{'=' * 50}\n")

    if total == 0:
        print("Nothing to send after dedupe. Exiting.")
        return

    # Get password
    password = getpass.getpass("Enter email password (will not be displayed): ")

    # Confirm before sending
    confirm = input(f"\nSend {total} emails? Type 'yes' to confirm: ").strip().lower()
    if confirm != "yes":
        print("Aborted.")
        return

    # Open log file
    log_exists = False
    try:
        with open(LOG_FILE, "r") as f:
            log_exists = True
    except FileNotFoundError:
        pass

    log_file = open(LOG_FILE, "a", newline="")
    log_writer = csv.writer(log_file)
    if not log_exists:
        log_writer.writerow(["timestamp", "email", "first_name", "company", "title",
                             "channel", "action", "status", "notes"])

    # Connect to SMTP
    print(f"\nConnecting to {SMTP_SERVER}...")
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(FROM_EMAIL, password)
        print("Connected.\n")
    except Exception as e:
        print(f"SMTP connection failed: {e}")
        log_file.close()
        return

    sent = 0
    errors = 0
    sent_leads = []

    for i, lead in enumerate(leads):
        to_email = lead["email"].strip()
        first_name = lead.get("first_name", "").strip()
        company = lead.get("company", "").strip()
        subject = lead.get("subject", "").strip()
        body_text = lead.get("body", "").strip()

        print(f"Sending {i + 1} of {total} - {first_name or 'Unknown'} at {company}")

        try:
            msg = MIMEMultipart()
            msg["From"] = FROM_EMAIL
            msg["To"] = to_email
            msg["Subject"] = subject

            # Strip old plain-text signature from body
            body_clean = body_text
            for sig_marker in ["Best regards,", "Best,", "Cheers,"]:
                idx = body_clean.rfind(sig_marker)
                if idx != -1:
                    body_clean = body_clean[:idx].strip()
                    break

            # Fix greeting: "Hi Name," → "Hey Name,"
            body_clean = re.sub(r'^Hi\b', 'Hey', body_clean)

            # Extract greeting line (everything up to and including first comma)
            greeting = ""
            greeting_match = re.match(r'^(Hey\s+[^,]+,)\s*', body_clean)
            if greeting_match:
                greeting = greeting_match.group(1)
                body_clean = body_clean[greeting_match.end():].strip()

            # Split on real paragraph breaks (double newlines) to preserve natural grouping
            raw_paragraphs = re.split(r'\n\s*\n', body_clean)
            cleaned_paragraphs = []
            for rp in raw_paragraphs:
                cleaned = rp.strip().replace('\n', ' ')
                if cleaned:
                    cleaned_paragraphs.append(cleaned)

            # Condense: keep opener, merge middle into one paragraph, keep closer
            if len(cleaned_paragraphs) > 3:
                opener = cleaned_paragraphs[0]
                closer = cleaned_paragraphs[-1]
                middle = " ".join(cleaned_paragraphs[1:-1])
                cleaned_paragraphs = [opener, middle, closer]

            paragraphs = []
            if greeting:
                paragraphs.append(greeting)

            for cp in cleaned_paragraphs:
                paragraphs.append(cp)

            # Append booking-link CTA if body doesn't already include one
            has_booking_cta = any("example.com/book-meeting" in p or "Grab any slot" in p for p in paragraphs)
            if not has_booking_cta:
                paragraphs.append(f'Worth 15 minutes this week? Grab any slot here: <a href="{BOOKING_URL}">book a 15-minute call</a>.')

            # Linkify bare booking URL in paragraphs into a clickable anchor
            def linkify_booking(text):
                if BOOKING_URL in text:
                    return text.replace(
                        f"Grab any slot here: {BOOKING_URL}",
                        f'Grab any slot here: <a href="{BOOKING_URL}">book a 15-minute call</a>.'
                    )
                return text

            paragraphs = [linkify_booking(p) for p in paragraphs]

            body_html_parts = [f"<p>{p}</p>" for p in paragraphs]
            body_html = "\n".join(body_html_parts)

            # HTML signature
            signature = """<p>Cheers,</p>
<p>Hugh</p>

<p>Hugh Robertson<br>
SDR<br>
Your Company | Your City<br>
&#x1F4DE; <a href="tel:+10000000000">+1 (000) 000-0000</a><br>
&#x2709;&#xFE0F; <a href="mailto:your-email@company.com">your-email@company.com</a><br>
&#x1F4C5; Book a 15-minute call: <a href="https://example.com/book-meeting">Click here</a><br>
&#x1F517; LinkedIn: <a href="https://www.linkedin.com/in/your-profile/">Connect with me here</a></p>
<p><em>Tagline goes here.</em></p>
"""

            full_html = f"""<html>
<body style="font-family: Calibri, Arial, sans-serif; font-size: 14px; color: #333;">
{body_html}

{signature}
</body>
</html>"""

            msg.attach(MIMEText(full_html, "html"))

            # Recipients: To + BCC
            all_recipients = [to_email, BCC_SALESFORCE]
            try:
                server.sendmail(FROM_EMAIL, all_recipients, msg.as_string())
            except (smtplib.SMTPServerDisconnected, smtplib.SMTPSenderRefused, OSError):
                print("  Connection lost — reconnecting...")
                try:
                    server.quit()
                except Exception:
                    pass
                server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
                server.starttls()
                server.login(FROM_EMAIL, password)
                print("  Reconnected — resending...")
                server.sendmail(FROM_EMAIL, all_recipients, msg.as_string())

            sent += 1
            sent_leads.append(lead)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_writer.writerow([timestamp, to_email, first_name, company, lead.get("title", ""),
                                 "email", "sent", "delivered", f"subject={subject}"])
            log_file.flush()
            print(f"  Sent OK")

        except Exception as e:
            errors += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_msg = str(e).replace("\n", " ")[:200]
            log_writer.writerow([timestamp, to_email, first_name, company, lead.get("title", ""),
                                 "email", "sent", "error", f"{error_msg} | subject={subject}"])
            log_file.flush()
            print(f"  ERROR: {error_msg}")

        # Wait between sends (skip delay on last email)
        if i < total - 1:
            print(f"  Waiting {DELAY_SECONDS}s...")
            time.sleep(DELAY_SECONDS)

    # Cleanup
    server.quit()
    log_file.close()

    print(f"\n{'=' * 50}")
    print(f"DONE")
    print(f"  Sent:   {sent}")
    print(f"  Errors: {errors}")
    print(f"  Log:    {LOG_FILE}")
    print(f"{'=' * 50}")

    # Always generate a call sheet — call-only leads first, then emailed leads east→west by TZ
    batch_name = os.path.splitext(os.path.basename(INPUT_FILE))[0]
    generate_call_sheet(sent_leads, phone_lookup, batch_name, call_only_leads=CALL_ONLY_LEADS)

    # Sync total_sent + bounce_rate to the active sprint in Notion. Never blocks —
    # Notion outage logs to stderr and returns {}.
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from core.notion import sync_sprint_stats_from_log
        sync_sprint_stats_from_log(LOG_FILE)
    except Exception as _sync_e:
        print(f"[notion] sprint sync skipped — {type(_sync_e).__name__}: {_sync_e}")


if __name__ == "__main__":
    main()
